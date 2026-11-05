/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, Id64Array, Id64Set, Id64String } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";

/** Native deletion call needed to delete a set of element trees without a partial result.
 * @internal
 */
export interface BulkDeletePlan {
  /** Roots for one native deletion call. Empty when nothing needs deleting. */
  readonly roots: Id64Array;
  /** Elements kept because an element outside the deleted trees still references them, each mapped to one such
   * referencing element. Their child elements and sub-models, and the elements whose deletion would delete them,
   * are kept too.
   */
  readonly keptReferences: ReadonlyMap<Id64String, Id64String>;
}

interface DeletionTrees {
  /** Roots that the native API must receive. */
  readonly roots: Id64Set;
  /** Every element that deleting the roots removes. */
  readonly elementIds: Id64Set;
}

/** Plans native bulk deletion of the specified element trees.
 *
 * An element that something outside the deleted trees still references is kept, together with what it
 * needs to stay valid, and the rest is deleted. Keeping elements can leave new outside references, so the
 * plan repeats until none remain.
 *
 * Native validation checks the whole delete set at once and ignores references between its members, so a
 * definition, such as a category, a view, or a geometry part, is deleted in the same call as the elements in the
 * trees that use it. Validation stays enabled, so native code refuses an element that something this plan does
 * not know about still uses instead of leaving a dangling reference. That includes elements outside the trees
 * whose geometry uses a definition in them, such as a geometry part: only native code can read geometry, so those
 * uses are not checked in advance. The importer keeps the refused elements and plans again without them.
 * @internal
 */
export async function planBulkDelete(
  targetDb: IModelDb,
  elementIds: ReadonlySet<Id64String>
): Promise<BulkDeletePlan> {
  const requestedIds = new Set(elementIds);
  const keptReferences = new Map<Id64String, Id64String>();
  for (;;) {
    const trees = await queryDeletionTrees(targetDb, requestedIds);
    if (trees.roots.size === 0) return { roots: [], keptReferences };

    const blockedIds = new Set<Id64String>();
    for (const { referencingId, referencedId } of await queryReferences(
      targetDb,
      trees.elementIds
    )) {
      if (trees.elementIds.has(referencingId)) continue;
      blockedIds.add(referencedId);
      if (!keptReferences.has(referencedId))
        keptReferences.set(referencedId, referencingId);
    }
    if (blockedIds.size === 0)
      return { roots: [...trees.roots], keptReferences };

    // Every blocked element is reached from a requested element, and queryKeptElements walks back up to it,
    // so each pass removes at least one requested element and the loop ends.
    const requestedCount = requestedIds.size;
    for (const id of await queryKeptElements(targetDb, blockedIds))
      requestedIds.delete(id);
    assert(
      requestedIds.size < requestedCount,
      "Keeping referenced elements must remove a requested element"
    );
  }
}

/** Finds the native deletion roots and every element their deletion removes.
 *
 * Native deletion already cascades through child elements and modeled contents. This query follows
 * those dependencies to find top-level elements whose codes are scoped by an element in a deleted tree.
 * Each such code-dependent element becomes another native deletion root. A code dependent whose parent is
 * outside the trees is not added; it references its code scope from outside, so the scope is kept instead.
 */
async function queryDeletionTrees(
  targetDb: IModelDb,
  elementIds: ReadonlySet<Id64String>
): Promise<DeletionTrees> {
  const roots: Id64Set = new Set<Id64String>();
  const treeElementIds: Id64Set = new Set<Id64String>();
  // CascadeIds pairs each traversed element with a root that the native API must delete. The anchor
  // adds requested elements that still exist, except those whose parent or modeled element is also
  // requested: the recursion reaches them anyway, and anchoring them too would walk a nested tree once
  // per requested ancestor. Recursive branches walk child elements and modeled contents while
  // preserving DeleteRootId. An element whose code is scoped by anything already traversed becomes a
  // new root, so recursion also covers its tree and code dependents.
  const query = `
    WITH RECURSIVE CascadeIds(Id, DeleteRootId) AS (
      SELECT element.ECInstanceId, element.ECInstanceId
      FROM bis.Element element
      INNER JOIN IdSet(:elementIds) ids ON ids.id = element.ECInstanceId
      WHERE (element.Parent.Id IS NULL OR element.Parent.Id NOT IN (SELECT id FROM IdSet(:elementIds)))
        AND element.Model.Id NOT IN (SELECT id FROM IdSet(:elementIds))
      UNION
      SELECT child.ECInstanceId, deletionParent.DeleteRootId
      FROM bis.Element child
      INNER JOIN CascadeIds deletionParent ON child.Parent.Id = deletionParent.Id
      UNION
      SELECT modelElement.ECInstanceId, modeledElement.DeleteRootId
      FROM bis.Element modelElement
      INNER JOIN CascadeIds modeledElement ON modelElement.Model.Id = modeledElement.Id
      UNION
      SELECT codeDependent.ECInstanceId, codeDependent.ECInstanceId
      FROM bis.Element codeDependent
      INNER JOIN CascadeIds scope ON codeDependent.CodeScope.Id = scope.Id
      WHERE codeDependent.Parent.Id IS NULL
    )
    SELECT Id AS id, DeleteRootId AS rootId FROM CascadeIds
  `;
  const params = new QueryBinder().bindIdSet("elementIds", elementIds);
  for await (const row of targetDb.createQueryReader(query, params, {
    usePrimaryConn: true,
  })) {
    roots.add(row.rootId);
    treeElementIds.add(row.id);
  }
  return { roots, elementIds: treeElementIds };
}

/** Finds the elements to keep so that the blocked elements stay valid: each blocked element and its child
 * elements and sub-model contents, plus every element whose deletion would delete a kept element or leave its
 * parent, model, or code scope dangling.
 *
 * The whole subtree below a blocked element is kept, even when the element is blocked only as a code scope and
 * its children could be deleted. A kept category needs its sub-categories, because the plan can't see geometry
 * outside the trees that uses them, and keeping too much is safe.
 * @internal
 */
export async function queryKeptElements(
  targetDb: IModelDb,
  blockedIds: ReadonlySet<Id64String>
): Promise<Id64Set> {
  const query = `
    WITH RECURSIVE
      Above(Id) AS (
        SELECT ids.id FROM IdSet(:blockedIds) ids
        UNION
        SELECT element.Parent.Id FROM bis.Element element
        INNER JOIN Above ON element.ECInstanceId = Above.Id
        WHERE element.Parent.Id IS NOT NULL
        UNION
        SELECT element.Model.Id FROM bis.Element element
        INNER JOIN Above ON element.ECInstanceId = Above.Id
        UNION
        SELECT element.CodeScope.Id FROM bis.Element element
        INNER JOIN Above ON element.ECInstanceId = Above.Id
      ),
      Below(Id) AS (
        SELECT ids.id FROM IdSet(:blockedIds) ids
        UNION
        SELECT element.ECInstanceId FROM bis.Element element
        INNER JOIN Below ON element.Parent.Id = Below.Id
        UNION
        SELECT element.ECInstanceId FROM bis.Element element
        INNER JOIN Below ON element.Model.Id = Below.Id
      )
    SELECT Id AS id FROM Above
    UNION
    SELECT Id FROM Below
  `;
  const keptIds: Id64Set = new Set<Id64String>();
  const params = new QueryBinder().bindIdSet("blockedIds", blockedIds);
  for await (const row of targetDb.createQueryReader(query, params, {
    usePrimaryConn: true,
  }))
    keptIds.add(row.id);
  return keptIds;
}

interface TreeReference {
  readonly referencingId: Id64String;
  readonly referencedId: Id64String;
}

/** Finds BisCore references to the specified elements that native deletion validates and that [[queryDeletionTrees]]
 * does not follow. Parent and model references are not included because the deleted trees contain every child and
 * modeled element. References from geometry, such as to a geometry part or a material, are not included because
 * only native code can read geometry.
 */
async function queryReferences(
  targetDb: IModelDb,
  elementIds: Iterable<Id64String>
): Promise<TreeReference[]> {
  const references: TreeReference[] = [];
  // Any element can be a code scope. Every other validated reference targets a definition element, so those are
  // queried only for the definitions among the elements, which are usually few or none.
  const definitionIds: Id64String[] = [];
  const firstQuery = `
    SELECT element.ECInstanceId AS referencingId, element.CodeScope.Id AS referencedId, 0 AS isDefinition
    FROM bis.Element element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.CodeScope.Id
    UNION ALL
    SELECT definition.ECInstanceId, definition.ECInstanceId, 1
    FROM bis.DefinitionElement definition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = definition.ECInstanceId
  `;
  for await (const row of targetDb.createQueryReader(
    firstQuery,
    new QueryBinder().bindIdSet("elementIds", elementIds),
    { usePrimaryConn: true }
  )) {
    if (row.isDefinition === 1) definitionIds.push(row.referencedId);
    else
      references.push({
        referencingId: row.referencingId,
        referencedId: row.referencedId,
      });
  }
  if (definitionIds.length === 0) return references;

  // Classes and properties added in later BisCore versions.
  const sectionReferences = await querySectionViewReferences(targetDb);
  // A default sub-category, whose ID follows its category's, counts as used by the category.
  const definitionQuery = `
    SELECT element.ECInstanceId AS referencingId, element.Category.Id AS referencedId
    FROM bis.GeometricElement3d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.Category.Id
    UNION ALL
    SELECT element.ECInstanceId, element.Category.Id
    FROM bis.GeometricElement2d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.Category.Id
    UNION ALL
    SELECT element.ECInstanceId, element.View.Id
    FROM bis.ViewAttachment element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.View.Id
    ${sectionReferences
      .map(
        ({ className, propertyName }) => `
    UNION ALL
    SELECT element.ECInstanceId, element.${propertyName}.Id
    FROM bis.${className} element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.${propertyName}.Id`
      )
      .join("")}
    UNION ALL
    SELECT viewDefinition.ECInstanceId, viewDefinition.DisplayStyle.Id
    FROM bis.ViewDefinition viewDefinition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = viewDefinition.DisplayStyle.Id
    UNION ALL
    SELECT viewDefinition.ECInstanceId, viewDefinition.CategorySelector.Id
    FROM bis.ViewDefinition viewDefinition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = viewDefinition.CategorySelector.Id
    UNION ALL
    SELECT viewDefinition.ECInstanceId, viewDefinition.ModelSelector.Id
    FROM bis.SpatialViewDefinition viewDefinition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = viewDefinition.ModelSelector.Id
    UNION ALL
    SELECT subCategory.Parent.Id, subCategory.ECInstanceId
    FROM bis.SubCategory subCategory
    INNER JOIN IdSet(:elementIds) ids ON ids.id = subCategory.ECInstanceId
    WHERE CAST(subCategory.ECInstanceId AS INTEGER) = CAST(subCategory.Parent.Id AS INTEGER) + 1
  `;
  for await (const row of targetDb.createQueryReader(
    definitionQuery,
    new QueryBinder().bindIdSet("elementIds", definitionIds),
    { usePrimaryConn: true }
  ))
    references.push({
      referencingId: row.referencingId,
      referencedId: row.referencedId,
    });
  return references;
}

/** Finds the BisCore section classes and view properties that exist in the iModel's schema. */
async function querySectionViewReferences(
  targetDb: IModelDb
): Promise<{ className: string; propertyName: string }[]> {
  const query = `
    SELECT classDef.Name AS className, propertyDef.Name AS propertyName
    FROM meta.ECPropertyDef propertyDef
    INNER JOIN meta.ECClassDef classDef ON classDef.ECInstanceId = propertyDef.Class.Id
    INNER JOIN meta.ECSchemaDef schemaDef ON schemaDef.ECInstanceId = classDef.Schema.Id
    WHERE schemaDef.Name = 'BisCore'
      AND ((classDef.Name = 'SectionDrawing' AND propertyDef.Name = 'SpatialView')
        OR (classDef.Name = 'SectionDrawingLocation' AND propertyDef.Name = 'SectionView'))
  `;
  const references: { className: string; propertyName: string }[] = [];
  for await (const row of targetDb.createQueryReader(query, undefined, {
    usePrimaryConn: true,
  }))
    references.push({
      className: row.className,
      propertyName: row.propertyName,
    });
  return references;
}
