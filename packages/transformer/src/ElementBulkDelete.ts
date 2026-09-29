/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, Id64Array, Id64Set, Id64String } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";

/** Native deletion calls needed to delete a set of element trees without a partial result.
 * @internal
 */
export interface BulkDeletePlan {
  /** Roots for each native deletion call, in execution order. Empty when nothing needs deleting. */
  readonly phases: readonly Id64Array[];
  /** Elements kept because an element outside the deleted trees still references them, each mapped to one such
   * referencing element. Their child elements and sub-models, and the elements whose deletion would delete them,
   * are kept too.
   */
  readonly keptReferences: ReadonlyMap<Id64String, Id64String>;
}

interface DeletionTrees {
  /** Roots that the native API must receive. */
  readonly roots: Id64Set;
  /** Every element that deleting the roots removes, mapped to the roots whose trees contain it. */
  readonly rootsByElement: ReadonlyMap<Id64String, readonly Id64String[]>;
}

/** Plans native bulk deletion of the specified element trees.
 *
 * An element that something outside the deleted trees still references is kept, together with what it
 * needs to stay valid, and the rest is deleted. Keeping elements can leave new outside references, so the
 * plan repeats until none remain.
 *
 * Native validation checks each root against the iModel before the call, so a root is refused when its tree
 * contains a definition, such as a category or a view, that a non-definition element in the same call still
 * uses. The plan deletes those users in an earlier call than the definitions they use. Validation stays enabled
 * for every call, so a reference this plan does not know about fails loudly instead of leaving a dangling
 * reference. That includes elements outside the trees whose geometry uses a definition in them, such as a
 * geometry part: only native code can read geometry, so those uses are not checked in advance.
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
    if (trees.roots.size === 0) return { phases: [], keptReferences };

    const references = await queryReferences(
      targetDb,
      trees.rootsByElement.keys()
    );
    const blockedIds = new Set<Id64String>();
    for (const { referencingId, referencedId } of references.references) {
      if (trees.rootsByElement.has(referencingId)) continue;
      blockedIds.add(referencedId);
      if (!keptReferences.has(referencedId))
        keptReferences.set(referencedId, referencingId);
    }
    if (blockedIds.size === 0)
      return {
        phases: await planPhases(targetDb, trees, references),
        keptReferences,
      };

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

/** Orders the roots of deletion trees that nothing outside them references into native calls. */
async function planPhases(
  targetDb: IModelDb,
  trees: DeletionTrees,
  { references, geometryDefinitionIds }: TreeReferences
): Promise<Id64Array[]> {
  // Elements in the trees that use a definition that is also in the trees. Native validation refuses the definition while they exist.
  const usersOfDeletedDefinitions = new Set<Id64String>();
  // Roots whose trees contain such a definition. They must be deleted after its users.
  const rootsWithUsedDefinitions = new Set<Id64String>();
  const addUsedDefinition = (definitionId: Id64String) => {
    for (const rootId of trees.rootsByElement.get(definitionId) ?? [])
      rootsWithUsedDefinitions.add(rootId);
  };
  const sameCallReferences: TreeReference[] = [];
  for (const reference of references) {
    if (reference.usersFirst) {
      usersOfDeletedDefinitions.add(reference.referencingId);
      addUsedDefinition(reference.referencedId);
    } else {
      sameCallReferences.push(reference);
    }
  }
  // Only native code can tell which geometry uses a geometry part, material, texture, line style, or sub-category,
  // so every geometric element in the trees counts as a user of every such definition. Deleting the trees that
  // contain those definitions last is enough unless one of them also contains a geometric element; only then are
  // all geometric elements queried, to delete them as roots of their own.
  const geometryRoots = new Set<Id64String>();
  for (const id of geometryDefinitionIds)
    for (const rootId of trees.rootsByElement.get(id) ?? [])
      geometryRoots.add(rootId);
  if (geometryRoots.size > 0) {
    const elementsInGeometryTrees = [...trees.rootsByElement]
      .filter(([, rootIds]) => rootIds.some((id) => geometryRoots.has(id)))
      .map(([id]) => id);
    if (
      (await queryGeometricElements(targetDb, elementsInGeometryTrees)).length >
      0
    )
      for (const id of await queryGeometricElements(
        targetDb,
        trees.rootsByElement.keys()
      ))
        usersOfDeletedDefinitions.add(id);
    for (const rootId of geometryRoots) rootsWithUsedDefinitions.add(rootId);
  }
  if (rootsWithUsedDefinitions.size === 0) return [[...trees.roots]];
  // True when a root deleted in the last call removes the element.
  const isInTreeWithUsedDefinition = (id: Id64String) =>
    (trees.rootsByElement.get(id) ?? []).some((rootId) =>
      rootsWithUsedDefinitions.has(rootId)
    );
  // True when a root deleted in the first call removes the element. An element in overlapping trees can satisfy both checks.
  const isInOtherTree = (id: Id64String) =>
    (trees.rootsByElement.get(id) ?? []).some(
      (rootId) => !rootsWithUsedDefinitions.has(rootId)
    );
  // Deleting the other roots first works unless a user shares a tree with a used definition, or an other tree
  // contains an element that an element deleted later references, such as its code scope.
  const canDeleteOtherRootsFirst =
    ![...usersOfDeletedDefinitions].some(isInTreeWithUsedDefinition) &&
    !sameCallReferences.some(
      ({ referencingId, referencedId }) =>
        isInTreeWithUsedDefinition(referencingId) && isInOtherTree(referencedId)
    );
  if (canDeleteOtherRootsFirst) {
    // There are no other roots only when every tree contains a geometry definition and nothing in the trees uses a
    // definition through a queried reference.
    const otherRoots = [...trees.roots].filter(
      (id) => !rootsWithUsedDefinitions.has(id)
    );
    return otherRoots.length > 0
      ? [otherRoots, [...rootsWithUsedDefinitions]]
      : [[...rootsWithUsedDefinitions]];
  }

  // Otherwise delete the users and their code dependents as roots of their own. Core refuses to delete an
  // element whose code scopes an element deleted in a later call, so the first call also takes code dependents
  // that have a parent. They are all in the trees; one outside them would have kept its code scope above.
  const userTrees = await queryDeletionTrees(
    targetDb,
    usersOfDeletedDefinitions,
    true
  );
  const remainingRoots = [...trees.roots].filter(
    (id) => !userTrees.rootsByElement.has(id)
  );
  const phases = [[...userTrees.roots]];
  if (remainingRoots.length > 0) phases.push(remainingRoots);
  return phases;
}

/** Finds the native deletion roots and every element their deletion removes.
 *
 * Native deletion already cascades through child elements and modeled contents. This query follows
 * those dependencies to find elements whose codes are scoped by an element in a deleted tree. Each
 * code-dependent element becomes another native deletion root. Only top-level code dependents are
 * included unless `includeParentedCodeDependents` is true.
 */
async function queryDeletionTrees(
  targetDb: IModelDb,
  elementIds: ReadonlySet<Id64String>,
  includeParentedCodeDependents = false
): Promise<DeletionTrees> {
  const roots: Id64Set = new Set<Id64String>();
  // An element has more than one root only when requested trees overlap. UNION in the query already removes duplicate
  // (element, root) pairs, so an array holds distinct roots without allocating a Set per element.
  const rootsByElement = new Map<Id64String, Id64String[]>();
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
      ${includeParentedCodeDependents ? "" : "WHERE codeDependent.Parent.Id IS NULL"}
    )
    SELECT Id AS id, DeleteRootId AS rootId FROM CascadeIds
  `;
  const params = new QueryBinder().bindIdSet("elementIds", elementIds);
  for await (const row of targetDb.createQueryReader(query, params, {
    usePrimaryConn: true,
  })) {
    roots.add(row.rootId);
    const elementRoots = rootsByElement.get(row.id);
    if (elementRoots) elementRoots.push(row.rootId);
    else rootsByElement.set(row.id, [row.rootId]);
  }
  return { roots, rootsByElement };
}

/** Finds the elements to keep so that the blocked elements stay valid: each blocked element and its child
 * elements and sub-model contents, plus every element whose deletion would delete a kept element or leave its
 * parent, model, or code scope dangling.
 */
async function queryKeptElements(
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
  /** True when native validation refuses the referenced element while the referencing element exists, even when
   * both are deleted in the same call. False when deleting them in the same call is enough.
   */
  readonly usersFirst: boolean;
}

interface TreeReferences {
  readonly references: readonly TreeReference[];
  /** Geometry parts, materials, textures, line styles, and non-default sub-categories in the trees. Geometry
   * references them, so their users can't be queried.
   */
  readonly geometryDefinitionIds: readonly Id64String[];
}

/** Finds BisCore references to the specified elements that native deletion validates and that [[queryDeletionTrees]]
 * does not follow. Parent and model references are not included because the deleted trees contain every child and
 * modeled element.
 */
async function queryReferences(
  targetDb: IModelDb,
  elementIds: Iterable<Id64String>
): Promise<TreeReferences> {
  const references: TreeReference[] = [];
  const geometryDefinitionIds: Id64String[] = [];
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
        usersFirst: false,
      });
  }
  if (definitionIds.length === 0) return { references, geometryDefinitionIds };

  // Classes and properties added in later BisCore versions.
  const sectionReferences = await querySectionViewReferences(targetDb);
  // Kind 1: native validation refuses the definition while the referencing element exists, even in the same call.
  // Kind 0: deleting both in the same call is enough. Native validation treats a default sub-category, whose ID
  // follows its category's, as used by the category unless both are deleted.
  // Kind 2: a definition that geometry references, so its users can't be queried.
  const definitionQuery = `
    SELECT element.ECInstanceId AS referencingId, element.Category.Id AS referencedId, 1 AS kind
    FROM bis.GeometricElement3d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.Category.Id
    UNION ALL
    SELECT element.ECInstanceId, element.Category.Id, 1
    FROM bis.GeometricElement2d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.Category.Id
    UNION ALL
    SELECT element.ECInstanceId, element.View.Id, 1
    FROM bis.ViewAttachment element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.View.Id
    ${sectionReferences
      .map(
        ({ className, propertyName }) => `
    UNION ALL
    SELECT element.ECInstanceId, element.${propertyName}.Id, 1
    FROM bis.${className} element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.${propertyName}.Id`
      )
      .join("")}
    UNION ALL
    SELECT viewDefinition.ECInstanceId, viewDefinition.DisplayStyle.Id, 0
    FROM bis.ViewDefinition viewDefinition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = viewDefinition.DisplayStyle.Id
    UNION ALL
    SELECT viewDefinition.ECInstanceId, viewDefinition.CategorySelector.Id, 0
    FROM bis.ViewDefinition viewDefinition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = viewDefinition.CategorySelector.Id
    UNION ALL
    SELECT viewDefinition.ECInstanceId, viewDefinition.ModelSelector.Id, 0
    FROM bis.SpatialViewDefinition viewDefinition
    INNER JOIN IdSet(:elementIds) ids ON ids.id = viewDefinition.ModelSelector.Id
    UNION ALL
    SELECT subCategory.Parent.Id, subCategory.ECInstanceId, 0
    FROM bis.SubCategory subCategory
    INNER JOIN IdSet(:elementIds) ids ON ids.id = subCategory.ECInstanceId
    WHERE CAST(subCategory.ECInstanceId AS INTEGER) = CAST(subCategory.Parent.Id AS INTEGER) + 1
    UNION ALL
    SELECT subCategory.ECInstanceId, subCategory.ECInstanceId, 2
    FROM bis.SubCategory subCategory
    INNER JOIN IdSet(:elementIds) ids ON ids.id = subCategory.ECInstanceId
    WHERE CAST(subCategory.ECInstanceId AS INTEGER) <> CAST(subCategory.Parent.Id AS INTEGER) + 1
    ${["GeometryPart", "RenderMaterial", "Texture", "LineStyle"]
      .map(
        (className) => `
    UNION ALL
    SELECT element.ECInstanceId, element.ECInstanceId, 2
    FROM bis.${className} element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.ECInstanceId`
      )
      .join("")}
  `;
  for await (const row of targetDb.createQueryReader(
    definitionQuery,
    new QueryBinder().bindIdSet("elementIds", definitionIds),
    { usePrimaryConn: true }
  )) {
    if (row.kind === 2) geometryDefinitionIds.push(row.referencedId);
    else
      references.push({
        referencingId: row.referencingId,
        referencedId: row.referencedId,
        usersFirst: row.kind === 1,
      });
  }
  return { references, geometryDefinitionIds };
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

/** Finds the 2D and 3D geometric elements among the specified elements. */
async function queryGeometricElements(
  targetDb: IModelDb,
  elementIds: Iterable<Id64String>
): Promise<Id64String[]> {
  // Joining the abstract bis.GeometricElement with an IdSet is quadratic, so query its 3D and 2D subclasses.
  const query = `
    SELECT element.ECInstanceId AS id
    FROM bis.GeometricElement3d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.ECInstanceId
    UNION ALL
    SELECT element.ECInstanceId
    FROM bis.GeometricElement2d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.ECInstanceId
  `;
  const ids: Id64String[] = [];
  const params = new QueryBinder().bindIdSet("elementIds", elementIds);
  for await (const row of targetDb.createQueryReader(query, params, {
    usePrimaryConn: true,
  }))
    ids.push(row.id);
  return ids;
}
