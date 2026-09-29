/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64Array, Id64Set, Id64String } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";

/** Native deletion calls needed to delete a set of element trees without a partial result.
 * @internal
 */
export interface BulkDeletePlan {
  /** Roots for each native deletion call, in execution order. Empty when nothing needs deleting. */
  readonly phases: readonly Id64Array[];
  /** Elements in the deleted trees that an element outside the trees still references, each mapped to one such referencing element. */
  readonly blockedReferences: ReadonlyMap<Id64String, Id64String>;
}

interface DeletionTrees {
  /** Roots that the native API must receive. */
  readonly roots: Id64Set;
  /** Every element that deleting the roots removes, mapped to the roots whose trees contain it. */
  readonly rootsByElement: ReadonlyMap<Id64String, readonly Id64String[]>;
}

/** Plans native bulk deletion of the specified element trees.
 *
 * Native validation checks each root against the iModel before the call, so a root is refused when its tree
 * contains an element, such as a category, that another element in the same call still references. The plan
 * deletes the other roots first, then the roots whose trees contain a category used by a deleted element.
 * Validation stays enabled for every call, so a reference this plan does not know about fails loudly instead
 * of leaving a dangling reference.
 * @internal
 */
export async function planBulkDelete(
  targetDb: IModelDb,
  elementIds: ReadonlySet<Id64String>
): Promise<BulkDeletePlan> {
  const trees = await queryDeletionTrees(targetDb, elementIds);
  if (trees.roots.size === 0)
    return { phases: [], blockedReferences: new Map() };

  const blockedReferences = new Map<Id64String, Id64String>();
  // Elements in the trees whose category is also in the trees. Native validation refuses the category while they exist.
  const usersOfDeletedCategories = new Set<Id64String>();
  // Roots whose trees contain such a category. They must be deleted after the category users.
  const rootsWithUsedCategories = new Set<Id64String>();
  const codeScopeReferencesInTrees: BlockingReference[] = [];
  for (const reference of await queryBlockingReferences(
    targetDb,
    trees.rootsByElement.keys()
  )) {
    const { referencingId, referencedId, isCategory } = reference;
    if (!trees.rootsByElement.has(referencingId)) {
      if (!blockedReferences.has(referencedId))
        blockedReferences.set(referencedId, referencingId);
    } else if (isCategory) {
      // Native validation accepts code-scope references from the same call, but not category references.
      usersOfDeletedCategories.add(referencingId);
      for (const rootId of trees.rootsByElement.get(referencedId) ?? [])
        rootsWithUsedCategories.add(rootId);
    } else {
      codeScopeReferencesInTrees.push(reference);
    }
  }
  if (blockedReferences.size > 0) return { phases: [], blockedReferences };
  if (usersOfDeletedCategories.size === 0)
    return { phases: [[...trees.roots]], blockedReferences };

  // True when a root deleted in the last call removes the element.
  const isInTreeWithUsedCategory = (id: Id64String) =>
    (trees.rootsByElement.get(id) ?? []).some((rootId) =>
      rootsWithUsedCategories.has(rootId)
    );
  // True when a root deleted in the first call removes the element. An element in overlapping trees can satisfy both checks.
  const isInOtherTree = (id: Id64String) =>
    (trees.rootsByElement.get(id) ?? []).some(
      (rootId) => !rootsWithUsedCategories.has(rootId)
    );
  // Deleting the other roots first works unless a category user shares a tree with a used category, or an other tree scopes the code of an element deleted later.
  const canDeleteOtherRootsFirst =
    ![...usersOfDeletedCategories].some(isInTreeWithUsedCategory) &&
    !codeScopeReferencesInTrees.some(
      ({ referencingId, referencedId }) =>
        isInTreeWithUsedCategory(referencingId) && isInOtherTree(referencedId)
    );
  if (canDeleteOtherRootsFirst) {
    const otherRoots = [...trees.roots].filter(
      (id) => !rootsWithUsedCategories.has(id)
    );
    return {
      phases:
        otherRoots.length > 0
          ? [otherRoots, [...rootsWithUsedCategories]]
          : [[...rootsWithUsedCategories]],
      blockedReferences,
    };
  }

  // Otherwise delete the category users and their code dependents as roots of their own. Core refuses to delete
  // an element whose code scopes an element deleted in a later call, so the first call also takes code dependents
  // that have a parent. They are all in the trees; one outside them would have blocked the plan above.
  const userTrees = await queryDeletionTrees(
    targetDb,
    usersOfDeletedCategories,
    true
  );
  const remainingRoots = [...trees.roots].filter(
    (id) => !userTrees.rootsByElement.has(id)
  );
  const phases = [[...userTrees.roots]];
  if (remainingRoots.length > 0) phases.push(remainingRoots);
  return { phases, blockedReferences };
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

interface BlockingReference {
  readonly referencingId: Id64String;
  readonly referencedId: Id64String;
  readonly isCategory: boolean;
}

/** Finds BisCore references that block deleting the specified elements and that [[queryDeletionTrees]] does not follow.
 * Parent and model references are not included because the deleted trees contain every child and modeled element.
 */
async function queryBlockingReferences(
  targetDb: IModelDb,
  elementIds: Iterable<Id64String>
): Promise<BlockingReference[]> {
  const query = `
    SELECT element.ECInstanceId AS referencingId, element.Category.Id AS referencedId, 1 AS isCategory
    FROM bis.GeometricElement3d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.Category.Id
    UNION ALL
    SELECT element.ECInstanceId, element.Category.Id, 1
    FROM bis.GeometricElement2d element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.Category.Id
    UNION ALL
    SELECT element.ECInstanceId, element.CodeScope.Id, 0
    FROM bis.Element element
    INNER JOIN IdSet(:elementIds) ids ON ids.id = element.CodeScope.Id
  `;
  const references: BlockingReference[] = [];
  const params = new QueryBinder().bindIdSet("elementIds", elementIds);
  for await (const row of targetDb.createQueryReader(query, params, {
    usePrimaryConn: true,
  })) {
    references.push({
      referencingId: row.referencingId,
      referencedId: row.referencedId,
      isCategory: row.isCategory === 1,
    });
  }
  return references;
}
