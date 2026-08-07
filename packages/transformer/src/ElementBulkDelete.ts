/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64Set, Id64String } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";

/** Finds the target roots required for native bulk deletion.
 *
 * Native deletion already cascades through child elements and modeled contents. This query follows
 * those dependencies to find top-level elements whose codes are scoped by an element in a deleted
 * tree. Each code-dependent element becomes another native deletion root.
 * @internal
 */
export async function findBulkDeleteRoots(
  targetDb: IModelDb,
  elementIds: ReadonlySet<Id64String>
): Promise<Id64Set> {
  const deleteRoots = new Set<Id64String>() as Id64Set;
  const query = `
    WITH RECURSIVE CascadeIds(Id, DeleteRootId) AS (
      SELECT element.ECInstanceId, element.ECInstanceId
      FROM bis.Element element
      INNER JOIN IdSet(:elementIds) ids ON ids.id = element.ECInstanceId
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
    ),
    DeleteRoots(Id) AS (
      SELECT DISTINCT DeleteRootId FROM CascadeIds
    )
    SELECT Id AS id FROM DeleteRoots
  `;
  const params = new QueryBinder().bindIdSet("elementIds", elementIds);
  for await (const row of targetDb.createQueryReader(query, params, {
    usePrimaryConn: true,
  })) {
    deleteRoots.add(row.id);
  }
  return deleteRoots;
}
