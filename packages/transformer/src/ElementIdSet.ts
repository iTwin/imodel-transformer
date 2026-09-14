/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64, Id64String } from "@itwin/core-bentley";
import { BriefcaseIdValue } from "@itwin/core-common";

/** Whether the ID set includes the namespace rejected by native IdSet. @internal */
export function hasReservedElementIds(
  elementIds: Iterable<Id64String>
): boolean {
  for (const id of elementIds) {
    if (Id64.getBriefcaseId(id) === BriefcaseIdValue.SchemaSyncElementReserved)
      return true;
  }
  return false;
}

/** An IdSet source for inner joins to existing elements or their models. @internal */
export function elementIdSetSource(
  parameterName: string,
  elementIds: Iterable<Id64String>
): string {
  if (hasReservedElementIds(elementIds)) {
    // Native 5.13.28's IdSet vtab rejects valid SchemaSync-reserved IDs.
    // InVirtualSet accepts the same compressed binding. Keep IdSet's query
    // plan for ordinary IDs; this alternative contains only existing elements.
    return `(SELECT idSetElement.ECInstanceId AS id FROM bis.Element idSetElement WHERE InVirtualSet(:${parameterName}, idSetElement.ECInstanceId))`;
  }
  return `IdSet(:${parameterName})`;
}
