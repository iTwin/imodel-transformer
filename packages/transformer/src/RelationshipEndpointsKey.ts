/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import { Id64String } from "@itwin/core-bentley";

/** Key of a relationship's source and target element ids. */
export function relationshipEndpointsKey(
  sourceId: Id64String,
  targetId: Id64String
): string {
  return `${sourceId}|${targetId}`;
}
