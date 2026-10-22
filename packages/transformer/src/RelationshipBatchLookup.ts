/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import { IModelDb, Relationship, SourceAndTarget } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { RelationshipProps } from "@itwin/core-common";

/** The batched relationship lookups `Relationships.queryInstances` and `queryInstancesProps` added by iTwin/itwinjs-core#9837.
 * TODO: Remove this shim and call `db.relationships` directly once the `@itwin/core-backend` peer dependency includes these methods.
 */
interface BatchedRelationshipLookups {
  queryInstances<T extends Relationship>(
    relClassFullName: string,
    criteria: ReadonlyArray<Id64String | SourceAndTarget>,
    options?: { usePrimaryConn?: boolean }
  ): Promise<Array<T | undefined>>;
  queryInstancesProps<T extends RelationshipProps>(
    relClassFullName: string,
    criteria: ReadonlyArray<Id64String | SourceAndTarget>,
    options?: { usePrimaryConn?: boolean }
  ): Promise<Array<T | undefined>>;
}

/** Returns the batched relationship lookups of an iModel. */
export function batchedRelationshipLookups(
  db: IModelDb
): BatchedRelationshipLookups {
  // The assertion is unnecessary with 5.15 typings but needed with the 5.14 peer range.
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  return db.relationships as unknown as BatchedRelationshipLookups;
}

/** Key of a relationship's source and target element ids. */
export function relationshipEndpointsKey(
  sourceId: Id64String,
  targetId: Id64String
): string {
  return `${sourceId}|${targetId}`;
}
