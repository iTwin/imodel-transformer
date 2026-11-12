/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import {
  Guid,
  GuidString,
  Id64,
  Id64String,
  OrderedId64Iterable,
} from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";
import { ExternalSourceAspect, IModelDb } from "@itwin/core-backend";
import type {
  DeletionBatch,
  ExternalSourceAspectDeletionRecord,
} from "./ChangesetScanner.js";

function sortedValidIds(ids: Iterable<Id64String>): Id64String[] {
  const unique = new Set<Id64String>();
  for (const id of ids) if (Id64.isValidId64(id)) unique.add(id);
  return OrderedId64Iterable.sortArray([...unique]);
}

/**
 * Reads the identifier of each element's element-kind ExternalSourceAspect in `scopeId`, in one query.
 * When an element has several, the one with the lowest ECInstanceId is used.
 * @internal
 */
export async function queryScopeEsaIdentifiersByElementId(
  db: IModelDb,
  scopeId: Id64String,
  elementIds: Iterable<Id64String>
): Promise<Map<Id64String, string>> {
  const result = new Map<Id64String, string>();
  const ids = sortedValidIds(elementIds);
  if (ids.length === 0) return result;
  const params = new QueryBinder()
    .bindIdSet("elementIds", ids)
    .bindId("scopeId", scopeId)
    .bindString("kind", ExternalSourceAspect.Kind.Element);
  const reader = db.createQueryReader(
    `SELECT esa.Element.Id, esa.Identifier
     FROM bis.ExternalSourceAspect esa
     INNER JOIN IdSet(:elementIds) ids ON ids.id = esa.Element.Id
     WHERE esa.Scope.Id = :scopeId AND esa.Kind = :kind
     ORDER BY esa.ECInstanceId`,
    params,
    { usePrimaryConn: true }
  );
  while (await reader.step()) {
    const elementId: Id64String = reader.current[0];
    if (!result.has(elementId)) result.set(elementId, reader.current[1]);
  }
  return result;
}

/**
 * Reads the federation GUIDs of the elements in `elementIds` that exist in `db`, in one query.
 * Elements without a federation GUID are omitted.
 * @internal
 */
export async function queryFederationGuidsByElementId(
  db: IModelDb,
  elementIds: Iterable<Id64String>
): Promise<Map<Id64String, GuidString>> {
  const result = new Map<Id64String, GuidString>();
  const ids = sortedValidIds(elementIds);
  if (ids.length === 0) return result;
  const reader = db.createQueryReader(
    `SELECT e.ECInstanceId, e.FederationGuid
     FROM bis.Element e
     INNER JOIN IdSet(:elementIds) ids ON ids.id = e.ECInstanceId
     WHERE e.FederationGuid IS NOT NULL`,
    new QueryBinder().bindIdSet("elementIds", ids),
    { usePrimaryConn: true }
  );
  while (await reader.step())
    result.set(reader.current[0], Guid.normalize(reader.current[1]));
  return result;
}

/**
 * Finds the target elements of the source elements referenced by one [[DeletionBatch]]: its deleted elements and
 * models, and the endpoints of its deleted relationships. Source ExternalSourceAspects and endpoint federation GUIDs
 * are read up front with set-based queries, because reading them separately costs an asynchronous query or a full
 * element load for each record.
 * @note Target elements are still found one federation GUID at a time, through a cached synchronous statement. That
 * measured several times faster than set-based queries, whose parameter binding and asynchronous row reads cost more
 * than the indexed lookups they replace.
 * @internal
 */
export class DeletionTargetResolver {
  private constructor(
    private readonly _targetDb: IModelDb,
    private readonly _liveScopeEsaIdentifiers:
      ReadonlyMap<Id64String, string> | undefined,
    private readonly _deletedScopeEsaByElementId: ReadonlyMap<
      Id64String,
      ExternalSourceAspectDeletionRecord
    >,
    private readonly _liveFederationGuids: ReadonlyMap<Id64String, GuidString>,
    private readonly _deletedFederationGuidByElementId: ReadonlyMap<
      Id64String,
      string
    >
  ) {}

  /**
   * @param args.sourceHoldsProvenance whether the source holds this transformation's ExternalSourceAspects, so that
   * they are checked before federation GUIDs.
   * @param args.deletedScopeEsaByElementId this transformation scope's ESAs deleted in the batch, keyed by the ID of
   * the element that owned each one.
   * @param args.deletedFederationGuidByElementId federation GUIDs of the elements deleted in the batch.
   */
  public static async create(args: {
    sourceDb: IModelDb;
    targetDb: IModelDb;
    targetScopeElementId: Id64String;
    sourceHoldsProvenance: boolean;
    batch: DeletionBatch;
    deletedScopeEsaByElementId: ReadonlyMap<
      Id64String,
      ExternalSourceAspectDeletionRecord
    >;
    deletedFederationGuidByElementId: ReadonlyMap<Id64String, string>;
  }): Promise<DeletionTargetResolver> {
    const { batch } = args;
    const endpointIds = new Set<Id64String>();
    for (const relationship of batch.relationships) {
      endpointIds.add(relationship.sourceECInstanceId);
      endpointIds.add(relationship.targetECInstanceId);
    }
    const liveFederationGuids = await queryFederationGuidsByElementId(
      args.sourceDb,
      endpointIds
    );
    const liveScopeEsaIdentifiers = args.sourceHoldsProvenance
      ? await queryScopeEsaIdentifiersByElementId(
          args.sourceDb,
          args.targetScopeElementId,
          [
            ...endpointIds,
            ...batch.elements.map((element) => element.ecInstanceId),
            ...batch.models.map((model) => model.ecInstanceId),
          ]
        )
      : undefined;
    return new DeletionTargetResolver(
      args.targetDb,
      liveScopeEsaIdentifiers,
      args.deletedScopeEsaByElementId,
      liveFederationGuids,
      args.deletedFederationGuidByElementId
    );
  }

  /** The federation GUID of a deleted relationship's endpoint, which may itself have been deleted in the batch. */
  public endpointFederationGuid(sourceId: Id64String): string | undefined {
    return (
      this._liveFederationGuids.get(sourceId) ??
      this._deletedFederationGuidByElementId.get(sourceId)
    );
  }

  /**
   * Finds the target ID of a source element, first through this transformation's ExternalSourceAspects when the
   * source holds the provenance, then through the federation GUID.
   * @param sourceId an element or model deleted in the batch, or an endpoint of a relationship deleted in it.
   * @param federationGuid the source element's federation GUID, if known.
   */
  public findTargetElementId(
    sourceId: Id64String,
    federationGuid: string | undefined
  ): Id64String | undefined {
    if (this._liveScopeEsaIdentifiers !== undefined) {
      // A live ESA takes precedence over one deleted in the batch. Empty identifiers are ignored.
      const identifier =
        this._liveScopeEsaIdentifiers.get(sourceId) ??
        this._deletedScopeEsaByElementId.get(sourceId)?.identifier;
      if (identifier) return identifier;
    }
    return federationGuid
      ? this._targetDb.elements.getIdFromFederationGuid(federationGuid)
      : undefined;
  }
}
