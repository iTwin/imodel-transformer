/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import {
  ChangeInstance,
  ChangesetReader,
  IModelDb,
  PartialChangeUnifier,
  PropertyFilter,
} from "@itwin/core-backend";
import { Id64String, ITwinError } from "@itwin/core-bentley";
import { ChangesetFileProps } from "@itwin/core-common";
import type { ChangedInstanceIds } from "./IModelExporter";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";
import { SourceClassKinds } from "./SourceClassKinds";

/**
 * Identity shared by all deletion records.
 * @beta
 */
export interface DeletionRecordBase {
  /** ID of the deleted source instance. */
  ecInstanceId: Id64String;
  /** EC class ID of the deleted source instance. */
  ecClassId: Id64String;
}

/**
 * A deleted source element.
 * @beta
 */
export interface ElementDeletionRecord extends DeletionRecordBase {
  /** Federation GUID used to find the corresponding target element. Undefined when the element had none. */
  federationGuid?: string;
}

/**
 * A deleted source model.
 * @beta
 */
export type ModelDeletionRecord = DeletionRecordBase;

/**
 * A deleted source relationship derived from `BisCore:ElementRefersToElements`, excluding `BisCore:ElementDrivesElement`.
 * @beta
 */
export interface RelationshipDeletionRecord extends DeletionRecordBase {
  /** Source endpoint of the deleted relationship. */
  sourceECInstanceId: Id64String;
  /** Target endpoint of the deleted relationship. */
  targetECInstanceId: Id64String;
}

/**
 * A deleted source ExternalSourceAspect, used to find the target of an element deleted in the same batch.
 * Only aspects with a scope, kind, and identifier are recorded.
 * @beta
 */
export interface ExternalSourceAspectDeletionRecord extends DeletionRecordBase {
  /** Element that owned the deleted aspect. */
  elementId: Id64String;
  /** Scope of the deleted aspect. */
  scopeId: Id64String;
  /** Kind of the deleted aspect. */
  kind: string;
  /** Identifier of the deleted aspect. */
  identifier: string;
}

/**
 * Deleted source instances from one batch of changes.
 * @note Batches are processed in order. An ExternalSourceAspect deletion is used to identify element deletions in the same batch. An empty array means nothing of that kind was deleted.
 * @beta
 */
export interface DeletionBatch {
  /** Deleted elements. */
  elements: ElementDeletionRecord[];
  /** Deleted models. */
  models: ModelDeletionRecord[];
  /** Deleted relationships derived from `BisCore:ElementRefersToElements`, excluding `BisCore:ElementDrivesElement`. */
  relationships: RelationshipDeletionRecord[];
  /** Deleted ExternalSourceAspects with a scope, kind, and identifier. */
  externalSourceAspects: ExternalSourceAspectDeletionRecord[];
}

/**
 * Changes read for [[IModelTransformer.scanChanges]].
 * @note The result must describe every change in the scanned ranges, as the default scan does. Changes missing from
 * `changedInstanceIds`, including CodeSpec changes, aren't exported. Every relationship in
 * `changedInstanceIds.relationship.deleteIds` needs a matching deletion record, or its deletion is skipped with a warning.
 * An element or model deletion whose target can't be found through its record or the transformation's provenance is ignored.
 * @beta
 */
export interface ChangeScanResult {
  /** Changed instance IDs, including the owning elements of changed aspects. */
  changedInstanceIds: ChangedInstanceIds;
  /** Deletion batches of any granularity: one per changeset, one per range, or one for all ranges. */
  deletionBatches: DeletionBatch[];
}

/**
 * The identifiers of a deleted instance that deletion records are built from, as read from a changeset with
 * `PropertyFilter.InstanceKeyAndIdentifiers`.
 * @internal
 */
export interface DeletedInstance {
  ecInstanceId: Id64String;
  ecClassId: Id64String;
  federationGuid?: string;
  sourceECInstanceId?: Id64String;
  targetECInstanceId?: Id64String;
  /** Owner of a deleted aspect. */
  elementId?: Id64String;
  scopeId?: Id64String;
  kind?: string;
  identifier?: string;
}

interface ScanOptions {
  /** Receives the changes. When undefined, changes are read only for their deletion records. */
  changedInstanceIds?: ChangedInstanceIds;
  /** Whether to collect deletion records. Defaults to true. */
  collectDeletionRecords?: boolean;
}

/**
 * Adds a deleted instance to the deletion batch for its class.
 * @note Throws `IModelStatus.NotFound` when the instance's class is missing from the current schemas, because such a
 * deletion can't be classified.
 * @internal
 */
export function addDeletionRecord(
  batch: DeletionBatch,
  deleted: DeletedInstance,
  classKinds: SourceClassKinds,
  iModel: IModelDb
): void {
  const { ecInstanceId, ecClassId } = deleted;
  switch (classKinds.kindOf(ecClassId)) {
    case "element":
      batch.elements.push({
        ecInstanceId,
        ecClassId,
        federationGuid: deleted.federationGuid,
      });
      return;
    case "model":
      batch.models.push({ ecInstanceId, ecClassId });
      return;
    case "relationship": {
      const { sourceECInstanceId, targetECInstanceId } = deleted;
      if (sourceECInstanceId === undefined || targetECInstanceId === undefined)
        ITwinError.throwError({
          iTwinErrorId: {
            scope: IModelTransformerErrorScope,
            key: IModelTransformerError.ChangedInstanceMetadataMissing,
          },
          message: `Relationship deletion ${ecInstanceId} is missing an endpoint.`,
        });
      batch.relationships.push({
        ecInstanceId,
        ecClassId,
        sourceECInstanceId,
        targetECInstanceId,
      });
      return;
    }
    case "externalSourceAspect": {
      const { elementId, scopeId, kind, identifier } = deleted;
      // An aspect without these can't identify a target element.
      if (
        elementId !== undefined &&
        scopeId !== undefined &&
        kind !== undefined &&
        identifier !== undefined
      )
        batch.externalSourceAspects.push({
          ecInstanceId,
          ecClassId,
          elementId,
          scopeId,
          kind,
          identifier,
        });
      return;
    }
    case "aspect":
    case "codeSpec":
    case "skippedRelationship":
      return;
    case undefined:
      // Throws NotFound for a class missing from the current schemas. Other unhandled classes need no records.
      iModel.getClassNameFromId(ecClassId);
      return;
  }
}

/**
 * Reads each changeset once, unifies table changes into EC instance changes,
 * normalizes overflow-only inserts and deletes to updates, writes all operations
 * to [[ChangedInstanceIds]], and records the deleted instances needed to process deletions.
 * @internal
 */
export class ChangesetScanner {
  /**
   * Scans each group of changeset files in order with one reader and unifier per file.
   * @param iModel Database used to resolve EC classes.
   * @param csFileGroups Ordered groups of changeset files. Each group produces one deletion batch.
   * @param options Where to write the changes, and whether to collect deletion records.
   * @returns One deletion batch per group, empty when deletion records aren't collected.
   */
  public static async scan(
    iModel: IModelDb,
    csFileGroups: ChangesetFileProps[][],
    { changedInstanceIds, collectDeletionRecords = true }: ScanOptions
  ): Promise<DeletionBatch[]> {
    const classKinds = collectDeletionRecords
      ? await SourceClassKinds.query(iModel)
      : undefined;
    const batches = csFileGroups.map((): DeletionBatch => ({
      elements: [],
      models: [],
      relationships: [],
      externalSourceAspects: [],
    }));
    const readChanges = function* (): Generator<ChangeInstance> {
      for (const [index, csFiles] of csFileGroups.entries()) {
        for (const change of ChangesetScanner.readFiles(iModel, csFiles)) {
          if (classKinds !== undefined && change.$meta.op === "Deleted")
            addDeletionRecord(
              batches[index],
              {
                ecInstanceId: change.ECInstanceId,
                ecClassId: change.ECClassId,
                federationGuid: change.FederationGuid,
                sourceECInstanceId: change.SourceECInstanceId,
                targetECInstanceId: change.TargetECInstanceId,
                elementId: change.Element?.Id,
                scopeId: change.Scope?.Id,
                kind: change.Kind,
                identifier: change.Identifier,
              },
              classKinds,
              iModel
            );
          yield change;
        }
      }
    };
    if (changedInstanceIds !== undefined)
      await changedInstanceIds.addChanges(readChanges());
    // Without an aggregate, the changes are read only to record their deletions.
    else for (const _change of readChanges());
    return batches;
  }

  private static *readFiles(
    iModel: IModelDb,
    csFiles: ChangesetFileProps[]
  ): Generator<ChangeInstance> {
    for (const csFile of csFiles) {
      const csReader = ChangesetReader.openFile({
        fileName: csFile.pathname,
        db: iModel,
        propFilter: PropertyFilter.InstanceKeyAndIdentifiers,
      });
      const changeUnifier = new PartialChangeUnifier();
      try {
        while (csReader.step()) changeUnifier.appendFrom(csReader);
        for (const change of changeUnifier.instances) {
          if (change.ECClassId === undefined)
            ITwinError.throwError({
              iTwinErrorId: {
                scope: IModelTransformerErrorScope,
                key: IModelTransformerError.ChangedInstanceMetadataMissing,
              },
              message: `ECClassId was not found for id: ${change.ECInstanceId}! Table is : ${change.$meta.tables}`,
            });
          // Change is recorded at table level, not EC entity level.
          // This normalizes overflow-table expansion records so they do not
          // appear as element inserts or deletes.
          if (
            (change.$meta.op === "Inserted" || change.$meta.op === "Deleted") &&
            change.$meta.tables.every((table) => table.endsWith("Overflow"))
          ) {
            change.$meta.op = "Updated";
          }
          yield change;
        }
      } finally {
        try {
          changeUnifier[Symbol.dispose]();
        } finally {
          csReader[Symbol.dispose]();
        }
      }
    }
  }
}
