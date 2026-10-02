/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ChangeInstance,
  ChangesetReader,
  ExternalSourceAspect,
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
  /** Full EC class name of the deleted relationship. */
  classFullName: string;
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

/** Values kept from a deleted row until its class is known. */
interface DeletedRow {
  ecInstanceId: Id64String;
  ecClassId: Id64String;
  federationGuid?: string;
  sourceECInstanceId?: Id64String;
  targetECInstanceId?: Id64String;
  elementId?: Id64String;
  scopeId?: Id64String;
  kind?: string;
  identifier?: string;
}

interface ScanOptions {
  /** Whether to write the unified changes to the aggregate. Defaults to true. */
  populateChangedInstanceIds?: boolean;
  /** Whether to collect deletion records. Defaults to true. */
  collectDeletionRecords?: boolean;
}

interface DeletionClassIds {
  elements: Set<Id64String>;
  models: Set<Id64String>;
  relationships: Set<Id64String>;
  relationshipsToSkip: Set<Id64String>;
}

/**
 * Reads each changeset once, unifies table changes into EC instance changes,
 * normalizes overflow-only inserts and deletes to updates, writes all operations
 * to [[ChangedInstanceIds]], and retains properties needed to process deletions.
 * @internal
 */
export class ChangesetScanner {
  /**
   * Scans each group of changeset files in order with one reader and unifier per file.
   * @param iModel Database used to resolve EC classes.
   * @param csFileGroups Ordered groups of changeset files. Each group produces one deletion batch.
   * @param changedInstanceIds Aggregate updated with the unified changes unless disabled by [[options]].
   * @param options Controls whether the aggregate is populated and whether deletion records are collected.
   * @returns One deletion batch per group, empty when deletion records aren't collected; changed IDs are written to [[changedInstanceIds]].
   */
  public static async scan(
    iModel: IModelDb,
    csFileGroups: ChangesetFileProps[][],
    changedInstanceIds: ChangedInstanceIds,
    options: ScanOptions = {}
  ): Promise<DeletionBatch[]> {
    const deletedRowGroups: DeletedRow[][] = [];
    await changedInstanceIds.addChanges(
      this.readChanges(iModel, csFileGroups, deletedRowGroups, options)
    );
    const classIds = deletedRowGroups.some((rows) => rows.length > 0)
      ? await this.queryDeletionClassIds(iModel)
      : undefined;
    return deletedRowGroups.map((rows) =>
      this.toDeletionBatch(iModel, rows, classIds)
    );
  }

  private static *readChanges(
    iModel: IModelDb,
    csFileGroups: ChangesetFileProps[][],
    deletedRowGroups: DeletedRow[][],
    options: ScanOptions
  ): Generator<ChangeInstance> {
    for (const csFileProps of csFileGroups) {
      const deletedRows: DeletedRow[] = [];
      deletedRowGroups.push(deletedRows);
      for (const csFile of csFileProps) {
        const csReader = ChangesetReader.openFile({
          fileName: csFile.pathname,
          db: iModel,
          propFilter: PropertyFilter.InstanceKeyAndIdentifiers,
        });
        const changeUnifier = new PartialChangeUnifier();
        try {
          while (csReader.step()) changeUnifier.appendFrom(csReader);
          for (const change of changeUnifier.instances) {
            const ecClassId = change.ECClassId;
            if (ecClassId === undefined)
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
              (change.$meta.op === "Inserted" ||
                change.$meta.op === "Deleted") &&
              change.$meta.tables.every((table) => table.endsWith("Overflow"))
            ) {
              change.$meta.op = "Updated";
            }

            if (options.populateChangedInstanceIds !== false) yield change;
            if (
              change.$meta.op === "Deleted" &&
              options.collectDeletionRecords !== false
            ) {
              deletedRows.push({
                ecInstanceId: change.ECInstanceId,
                ecClassId,
                federationGuid: change.FederationGuid,
                sourceECInstanceId: change.SourceECInstanceId,
                targetECInstanceId: change.TargetECInstanceId,
                elementId: change.Element?.Id,
                scopeId: change.Scope?.Id,
                kind: change.Kind,
                identifier: change.Identifier,
              });
            }
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

  private static async queryDeletionClassIds(
    iModel: IModelDb
  ): Promise<DeletionClassIds> {
    const queryClassIds = async (baseClass: string) => {
      const classIds = new Set<Id64String>();
      for await (const row of iModel.createQueryReader(
        `SELECT ECInstanceId FROM ECDbMeta.ECClassDef WHERE ECInstanceId IS (${baseClass})`,
        undefined,
        { usePrimaryConn: true }
      ))
        classIds.add(row.ECInstanceId);
      return classIds;
    };
    const [elements, models, relationships, relationshipsToSkip] =
      await Promise.all([
        queryClassIds("BisCore.Element"),
        queryClassIds("BisCore.Model"),
        queryClassIds("BisCore.ElementRefersToElements"),
        queryClassIds("BisCore.ElementDrivesElement"),
      ]);
    return { elements, models, relationships, relationshipsToSkip };
  }

  private static toDeletionBatch(
    iModel: IModelDb,
    rows: DeletedRow[],
    classIds: DeletionClassIds | undefined
  ): DeletionBatch {
    const records: DeletionBatch = {
      elements: [],
      models: [],
      relationships: [],
      externalSourceAspects: [],
    };
    if (classIds === undefined) return records;

    for (const row of rows) {
      const { ecInstanceId, ecClassId } = row;
      if (classIds.relationshipsToSkip.has(ecClassId)) continue;
      if (classIds.relationships.has(ecClassId)) {
        const { sourceECInstanceId, targetECInstanceId } = row;
        if (
          sourceECInstanceId === undefined ||
          targetECInstanceId === undefined
        )
          ITwinError.throwError({
            iTwinErrorId: {
              scope: IModelTransformerErrorScope,
              key: IModelTransformerError.ChangedInstanceMetadataMissing,
            },
            message: `Relationship deletion ${ecInstanceId} is missing an endpoint.`,
          });
        records.relationships.push({
          ecInstanceId,
          ecClassId,
          classFullName: iModel.getClassNameFromId(ecClassId),
          sourceECInstanceId,
          targetECInstanceId,
        });
      } else if (classIds.models.has(ecClassId)) {
        records.models.push({ ecInstanceId, ecClassId });
      } else if (classIds.elements.has(ecClassId)) {
        records.elements.push({
          ecInstanceId,
          ecClassId,
          federationGuid: row.federationGuid,
        });
      } else if (
        // Throws for classes missing from the current schema. Other classes need no target lookup.
        iModel.getClassNameFromId(ecClassId) ===
          ExternalSourceAspect.classFullName &&
        row.elementId !== undefined &&
        row.scopeId !== undefined &&
        row.kind !== undefined &&
        row.identifier !== undefined
      ) {
        records.externalSourceAspects.push({
          ecInstanceId,
          ecClassId,
          elementId: row.elementId,
          scopeId: row.scopeId,
          kind: row.kind,
          identifier: row.identifier,
        });
      }
    }
    return records;
  }
}
