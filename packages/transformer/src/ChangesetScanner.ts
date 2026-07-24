/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ChangeInstance,
  ChangesetReader,
  IModelDb,
  PartialChangeUnifier,
  PropertyFilter,
} from "@itwin/core-backend";
import { Id64String, ITwinError } from "@itwin/core-bentley";
import { ChangesetFileProps } from "@itwin/core-common";
import type { ChangedInstanceIds } from "./ChangedInstanceIds";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";

/**
 * Metadata retained from a deleted EC instance for later target remapping.
 * Properties are optional when the deleted instance did not contain that value.
 * @internal
 */
export interface ChangesetDeletionRecord {
  /** ID of the deleted source instance. */
  ecInstanceId: Id64String;
  /** EC class ID of the deleted source instance. */
  ecClassId: Id64String;
  /** Full EC class name resolved from [[ecClassId]]. */
  classFullName?: string;
  /** Federation GUID used to find the corresponding target element. */
  federationGuid?: string;
  /** Source endpoint of a deleted relationship. */
  sourceECInstanceId?: Id64String;
  /** Target endpoint of a deleted relationship. */
  targetECInstanceId?: Id64String;
  /** Scope of a deleted ExternalSourceAspect. */
  scopeId?: Id64String;
  /** Element owning a deleted ElementAspect. */
  elementId?: Id64String;
  /** Kind of a deleted ExternalSourceAspect. */
  kind?: string;
  /** Identifier of a deleted ExternalSourceAspect. */
  identifier?: string;
}

/**
 * Pre-delete properties grouped by changeset. [[ChangedInstanceIds]] retains
 * operation sets but not the properties needed to remap deleted instances.
 * Grouping keeps scoped ExternalSourceAspect metadata paired with deletions
 * from the same changeset.
 * @internal
 */
export type ChangesetDeletionRecordsByChangeset = ChangesetDeletionRecord[][];

/**
 * Reads each changeset once, unifies table changes into EC instance changes,
 * normalizes overflow-only inserts and deletes to updates, writes all operations
 * to [[ChangedInstanceIds]], and retains properties needed to process deletions.
 * @internal
 */
export class ChangesetScanner {
  /**
   * Scans each changeset file in order with one reader and unifier per file.
   * @param iModel Database used to resolve EC class names.
   * @param csFileProps Ordered changeset files to scan.
   * @param changedInstanceIds Aggregate updated with the unified changes unless disabled by [[options]].
   * @param options Controls whether the aggregate is populated while deletion records are collected.
   * @returns Deleted-instance properties needed after the scan; changed IDs are written to [[changedInstanceIds]].
   */
  public static async scan(
    iModel: IModelDb,
    csFileProps: ChangesetFileProps[],
    changedInstanceIds: ChangedInstanceIds,
    options: { populateChangedInstanceIds?: boolean } = {}
  ): Promise<ChangesetDeletionRecordsByChangeset> {
    const deletionRecordsByChangeset: ChangesetDeletionRecord[][] = [];
    for (const csFile of csFileProps) {
      const csReader = ChangesetReader.openFile({
        fileName: csFile.pathname,
        db: iModel,
        propFilter: PropertyFilter.BisCoreElement,
      });
      const changeUnifier = new PartialChangeUnifier();
      try {
        while (csReader.step()) changeUnifier.appendFrom(csReader);
        const deletionRecords: ChangesetDeletionRecord[] = [];
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
            (change.$meta.op === "Inserted" || change.$meta.op === "Deleted") &&
            change.$meta.tables.every((table) => table.endsWith("Overflow"))
          ) {
            change.$meta.op = "Updated";
          }

          if (options.populateChangedInstanceIds !== false)
            await changedInstanceIds.addChange(change);
          if (change.$meta.op === "Deleted") {
            deletionRecords.push(this.toDeletionRecord(iModel, change));
          }
        }
        deletionRecordsByChangeset.push(deletionRecords);
      } finally {
        try {
          changeUnifier[Symbol.dispose]();
        } finally {
          csReader[Symbol.dispose]();
        }
      }
    }

    return deletionRecordsByChangeset;
  }

  private static toDeletionRecord(
    iModel: IModelDb,
    change: ChangeInstance
  ): ChangesetDeletionRecord {
    const ecClassId = change.ECClassId;
    if (ecClassId === undefined) {
      ITwinError.throwError({
        iTwinErrorId: {
          scope: IModelTransformerErrorScope,
          key: IModelTransformerError.ChangedInstanceMetadataMissing,
        },
        message: `ECClassId was not found for id: ${change.ECInstanceId}! Table is : ${change.$meta.tables}`,
      });
    }

    return {
      ecInstanceId: change.ECInstanceId,
      ecClassId,
      classFullName: iModel.getClassNameFromId(ecClassId),
      federationGuid: change.FederationGuid,
      sourceECInstanceId: change.SourceECInstanceId,
      targetECInstanceId: change.TargetECInstanceId,
      scopeId: change.Scope?.Id,
      elementId: change.Element?.Id,
      kind: change.Kind,
      identifier: change.Identifier,
    };
  }
}
