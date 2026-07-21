/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ChangeInstance,
  ChangesetReader,
  IModelDb,
  PartialChangeUnifier,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ChangesetFileProps } from "@itwin/core-common";
import type { ChangedInstanceIds } from "./IModelExporter";
import {
  changesetScanPass,
  getActiveChangesetScanMetrics,
} from "./ChangesetScanInstrumentation";

/** @internal */
export interface ChangesetDeletionRecord {
  ecInstanceId: Id64String;
  ecClassId: Id64String;
  classFullName?: string;
  federationGuid?: string;
  sourceECInstanceId?: Id64String;
  targetECInstanceId?: Id64String;
  scopeId?: Id64String;
  elementId?: Id64String;
  kind?: string;
  identifier?: string;
}

/** @internal */
export interface ChangesetScanResult {
  deletionRecordsByChangeset: ChangesetDeletionRecord[][];
}

/** @internal */
export class ChangesetScanner {
  public static async scan(
    iModel: IModelDb,
    csFileProps: ChangesetFileProps[],
    changedInstanceIds: ChangedInstanceIds,
    options: { populateChangedInstanceIds?: boolean } = {}
  ): Promise<ChangesetScanResult> {
    const deletionRecordsByChangeset: ChangesetDeletionRecord[][] = [];
    const scanMetrics = getActiveChangesetScanMetrics();
    scanMetrics?.startPass(changesetScanPass.singleScanner);
    try {
      for (const csFile of csFileProps) {
        scanMetrics?.recordFileOpen(
          changesetScanPass.singleScanner,
          csFile.pathname
        );
        const csReader = ChangesetReader.openFile({
          fileName: csFile.pathname,
          db: iModel,
        });
        const changeUnifier = new PartialChangeUnifier();
        try {
          while (csReader.step()) changeUnifier.appendFrom(csReader);
          const changes = [...changeUnifier.instances];
          scanMetrics?.recordUnifiedRows(
            changesetScanPass.singleScanner,
            changes.length
          );

          const deletionRecords: ChangesetDeletionRecord[] = [];
          for (const change of changes) {
            const ecClassId = change.ECClassId;
            if (ecClassId === undefined)
              throw new Error(
                `ECClassId was not found for id: ${change.ECInstanceId}! Table is : ${change.$meta.tables}`
              );
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

            if (options.populateChangedInstanceIds !== false)
              await changedInstanceIds.addChange({
                ECInstanceId: change.ECInstanceId,
                ECClassId: ecClassId,
                $meta: {
                  tables: change.$meta.tables,
                  op: change.$meta.op,
                  stage: change.$meta.stage,
                  changeIndexes: change.$meta.changeIndexes,
                },
              });
            if (change.$meta.op === "Deleted") {
              deletionRecords.push(this.toDeletionRecord(iModel, change));
            }
          }
          scanMetrics?.recordDeletionRecords(
            changesetScanPass.singleScanner,
            deletionRecords.length
          );
          deletionRecordsByChangeset.push(deletionRecords);
        } finally {
          try {
            changeUnifier[Symbol.dispose]();
          } finally {
            csReader[Symbol.dispose]();
          }
        }
        scanMetrics?.recordFileScan(changesetScanPass.singleScanner);
      }
    } finally {
      scanMetrics?.finishPass(changesetScanPass.singleScanner);
    }

    return {
      deletionRecordsByChangeset,
    };
  }

  private static toDeletionRecord(
    iModel: IModelDb,
    change: ChangeInstance
  ): ChangesetDeletionRecord {
    const ecClassId = change.ECClassId;
    if (ecClassId === undefined) {
      throw new Error(
        `ECClassId was not found for id: ${change.ECInstanceId}! Table is : ${change.$meta.tables}`
      );
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
