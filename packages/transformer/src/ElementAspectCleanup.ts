/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  ElementAspect,
  ElementMultiAspect,
  ElementUniqueAspect,
  ExternalSourceAspect,
  IModelDb,
} from "@itwin/core-backend";
import { Id64Set, Id64String, StopWatch } from "@itwin/core-bentley";
import { ElementAspectProps, QueryBinder } from "@itwin/core-common";

/** Accumulated wall time, call count, and candidate/deleted totals for
 * {@link ElementAspectCleanup.delete}. For perf investigation only, not a stable API.
 * @internal
 */
export interface ElementAspectCleanupDiagnostics {
  wallMs: number;
  callCount: number;
  candidateCount: number;
  deletedCount: number;
}

/** Deletes replaceable ElementAspects for target owners while preserving excluded classes and transformer provenance aspects.
 * @internal
 */
export class ElementAspectCleanup {
  private static _diagnostics: ElementAspectCleanupDiagnostics = {
    wallMs: 0,
    callCount: 0,
    candidateCount: 0,
    deletedCount: 0,
  };

  /** Resets accumulated diagnostics. For perf investigation only, not a stable API.
   * @internal
   */
  public static resetDiagnostics(): void {
    this._diagnostics = {
      wallMs: 0,
      callCount: 0,
      candidateCount: 0,
      deletedCount: 0,
    };
  }

  /** Accumulated diagnostics across every {@link delete} call since the last
   * {@link resetDiagnostics}. For perf investigation only, not a stable API.
   * @internal
   */
  public static get diagnostics(): Readonly<ElementAspectCleanupDiagnostics> {
    return this._diagnostics;
  }

  public constructor(
    private readonly _targetDb: IModelDb,
    private readonly _editTxn: EditTxn,
    private readonly _deleteAspect: (aspect: ElementAspect) => Promise<void>
  ) {}

  /** Deletes replaceable unique and multi-aspects owned by the supplied target elements.
   * Excluded classes and transformer provenance aspects for `provenanceScopeId` are preserved. Each deletion is performed through the configured callback and requires the target `EditTxn` to be active.
   */
  public async delete(
    targetElementIds: ReadonlySet<Id64String>,
    excludedElementAspectClassFullNames: ReadonlySet<string>,
    provenanceScopeId?: Id64String,
    pageSize = IModelDb.maxLimit - 1
  ): Promise<void> {
    const stopwatch = new StopWatch();
    stopwatch.start();
    try {
      await this.deleteImpl(
        targetElementIds,
        excludedElementAspectClassFullNames,
        provenanceScopeId,
        pageSize
      );
    } finally {
      stopwatch.stop();
      ElementAspectCleanup._diagnostics.wallMs +=
        stopwatch.elapsedSeconds * 1000;
      ElementAspectCleanup._diagnostics.callCount++;
    }
  }

  private async deleteImpl(
    targetElementIds: ReadonlySet<Id64String>,
    excludedElementAspectClassFullNames: ReadonlySet<string>,
    provenanceScopeId?: Id64String,
    pageSize = IModelDb.maxLimit - 1
  ): Promise<void> {
    if (!this._editTxn.isActive) {
      throw new Error(
        "The target EditTxn must be active when deleting ElementAspects."
      );
    }
    if (pageSize <= 0 || !Number.isSafeInteger(pageSize)) {
      throw new Error(
        "ElementAspect deletion pageSize must be a positive integer."
      );
    }
    if (targetElementIds.size === 0) return;

    const ids = new Set<Id64String>(targetElementIds) as Id64Set;
    const targetExcludedElementAspectClassFullNames = [
      ...excludedElementAspectClassFullNames,
    ].filter((classFullName) => this._targetDb.containsClass(classFullName));
    for (const aspectClassFullName of [
      ElementUniqueAspect.classFullName,
      ElementMultiAspect.classFullName,
    ]) {
      while (true) {
        const params = new QueryBinder().bindIdSet("elementIds", ids);
        let whereClause = "TRUE";
        if (provenanceScopeId !== undefined) {
          params.bindId("provenanceScopeId", provenanceScopeId);
          whereClause += ` AND ECInstanceId NOT IN (
            SELECT ECInstanceId FROM ${ExternalSourceAspect.classFullName}
            WHERE Element.Id = :provenanceScopeId OR Scope.Id = :provenanceScopeId
          )`;
        }
        if (targetExcludedElementAspectClassFullNames.length > 0) {
          whereClause += ` AND ECInstanceId NOT IN (
            SELECT ECInstanceId FROM ${aspectClassFullName}
            WHERE ECClassId IS (${[
              ...targetExcludedElementAspectClassFullNames,
            ].join(", ")})
          )`;
        }

        // onDeleteElementAspect only needs id/classFullName/owner id, so select exactly
        // those instead of the concrete class's own properties.
        const query = `SELECT aspect.ECInstanceId as id,
            (ec_className(aspect.ECClassId, 's')) as schemaName,
            (ec_className(aspect.ECClassId, 'c')) as className,
            aspect.Element.Id as elementId
          FROM ${aspectClassFullName} aspect
          INNER JOIN IdSet(:elementIds) ids ON ids.id = aspect.Element.Id
          WHERE ${whereClause}
          LIMIT ${pageSize}
          OPTIONS ENABLE_EXPERIMENTAL_FEATURES`;
        // Drain the full page before deleting: mutating a table while a reader is still
        // scanning it is unsafe.
        const candidates: ElementAspectProps[] = [];
        for await (const row of this._targetDb.createQueryReader(
          query,
          params,
          { usePrimaryConn: true }
        )) {
          candidates.push({
            classFullName: `${row.schemaName}:${row.className}`,
            id: row.id,
            element: { id: row.elementId },
          });
        }
        if (candidates.length === 0) break;
        ElementAspectCleanup._diagnostics.candidateCount += candidates.length;

        for (const aspectProps of candidates) {
          await this._deleteAspect(
            this._targetDb.constructEntity<ElementAspect>(aspectProps)
          );
          ElementAspectCleanup._diagnostics.deletedCount++;
        }
      }
    }
  }
}
