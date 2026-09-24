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
import { Id64String } from "@itwin/core-bentley";
import { QueryBinder } from "@itwin/core-common";

/** Deletes replaceable ElementAspects for target owners while preserving excluded classes and transformer provenance aspects.
 * @internal
 */
export class ElementAspectCleanup {
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

    const ids = new Set<Id64String>(targetElementIds);
    const targetExcludedElementAspectClassFullNames = [
      ...excludedElementAspectClassFullNames,
    ].filter((classFullName) => this._targetDb.containsClass(classFullName));
    for (const aspectClassFullName of [
      ElementUniqueAspect.classFullName,
      ElementMultiAspect.classFullName,
    ]) {
      // ExternalSourceAspect is a multi-aspect, so provenance aspects are only found
      // when querying ElementMultiAspect. For each candidate, check whether it is a
      // provenance aspect rather than listing every provenance aspect on each page.
      const preserveProvenance =
        provenanceScopeId !== undefined &&
        aspectClassFullName === ElementMultiAspect.classFullName;
      const conditions: string[] = [];
      if (targetExcludedElementAspectClassFullNames.length > 0) {
        conditions.push(
          `aspect.ECClassId IS NOT (${targetExcludedElementAspectClassFullNames.join(", ")})`
        );
      }
      if (preserveProvenance) {
        conditions.push(`NOT EXISTS (
            SELECT 1 FROM ${ExternalSourceAspect.classFullName} esa
            WHERE esa.ECInstanceId = aspect.ECInstanceId
              AND (esa.Element.Id = :provenanceScopeId OR esa.Scope.Id = :provenanceScopeId)
          )`);
      }
      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
      const query = `SELECT aspect.ECInstanceId as id
          FROM ${aspectClassFullName} aspect
          INNER JOIN IdSet(:elementIds) ids ON ids.id = aspect.Element.Id
          ${whereClause}
          LIMIT ${pageSize}`;
      while (true) {
        const params = new QueryBinder().bindIdSet("elementIds", ids);
        if (preserveProvenance)
          params.bindId("provenanceScopeId", provenanceScopeId);

        // Drain the full page before deleting: mutating a table while a reader is still
        // scanning it is unsafe.
        const candidateIds: Id64String[] = [];
        for await (const row of this._targetDb.createQueryReader(
          query,
          params,
          { usePrimaryConn: true }
        )) {
          candidateIds.push(row.id);
        }
        if (candidateIds.length === 0) break;

        for (const aspectId of candidateIds) {
          const aspect = this._targetDb.elements.getAspect(aspectId);
          await this._deleteAspect(aspect);
        }
      }
    }
  }
}
