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
import { Id64Set, Id64String } from "@itwin/core-bentley";
import { ElementAspectProps, QueryBinder } from "@itwin/core-common";

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
        let whereClause = "InVirtualSet(:elementIds, Element.Id)";
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

        // Deletion only ever needs id/classFullName/owner id (see onDeleteElementAspect),
        // never the concrete class's own properties, so select exactly those three things
        // with explicit aliases instead of `SELECT *`. That sidesteps QueryRowFormat
        // entirely: explicit aliases resolve the same way under every row format, so
        // there's no need for the deprecated UseJsPropertyNames remap this file used to
        // rely on to turn `Element.Id` into a usable owner id.
        const query = `SELECT ECInstanceId as id,
            (ec_className(ECClassId, 's')) as schemaName,
            (ec_className(ECClassId, 'c')) as className,
            Element.Id as elementId
          FROM ${aspectClassFullName}
          WHERE ${whereClause}
          LIMIT ${pageSize}`;
        // Fully drain the candidate page before deleting anything: deleting while this
        // reader is still stepping through the same table it's scanning is unsafe.
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

        for (const aspectProps of candidates) {
          await this._deleteAspect(
            this._targetDb.constructEntity<ElementAspect>(aspectProps)
          );
        }
      }
    }
  }
}
