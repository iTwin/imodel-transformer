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

        const query = `SELECT ECInstanceId FROM ${aspectClassFullName}
          WHERE ${whereClause}
          LIMIT ${pageSize}`;
        const aspectIds: Id64String[] = [];
        for await (const row of this._targetDb.createQueryReader(
          query,
          params,
          { usePrimaryConn: true }
        )) {
          aspectIds.push(row.id);
        }
        if (aspectIds.length === 0) break;

        for (const aspectId of aspectIds) {
          await this._deleteAspect(this._targetDb.elements.getAspect(aspectId));
        }
      }
    }
  }
}

const cleanups = new WeakMap<object, ElementAspectCleanup>();

export function registerElementAspectCleanup(
  owner: object,
  cleanup: ElementAspectCleanup
): void {
  cleanups.set(owner, cleanup);
}

export function getElementAspectCleanup(owner: object): ElementAspectCleanup {
  const cleanup = cleanups.get(owner);
  if (cleanup === undefined) {
    throw new Error("ElementAspect cleanup is not registered.");
  }
  return cleanup;
}
