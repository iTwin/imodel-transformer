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
import { Id64String, IModelStatus } from "@itwin/core-bentley";
import { IModelError, QueryBinder } from "@itwin/core-common";

/** Deletes replaceable ElementAspects of target owners that an owner batch did not reuse, while preserving excluded classes and transformer provenance aspects.
 *
 * Usage per owner batch: [[collect]] the replaceable aspects the owners have before import, let the importer [[retain]] each aspect it reuses or deletes itself, then [[deleteUnretained]] to delete the rest.
 * @internal
 */
export class ElementAspectCleanup {
  private _candidateIds = new Set<Id64String>();

  public constructor(
    private readonly _targetDb: IModelDb,
    private readonly _editTxn: EditTxn,
    private readonly _deleteAspect: (aspect: ElementAspect) => Promise<void>
  ) {}

  /** Records the replaceable unique and multi-aspects owned by the supplied target elements as deletion candidates, replacing any previous candidates.
   * Excluded classes and transformer provenance aspects for `provenanceScopeId` are never candidates.
   */
  public async collect(
    targetElementIds: ReadonlySet<Id64String>,
    excludedElementAspectClassFullNames: ReadonlySet<string>,
    provenanceScopeId?: Id64String
  ): Promise<void> {
    this._candidateIds = new Set<Id64String>();
    if (targetElementIds.size === 0) return;

    const targetExcludedElementAspectClassFullNames = [
      ...excludedElementAspectClassFullNames,
    ].filter((classFullName) => this._targetDb.containsClass(classFullName));
    for (const aspectClassFullName of [
      ElementUniqueAspect.classFullName,
      ElementMultiAspect.classFullName,
    ]) {
      const { ecsql, params } = replaceableAspectQuery(
        aspectClassFullName,
        targetElementIds,
        targetExcludedElementAspectClassFullNames,
        provenanceScopeId
      );
      for await (const row of this._targetDb.createQueryReader(ecsql, params, {
        usePrimaryConn: true,
      })) {
        this._candidateIds.add(row.id);
      }
    }
  }

  /** Removes an aspect from the deletion candidates because the importer reused or already deleted it. */
  public retain(aspectId: Id64String): void {
    this._candidateIds.delete(aspectId);
  }

  /** Deletes the remaining candidates through the configured callback and clears them.
   * Requires the target `EditTxn` to be active when there is anything to delete.
   */
  public async deleteUnretained(): Promise<void> {
    const candidateIds = this._candidateIds;
    this._candidateIds = new Set<Id64String>();
    if (candidateIds.size === 0) return;

    if (!this._editTxn.isActive) {
      throw new Error(
        "The target EditTxn must be active when deleting ElementAspects."
      );
    }
    for (const aspectId of candidateIds) {
      // Deleting a unique aspect also deletes unique aspects of derived
      // classes on the same element, so a candidate may already be gone.
      const aspect = tryGetAspect(this._targetDb, aspectId);
      if (aspect === undefined) continue;
      await this._deleteAspect(aspect);
    }
  }
}

/** Builds the query for aspects of `aspectClassFullName` owned by `elementIds`, skipping excluded classes and provenance aspects for `provenanceScopeId`. */
function replaceableAspectQuery(
  aspectClassFullName: string,
  elementIds: ReadonlySet<Id64String>,
  excludedClassFullNames: readonly string[],
  provenanceScopeId: Id64String | undefined
): { ecsql: string; params: QueryBinder } {
  const params = new QueryBinder().bindIdSet("elementIds", elementIds);
  const conditions: string[] = [];
  if (excludedClassFullNames.length > 0) {
    conditions.push(
      `aspect.ECClassId IS NOT (${excludedClassFullNames.join(", ")})`
    );
  }
  // ExternalSourceAspect is a multi-aspect, so provenance aspects are only found
  // when querying ElementMultiAspect. For each candidate, check whether it is a
  // provenance aspect rather than listing every provenance aspect. This must
  // match isTransformerProvenanceAspect.
  if (
    provenanceScopeId !== undefined &&
    aspectClassFullName === ElementMultiAspect.classFullName
  ) {
    params
      .bindId("provenanceScopeId", provenanceScopeId)
      .bindString("elementKind", ExternalSourceAspect.Kind.Element)
      .bindString("relationshipKind", ExternalSourceAspect.Kind.Relationship)
      .bindString("scopeKind", ExternalSourceAspect.Kind.Scope);
    conditions.push(`NOT EXISTS (
      SELECT 1 FROM ${ExternalSourceAspect.classFullName} esa
      WHERE esa.ECInstanceId = aspect.ECInstanceId
        AND (
          (esa.Scope.Id = :provenanceScopeId AND esa.Kind IN (:elementKind, :relationshipKind))
          OR (esa.Element.Id = :provenanceScopeId AND esa.Kind = :scopeKind)
        )
    )`);
  }
  const whereClause =
    conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const ecsql = `SELECT aspect.ECInstanceId as id
    FROM ${aspectClassFullName} aspect
    INNER JOIN IdSet(:elementIds) ids ON ids.id = aspect.Element.Id
    ${whereClause}`;
  return { ecsql, params };
}

/** Whether an ExternalSourceAspect is provenance that a transformation into `provenanceScopeId` writes: element or relationship provenance scoped to it, or scope provenance owned by it.
 * Other ExternalSourceAspects, including source provenance cloned with a scope that maps to `provenanceScopeId`, are replaceable.
 * @internal
 */
export function isTransformerProvenanceAspect(
  aspect: ExternalSourceAspect,
  provenanceScopeId: Id64String
): boolean {
  return (
    (aspect.scope?.id === provenanceScopeId &&
      (aspect.kind === ExternalSourceAspect.Kind.Element ||
        aspect.kind === ExternalSourceAspect.Kind.Relationship)) ||
    (aspect.element.id === provenanceScopeId &&
      aspect.kind === ExternalSourceAspect.Kind.Scope)
  );
}

/** Returns the aspect, or undefined if it no longer exists.
 * @internal
 */
export function tryGetAspect(
  db: IModelDb,
  aspectId: Id64String
): ElementAspect | undefined {
  try {
    return db.elements.getAspect(aspectId);
  } catch (error) {
    if (
      error instanceof IModelError &&
      error.errorNumber === IModelStatus.NotFound
    )
      return undefined;
    throw error;
  }
}
