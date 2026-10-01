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
 * Usage per owner batch: [[collect]] the aspects the owners have before import, let the importer [[retain]] each aspect it reuses or deletes itself, then [[deleteUnretained]] to delete the rest, or [[discard]] the batch if its export failed.
 * While a batch is active, [[getAspects]] answers target aspect reads for its owners from the aspects loaded by [[collect]], so the importer doesn't query each owner separately.
 * The importer brackets its own aspect writes with [[beforeWrite]] and [[afterWrite]]. If anything else writes to the target during the batch, including direct `EditTxn` calls, the loaded aspects are dropped and reads go to the target for the rest of the batch.
 * @internal
 */
export class ElementAspectCleanup {
  private _candidateIds = new Set<Id64String>();
  /** Every target aspect of each batch owner that has not been written since [[collect]], keyed by owner. */
  private _aspectsByOwner = new Map<Id64String, ElementAspect[]>();
  /** The target connection's change count after [[collect]] or the importer's last write; undefined when no aspects are loaded. */
  private _expectedChangeCount?: number;

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
    // Build the batch locally so a failed query leaves no partial batch behind.
    this.discard();
    if (targetElementIds.size === 0) return;

    const candidateIds = new Set<Id64String>();
    const aspectsByOwner = new Map<Id64String, ElementAspect[]>();

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
        candidateIds.add(row.id);
      }
    }

    // Load every aspect, including excluded classes and provenance, so reads
    // answer exactly what getAspects would for these owners.
    for (const elementId of targetElementIds) aspectsByOwner.set(elementId, []);
    for await (const aspect of this._targetDb.elements.queryAspects({
      elementIds: [...targetElementIds],
      groupByOwner: true,
      usePrimaryConn: true,
    })) {
      aspectsByOwner.get(aspect.element.id)?.push(aspect);
    }

    this._candidateIds = candidateIds;
    this._aspectsByOwner = aspectsByOwner;
    this._expectedChangeCount = this.targetChangeCount();
  }

  /** Returns the owner's target aspects of exactly `classFullName`, in ECInstanceId order, from the aspects loaded by [[collect]].
   * Returns undefined when the owner is not in the active batch or has been written since [[collect]]; the caller must then read the target.
   */
  public getAspects(
    elementId: Id64String,
    classFullName: string
  ): ElementAspect[] | undefined {
    this.dropLoadedAspectsIfChangedElsewhere();
    return this._aspectsByOwner
      .get(elementId)
      ?.filter((aspect) => isSameClass(aspect, classFullName));
  }

  /** Stops answering reads for an owner from the loaded aspects. Call after writing any of its aspects. */
  public invalidate(elementId: Id64String): void {
    this._aspectsByOwner.delete(elementId);
  }

  /** Call right before the importer writes an aspect of `elementId`. */
  public beforeWrite(elementId: Id64String): void {
    this.dropLoadedAspectsIfChangedElsewhere();
    this.invalidate(elementId);
  }

  /** Call right after the importer's write so that it isn't mistaken for a write made elsewhere. */
  public afterWrite(): void {
    if (this._expectedChangeCount !== undefined)
      this._expectedChangeCount = this.targetChangeCount();
  }

  /** Discards the batch without deleting anything. */
  public discard(): void {
    this._candidateIds = new Set<Id64String>();
    this._aspectsByOwner = new Map<Id64String, ElementAspect[]>();
    this._expectedChangeCount = undefined;
  }

  /** Drops all loaded aspects if the target changed since [[collect]] or the importer's last write, which means code other than the importer wrote to it. */
  private dropLoadedAspectsIfChangedElsewhere(): void {
    if (
      this._expectedChangeCount === undefined ||
      this.targetChangeCount() === this._expectedChangeCount
    )
      return;
    this._aspectsByOwner = new Map<Id64String, ElementAspect[]>();
    this._expectedChangeCount = undefined;
  }

  /** SQLite's count of rows inserted, updated, or deleted on the target connection since it opened. Reads don't change it. */
  private targetChangeCount(): number {
    return this._targetDb.withPreparedSqliteStatement(
      "SELECT total_changes()",
      (statement) => {
        statement.step();
        return statement.getValue(0).getInteger();
      }
    );
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
    this.discard();
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

/** Whether an aspect is exactly of class `classFullName`, which may use either `:` or `.` as the separator.
 * @internal
 */
export function isSameClass(
  aspect: ElementAspect,
  classFullName: string
): boolean {
  return (
    aspect.classFullName.toLowerCase() ===
    classFullName.replace(".", ":").toLowerCase()
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
