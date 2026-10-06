/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, ElementAspect } from "@itwin/core-backend";
import { DbResult, GuidString, Id64String } from "@itwin/core-bentley";
import { ElementAspectProps, FilePropertyProps } from "@itwin/core-common";
import {
  isTransformerProvenanceAspect,
  sourceProvenanceMatchFilter,
} from "./ProvenanceManager";

/** The target scope elements that transformer provenance can be scoped to, and the current transformation's.
 * @internal
 */
export interface TargetScopes {
  readonly elementIds: ReadonlySet<Id64String>;
  readonly current: Id64String;
}

/** Which of one target element's aspects the current transformation may touch.
 * @internal
 */
export interface OwnerAspectOwnership {
  /** Whether cleanup may delete `aspect` when the source no longer has it.
   * Aspects that this source recorded may be deleted; another source's aspects and unrecorded transformer provenance may not.
   */
  canDelete(aspect: ElementAspect): boolean;
  /** The importer filter for an import of `incoming`. Aspects that this source recorded always match; another source's aspects never do; unrecorded transformer provenance matches only an incoming aspect with the same kind, scope, and identifier. */
  matchFilter(
    incoming: readonly ElementAspectProps[]
  ): (aspect: ElementAspect) => boolean;
  /** The target aspect that this source already copied `sourceAspectId` to. */
  ownedTarget(sourceAspectId: Id64String): Id64String | undefined;
  /** Records that this source copied `sourceAspectId` to `targetAspectId`. Written by [[AspectOwnership.flush]]. */
  record(sourceAspectId: Id64String, targetAspectId: Id64String): void;
}

/** Which source each target ElementAspect was copied from, for one transformation.
 * @internal
 */
export interface AspectOwnership {
  /** The records of one target element's aspects. Read once and kept until [[flush]]. */
  forOwner(ownerId: Id64String): OwnerAspectOwnership;
  /** Writes the records added since the last flush, then forgets the loaded owners; or forgets them without writing when `write` is false. */
  flush(write: boolean): void;
  /** Deletes records whose target aspect no longer exists. */
  sweep(): void;
}

interface AspectRecord {
  sourceIModel: GuidString;
  sourceAspect: Id64String;
}

const namespace = "IModelTransformer";
const name = "ElementAspectSource";

/** Records which source iModel and source ElementAspect each target ElementAspect was copied from, so that transformations
 * from different source iModels into shared target elements leave each other's aspects alone.
 *
 * Each record is a `be_Prop` row: Namespace `IModelTransformer`, Name `ElementAspectSource`, Id = target aspect ID,
 * value `{ sourceIModel, sourceAspect }`. File properties are not ElementAspects, so records are invisible to `getAspects`
 * and are never exported. Records whose target aspect no longer exists are removed by [[sweep]].
 * @internal
 */
export class ElementAspectOwnership implements AspectOwnership {
  private readonly _owners = new Map<Id64String, OwnerView>();

  public constructor(
    private readonly _editTxn: EditTxn,
    private readonly _sourceIModelId: GuidString,
    private readonly _scopes: TargetScopes
  ) {}

  public forOwner(ownerId: Id64String): OwnerAspectOwnership {
    let view = this._owners.get(ownerId);
    if (view === undefined) {
      view = new OwnerView(
        this._sourceIModelId,
        readOwnerRecords(this._editTxn, ownerId),
        this._scopes
      );
      this._owners.set(ownerId, view);
    }
    return view;
  }

  public flush(write: boolean): void {
    if (write)
      for (const view of this._owners.values())
        for (const [targetAspectId, record] of view.pending)
          this._editTxn.saveFileProperty(
            elementAspectRecordProperty(targetAspectId),
            JSON.stringify(record)
          );
    this._owners.clear();
  }

  /** Deletes records whose target aspect no longer exists: deleted with its element, by the cleanup of an owner batch, or outside the transformer. */
  public sweep(): void {
    for (const targetAspectId of selectIds(
      this._editTxn,
      `SELECT p.Id FROM be_Prop p INDEXED BY sqlite_autoindex_be_Prop_1
       WHERE p.Namespace=? AND p.Name=?
         AND NOT EXISTS (SELECT 1 FROM bis_ElementMultiAspect WHERE Id=p.Id)
         AND NOT EXISTS (SELECT 1 FROM bis_ElementUniqueAspect WHERE Id=p.Id)`
    ))
      this._editTxn.deleteFileProperty(
        elementAspectRecordProperty(targetAspectId)
      );
  }

  /** Deletes every record. A target copied from the source inherits the source's own records, which describe the source's upstream imports, not this source. */
  public clear(): void {
    for (const targetAspectId of selectIds(
      this._editTxn,
      "SELECT Id FROM be_Prop INDEXED BY sqlite_autoindex_be_Prop_1 WHERE Namespace=? AND Name=?"
    ))
      this._editTxn.deleteFileProperty(
        elementAspectRecordProperty(targetAspectId)
      );
  }
}

/** Ownership for a transformation that records nothing, with `noProvenance` or in reverse synchronization: no aspect is recorded or foreign, so only transformer provenance is protected.
 * @internal
 */
export function untrackedAspectOwnership(
  scopes: TargetScopes
): AspectOwnership {
  return {
    forOwner: () => new OwnerView(undefined, new Map(), scopes),
    flush: () => {},
    sweep: () => {},
  };
}

class OwnerView implements OwnerAspectOwnership {
  private readonly _foreignAspectIds = new Set<Id64String>();
  private readonly _ownedTargets = new Map<Id64String, Id64String>();
  /** Records to write, by target aspect ID. */
  public readonly pending = new Map<Id64String, AspectRecord>();

  /** @param _sourceIModelId the current source iModel, or undefined when nothing is recorded */
  public constructor(
    private readonly _sourceIModelId: GuidString | undefined,
    private readonly _records: Map<Id64String, AspectRecord>,
    private readonly _scopes: TargetScopes
  ) {
    for (const [targetAspectId, record] of _records) {
      if (record.sourceIModel === _sourceIModelId)
        this._ownedTargets.set(record.sourceAspect, targetAspectId);
      else this._foreignAspectIds.add(targetAspectId);
    }
  }

  public canDelete(aspect: ElementAspect): boolean {
    if (this.isOwned(aspect.id)) return true;
    return (
      !this._foreignAspectIds.has(aspect.id) &&
      !isTransformerProvenanceAspect(aspect, this._scopes.elementIds)
    );
  }

  public matchFilter(
    incoming: readonly ElementAspectProps[]
  ): (aspect: ElementAspect) => boolean {
    const isReusableProvenance = sourceProvenanceMatchFilter(
      incoming,
      this._scopes.elementIds,
      this._scopes.current
    );
    return (aspect) =>
      this.isOwned(aspect.id) ||
      (!this._foreignAspectIds.has(aspect.id) && isReusableProvenance(aspect));
  }

  public ownedTarget(sourceAspectId: Id64String): Id64String | undefined {
    return this._ownedTargets.get(sourceAspectId);
  }

  public record(sourceAspectId: Id64String, targetAspectId: Id64String): void {
    if (
      this._sourceIModelId === undefined ||
      this._ownedTargets.get(sourceAspectId) === targetAspectId
    )
      return;
    const record = {
      sourceIModel: this._sourceIModelId,
      sourceAspect: sourceAspectId,
    };
    this._records.set(targetAspectId, record);
    this._ownedTargets.set(sourceAspectId, targetAspectId);
    this.pending.set(targetAspectId, record);
  }

  private isOwned(targetAspectId: Id64String): boolean {
    const record = this._records.get(targetAspectId);
    return record !== undefined && record.sourceIModel === this._sourceIModelId;
  }
}

/** The file property holding the record of `targetAspectId`.
 * @internal
 */
export function elementAspectRecordProperty(
  targetAspectId: Id64String
): FilePropertyProps {
  return { namespace, name, id: targetAspectId };
}

/** Reads the records of an element's aspects in one indexed query.
 * Queries force be_Prop's primary key index: the connection that created the iModel otherwise scans be_Prop, because
 * BeSQLite's `analyze` + `delete from sqlite_stat1` on create leaves it with statistics for a nearly empty table.
 */
function readOwnerRecords(
  editTxn: EditTxn,
  ownerId: Id64String
): Map<Id64String, AspectRecord> {
  const records = new Map<Id64String, AspectRecord>();
  for (const table of ["bis_ElementMultiAspect", "bis_ElementUniqueAspect"])
    editTxn.iModel.withPreparedSqliteStatement(
      `SELECT p.Id, p.StrData FROM ${table} a CROSS JOIN be_Prop p INDEXED BY sqlite_autoindex_be_Prop_1
       WHERE a.ElementId=? AND p.Namespace=? AND p.Name=? AND p.Id=a.Id AND p.SubId=0`,
      (statement) => {
        statement.bindId(1, ownerId);
        statement.bindString(2, namespace);
        statement.bindString(3, name);
        while (statement.step() === DbResult.BE_SQLITE_ROW)
          records.set(
            statement.getValue(0).getId(),
            parseRecord(statement.getValue(1).getString())
          );
      }
    );
  return records;
}

function parseRecord(value: string): AspectRecord {
  const record = JSON.parse(value) as Partial<AspectRecord>;
  if (
    typeof record.sourceIModel !== "string" ||
    typeof record.sourceAspect !== "string"
  )
    throw new Error(`Malformed ElementAspect ownership record: ${value}`);
  return {
    sourceIModel: record.sourceIModel,
    sourceAspect: record.sourceAspect,
  };
}

function selectIds(editTxn: EditTxn, sql: string): Id64String[] {
  const ids: Id64String[] = [];
  editTxn.iModel.withSqliteStatement(sql, (statement) => {
    statement.bindString(1, namespace);
    statement.bindString(2, name);
    while (statement.step() === DbResult.BE_SQLITE_ROW)
      ids.push(statement.getValue(0).getId());
  });
  return ids;
}
