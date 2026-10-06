/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn } from "@itwin/core-backend";
import { DbResult, GuidString, Id64String } from "@itwin/core-bentley";
import { FilePropertyProps } from "@itwin/core-common";

/** Which source ElementAspects one target element's aspects were copied from.
 * @internal
 */
export interface OwnerAspectOwnership {
  /** Target aspects that another source iModel recorded. They are never reused, replaced, or deleted. */
  readonly foreignAspectIds: ReadonlySet<Id64String>;
  /** Whether this source recorded `targetAspectId`. This source may reuse, replace, or delete it, whatever its class or kind. */
  isOwned(targetAspectId: Id64String): boolean;
  /** The target aspect that this source already copied `sourceAspectId` to. */
  ownedTarget(sourceAspectId: Id64String): Id64String | undefined;
  /** Records that this source copied `sourceAspectId` to `targetAspectId`. Written by [[ElementAspectOwnership.flush]]. */
  record(sourceAspectId: Id64String, targetAspectId: Id64String): void;
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
 *
 * Use [[untracked]] when ownership is not tracked: every aspect is then unrecorded, as before records existed.
 * @internal
 */
export class ElementAspectOwnership {
  /** Tracks nothing: no aspect is recorded or foreign, and nothing is written. */
  public static readonly untracked: ElementAspectOwnership =
    new ElementAspectOwnership(undefined);

  private readonly _owners = new Map<Id64String, OwnerView>();

  /** @param _tracked the target transaction and the current source iModel, or undefined for [[untracked]] */
  public constructor(
    private readonly _tracked:
      | { editTxn: EditTxn; sourceIModelId: GuidString }
      | undefined
  ) {}

  /** The records of one target element's aspects. Read once and kept until [[flush]]. */
  public forOwner(ownerId: Id64String): OwnerAspectOwnership {
    if (this._tracked === undefined) return untrackedOwner;
    let view = this._owners.get(ownerId);
    if (view === undefined) {
      view = new OwnerView(
        this._tracked.sourceIModelId,
        readOwnerRecords(this._tracked.editTxn, ownerId)
      );
      this._owners.set(ownerId, view);
    }
    return view;
  }

  /** Writes the records added since the last flush, then forgets the loaded owners; or forgets them without writing when `write` is false. */
  public flush(write: boolean): void {
    if (write && this._tracked !== undefined)
      for (const view of this._owners.values())
        for (const [targetAspectId, record] of view.pending)
          this._tracked.editTxn.saveFileProperty(
            elementAspectRecordProperty(targetAspectId),
            JSON.stringify(record)
          );
    this._owners.clear();
  }

  /** Deletes records whose target aspect no longer exists: deleted with its element, by the cleanup of an owner batch, or outside the transformer. */
  public sweep(): void {
    if (this._tracked === undefined) return;
    const { editTxn } = this._tracked;
    for (const targetAspectId of selectIds(
      editTxn,
      `SELECT p.Id FROM be_Prop p INDEXED BY sqlite_autoindex_be_Prop_1
       WHERE p.Namespace=? AND p.Name=?
         AND NOT EXISTS (SELECT 1 FROM bis_ElementMultiAspect WHERE Id=p.Id)
         AND NOT EXISTS (SELECT 1 FROM bis_ElementUniqueAspect WHERE Id=p.Id)`
    ))
      editTxn.deleteFileProperty(elementAspectRecordProperty(targetAspectId));
  }

  /** Deletes every record. A target copied from the source inherits the source's own records, which describe the source's upstream imports, not this source. */
  public clear(): void {
    if (this._tracked === undefined) return;
    const { editTxn } = this._tracked;
    for (const targetAspectId of selectIds(
      editTxn,
      "SELECT Id FROM be_Prop INDEXED BY sqlite_autoindex_be_Prop_1 WHERE Namespace=? AND Name=?"
    ))
      editTxn.deleteFileProperty(elementAspectRecordProperty(targetAspectId));
  }
}

const untrackedOwner: OwnerAspectOwnership = {
  foreignAspectIds: new Set(),
  isOwned: () => false,
  ownedTarget: () => undefined,
  record: () => {},
};

class OwnerView implements OwnerAspectOwnership {
  public readonly foreignAspectIds = new Set<Id64String>();
  private readonly _ownedTargets = new Map<Id64String, Id64String>();
  /** Records to write, by target aspect ID. */
  public readonly pending = new Map<Id64String, AspectRecord>();

  public constructor(
    private readonly _sourceIModelId: GuidString,
    private readonly _records: Map<Id64String, AspectRecord>
  ) {
    for (const [targetAspectId, record] of _records) {
      if (record.sourceIModel === _sourceIModelId)
        this._ownedTargets.set(record.sourceAspect, targetAspectId);
      else this.foreignAspectIds.add(targetAspectId);
    }
  }

  public isOwned(targetAspectId: Id64String): boolean {
    return (
      this._records.get(targetAspectId)?.sourceIModel === this._sourceIModelId
    );
  }

  public ownedTarget(sourceAspectId: Id64String): Id64String | undefined {
    return this._ownedTargets.get(sourceAspectId);
  }

  public record(sourceAspectId: Id64String, targetAspectId: Id64String): void {
    if (this._ownedTargets.get(sourceAspectId) === targetAspectId) return;
    const record = {
      sourceIModel: this._sourceIModelId,
      sourceAspect: sourceAspectId,
    };
    this._records.set(targetAspectId, record);
    this._ownedTargets.set(sourceAspectId, targetAspectId);
    this.pending.set(targetAspectId, record);
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
