/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { SQLiteDb, SqliteStatement } from "@itwin/core-backend";

/** Closes databases whose owning table was garbage collected without being disposed. Letting the native database and
 * its cached statements be finalized independently can crash the process, so the registry keeps the database alive
 * until it can be closed in order.
 */
const undisposedDbs = new FinalizationRegistry<SQLiteDb>((db) => db.closeDb());

/** Stores source-to-target ElementAspect id mappings in a private, temporary on-disk SQLite database.
 * SQLite keeps recently used pages in its page cache and spills the rest to a temporary file, so memory use stays
 * bounded regardless of how many aspects a transformation remaps. The database is created on first write and deleted
 * by SQLite when it is closed.
 * @see https://github.com/iTwin/imodel-transformer/issues/83
 * @internal
 */
export class AspectRemapTable implements Disposable {
  private static readonly _insertSql =
    "INSERT OR REPLACE INTO AspectRemap(SourceId,TargetId) VALUES(?,?)";
  private static readonly _selectSql =
    "SELECT TargetId FROM AspectRemap WHERE SourceId=?";
  private static readonly _deleteSql =
    "DELETE FROM AspectRemap WHERE SourceId=?";

  private _db: SQLiteDb | undefined;

  private openDb(): SQLiteDb {
    if (this._db !== undefined) return this._db;
    const db = new SQLiteDb();
    // An empty file name asks SQLite for a private temporary on-disk database that is deleted when closed.
    db.createDb("");
    try {
      db.executeSQL("PRAGMA journal_mode=OFF");
      db.executeSQL(
        "CREATE TABLE AspectRemap(SourceId INTEGER PRIMARY KEY, TargetId INTEGER NOT NULL)"
      );
    } catch (err) {
      db.closeDb();
      throw err;
    }
    undisposedDbs.register(this, db, this);
    this._db = db;
    return db;
  }

  /** Map `sourceId` to `targetId`, replacing any existing mapping. An invalid `targetId` removes the mapping. */
  public set(sourceId: Id64String, targetId: Id64String): void {
    if (!Id64.isValidId64(targetId)) {
      this.delete(sourceId);
      return;
    }
    if (!Id64.isValidId64(sourceId)) return;
    this.step(this.openDb(), AspectRemapTable._insertSql, (stmt) => {
      stmt.bindId(1, sourceId);
      stmt.bindId(2, targetId);
    });
  }

  /** Remove the mapping for `sourceId`, if any. */
  public delete(sourceId: Id64String): void {
    if (this._db === undefined || !Id64.isValidId64(sourceId)) return;
    this.step(this._db, AspectRemapTable._deleteSql, (stmt) =>
      stmt.bindId(1, sourceId)
    );
  }

  /** @returns the target id mapped from `sourceId`, or [Id64.invalid]($bentley) if there is no mapping. */
  public get(sourceId: Id64String): Id64String {
    if (this._db === undefined || !Id64.isValidId64(sourceId))
      return Id64.invalid;
    return this.step(
      this._db,
      AspectRemapTable._selectSql,
      (stmt) => stmt.bindId(1, sourceId),
      (stmt) => stmt.getValueId(0)
    );
  }

  private step(
    db: SQLiteDb,
    sql: string,
    bind: (stmt: SqliteStatement) => void,
    readRow?: (stmt: SqliteStatement) => Id64String
  ): Id64String {
    return db.withPreparedSqliteStatement(sql, (stmt) => {
      try {
        bind(stmt);
        const result = stmt.step();
        if (result === DbResult.BE_SQLITE_ROW && readRow !== undefined)
          return readRow(stmt);
        if (result !== DbResult.BE_SQLITE_DONE)
          throw new Error(`Aspect remap table statement failed (${result})`);
        return Id64.invalid;
      } finally {
        stmt.reset();
      }
    });
  }

  /** Close and delete the temporary database, discarding all mappings. A later [[set]] starts a new, empty database. */
  public [Symbol.dispose](): void {
    if (this._db === undefined) return;
    const db = this._db;
    this._db = undefined;
    undisposedDbs.unregister(this);
    db.closeDb();
  }
}
