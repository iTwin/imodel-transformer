import { SQLiteDb } from "@itwin/core-backend";
import { Id64, Id64String } from "@itwin/core-bentley";
import { ChangeOpCode, DbResult } from "@itwin/core-common";

export enum EntityKind {
  /** Indicates that the [[LastEntity]] is storing [[Element]] entity */
  Element = "Element",
  /** Indicates that the [[LastEntity]] is storing [[Relationship]] entity */
  Relationship = "Relationship",
}

export interface LastElementArgs {
  sourceEntityECInstanceId: Id64String;
  targetEntityECInstanceId: Id64String;
  entityKind: EntityKind.Element;
  operationCode: ChangeOpCode;
}

export interface LastRelationshipArgs {
  sourceEntityECInstanceId: Id64String;
  targetEntityECInstanceId: Id64String;
  entityClassFullName: string;
  entityKind: EntityKind.Relationship;
  operationCode: ChangeOpCode;
}

export type LastEntityArgs = LastElementArgs | LastRelationshipArgs;

export class LastEntity {
  public transformerVersion: string = "";
  public itwinJsVersion: string = "";
  public sourceEntityECInstanceId: string = Id64.invalid;
  public targetEntityECInstanceId: string = Id64.invalid;
  public entityClassFullName: string = "";
  public operationCode: ChangeOpCode | undefined = undefined;
  public entityKind: EntityKind | undefined = undefined;

  public static readonly lastEntityInfoTable = "LastEntityInfoTable";

  public isNull(): boolean {
    return this.sourceEntityECInstanceId === Id64.invalid && this.targetEntityECInstanceId === Id64.invalid;
  }

  public markLastEntity(args: LastEntityArgs): void {
    this.sourceEntityECInstanceId = args.sourceEntityECInstanceId;
    this.targetEntityECInstanceId = args.targetEntityECInstanceId;
    this.entityClassFullName = "entityClassFullName" in args ? args.entityClassFullName : "";
    this.entityKind = args.entityKind;
    this.operationCode = args.operationCode;
  }

  public loadStateFromDb(db: SQLiteDb): void {
    const selectQuery = `
    SELECT 
      transformerVersion, 
      itwinJsVersion, 
      sourceEntityECInstanceId, 
      targetEntityECInstanceId, 
      entityClassFullName, 
      entityKind, 
      operationCode 
    FROM ${LastEntity.lastEntityInfoTable}`;

    db.withSqliteStatement(selectQuery,
      (stmt) => {
        if (DbResult.BE_SQLITE_ROW !== stmt.step()) {
          throw Error("expected row when getting lastProvenanceEntityId from target state table");
        }
        this.transformerVersion = stmt.getValueString(0);
        this.itwinJsVersion = stmt.getValueString(1);
        this.sourceEntityECInstanceId = stmt.getValueString(2);
        this.targetEntityECInstanceId = stmt.getValueString(3);
        this.entityClassFullName = stmt.getValueString(4);
        this.entityKind = this._getEntityKindFromString(stmt.getValueString(5));
        this.operationCode = this._getOperationCodeFromNumber(stmt.getValueInteger(6));
        // TODO: remove return by adding while loop and LIMIT 1 to SQL statement
        return;
      }
    );
  }

  public saveStateToDb(db: SQLiteDb): void {
    this._createDbTable(db);

    db.withSqliteStatement(
      `INSERT INTO ${LastEntity.lastEntityInfoTable} (
        transformerVersion, 
        itwinJsVersion, 
        sourceEntityECInstanceId, 
        targetEntityECInstanceId, 
        entityClassFullName, 
        entityKind, 
        operationCode
      ) VALUES (?,?,?,?,?,?,?)`,
      (stmt) => {
        stmt.bindString(1, this.transformerVersion);
        stmt.bindString(2, this.itwinJsVersion);
        stmt.bindString(3, this.sourceEntityECInstanceId);
        stmt.bindString(4, this.targetEntityECInstanceId);
        stmt.bindString(5, this.entityClassFullName);
        stmt.bindString(6, this.entityKind ?? "");
        stmt.bindInteger(7, this.operationCode ?? 0);
        if (DbResult.BE_SQLITE_DONE !== stmt.step())
          throw Error("Failed to insert options into the state database");
      });

    db.saveChanges();
  }

  private _createDbTable(db: SQLiteDb): void {
    const tableCreationSql = `
    CREATE TABLE ${LastEntity.lastEntityInfoTable} (
      transformerVersion TEXT,
      itwinJsVersion TEXT,
      sourceEntityECInstanceId TEXT,
      targetEntityECInstanceId TEXT,
      entityClassFullName TEXT,
      entityKind TEXT,
      operationCode INTEGER
    )`;

    if (DbResult.BE_SQLITE_DONE !== db.executeSQL(tableCreationSql)) {
      throw Error("Failed to create the target state table in the state database");
    }

    db.saveChanges();
  }
  
  private _getEntityKindFromString(entityKind: string): EntityKind | undefined {
    switch (entityKind) {
      case EntityKind.Element:
        return EntityKind.Element;
      case EntityKind.Relationship:
        return EntityKind.Relationship;
      default:
        return undefined;      
    }
  }

  private _getOperationCodeFromNumber(operationCode: number): ChangeOpCode | undefined {
    switch (operationCode) {
      case ChangeOpCode.Insert:
        return ChangeOpCode.Insert;
      case ChangeOpCode.Update:
        return ChangeOpCode.Update;
      case ChangeOpCode.Delete:
        return ChangeOpCode.Delete;
      default:
        return undefined;
    }
  }
}