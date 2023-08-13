import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { EntityProps } from "@itwin/core-common";
import { Property, PropertyType, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";

interface PropInfo {
  name: string;
  type: PropertyType;
}

/**
 * Create a polymorphic insert query for a given db,
 * by expanding its class hiearchy into a giant case statement and using JSON_Extract
 */
async function createPolymorphicEntityInsertQueryMap(db: IModelDb): Promise<Map<string, string>> {
  const schemaNames = db.withPreparedStatement(
    "SELECT Name FROM ECDbMeta.ECSchemaDef",
    (stmt) => {
      const result: string[] = [];
      while (stmt.step() === DbResult.BE_SQLITE_ROW)
        result.push(stmt.getValue(0).getString());
      return result;
    }
  );

  const schemaLoader = new SchemaLoader((name: string) => db.getSchemaProps(name));
  const classFullNameAndProps = new Map<string, PropInfo[]>();

  for (const schemaName of schemaNames) {
    const schema = schemaLoader.getSchema(schemaName);
    for (const ecclass of schema.getClasses()) {
      const classProps: PropInfo[] = [];

      // const testExcludedProps = new Set(["LastMod"]);

      for (const prop of await ecclass.getProperties()) {
        // FIXME: exceptions should be replaced with transformations
        // if (testExcludedProps.has(prop.name))
        //   continue;

        classProps.push({ name: prop.name, type: prop.propertyType });
      }

      classFullNameAndProps.set(ecclass.fullName, classProps);
    }
  }

  const queryMap = new Map<string, string>();

  for (const [classFullName, properties] of classFullNameAndProps) {
    /* eslint-disable @typescript-eslint/indent */
    queryMap.set(classFullName, `
      INSERT INTO ${classFullName}
      (${properties
        .map((p) =>
          p.type === PropertyType.Navigation
          ? `${p.name}.Id`
          // : p.type === PropertyType.DateTime
          // ? `${p.name}.Id`
          : p.name
        )
        .join(",\n  ")
      })
      VALUES
      (${properties
        .map((p) =>
          p.type === PropertyType.Navigation
          // sqlite cast doesn't understand hexadecimal strings so can't use this
          // FIXME: custom sql function will be wayyyy better than this
          // ? `CAST(JSON_EXTRACT(:x, '$.${p.name}.Id') AS INTEGER)`SELECT
          ? `(
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -1, 1)) << 0) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -2, 1)) << 4) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -3, 1)) << 8) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -4, 1)) << 12) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -5, 1)) << 16) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -6, 1)) << 20) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -7, 1)) << 24) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -8, 1)) << 28) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -9, 1)) << 32) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -10, 1)) << 36) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -11, 1)) << 40) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -12, 1)) << 44) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -13, 1)) << 48) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -14, 1)) << 52) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -15, 1)) << 56) |
          (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}.Id')), -16, 1)) << 60)
          )`
          : `JSON_EXTRACT(:x, '$.${p.name}')`
        )
        .join(",\n  ")
      })
    `);
    /* eslint-enable @typescript-eslint/indent */
  }

  return queryMap;
}

function batchDataByClass(data: RawPolymorphicRow[]) {
  const result = new Map<string, RawPolymorphicRow[]>();
  for (const row of data) {
    let subrows = result.get(row.parsed.classFullName);
    if (subrows === undefined) {
      subrows = [];
      result.set(row.parsed.classFullName, subrows);
    }
    subrows.push(row);
  }

  return result;
}

export interface RawPolymorphicRow {
  parsed: EntityProps;
  jsonString: string;
}

// FIXME: replace entirely with ec_classname and ec_classid builtin functions
function sourceToTargetClassIds(source: IModelDb, target: IModelDb) {
  const makeClassNameToIdMap = (db: IModelDb) => db.withPreparedStatement(
    "SELECT ECInstanceId FROM ECDbMeta.ECClassDef",
    (stmt) => {
      const result = new Map<string, Id64String>();
      while (stmt.step() === DbResult.BE_SQLITE_ROW) {
        const classId = stmt.getValue(0).getId();
        const classFullName = stmt.getValue(0).getClassNameForClassId();
        result.set(classFullName, classId);
      }
      return result;
    }
  );
  // FIXME: use attached target
  const sourceClassNameToId = makeClassNameToIdMap(source);
  const targetClassNameToId = makeClassNameToIdMap(target);

  const result = new Map<Id64String, Id64String>();

  for (const [classFullName, sourceClassId] of sourceClassNameToId) {
    const targetClassId = targetClassNameToId.get(classFullName);
    if (targetClassId === undefined)
      continue;
    result.set(sourceClassId, targetClassId);
  }

  return result;
}

export async function rawEmulatedPolymorphicInsertTransform(source: IModelDb, target: IModelDb) {
  const queryMap = await createPolymorphicEntityInsertQueryMap(target);

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const writeableTarget = new ECDb();
  writeableTarget.openDb(target.pathName, ECDbOpenMode.ReadWrite);
  target.close();

  // FIXME: I think federation guid needs to be blobbified
  // TODO: do this in a sqlite function that executes javascript (possibly from a path)
  function transformJson(jsonString: string): string {
    const json = JSON.parse(jsonString);
    const sourceClassId = json.ECClassId;
    //json.ECClassId = classIdMap.get(sourceClassId);
    assert(json.ECClassId, `couldn't remap class with id: ${sourceClassId}`);
    return JSON.stringify(json);
  }

  // FIXME
  writeableTarget.withPreparedSqliteStatement(`
    CREATE TEMP TABLE temp.element_remap(
      SourceId INTEGER PRIMARY KEY, -- create index
      TargetId INTEGER
    )
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  // FIXME: this doesn't work... using a workaround of setting all references to 0x1
  writeableTarget.withPreparedSqliteStatement(`
    PRAGMA defer_foreign_keys_pragma = true;
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));


  source.withPreparedStatement(`
    SELECT $, ECClassId, ECInstanceId
    FROM bis.Element
    WHERE ECInstanceId NOT IN (0x1, 0xe, 0x10) 
    -- FIXME: would be much faster to temporarily disable FK constraints
    -- FIXME: ordering by class *might* be faster due to less cache busting
    -- ORDER BY ECClassId, ECInstanceId ASC
    ORDER BY ECInstanceId ASC
  `, (sourceStmt) => {
    while (sourceStmt.step() === DbResult.BE_SQLITE_ROW) {
      const jsonString = sourceStmt.getValue(0).getString();
      const classFullName = sourceStmt.getValue(1).getClassNameForClassId();
      const sourceId = sourceStmt.getValue(2).getId();

      const query = queryMap.get(classFullName);
      assert(query, `couldn't find query for class '${classFullName}`);

      const transformed = transformJson(jsonString);

      let nativeSql;

      try {
        // must insert things in id order... maybe need two passes?
        writeableTarget.withPreparedStatement(query, (targetStmt) => {
          nativeSql = targetStmt.getNativeSql();
          targetStmt.bindString("x", transformed);
          assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
        });
        writeableTarget.withSqliteStatement(`
          INSERT INTO temp.element_remap
          SELECT ?, Val FROM be_Local WHERE Name='bis_elementidsequence'
        `, (targetStmt) => {
          targetStmt.bindId(1, sourceId);
          assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
        });
      } catch (err) {
        console.log("ERROR", writeableTarget.nativeDb.getLastError());

        writeableTarget.saveChanges();
        require("fs").copyFileSync(writeableTarget.nativeDb.getFilePath(), "/tmp/out.db");

        console.log("SCOPE", writeableTarget.withStatement(`SELECT * FROM bis.Element WHERE CodeScope.Id=${JSON.parse(transformed).CodeScope.Id}`, s=>[...s]));
        console.log("SPEC", writeableTarget.withStatement(`SELECT * FROM bis.CodeSpec WHERE ECInstanceId=${JSON.parse(transformed).CodeSpec.Id}`, s=>[...s]));
        console.log("PARENT", writeableTarget.withStatement(`SELECT * FROM bis.Element WHERE ECInstanceId=${JSON.parse(transformed).Parent.Id}`, s=>[...s]));
        console.log("MODEL", writeableTarget.withStatement(`SELECT * FROM bis.Model WHERE ECInstanceId=${JSON.parse(transformed).Model.Id}`, s=>[...s]));
        // console.log("query:", query);
        // console.log("original:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
        console.log("transformed:", JSON.stringify(JSON.parse(transformed), undefined, " "));
        // console.log("native sql:", nativeSql);
        throw err;
      }
    }
  });

  writeableTarget.withPreparedSqliteStatement(`
    PRAGMA defer_foreign_keys_pragma = false;
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  writeableTarget.saveChanges();
  writeableTarget.dispose();
}
