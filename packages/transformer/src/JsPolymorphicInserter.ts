import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { EntityProps } from "@itwin/core-common";
import { PropertyType, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import { IModelTransformer } from "./IModelTransformer";

interface PropInfo {
  name: string;
  type: PropertyType;
  isReadOnly: boolean;
}

/**
 * Create a polymorphic insert query for a given db,
 * by expanding its class hiearchy into a giant case statement and using JSON_Extract
 */
async function createPolymorphicEntityInsertQueryMap(db: IModelDb, update = false): Promise<Map<string, string>> {
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

        classProps.push({ name: prop.name, type: prop.propertyType, isReadOnly: prop.isReadOnly });
      }

      classFullNameAndProps.set(ecclass.fullName, classProps);
    }
  }

  const queryMap = new Map<string, string>();

  // sqlite cast doesn't understand hexadecimal strings so can't use this
  // FIXME: custom sql function will be wayyyy better than this
  // ? `CAST(JSON_EXTRACT(:x, '$.${p.name}.Id') AS INTEGER)`SELECT
  const readNavPropFromJson = (p: PropInfo) => `(
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
  )`;

  for (const [classFullName, properties] of classFullNameAndProps) {
    /* eslint-disable @typescript-eslint/indent */
    const query = update
      ? `
        UPDATE ${classFullName}
        SET ${
          properties
            .filter((p) => !p.isReadOnly)
            .map((p) => `${
                p.type === PropertyType.Navigation ? `${p.name}.Id` : p.name
              } = ${
                p.type === PropertyType.Navigation
                  ? readNavPropFromJson(p)
                  : `JSON_EXTRACT(:x, '$.${p.name}')`
              }`)
            .join(",\n  ")
        }
      WHERE ECInstanceId=(
        SELECT TargetId
        FROM temp.element_remap
        WHERE SourceId=JSON_EXTRACT(:x, '$.ECInstanceId')
      )
      ` : `
        INSERT INTO ${classFullName}
        (${properties
          .map((p) =>
            p.type === PropertyType.Navigation
            ? `${p.name}.Id`
            : p.type === PropertyType.Point2d
            ? `${p.name}.x, ${p.name}.y`
            : p.type === PropertyType.Point3d
            ? `${p.name}.x, ${p.name}.y, ${p.name}.z`
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
            ? "0x1"
            : p.type === PropertyType.Point2d
            ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`
            : p.type === PropertyType.Point3d
            ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y'), JSON_EXTRACT(:x, '$.${p.name}.z')`
            : `JSON_EXTRACT(:x, '$.${p.name}')`
          )
          .join(",\n  ")
        })
      `;

    queryMap.set(classFullName, query);
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
  const schemaExporter = new IModelTransformer(source, target);
  await schemaExporter.processSchemas();
  schemaExporter.dispose();

  const insertQueryMap = await createPolymorphicEntityInsertQueryMap(target);
  const updateQueryMap = await createPolymorphicEntityInsertQueryMap(target, true);

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const writeableTarget = new ECDb();
  writeableTarget.openDb(target.pathName, ECDbOpenMode.ReadWrite);
  target.close();

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

  // FIXME: reinstate triggers after by caching them from sqlite_master
  const triggers = writeableTarget.withPreparedSqliteStatement(`
    SELECT name, sql FROM sqlite_master
    WHERE type='trigger'
  `, (s) => {
    const result = new Map<string, string>();
    while(s.step() === DbResult.BE_SQLITE_ROW) {
      const triggerName = s.getValue(0).getString();
      const sql = s.getValue(1).getString();
      result.set(triggerName, sql);
    }
    return result;
  });

  for (const [trigger] of triggers) {
    writeableTarget.withSqliteStatement(`
      DROP TRIGGER ${trigger}
    `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));
  }

  // remove (unique constraint) violations
  // NOTE: most (FK constraint) violation removal is done by using the !update (default)
  // option in @see createPolymorphicEntityInsertQueryMap
  function unviolate(jsonString: string): string {
    const parsed = JSON.parse(jsonString);
    parsed._CodeValue = parsed.CodeValue; // for debugging
    delete parsed.CodeValue;
    return JSON.stringify(parsed);
  }

  const sourcePolymorphicSelect = `
    SELECT $, ECClassId, ECInstanceId
    FROM bis.Element
    WHERE ECInstanceId NOT IN (0x1, 0xe, 0x10) 
    -- FIXME: would be much faster to temporarily disable FK constraints
    -- FIXME: ordering by class *might* be faster due to less cache busting
    -- ORDER BY ECClassId, ECInstanceId ASC
    ORDER BY ECInstanceId ASC
  `;

  // first pass, update everything with trivial references (0x1 and null codes)
  source.withPreparedStatement(sourcePolymorphicSelect, (sourceStmt) => {
    while (sourceStmt.step() === DbResult.BE_SQLITE_ROW) {
      const jsonString = sourceStmt.getValue(0).getString();
      const classFullName = sourceStmt.getValue(1).getClassNameForClassId();
      const sourceId = sourceStmt.getValue(2).getId();

      const insertQuery = insertQueryMap.get(classFullName);
      assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

      const transformed = unviolate(jsonString);

      writeableTarget.withPreparedStatement(insertQuery, (targetStmt) => {
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
    }
  });

  // second pass, update now that everything has been inserted
  source.withPreparedStatement(sourcePolymorphicSelect, (sourceStmt) => {
    while (sourceStmt.step() === DbResult.BE_SQLITE_ROW) {
      const jsonString = sourceStmt.getValue(0).getString();
      const classFullName = sourceStmt.getValue(1).getClassNameForClassId();
      const sourceId = sourceStmt.getValue(2).getId();

      const updateQuery = updateQueryMap.get(classFullName);
      assert(updateQuery, `couldn't find update query for class '${classFullName}`);

      let nativeSql!: string;
      try {
        writeableTarget.withPreparedStatement(updateQuery, (targetStmt) => {
          nativeSql = targetStmt.getNativeSql();
          targetStmt.bindString("x", jsonString);
          assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
        });
      } catch (err) {
        console.log("SOURCE", source.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${sourceId}`, s=>[...s]));
        console.log("ERROR", writeableTarget.nativeDb.getLastError());

        const targetPath = writeableTarget.nativeDb.getFilePath();
        writeableTarget.saveChanges();
        writeableTarget.closeDb();
        require("fs").copyFileSync(targetPath, "/tmp/out.db");

        console.log("transformed:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
        //console.log("SCOPE", writeableTarget.withStatement(`SELECT * FROM bis.Element WHERE ECInstanceId=${JSON.parse(transformed).CodeScope?.Id ?? 0}`, s=>[...s]));
        //console.log("SPEC", writeableTarget.withStatement(`SELECT * FROM bis.CodeSpec WHERE ECInstanceId=${JSON.parse(transformed).CodeSpec?.Id ?? 0}`, s=>[...s]));
        //console.log("PARENT", writeableTarget.withStatement(`SELECT * FROM bis.Element WHERE ECInstanceId=${JSON.parse(transformed).Parent?.Id ?? 0}`, s=>[...s]));
        //console.log("MODEL", writeableTarget.withStatement(`SELECT * FROM bis.Model WHERE ECInstanceId=${JSON.parse(transformed).Model?.Id ?? 0}`, s=>[...s]));
        console.log("query:", updateQuery);
        console.log("native sql:", nativeSql);
        throw err;
      }
    }
  });

  writeableTarget.withPreparedSqliteStatement(`
    PRAGMA defer_foreign_keys_pragma = false;
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  // FIXME: this is a hack! need to recalculate entire spatial index after this... probably better
  // to just modify the native end to allow writes?
  for (const [, triggerSql] of triggers) {
    writeableTarget.withSqliteStatement(triggerSql, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));
  }

  writeableTarget.saveChanges();
  writeableTarget.dispose();
}
