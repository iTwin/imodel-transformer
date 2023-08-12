import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult } from "@itwin/core-bentley";
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
          ? `CAST(JSON_EXTRACT(:x, '$.${p.name}.Id') AS INTEGER)`
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

/**
 * Insert data into the db regardless of class. This "emulates" an actual polymorphic insert
 * feature, which does not (yet) exist in ecdb
 * @param data - if is a map, must be keyed on classFullName in each row
 */
async function doEmulatedPolymorphicEntityInsert(
  db: IModelDb,
  /** if is a map, must be presorted */
  data: RawPolymorphicRow[] | Map<string, RawPolymorphicRow[]>,
) {
  const queryMap = await createPolymorphicEntityInsertQueryMap(db);

  const perClassInsertData = data instanceof Map ? data : batchDataByClass(data);

  for (const [classFullName, query] of queryMap) {
    const classInsertData = perClassInsertData.get(classFullName);
    assert(classInsertData);
    // FIXME: this assumes we can't do a single-class polymorphic select... should check, that would
    // be much much faster
    for (const { jsonString } of classInsertData) {
      db.withPreparedStatement(query, (s) => {
        s.bindString(1, jsonString);
        assert(s.step() === DbResult.BE_SQLITE_DONE);
      });
    }
  }
}

export async function rawEmulatedPolymorphicInsertTransform(source: IModelDb, target: IModelDb) {
  const queryMap = await createPolymorphicEntityInsertQueryMap(target);

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const writeableTarget = new ECDb();
  writeableTarget.openDb(target.pathName, ECDbOpenMode.ReadWrite);
  target.close();

  source.withPreparedStatement(`
    SELECT $, ECClassId
    FROM bis.Element
    -- is sorting this slow (index?)... prevents thrashing the stmt cache tho...
    -- would be much faster to temporarily disable FK constraints
    -- ORDER BY ECClassId, ECInstanceId ASC
    WHERE ECInstanceId NOT IN (0x1, 0xe, 0x10) 
    ORDER BY ECInstanceId ASC
  `, (sourceStmt) => {
    while (sourceStmt.step() === DbResult.BE_SQLITE_ROW) {
      const jsonString = sourceStmt.getValue(0).getString();
      const classFullName = sourceStmt.getValue(1).getClassNameForClassId();

      const query = queryMap.get(classFullName);
      assert(query, `couldn't find query for class '${classFullName}`);

      // must insert things in id order... maybe need two passes?
      console.log(query);
      console.log(JSON.stringify(JSON.parse(jsonString), undefined, " "));
      console.log();

      writeableTarget.withPreparedStatement(query, (targetStmt) => {
        targetStmt.bindString("x", jsonString);
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
      });
    }
  });

  writeableTarget.dispose();
}
