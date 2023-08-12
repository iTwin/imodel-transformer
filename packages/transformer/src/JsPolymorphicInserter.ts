import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult } from "@itwin/core-bentley";
import { EntityProps } from "@itwin/core-common";
import * as assert from "assert";

/**
 * Create a polymorphic insert query for a given db,
 * by expanding its class hiearchy into a giant case statement and using JSON_Extract
 */
function createPolymorphicEntityInsertQueryMap(db: IModelDb): Map<string, string> {
  const classFullNameAndProps = db.withStatement(`
    SELECT c.ECInstanceId, p.Name
    FROM ECDbMeta.ECClassDef c
    JOIN ECDbMeta.ECPropertyDef p ON c.ECInstanceId=p.Class.Id
  `, (s) => {
    const result = new Map<string, string[]>();

    while (s.step() === DbResult.BE_SQLITE_ROW) {
      const classFullName = s.getValue(0).getClassNameForClassId();
      let properties = result.get(classFullName);
      if (properties === undefined) {
        properties = [];
        result.set(classFullName, properties);
      }
      const propertyName = s.getValue(1).getString();
      properties.push(propertyName);
    }

    return result;
  });

  const queryMap = new Map<string, string>();

  for (const [classFullName, properties] of classFullNameAndProps) {
    queryMap.set(classFullName, `
      INSERT INTO ${classFullName}
      (${properties.join(",")})
      VALUES
      (${properties.map((p) => `JSON_EXTRACT(:x, '$.${p}')`).join(",")})
    `);
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
export function doEmulatedPolymorphicEntityInsert(
  db: IModelDb,
  /** if is a map, must be presorted */
  data: RawPolymorphicRow[] | Map<string, RawPolymorphicRow[]>,
) {
  const queryMap  = createPolymorphicEntityInsertQueryMap(db);

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

export function rawEmulatedPolymorphicInsertTransform(source: IModelDb, target: IModelDb) {
  const queryMap = createPolymorphicEntityInsertQueryMap(target);

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const writeableTarget = new ECDb();
  writeableTarget.openDb(target.pathName, ECDbOpenMode.ReadWrite);
  target.close();

  source.withPreparedStatement(`
    SELECT $, ECClassId
    FROM bis.Element
    -- is sorting this slow (index?)... prevents thrashing the stmt cache tho...
    ORDER BY ECInstanceId ASC, ECClassId
  `, (sourceStmt) => {
    while (sourceStmt.step() === DbResult.BE_SQLITE_ROW) {
      const jsonString = sourceStmt.getValue(0).getString();
      const classFullName = sourceStmt.getValue(1).getClassNameForClassId();
      const query = queryMap.get(classFullName);
      assert(query, `couldn't find query for class '${classFullName}`);
      writeableTarget.withPreparedStatement(query, (targetStmt) => {
        targetStmt.bindString("x", jsonString);
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
      });
    }
  });

  writeableTarget.dispose();
}
