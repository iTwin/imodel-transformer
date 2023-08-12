import { IModelDb } from "@itwin/core-backend";
import { DbResult } from "@itwin/core-bentley";
import { EntityProps } from "@itwin/core-common";
import * as assert from "assert";

/**
 * Create a polymorphic insert query for a given db,
 * by expanding its class hiearchy into a giant case statement and using JSON_Extract
 */
function createPolymorphicEntityInsertQueryMap(db: IModelDb): Map<string, string> {
  const classFullNameAndProps = db.withStatement(`
    SELECT c.Id, p.Name
    FROM ECDbMeta.ECClassDef c
    JOIN ECDbMeta.ECPropertyDef p ON c.ECInstanceId=p.ClassId
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
      (${properties.map((p) => `?1->'$.${p}'`).join(",")})
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

interface RawPolymorphicRow {
  parsed: EntityProps;
  jsonString: string;
}

/**
 * Insert data into the db regardless of class. This "emulates" an actual polymorphic insert
 * feature, which does not (yet) exist in ecdb
 */
export function doEmulatedPolymorphicEntityInsert(db: IModelDb, data: RawPolymorphicRow[]) {
  const queryMap  = createPolymorphicEntityInsertQueryMap(db);

  const perClassInsertData = batchDataByClass(data);

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
