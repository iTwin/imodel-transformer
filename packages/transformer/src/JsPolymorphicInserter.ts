import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { EntityProps } from "@itwin/core-common";
import { PropertyType, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import { IModelTransformer } from "./IModelTransformer";

interface PropInfo {
  name: string;
  type: PropertyType;
  isReadOnly: boolean;
}

// some high entropy string
const injectionString = "SomeHighEntropyString_1243yu1";
const injectExpr = (s: string) => `(SELECT '${injectionString} ${escapeForSqlStr(s)}')`;

const escapeForSqlStr = (s: string) => s.replace(/'/g, "''");
const unescapeSqlStr = (s: string) => s.replace(/''/g, "'");

/**
 * Create a polymorphic insert query for a given db,
 * by expanding its class hiearchy into a giant case statement and using JSON_Extract
 */
async function createPolymorphicEntityInsertQueryMap(db: IModelDb, type: "insert-norefs" | "insert" | "update"): Promise<Map<string, string>> {
  const schemaNamesReader = db.createQueryReader("SELECT Name FROM ECDbMeta.ECSchemaDef", undefined, { usePrimaryConn: true });

  const schemaNames: string[] = [];
  while (await schemaNamesReader.step()) {
    schemaNames.push(schemaNamesReader.current[0]);
  }

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
  const readHexFromJson = (p: PropInfo) => {
    const navProp = p.type === PropertyType.Navigation;
    return `(
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -1, 1)) << 0) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -2, 1)) << 4) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -3, 1)) << 8) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -4, 1)) << 12) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -5, 1)) << 16) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -6, 1)) << 20) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -7, 1)) << 24) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -8, 1)) << 28) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -9, 1)) << 32) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -10, 1)) << 36) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -11, 1)) << 40) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -12, 1)) << 44) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -13, 1)) << 48) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -14, 1)) << 52) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -15, 1)) << 56) |
      (instr('123456789abcdef', substr('0000000000000000' || lower(JSON_EXTRACT(:x, '$.${p.name}${navProp ? ".Id" : ""}')), -16, 1)) << 60)
    )`;
  };

  for (const [classFullName, properties] of classFullNameAndProps) {
    /* eslint-disable @typescript-eslint/indent */
    const query
      = type === "update"
      ? `
        UPDATE ${classFullName}
        SET ${
          properties
            .filter((p) => !p.isReadOnly && p.type === PropertyType.Navigation || p.name === "CodeValue")
            .map((p) =>
              p.type === PropertyType.Navigation && p.name === "CodeSpec"
              ? `${p.name}.Id = ${injectExpr(`(
                SELECT TargetId
                FROM temp.codespec_remap
                WHERE SourceId=${readHexFromJson(p)}
              )`)}`
              : p.type === PropertyType.Navigation && p.name
              ? `${p.name}.Id = ${injectExpr(`(
                SELECT TargetId
                FROM temp.element_remap
                WHERE SourceId=${readHexFromJson(p)}
              )`)}`
              // is CodeValue if not nav prop
              : `${p.name} = JSON_EXTRACT(:x, '$.CodeValue')`
            )
            .join(",\n  ")
        }
      WHERE ECInstanceId=${injectExpr(`(
        SELECT TargetId
        FROM temp.element_remap
        WHERE SourceId=${readHexFromJson({ name: "ECInstanceId", type: PropertyType.Long, isReadOnly: false })}
      )`)}`
      : type === "insert-norefs"
      ? `
        INSERT INTO ${classFullName}
        (${properties
          .map((p) =>
            // FIXME: note that dynamic structs are completely unhandled
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
      `
      : `
        INSERT INTO ${classFullName}
        (${properties
          .map((p) =>
            // FIXME: note that dynamic structs are completely unhandled
            p.type === PropertyType.Navigation
            ? `${p.name}.Id, ${p.name}.RelECClassId`
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
            // FIXME: need to use ECReferenceCache to get reference type of this prop
            ? `${injectExpr(`(
              SELECT TargetId
              FROM temp.element_remap
              WHERE SourceId=${readHexFromJson(p)}
            )`)}, ${injectExpr(`(
              SELECT c.Id
              FROM source.ec_Class c
              JOIN source.ec_Schema s ON s.Id=c.SchemaId
              WHERE Name=${readHexFromJson(p)}
            )`)}`
            : p.type === PropertyType.Point2d
            ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`
            : p.type === PropertyType.Point3d
            ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y'), JSON_EXTRACT(:x, '$.${p.name}.z')`
            : `JSON_EXTRACT(:x, '$.${p.name}')`
          )
          .join(",\n  ")
        })
      `
    ;

    queryMap.set(classFullName, query);
    /* eslint-enable @typescript-eslint/indent */
  }

  return queryMap;
}

// FIXME: consolidate with assertIdentityTransform test, and maybe hide this return type
export interface Remapper {
  findTargetElementId(s: Id64String): Id64String;
  findTargetCodeSpecId(s: Id64String): Id64String;
  findTargetAspectId(s: Id64String): Id64String;
}

/** @alpha FIXME: official docs */
export async function rawEmulatedPolymorphicInsertTransform(source: IModelDb, target: IModelDb, options?: {
  returnRemapper?: false;
}): Promise<undefined>;
/** @internal */
export async function rawEmulatedPolymorphicInsertTransform(source: IModelDb, target: IModelDb, options?: {
  returnRemapper: true;
}): Promise<Remapper>;
export async function rawEmulatedPolymorphicInsertTransform(source: IModelDb, target: IModelDb, {
  returnRemapper = false,
} = {}): Promise<undefined | Remapper> {
  const schemaExporter = new IModelTransformer(source, target);
  await schemaExporter.processSchemas();
  schemaExporter.dispose();

  const insertQueryMap = await createPolymorphicEntityInsertQueryMap(target, "insert-norefs");
  const updateQueryMap = await createPolymorphicEntityInsertQueryMap(target, "update");

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const writeableTarget = new ECDb();
  writeableTarget.openDb(target.pathName, ECDbOpenMode.ReadWrite);
  target.close();

  for (const type of ["element", "codespec", "aspect"]) {
    writeableTarget.withSqliteStatement(`
      CREATE TEMP TABLE temp.${type}_remap(
        SourceId INTEGER NOT NULL PRIMARY KEY, -- do we need an index?
        TargetId INTEGER NOT NULL
      )
    `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));
  }

  writeableTarget.withPreparedSqliteStatement(`
    INSERT INTO temp.element_remap VALUES(0x1,0x1), (0xe,0xe), (0x10, 0x10)
  `, (targetStmt) => {
    assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
  });

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

  // tranform code specs
  const sourceCodeSpecSelect = `
    SELECT ECInstanceId, Name, JsonProperties
    FROM bis.CodeSpec
  `;

  const sourceCodeSpecReader = source.createQueryReader(sourceCodeSpecSelect, undefined, { usePrimaryConn: true });
  while (await sourceCodeSpecReader.step()) {
    const sourceId = sourceCodeSpecReader.current[0];
    const name = sourceCodeSpecReader.current[1];
    const jsonProps = sourceCodeSpecReader.current[2];

    // FIXME: use upsert but it doesn't seem to work :/
    let targetId: Id64String;
    try {
      targetId = writeableTarget.withPreparedStatement(`
        INSERT INTO bis.CodeSpec VALUES(?,?)
        -- ON CONFLICT (name) DO NOTHING
      `, (targetStmt) => {
        targetStmt.bindString(1, name);
        targetStmt.bindString(2, jsonProps);
        const result = targetStmt.stepForInsert();
        if (result.status !== DbResult.BE_SQLITE_DONE || !result.id) {
          const err = new Error(`Expected BE_SQLITE_DONE but got ${result.status}`);
          (err as any).result = result;
          throw err;
        }
        return result.id;
      }, false);
    } catch (err: any) {
      if (err?.result?.status !== DbResult.BE_SQLITE_CONSTRAINT_UNIQUE)
        throw err;

      targetId = writeableTarget.withPreparedStatement(
        "SELECT ECInstanceId FROM bis.CodeSpec WHERE Name=?",
        (targetStmt) => {
          targetStmt.bindString(1, name);
          assert(targetStmt.step() === DbResult.BE_SQLITE_ROW);
          return targetStmt.getValue(0).getId();
        }
      );
    }

    writeableTarget.withPreparedSqliteStatement(`
      INSERT INTO temp.codespec_remap VALUES(?,?)
    `, (targetStmt) => {
      targetStmt.bindId(1, sourceId);
      targetStmt.bindId(2, targetId);
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
    });
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

  const sourceElemSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.Element
    WHERE ECInstanceId NOT IN (0x1, 0xe, 0x10) 
    -- FIXME: would be much faster to temporarily disable FK constraints
    -- FIXME: ordering by class *might* be faster due to less cache busting
    -- ORDER BY ECClassId, ECInstanceId ASC
    ORDER BY ECInstanceId ASC
  `;

  // first pass, update everything with trivial references (0x1 and null codes)
  const sourceElemFirstPassReader = source.createQueryReader(sourceElemSelect, undefined, { usePrimaryConn: true });
  while (await sourceElemFirstPassReader.step()) {
    const jsonString = sourceElemFirstPassReader.current[0];
    const classFullName = sourceElemFirstPassReader.current[1];
    const sourceId = sourceElemFirstPassReader.current[2];

    const insertQuery = insertQueryMap.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}'`);

    const transformed = unviolate(jsonString);

    const targetId = writeableTarget.withPreparedStatement(insertQuery, (targetStmt) => {
      targetStmt.bindString("x", transformed);
      const result = targetStmt.stepForInsert();
      assert(result.status === DbResult.BE_SQLITE_DONE && result.id);
      return result.id;
    });

    writeableTarget.withPreparedSqliteStatement(`
      INSERT INTO temp.element_remap VALUES(?,?)
    `, (targetStmt) => {
      targetStmt.bindId(1, sourceId);
      targetStmt.bindId(2, targetId);
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
    });
  }

  // second pass, update now that everything has been inserted
  const sourceElemSecondPassReader = source.createQueryReader(sourceElemSelect, undefined, { usePrimaryConn: true });
  while (await sourceElemSecondPassReader.step()) {
    const jsonString = sourceElemSecondPassReader.current[0];
    const classFullName = sourceElemSecondPassReader.current[1];
    const sourceId = sourceElemSecondPassReader.current[2];

    const updateQuery = updateQueryMap.get(classFullName);
    assert(updateQuery, `couldn't find update query for class '${classFullName}`);

    // FIXME: move into updateQueryMap as a callback
    // HACK: create hybrid sqlite/ecsql query
    const hackedRemapUpdateSql = writeableTarget.withPreparedStatement(updateQuery, (targetStmt) => {
      const nativeSql = targetStmt.getNativeSql();
      return nativeSql.replace(
        new RegExp(`\\(SELECT '${injectionString} (.*?[^']('')*)'\\)`, "gs"),
        (_, p1) => unescapeSqlStr(p1),
      );
    });

    try {
      writeableTarget.withPreparedSqliteStatement(hackedRemapUpdateSql, (targetStmt) => {
        // can't use named bindings in raw sqlite statement apparently
        targetStmt.bindString(1, jsonString);
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
      });
    } catch (err) {
      console.log("SOURCE", source.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${sourceId}`, s=>[...s]));
      console.log("ERROR", writeableTarget.nativeDb.getLastError());
      console.log("transformed:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
      console.log("native sql:", hackedRemapUpdateSql);
      throw err;
    }
  }

  const sourceAspectSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.ElementAspect
  `;

  const aspectReader = source.createQueryReader(sourceAspectSelect, undefined, { usePrimaryConn: true });
  while (await aspectReader.step()) {
    const jsonString = aspectReader.current[0];
    const classFullName = aspectReader.current[1];
    const sourceId = aspectReader.current[2];

    const insertQuery = insertQueryMap.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);
    try {
      const targetId = writeableTarget.withPreparedStatement(insertQuery, (targetStmt) => {
        targetStmt.bindString("x", jsonString);
        const result = targetStmt.stepForInsert();
        assert(result.status === DbResult.BE_SQLITE_DONE && result.id);
        return result.id;
      });

      writeableTarget.withPreparedSqliteStatement(`
        INSERT INTO temp.aspect_remap VALUES(?,?)
      `, (targetStmt) => {
        targetStmt.bindId(1, sourceId);
        targetStmt.bindId(2, targetId);
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
      });
    } catch (err) {
      console.log("SOURCE", source.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${sourceId}`, s=>[...s]));
      console.log("ERROR", writeableTarget.nativeDb.getLastError());
      console.log("transformed:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
      console.log("native sql:", insertQuery);
      throw err;
    }
  }

  writeableTarget.withPreparedSqliteStatement(`
    PRAGMA defer_foreign_keys_pragma = false;
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  // FIXME: this is a hack! need to recalculate entire spatial index after this... probably better
  // to just modify the native end to allow writes?
  for (const [, triggerSql] of triggers) {
    writeableTarget.withSqliteStatement(triggerSql, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));
  }

  // TODO: make collecting/returning this optional
  let remapper: Remapper | undefined;

  if (returnRemapper) {
    const [elemRemaps, codeSpecRemaps, aspectRemaps] = ["codespec", "element", "aspect"].map((type) => {
      const remaps = new Map<string, string>();

      writeableTarget.withSqliteStatement(
        `SELECT format('0x%x', SourceId), format('0x%x', TargetId) FROM temp.${type}_remap`,
        (s) => {
          while (s.step() === DbResult.BE_SQLITE_ROW) {
            remaps.set(s.getValue(0).getString(), s.getValue(1).getString());
          }
        }
      );

      return remaps;
    });

    remapper = {
      findTargetElementId: (id: Id64String) => elemRemaps.get(id) ?? Id64.invalid,
      findTargetAspectId: (id: Id64String) => aspectRemaps.get(id) ?? Id64.invalid,
      findTargetCodeSpecId: (id: Id64String) => codeSpecRemaps.get(id) ?? Id64.invalid,
    };
  }

  writeableTarget.saveChanges();
  writeableTarget.closeDb();
  writeableTarget.dispose();

  return remapper;
}
