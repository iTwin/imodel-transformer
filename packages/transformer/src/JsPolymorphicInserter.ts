import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { PrimitiveOrEnumPropertyBase, Property, PropertyType, RelationshipClass, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import { IModelTransformer } from "./IModelTransformer";
import { QueryBinder } from "@itwin/core-common";

// NOTES:
// missing things:
// - arrays/struct properties
// - non-geometry binary

/* eslint-disable no-console, @itwin/no-internal */

// some high entropy string
const injectionString = "Inject_1243yu1";
const injectExpr = (s: string, type = "Integer") => `(CAST ((SELECT '${injectionString} ${escapeForSqlStr(s)}') AS ${type}))`;

const getInjectedSqlite = (query: string, db: ECDb | IModelDb) => {
  try {
    return db.withStatement(query, (stmt) => {
      const nativeSql = stmt.getNativeSql();
      return nativeSql.replace(
        new RegExp(`\\(SELECT '${injectionString} (.*?[^']('')*)'\\)`, "gs"),
        (_, p1) => unescapeSqlStr(p1),
      );
    });
  } catch (err) {
    console.log("query", query);
    debugger;
    throw err;
  }
};

const escapeForSqlStr = (s: string) => s.replace(/'/g, "''");
const unescapeSqlStr = (s: string) => s.replace(/''/g, "'");

type SupportedBindings = "bindId" | "bindBlob" | "bindInteger" | "bindString";

const supportedBindingToPropertyTypeMap: Record<SupportedBindings, PropertyType> = {
  bindId: PropertyType.Navigation,
  // FIXME: what about geometry?
  bindBlob: PropertyType.Binary,
  bindInteger: PropertyType.Integer,
  bindString: PropertyType.String,
};

interface Bindings {
  [k: string]: {
    type?: SupportedBindings;
    expr?: (binding: string) => string;
  } | undefined;
}

/** each key is a map of entity class names to its query for that key's type */
interface PolymorphicEntityQueries<
  PopulateExtraBindings extends Bindings,
  UpdateExtraBindings extends Bindings,
> {
  /** inserts without preserving references, must be updated */
  populate: Map<string, (
    db: ECDb,
    jsonString: string,
    binaryValues?: Record<string, Uint8Array>,
    /** extra bindings are ignored if they do not exist in the class */
    extraBindings?: Record<keyof PopulateExtraBindings, any>
  ) => Id64String>;
  insert: Map<string, (
    db: ECDb,
    /** for now you must provide the id to insert on */
    id: Id64String,
    jsonString: string,
    source?: { id: Id64String, db: IModelDb },
  ) => Id64String>;
  /** FIXME: rename to hydrate? since it's not an update but hydrating populated rows... */
  update: Map<string, (
    db: ECDb,
    jsonString: string,
    source?: { id: Id64String, db: IModelDb },
    /** extra bindings are ignored if they do not exist in the class */
    extraBindings?: Record<keyof UpdateExtraBindings, any>
  ) => void>;
}

interface PropInfo {
  name: Property["name"];
  propertyType: Property["propertyType"];
  extendedTypeName?: PrimitiveOrEnumPropertyBase["extendedTypeName"];
  isReadOnly?: Property["isReadOnly"];
}

/**
 * Create a polymorphic insert query for a given db,
 * by expanding its class hiearchy into a giant case statement and using JSON_Extract
 */
async function createPolymorphicEntityQueryMap<
  PopulateExtraBindings extends Bindings,
  UpdateExtraBindings extends Bindings
>(
  db: IModelDb,
  options: {
    extraBindings?: {
      populate?: PopulateExtraBindings;
      update?: UpdateExtraBindings;
    };
  } = {}
): Promise<PolymorphicEntityQueries<PopulateExtraBindings, UpdateExtraBindings>> {
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
      const classProps: PropInfo[] = [...await ecclass.getProperties()];
      classFullNameAndProps.set(ecclass.fullName, classProps);

      if (ecclass instanceof RelationshipClass) {
        classProps.push({
          name: "SourceECInstanceId",
          propertyType: PropertyType.Long,
        });
        classProps.push({
          name: "TargetECInstanceId",
          propertyType: PropertyType.Long,
        });
      }

      classFullNameAndProps.set(ecclass.fullName, classProps);
    }
  }

  const result: PolymorphicEntityQueries<PopulateExtraBindings, UpdateExtraBindings> = {
    insert: new Map(),
    populate: new Map(),
    update: new Map(),
  };

  const readHexFromJson = (p: Pick<PropInfo, "name" | "propertyType">, empty = "0", accessStr?: string) => {
    const navProp = p.propertyType === PropertyType.Navigation;
    // NOTE: currently we know this is only used inside `injectExpr`, so it will avoid ecsql param mangling
    // so premangle the parameter (add "_col1") so sqlite sees the parameters as the same... just in case
    // the query optimizer likes that
    if (empty)
      return `coalesce(HexToId(JSON_EXTRACT(:x_col1, '$.${accessStr ?? `${p.name}${navProp ? ".Id" : ""}`}')), ${empty})`;
    return `HexToId(JSON_EXTRACT(:x_col1, '$.${accessStr ?? `${p.name}${navProp ? ".Id" : ""}`}'))`;
  };

  for (const [classFullName, properties] of classFullNameAndProps) {
    const [schemaName, className] = classFullName.split(".");
    const escapedClassFullName = `[${schemaName}].[${className}]`;

    const updateBindings = Object.entries(options.extraBindings?.update ?? {})
      // FIXME: n^2
      .filter(([name]) => properties.find((p) => p.name === name));

    const defaultExpr = (binding: string) => binding;

    interface UpdateProp extends PropInfo {
      isExtraBinding?: boolean;
      expr: (binding: string) => string;
    }

    /* eslint-disable @typescript-eslint/indent */
    const updateProps = (properties as UpdateProp[])
      .filter((p) =>
        (p.propertyType === PropertyType.Navigation
          || p.name === "CodeValue" // FIXME: check correct CodeValue property
          || p.propertyType === PropertyType.Long)
        // FIXME: n^2
        && !updateBindings.some(([name]) => name === p.name))
      .concat(updateBindings.map(([name, data]) => ({
        name,
        isReadOnly: false,
        propertyType: data?.type ? supportedBindingToPropertyTypeMap[data.type] : PropertyType.Integer,
        isExtraBinding: true,
        expr: data?.expr ?? defaultExpr,
      })));

    const updateQuery = updateProps.length === 0 ? "" : `
      UPDATE ${escapedClassFullName}
      SET ${
        updateProps
          .map((p) =>
            p.isExtraBinding
            ? `[${p.name}] = ${p.expr(`:b_${p.name}`)}`
            // FIXME: use ECReferenceCache to get type of ref instead of checking name
            : p.propertyType === PropertyType.Navigation
            ? `[${p.name}].Id = ${injectExpr(`(
              SELECT TargetId
              FROM temp.${p.name === "CodeSpec" ? "codespec" : "element"}_remap
              WHERE SourceId=${readHexFromJson(p,
                // FIXME: only unconstrained columns need 0, so need to inspect constraints
                p.name === "Parent" || p.name === "TypeDefinition" ? "NULL" : "0"
              )}
            )`)}`
            // FIXME: use ecreferencetypes cache to determine which remap table to use
            : p.propertyType === PropertyType.Long
            ? `[${p.name}] = ${injectExpr(`(
              SELECT TargetId
              FROM temp.element_remap
              WHERE SourceId=${readHexFromJson(p, "0")}
            )`)}`

            // is CodeValue if not nav prop
            : `[${p.name}] = JSON_EXTRACT(:x, '$.CodeValue')`
          )
          .join(",\n  ")
      }
      WHERE ECInstanceId=${injectExpr(`(
        SELECT TargetId
        FROM temp.element_remap
        WHERE SourceId=${readHexFromJson({ name: "ECInstanceId", propertyType: PropertyType.Long }, "0")}
      )`)}
    `;

    const populateBindings = Object.entries(options.extraBindings?.populate ?? {})
      // FIXME: n^2
      .filter(([name]) => properties.some((p) => p.name === name));

    // TODO FIXME: support this
    const nonCompoundProperties = properties
      .filter((p) => !(
           p.propertyType === PropertyType.Struct
        || p.propertyType === PropertyType.Struct_Array
        || p.propertyType === PropertyType.Binary_Array
        || p.propertyType === PropertyType.Boolean_Array
        || p.propertyType === PropertyType.DateTime_Array
        || p.propertyType === PropertyType.Double_Array
        || p.propertyType === PropertyType.Integer_Array
        || p.propertyType === PropertyType.Integer_Enumeration_Array
        || p.propertyType === PropertyType.Long_Array
        || p.propertyType === PropertyType.Point2d_Array
        || p.propertyType === PropertyType.Point3d_Array
        || p.propertyType === PropertyType.String_Array
        || p.propertyType === PropertyType.String_Enumeration_Array
        || p.propertyType === PropertyType.IGeometry_Array
      ));

    // excludes currently unhandled prop types and GeometryStream which is separately bound
    const binaryProperties = nonCompoundProperties
      .filter((p) => p.propertyType === PropertyType.Binary
                  && p.name !== "GeometryStream");
    const nonBinaryProperties = nonCompoundProperties
      .filter((p) => p.propertyType !== PropertyType.Binary);

    const populateQuery = `
      INSERT INTO ${escapedClassFullName}
      (${[
        ...nonBinaryProperties
          .filter((p) => !(p.name in (options.extraBindings?.populate ?? {})))
          .map((p) =>
            // FIXME: note that dynamic structs are completely unhandled
            p.propertyType === PropertyType.Navigation
            ? `[${p.name}].[Id]`
            : p.propertyType === PropertyType.Point2d
            ? `[${p.name}].x, [${p.name}].y`
            : p.propertyType === PropertyType.Point3d
            ? `[${p.name}].x, [${p.name}].y, [${p.name}].z`
            : `[${p.name}]`
          ),
        ...binaryProperties
          .filter((p) => !(p.name in (options.extraBindings?.populate ?? {})))
          .map((p) => `[${p.name}]`),
        ...populateBindings.map(([name]) => name),
      ].join(",\n  ")})
      VALUES
      (${[
        ...nonBinaryProperties
          .filter((p) => !(p.name in (options.extraBindings?.populate ?? {})))
          .map((p) =>
            // FIXME: do qualified check for exact schema of CodeValue prop
            p.name === "CodeValue"
            ? "NULL"
            : p.propertyType === PropertyType.DateTime
            ? `JSON_EXTRACT(:x, '$.${p.name}')`
            : p.propertyType === PropertyType.Navigation || p.propertyType === PropertyType.Long
            ? "0x1"
            // : p.propertyType === PropertyType.Binary && p.extendedTypeName !== "BeGuid"
            // ? `Base64ToBlob(substr(JSON_EXTRACT(:x, '$.${p.name}'), 17))` // ignore encoding spec
            // : p.propertyType === PropertyType.Binary && p.extendedTypeName === "BeGuid"
            // ? `Base64ToBlob(JSON_EXTRACT(:x, '$.${p.name}'))`
            : p.propertyType === PropertyType.Point2d
            ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`
            : p.propertyType === PropertyType.Point3d
            ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y'), JSON_EXTRACT(:x, '$.${p.name}.z')`
            : `JSON_EXTRACT(:x, '$.${p.name}')`
          ),
        ...binaryProperties
          .filter((p) => !(p.name in (options.extraBindings?.populate ?? {})))
          .map((p) => `:p_${p.name}`),
        // FIXME: use the names from the values of the binding object
        ...populateBindings.map(([name]) => `:b_${name}`),
      ].join(",\n  ")})
    `;

    const insertQuery = `
      INSERT INTO ${escapedClassFullName}
      (
        ${
          [
            { name: "ECInstanceId", propertyType: PropertyType.Long },
            ...nonBinaryProperties,
            ...binaryProperties,
          ]
          .map((p) =>
          // FIXME: note that dynamic structs are completely unhandled
          p.propertyType === PropertyType.Navigation
          ? `[${p.name}].Id, [${p.name}].RelECClassId`
          : p.propertyType === PropertyType.Point2d
          ? `[${p.name}].x, [${p.name}].y`
          : p.propertyType === PropertyType.Point3d
          ? `[${p.name}].x, [${p.name}].y, [${p.name}].z`
          : `[${p.name}]`
        )
        .join(",\n  ")
      })
      VALUES (
        ${[
          ":id",
          ...nonBinaryProperties
            .map((p) =>
              p.propertyType === PropertyType.Navigation
              // FIXME: need to use ECReferenceCache to get type of reference, might not be an elem
              ? `${injectExpr(`(
                  SELECT TargetId
                  FROM temp.${p.name === "CodeSpec" ? "codespec" : "element"}_remap
                  WHERE SourceId=${readHexFromJson(p)}
                )`)}, ${injectExpr(`(
                  SELECT tc.Id
                  FROM source.ec_Class sc
                  JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
                  JOIN main.ec_Schema ts ON ts.Name=ss.Name
                  JOIN main.ec_Class tc ON tc.Name=sc.Name
                  WHERE sc.Id=${readHexFromJson(p, undefined, `${p.name}.RelECClassId`)}
                )`)}`
              // FIXME: use ecreferencetypes cache to determine which remap table to use
              : p.propertyType === PropertyType.Long
              ? injectExpr(`(
                SELECT TargetId
                FROM temp.element_remap
                WHERE SourceId=${readHexFromJson(p)}
              )`)
              : p.propertyType === PropertyType.Point2d
              ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`
              : p.propertyType === PropertyType.Point3d
              ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y'), JSON_EXTRACT(:x, '$.${p.name}.z')`
              : `JSON_EXTRACT(:x, '$.${p.name}')`
            ),
          ...binaryProperties.map((p) => `:p_${p.name}`),
        ].join(",\n")}
      )
    `;
    /* eslint-enable @typescript-eslint/indent */

    function populate(
      ecdb: ECDb,
      jsonString: string,
      binaryValues: Record<string, Uint8Array> = {},
      bindingValues: Partial<Record<keyof PopulateExtraBindings, any>> = {},
    ) {
      try {
        return ecdb.withPreparedStatement(populateQuery, (targetStmt) => {
          targetStmt.bindString("x", jsonString);
          for (const [name, data] of populateBindings) {
            const bindingValue = bindingValues[name];
            if (bindingValue)
              targetStmt[data?.type ?? "bindInteger"](`b_${name}`, bindingValue);
          }
          for (const [name, value] of Object.entries(binaryValues)) {
            targetStmt.bindBlob(`p_${name}`, value);
          }
          const stepRes = targetStmt.stepForInsert();
          assert(stepRes.status === DbResult.BE_SQLITE_DONE && stepRes.id);
          return stepRes.id;
        });
      } catch (err) {
        console.log("ERROR", ecdb.nativeDb.getLastError());
        console.log("json:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
        console.log("ecsql:", populateQuery);
        debugger;
        throw err;
      }
    }

    let hackedRemapInsertSql: string | undefined;
    let hackedRemapInsertSqls:
      | { sql: string, needsJson: boolean, needsEcJson: boolean, needsId: boolean, needsEcId: boolean }[]
      | undefined;
    const ecIdBinding = ":_ecdb_ecsqlparam_id_col1";

    function insert(ecdb: ECDb, id: string, jsonString: string, source?: { id: string, db: IModelDb }) {
      if (hackedRemapInsertSql === undefined) {
        hackedRemapInsertSql = getInjectedSqlite(insertQuery, ecdb);
        hackedRemapInsertSqls = hackedRemapInsertSql.split(";").map((sql) => ({
          sql,
          // NOTE: consolidating these two parameter mangling could improve query performance
          needsEcJson: sql.includes(":x_col1"), // NOTE: ECSQL parameter mangling
          needsJson: /:x\b/.test(sql), // FIXME: why is this unmangled? is it in an injection?
          needsId: /:id_col1\b/.test(sql), // NOTE: ECSQL parameter mangling
          needsEcId: sql.includes(ecIdBinding),
        }));
      }

      // NEXT FIXME: doesn't work on some relationships, need to explicitly know if it's a rel
      // class and then always add source/target to INSERT
      try {
        // eslint-disable-next-line
        for (let i = 0; i < hackedRemapInsertSqls!.length; ++i) {
          const sqlInfo = hackedRemapInsertSqls![i];
          ecdb.withPreparedSqliteStatement(sqlInfo.sql, (targetStmt) => {
            // FIXME: should calculate this ahead of time... really should cache all
            // per-class statements
            if (sqlInfo.needsId)
              targetStmt.bindId(":id_col1", id); // NOTE: ECSQL parameter mangling
            if (sqlInfo.needsJson) // FIXME: remove, should never occur
              targetStmt.bindString(":x", jsonString);
            if (sqlInfo.needsEcJson)
              targetStmt.bindString(":x_col1", jsonString);
            if (sqlInfo.needsEcId)
              targetStmt.bindId(ecIdBinding, id);
            assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
          });
        }

        return id;
      } catch (err) {
        console.log("SOURCE", source?.db.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${source.id}`, s=>[...s]));
        console.log("ERROR", ecdb.nativeDb.getLastError());
        console.log("transformed:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
        console.log("ecsql:", insertQuery);
        console.log("native sql:", hackedRemapInsertSql);
        debugger;
        throw err;
      }
    }

    let hackedRemapUpdateSql: string | undefined;
    let hackedRemapUpdateSqls: { sql: string, needsJson: boolean }[] | undefined;

    function update(
      ecdb: ECDb,
      jsonString: string,
      source?: { id: string, db: IModelDb },
      bindingValues: {[S in keyof UpdateExtraBindings]?: any} = {},
    ) {
      if (updateQuery === "")
        return; // ignore empty updates

      if (hackedRemapUpdateSql === undefined) {
        hackedRemapUpdateSql = getInjectedSqlite(updateQuery, ecdb);
        hackedRemapUpdateSqls = hackedRemapUpdateSql.split(";").map((sql) => ({
          sql,
          needsJson: sql.includes(":x_col1"),
        }));
      }

      try {
        // eslint-disable-next-line
        for (let i = 0; i < hackedRemapUpdateSqls!.length; ++i) {
          const { sql, needsJson } = hackedRemapUpdateSqls![i];
          ecdb.withPreparedSqliteStatement(sql, (targetStmt) => {
            if (needsJson)
              targetStmt.bindString(":x_col1", jsonString);

            for (const [name, data] of updateBindings) {
              // FIXME: why do I get a never type for this...
              // FIXME: in raw sqlite must append _col1
              const param = `:b_${name}_col1`;
              // HACK: work around bad param detection
              const bindingValue = bindingValues[name];
              if (bindingValue && sql.includes(param)) {
                (targetStmt[data?.type ?? "bindInteger"] as any)(param, bindingValue);
              }
            }

            assert(targetStmt.step() === DbResult.BE_SQLITE_DONE, ecdb.nativeDb.getLastError());
          });
        }
      } catch (err) {
        const _elemId = JSON.parse(jsonString).ECInstanceId;
        console.log("SOURCE", source?.db.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${source.id}`, s=>[...s]));
        console.log("ERROR", ecdb.nativeDb.getLastError());
        /*
        console.log("REMAPS");
        ecdb.withSqliteStatement(
          "SELECT format('0x%x->0x%x', SourceId, TargetId) FROM temp.element_remap",
          (s) => {
            while (s.step() === DbResult.BE_SQLITE_ROW)
              console.log(s.getValue(0).getString());
          }
        );
        */
        console.log("transformed:", JSON.stringify(JSON.parse(jsonString), undefined, " "));
        console.log("native sql:", hackedRemapUpdateSql);
        debugger;
        throw err;
      }
    }

    result.insert.set(classFullName, insert);
    result.populate.set(classFullName, populate);
    result.update.set(classFullName, update);
  }

  return result;
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
  // NOTE: initializing this transformer is expensive! it populates the ECReferenceCache for no reason
  const schemaExporter = new IModelTransformer(source, target);
  const fontRemaps = new Map<number, number>();

  schemaExporter.context.importFont = function (id) {
    this.targetDb.clearFontMap(); // so it will be reloaded with new font info
    // eslint-disable-next-line @typescript-eslint/dot-notation
    const result = this["_nativeContext"].importFont(id);
    fontRemaps.set(id, result);
    return result;
  };

  await schemaExporter.processFonts();
  await schemaExporter.processSchemas();
  schemaExporter.dispose();

  // FIXME: return all three queries instead of loading schemas
  const queryMap = await createPolymorphicEntityQueryMap(
    target,
    {
      extraBindings: {
        populate: { FederationGuid: { type: "bindBlob" } },
        update: {
          GeometryStream: {
            type: "bindBlob",
            expr: (b) => `CAST(RemapGeom(${b}, 'temp.font_remap', 'temp.element_remap') AS BINARY)`,
          },
        },
      },
    },
  );

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const targetPath = target.pathName;
  target.saveChanges();
  target.close();
  const writeableTarget = new ECDb();
  writeableTarget.openDb(targetPath, ECDbOpenMode.ReadWrite);

  for (const name of ["element_remap", "codespec_remap", "aspect_remap", "font_remap"]) {
    // FIXME: compress this table into "runs"
    writeableTarget.withSqliteStatement(`
      CREATE TEMP TABLE ${name} (
        SourceId INTEGER NOT NULL PRIMARY KEY, -- do we need an index?
        TargetId INTEGER NOT NULL
      )
    `, (s: any) => assert(s.step() === DbResult.BE_SQLITE_DONE));

    // always remap 0 to 0
    writeableTarget.withSqliteStatement(`
      INSERT INTO temp.${name} VALUES(0,0)
    `, (s: any) => assert(s.step() === DbResult.BE_SQLITE_DONE));
  }

  // fill already exported fonts
  for (const [sourceId, targetId] of fontRemaps) {
    writeableTarget.withPreparedSqliteStatement(`
      INSERT INTO temp.font_remap VALUES(?,?)
    `, (targetStmt) => {
      targetStmt.bindId(1, Id64.fromUint32Pair(sourceId, 0));
      targetStmt.bindId(2, Id64.fromUint32Pair(targetId, 0));
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
    });
  }

  writeableTarget.withSqliteStatement(`
    ATTACH DATABASE 'file://${source.pathName}?mode=ro' AS source
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  writeableTarget.withPreparedSqliteStatement(`
    INSERT INTO temp.element_remap VALUES(0x1,0x1), (0xe,0xe), (0x10, 0x10)
  `, (targetStmt) => {
    assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
  });

  // FIXME: this doesn't work... (maybe should disable foreign keys entirely?)
  // using a workaround of setting all references to 0x0
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

  console.log("insert codespecs");
  const sourceCodeSpecSelect = `
    SELECT s.Id, t.Id, s.Name, s.JsonProperties
    FROM source.bis_CodeSpec s
    LEFT JOIN main.bis_CodeSpec t ON s.Name=t.Name
  `;

  writeableTarget.withSqliteStatement(sourceCodeSpecSelect, (stmt) => {
    while (stmt.step() === DbResult.BE_SQLITE_ROW) {
      const sourceId = stmt.getValue(0).getId();
      let targetId = stmt.getValue(1).getId();
      const name = stmt.getValue(2).getString();
      const jsonProps = stmt.getValue(3).getString();

      if (!targetId) {
        targetId = writeableTarget.withPreparedStatement(`
          INSERT INTO bis.CodeSpec VALUES(?,?)
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
      }

      writeableTarget.withPreparedSqliteStatement(`
        INSERT INTO temp.codespec_remap VALUES(?,?)
      `, (targetStmt) => {
        targetStmt.bindId(1, sourceId);
        targetStmt.bindId(2, targetId);
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
      });
    }
  });

  const startTime = performance.now();
  let stmtsExeced = 0;
  const incrementStmtsExeced = () => {
    stmtsExeced += 1;
    const elapsedMs = performance.now() - startTime;
    if (stmtsExeced % 1000 === 0)
      console.log(`executed ${stmtsExeced} statements at ${elapsedMs/1000}s`);
  };

  let [_nextElemId, _nextInstanceId] = ["bis_elementidsequence", "ec_instanceidsequence"]
    .map((seq) => writeableTarget.withSqliteStatement(`
      SELECT Val
      FROM be_Local
      WHERE Name='${seq}'
    `, (s) => {
      assert(s.step() === DbResult.BE_SQLITE_ROW, writeableTarget.nativeDb.getLastError());
      return parseInt(s.getValue(0).getId(), 16);
    }));

  // FIXME: doesn't support high briefcase ids (> 2 << 13)!
  const useElemId = () => `0x${(_nextElemId++).toString(16)}`;
  const useInstanceId = () => `0x${(_nextInstanceId++).toString(16)}`;

  const sourceElemSelect = `
    SELECT e.$, ec_classname(e.ECClassId, 's.c'), e.ECInstanceId,
           m.$, ec_classname(m.ECClassId, 's.c')
    FROM bis.Element e
    -- FIXME: is it faster to use the new $->Blah syntax?
    LEFT JOIN bis.Model m ON e.ECInstanceId=m.ECInstanceId
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    -- FIXME: ordering by class *might* be faster due to less cache busting
    -- ORDER BY ECClassId, ECInstanceId ASC
    ORDER BY e.ECInstanceId ASC
  `;

  const sourceFedGuidSelect = `
    SELECT CAST(e.FederationGuid AS Binary)
    FROM bis.Element e
    -- NOTE: ORDER and WHERE must match the sourceElemSelect query above
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    ORDER BY e.ECInstanceId ASC
  `;

  // first pass, update everything with trivial references (0 and null codes)
  // FIXME: technically could do it all in one pass if we preserve distances between rows and
  // just offset all references by the count of rows in the source...
  //
  // Might be useful to still do two passes though in a filter-heavy transform... we can always
  // do the offsetting in the first pass, and then decide during the pass if there is too much sparsity
  // in the IDs and redo it?
  console.log("populate elements");
  const sourceElemFirstPassReader = source.createQueryReader(sourceElemSelect, undefined, { abbreviateBlobs: true });
  const sourceElemFedGuidReader = source.createQueryReader(sourceFedGuidSelect);
  while (await Promise.all([
    sourceElemFirstPassReader.step(),
    sourceElemFedGuidReader.step(),
  ]).then(([a,b]) => (assert(a === b), a))) {
    const elemJson = sourceElemFirstPassReader.current[0] as string;
    const elemClass = sourceElemFirstPassReader.current[1];
    const sourceId = sourceElemFirstPassReader.current[2];
    const modelJson = sourceElemFirstPassReader.current[3];
    const modelClass = sourceElemFirstPassReader.current[4];
    const federationGuid = sourceElemFedGuidReader.current[0];

    const elemPopulateQuery = queryMap.populate.get(elemClass);
    assert(elemPopulateQuery, `couldn't find insert query for class '${elemClass}'`);

    let binaryValues: Record<string, Uint8Array | undefined> = {};

    for (const binaryValue of elemJson.matchAll(/"(?<propName>[^"]+)":"\{\\"bytes\\":(?<byteLen>\d+)\}"/g)) {
      const propName = binaryValue.groups?.propName;
      // FIXME: we will handle geometry stream in the update, once the mapping is done
      if (propName === "GeometryStream")
        continue;
      // const byteLen = binaryValue.groups?.byteLen ? parseInt(binaryValue.groups.byteLen, 10) : undefined;
      // assert(byteLen && !Number.isNaN(byteLen), "invalid byte length parsed");
      assert(propName);
      binaryValues[propName] = undefined;
    }

    const binaryPropNames = Object.keys(binaryValues);

    if (binaryPropNames.length > 0) {
      const sourceElemBinariesReader = source.createQueryReader(
        `SELECT ${binaryPropNames} FROM ${elemClass} WHERE ECInstanceId=?`,
        QueryBinder.from([sourceId])
      );
      await sourceElemBinariesReader.step();
      binaryValues = sourceElemBinariesReader.current.toRow();
    }

    const targetId = elemPopulateQuery(
      writeableTarget,
      elemJson,
      binaryValues as Record<string, Uint8Array>,
      { FederationGuid: federationGuid }
    );

    if (modelJson) {
      const modelInsertQuery = queryMap.insert.get(modelClass);
      assert(modelInsertQuery, `couldn't find insert query for class '${modelClass}'`);

      // HACK: edit packaged json without parsing (FIXME: I don't think this is necessary any longer)
      const modelJsonWithTargetId = modelJson.replace(/(?<="ECInstanceId":")[^"]+(?=")/, targetId);
      // FIXME: not yet handling binary properties on these
      modelInsertQuery(writeableTarget, targetId, modelJsonWithTargetId);
    }

    writeableTarget.withPreparedSqliteStatement(`
      INSERT INTO temp.element_remap VALUES(?,?)
    `, (targetStmt) => {
      targetStmt.bindId(1, sourceId);
      targetStmt.bindId(2, targetId);
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
    });

    incrementStmtsExeced();
  }

  const sourceElemForHydrate = `
    SELECT e.$, ec_classname(e.ECClassId, 's.c'), e.ECInstanceId
    FROM bis.Element e
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    ORDER BY e.ECInstanceId ASC
  `;

  const sourceGeomForHydrate = `
    SELECT CAST (coalesce(g3d.GeometryStream, g2d.GeometryStream, gp.GeometryStream) AS Binary)
    FROM bis.Element e
    LEFT JOIN bis.GeometricElement3d g3d ON e.ECInstanceId=g3d.ECInstanceId
    LEFT JOIN bis.GeometricElement2d g2d ON e.ECInstanceId=g2d.ECInstanceId
    LEFT JOIN bis.GeometryPart       gp ON e.ECInstanceId=gp.ECInstanceId
    -- NOTE: ORDER and WHERE must match the query above
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    ORDER BY e.ECInstanceId ASC
  `;

  // first pass, update everything with trivial references (0 and null codes)
  // FIXME: technically could do it all in one pass if we preserve distances between rows and
  // just offset all references by the count of rows in the source...
  //
  // Might be useful to still do two passes though in a filter-heavy transform... we can always
  // do the offsetting in the first pass, and then decide during the pass if there is too much sparsity
  // in the IDs and redo it?
  console.log("populate elements");

  // second pass, update now that everything has been inserted
  console.log("hydrate elements");
  const sourceElemSecondPassReader = source.createQueryReader(sourceElemForHydrate, undefined, { abbreviateBlobs: true });
  const sourceElemGeomReader = source.createQueryReader(sourceGeomForHydrate);
  while (await Promise.all([
    sourceElemSecondPassReader.step(),
    sourceElemGeomReader.step(),
  ]).then(([a,b]) => (assert(a === b), a))) {
    const jsonString = sourceElemSecondPassReader.current[0];
    const classFullName = sourceElemSecondPassReader.current[1];
    const sourceId = sourceElemSecondPassReader.current[2];
    const geometryStream = sourceElemGeomReader.current[0];

    const updateQuery = queryMap.update.get(classFullName);
    assert(updateQuery, `couldn't find update query for class '${classFullName}`);

    updateQuery(
      writeableTarget,
      jsonString,
      { id: sourceId, db: source },
      { GeometryStream: geometryStream },
    );

    incrementStmtsExeced();
  }

  const sourceAspectSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.ElementAspect
  `;

  console.log("insert aspects");
  // FIXME: this slowly handles binary properties!
  const aspectReader = source.createQueryReader(sourceAspectSelect, undefined, { abbreviateBlobs: false });
  while (await aspectReader.step()) {
    const jsonString = aspectReader.current[0];
    const classFullName = aspectReader.current[1];
    const sourceId = aspectReader.current[2];

    const insertQuery = queryMap.insert.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

    const targetId = insertQuery(writeableTarget, useInstanceId(), jsonString, { id: sourceId, db: source });

    writeableTarget.withPreparedSqliteStatement(`
      INSERT INTO temp.aspect_remap VALUES(?,?)
    `, (targetStmt) => {
      // HACK: returned id is broken with sql_mismatch so just reusing source id
      targetStmt.bindId(1, sourceId);
      targetStmt.bindId(2, targetId);
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
    });

    incrementStmtsExeced();
  }

  const elemRefersSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.ElementRefersToElements
  `;

  console.log("insert ElementRefersToElements");
  // FIXME: this slowly handles binary properties!
  const elemRefersReader = source.createQueryReader(elemRefersSelect, undefined, { abbreviateBlobs: false });
  while (await elemRefersReader.step()) {
    const jsonString = elemRefersReader.current[0];
    const classFullName = elemRefersReader.current[1];
    const sourceId = elemRefersReader.current[2];

    const insertQuery = queryMap.insert.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

    insertQuery(writeableTarget, useInstanceId(), jsonString, { id: sourceId, db: source });

    incrementStmtsExeced();
  }

  // FIXME: also do ElementDrivesElements

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
    const [elemRemaps, codeSpecRemaps, aspectRemaps] = ["element", "codespec", "aspect"].map((type) => {
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

  // FIXME: detach... readonly attached db gets write-locked for some reason
  // writeableTarget.withSqliteStatement(`
  //   DETACH source
  // `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  writeableTarget.clearStatementCache(); // so we can detach attached db
  writeableTarget.saveChanges();
  writeableTarget.closeDb();

  return remapper;
}
