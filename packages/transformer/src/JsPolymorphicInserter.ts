import { ECDb, ECDbOpenMode, ECSqlStatement, IModelDb, IModelHost, IModelJsNative } from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { PrimitiveOrEnumPropertyBase, Property, PropertyType, RelationshipClass, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import { IModelTransformer } from "./IModelTransformer";
import { CompactRemapTable } from "./CompactRemapTable";

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

// FIXME: note that SQLite doesn't seem to have types/statistics that would let it consider using
// an optimized binary search for our range query, so we should not do this via SQLite. Once we
// get around to designing how we'll pass a JavaScript object to RemapGeom, then we can fix that.
// That said, this should be pretty fast in our cases here regardless, since the table _should_
// scale with briefcase count
const remapSql = (idExpr: string, remapType: "font" | "codespec" | "aspect" | "element") => `(
  SELECT TargetId + ((${idExpr}) - SourceId)
  FROM temp.${remapType}_remap
  WHERE ${idExpr} BETWEEN SourceId AND SourceId + Length - 1
)`;

const escapeForSqlStr = (s: string) => s.replace(/'/g, "''");
const unescapeSqlStr = (s: string) => s.replace(/''/g, "'");

/* eslint-disable */
const propBindings = (p: PropInfo): string[] =>
  p.propertyType === PropertyType.Point3d
  ? [`n_${p.name}_x`, `n_${p.name}_y`, `n_${p.name}_z`]
  : p.propertyType === PropertyType.Point2d
  ? [`n_${p.name}_x`, `n_${p.name}_y`]
  : [`n_${p.name}`]
;
/* eslint-enable */

function stmtBindProperty(
  stmt: ECSqlStatement,
  prop: PropInfo | PrimitiveOrEnumPropertyBase,
  val: any,
) {
  const bindings = propBindings(prop);
  const binding = bindings[0];
  if (val === undefined)
    return;
  if (prop.propertyType === PropertyType.Long/* && prop.extendedTypeName === "Id"*/)
    return stmt.bindId(binding, val);
  if (prop.propertyType === PropertyType.Binary && prop.extendedTypeName === "BeGuid")
    return stmt.bindGuid(binding, val);
  if (prop.propertyType === PropertyType.Binary)
    return stmt.bindBlob(binding, val);
  if (prop.propertyType === PropertyType.Integer)
    return stmt.bindInteger(binding, val);
  if (prop.propertyType === PropertyType.Integer_Enumeration)
    return stmt.bindInteger(binding, val);
  if (prop.propertyType === PropertyType.String)
    return stmt.bindString(binding, val);
  if (prop.propertyType === PropertyType.String_Enumeration)
    return stmt.bindString(binding, val);
  if (prop.propertyType === PropertyType.Double)
    return stmt.bindDouble(binding, val);
  if (prop.propertyType === PropertyType.Boolean)
    return stmt.bindBoolean(binding, val);
  if (prop.propertyType === PropertyType.DateTime)
    return stmt.bindDateTime(binding, val);
  if (prop.propertyType === PropertyType.Navigation)
    return stmt.bindId(binding, val.Id);
  if (prop.propertyType === PropertyType.Point2d) {
    stmt.bindDouble(bindings[0], val.X);
    stmt.bindDouble(bindings[1], val.Y);
    return;
  }
  if (prop.propertyType === PropertyType.Point3d) {
    stmt.bindDouble(bindings[0], val.X);
    stmt.bindDouble(bindings[1], val.Y);
    stmt.bindDouble(bindings[2], val.Z);
    return;
  }
  if (prop.propertyType === PropertyType.IGeometry)
    return stmt.bindBlob(binding, val.Id);
  console.warn(`ignoring binding unsupported property with type: ${prop.propertyType} (${prop.name})`);
}

function incrementalWriteBlob(...[db, blobOpenParams, value]: [...Parameters<IModelJsNative.BlobIO["openEc"]>, Uint8Array]) {
  const blobIO = new IModelHost.platform.BlobIO();
  try {
    blobIO.openEc(db, blobOpenParams);

    // FIXME: check if this is necessary
    const writeBlockSize = 4096;
    for (let i = value.byteOffset; i < value.byteLength; i += writeBlockSize) {
      const numBytes = Math.min(writeBlockSize, value.byteLength - i - value.byteOffset);
      blobIO.write({
        blob: value,
        offset: i,
        numBytes,
      });
    }

    blobIO.close();
  } catch (err) {
    // FIXME: should better handle null blobs
    if (!/cannot open value of type null/.test(db.getLastError())) {
      console.log("last sqlite error", db.getLastError());
      console.log("open params", blobOpenParams);
      debugger;
      throw err;
    }
  }
}

type SupportedBindings = "bindId" | "bindBlob" | "bindInteger" | "bindString";

const supportedBindingToPropertyTypeMap: Record<SupportedBindings, PropertyType> = {
  bindId: PropertyType.Navigation,
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
  InsertExtraBindings extends Bindings,
  UpdateExtraBindings extends Bindings,
> {
  selectBinaries: Map<string, (
    db: ECDb | IModelDb,
    id: Id64String,
  ) => Record<string, Uint8Array>>;
  /** inserts without preserving references, must be updated */
  populate: Map<string, (
    db: ECDb,
    json: any,
    binaryValues?: Record<string, Uint8Array>,
    /** extra bindings are ignored if they do not exist in the class */
    extraBindings?: Record<keyof PopulateExtraBindings, any>,
  ) => Id64String>;
  insert: Map<string, (
    db: ECDb,
    /** for now you must provide the id to insert on */
    id: Id64String,
    json: any,
    jsonString: any, // FIXME: TEMP
    binaryValues?: Record<string, Uint8Array>,
    extraBindings?: Record<keyof InsertExtraBindings, any>,
    source?: { id: Id64String, db: IModelDb },
  ) => Id64String>;
  /** FIXME: rename to hydrate? since it's not an update but hydrating populated rows... */
  update: Map<string, (
    db: ECDb,
    json: any, // FIXME: for now must include the correct id
    jsonString: any, // FIXME: TEMP
    binaryValues?: Record<string, Uint8Array>,
    /** extra bindings are ignored if they do not exist in the class */
    extraBindings?: Record<keyof UpdateExtraBindings, any>,
    source?: { id: Id64String, db: IModelDb },
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
  InsertExtraBindings extends Bindings,
  UpdateExtraBindings extends Bindings
>(
  db: IModelDb,
  options: {
    extraBindings?: {
      populate?: PopulateExtraBindings;
      update?: UpdateExtraBindings;
      insert?: InsertExtraBindings;
    };
  } = {}
): Promise<PolymorphicEntityQueries<PopulateExtraBindings, InsertExtraBindings, UpdateExtraBindings>> {
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

  const result: PolymorphicEntityQueries<PopulateExtraBindings, InsertExtraBindings, UpdateExtraBindings> = {
    insert: new Map(),
    populate: new Map(),
    update: new Map(),
    selectBinaries: new Map(),
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
      .filter((p) => p.propertyType === PropertyType.Binary && p.extendedTypeName !== "BeGuid")
      .filter((p) => p.name !== "GeometryStream"); // FIXME: hack ignore geomstream for most stuff
    const nonBinaryProperties = nonCompoundProperties
      .filter((p) => !(p.propertyType === PropertyType.Binary && p.extendedTypeName !== "BeGuid"));

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
            ? `[${p.name}].Id = ${injectExpr(remapSql(
                readHexFromJson(
                  p,
                  // FIXME: only unconstrained columns need 0, so need to inspect constraints
                  p.name === "Parent" || p.name === "TypeDefinition" ? "NULL" : "0"
                ),
                p.name === "CodeSpec" ? "codespec" : "element"
            ))}`
            // FIXME: use ecreferencetypes cache to determine which remap table to use
            : p.propertyType === PropertyType.Long
            ? `[${p.name}] = ${injectExpr(remapSql(readHexFromJson(p, "0"), "element"))}`
            // is CodeValue if not nav prop
            : `[${p.name}] = JSON_EXTRACT(:x, '$.CodeValue')`
          )
          .join(",\n  ")
      }
      WHERE ECInstanceId=${injectExpr(remapSql(
        readHexFromJson({ name: "ECInstanceId", propertyType: PropertyType.Long }, "0"),
        "element",
      ))}
    `;

    const populateBindings = Object.entries(options.extraBindings?.populate ?? {})
      // FIXME: n^2
      .filter(([name]) => properties.some((p) => p.name === name));

    const populateProperties = nonBinaryProperties
      .filter((p) =>
        p.name !== "CodeValue"
        && p.propertyType !== PropertyType.Navigation
        && p.propertyType !== PropertyType.Long
      );

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
          // FIXME: cleanup
          // geometry stream will be filled in by populate
          .filter((p) => p.name !== "GeometryStream")
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
            : p.propertyType === PropertyType.Navigation || p.propertyType === PropertyType.Long
            ? "0x1"
            : propBindings(p).map((b) => `:${b}`).join(",")
          ),
        ...binaryProperties
          .filter((p) => !(p.name in (options.extraBindings?.populate ?? {})))
          .map((p) => `zeroblob(:s_${p.name})`),
        // FIXME: use the names from the values of the binding object
        ...populateBindings.map(([name]) => `:b_${name}`),
      ].join(",\n  ")})
    `;

    const insertBindings = Object.entries(options.extraBindings?.insert ?? {})
      // FIXME: n^2
      .filter(([name]) => properties.some((p) => p.name === name));

    const insertQuery = `
      INSERT INTO ${escapedClassFullName}
      (
        ${
          [
            { name: "ECInstanceId", propertyType: PropertyType.Long },
            ...nonBinaryProperties
              .filter((p) => !(p.name in (options.extraBindings?.insert ?? {}))),
            ...binaryProperties
              .filter((p) => !(p.name in (options.extraBindings?.insert ?? {}))),
            ...insertBindings.map((name) => ({ name, propertyType: undefined })),
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
            .filter((p) => !(p.name in (options.extraBindings?.insert ?? {})))
            .map((p) =>
              p.propertyType === PropertyType.Navigation
              // FIXME: need to use ECReferenceCache to get type of reference, might not be an elem
              ? `${injectExpr(remapSql(readHexFromJson(p), p.name === "CodeSpec" ? "codespec" : "element"))},
                ${injectExpr(`(
                  SELECT tc.Id
                  FROM source.ec_Class sc
                  JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
                  JOIN main.ec_Schema ts ON ts.Name=ss.Name
                  JOIN main.ec_Class tc ON tc.Name=sc.Name
                  WHERE sc.Id=${readHexFromJson(p, undefined, `${p.name}.RelECClassId`)}
                )`)}`
              // FIXME: use ecreferencetypes cache to determine which remap table to use
              : p.propertyType === PropertyType.Long
              ? injectExpr(remapSql(readHexFromJson(p), "element"))
              : p.propertyType === PropertyType.Point2d
              ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`
              : p.propertyType === PropertyType.Point3d
              ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y'), JSON_EXTRACT(:x, '$.${p.name}.z')`
              : `JSON_EXTRACT(:x, '$.${p.name}')`
            ),
          ...binaryProperties
            .filter((p) => !(p.name in (options.extraBindings?.insert ?? {})))
            .map((p) => `zeroblob(:s_${p.name})`),
          ...insertBindings.map(([name]) => `:b_${name}`),
        ].join(",\n")}
      )
    `;
    /* eslint-enable @typescript-eslint/indent */

    function populate(
      ecdb: ECDb,
      json: any,
      binaryValues: Record<string, Uint8Array> = {},
      bindingValues: Partial<Record<keyof PopulateExtraBindings, any>> = {},
    ) {
      try {
        return ecdb.withPreparedStatement(populateQuery, (targetStmt) => {
          for (const p of populateProperties) {
            stmtBindProperty(targetStmt, p, json[p.name]);
          }
          for (const [name, data] of populateBindings) {
            const bindingValue = bindingValues[name];
            if (bindingValue)
              targetStmt[data?.type ?? "bindInteger"](`b_${name}`, bindingValue);
          }
          for (const [name, value] of Object.entries(binaryValues)) {
            targetStmt.bindInteger(`s_${name}`, value.byteLength);
          }
          const stepRes = targetStmt.stepForInsert();
          assert(stepRes.status === DbResult.BE_SQLITE_DONE && stepRes.id);
          return stepRes.id;
        });
      } catch (err) {
        console.log("ERROR", ecdb.nativeDb.getLastError());
        console.log("json:", JSON.stringify(json, undefined, " "));
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

    function insert(
      ecdb: ECDb,
      id: string,
      _jsonObj: any,
      json: string,
      binaryValues: Record<string, Uint8Array> = {},
      bindingValues: {[S in keyof InsertExtraBindings]?: any} = {},
      source?: { id: string, db: IModelDb }
    ) {
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
              targetStmt.bindString(":x", json);
            if (sqlInfo.needsEcJson)
              targetStmt.bindString(":x_col1", json);
            if (sqlInfo.needsEcId)
              targetStmt.bindId(ecIdBinding, id);

            for (const [name, value] of Object.entries(binaryValues))
              targetStmt.bindInteger(`:s_${name}_col1`, value.byteLength); // NOTE: ECSQL param mangling

            for (const [name, data] of populateBindings) {
              const bindingValue = bindingValues[name];
              // FIXME: why does typescript hate me
              if (bindingValue)
                (targetStmt as any)[data?.type ?? "bindInteger"](`b_${name}`, bindingValue);
            }

            assert(targetStmt.step() === DbResult.BE_SQLITE_DONE, ecdb.nativeDb.getLastError());
          });
        }

        for (const [name, value] of Object.entries(binaryValues)) {
          incrementalWriteBlob(ecdb.nativeDb, {
            classFullName,
            id,
            propertyAccessString: name,
            writeable: true,
          }, value);
        }

        return id;
      } catch (err) {
        console.log("SOURCE", source?.db.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${source.id}`, s=>[...s]));
        console.log("ERROR", ecdb.nativeDb.getLastError());
        console.log("transformed:", JSON.stringify(json, undefined, " "));
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
      _jsonObj: any,
      json: any,
      binaryValues: Record<string, Uint8Array> = {},
      bindingValues: {[S in keyof UpdateExtraBindings]?: any} = {},
      source?: { id: string, db: IModelDb },
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
              targetStmt.bindString(":x_col1", json);

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
        const _elemId = json.ECInstanceId;
        console.log("SOURCE", source?.db.withStatement(`SELECT * FROM ${classFullName} WHERE ECInstanceId=${source.id}`, s=>[...s]));
        console.log("ERROR", ecdb.nativeDb.getLastError());
        console.log("transformed:", JSON.stringify(json, undefined, " "));
        console.log("native sql:", hackedRemapUpdateSql);
        debugger;
        throw err;
      }

      for (const [name, value] of Object.entries(binaryValues)) {
        incrementalWriteBlob(ecdb.nativeDb, {
          classFullName,
          id: _jsonObj.ECInstanceId,
          propertyAccessString: name,
          writeable: true,
        }, value);
      }
    }

    // NOTE: ignored fields are still queried
    const selectBinariesQuery = `
      SELECT ${binaryProperties.map((p) => `CAST([${p.name}] AS BINARY)`)}
      FROM ${escapedClassFullName}
      WHERE ECInstanceId=?
    `;

    function selectBinaries(ecdb: ECDb | IModelDb, id: Id64String): Record<string, Uint8Array> {
      if (binaryProperties.length <= 0)
        return {};

      return ecdb.withPreparedStatement(selectBinariesQuery, (stmt) => {
        stmt.bindId(1, id);
        assert(stmt.step() === DbResult.BE_SQLITE_ROW, ecdb.nativeDb.getLastError());
        // FIXME: maybe this should be a map?
        const row = {} as Record<string, Uint8Array>;
        for (let i = 0; i < binaryProperties.length; ++i) {
          const prop = binaryProperties[i];
          // FIXME: ignore is unused, remove this condition
          const value = stmt.getValue(i);
          if (!value.isNull)
            row[prop.name] = value.getBlob();
        }
        assert(stmt.step() === DbResult.BE_SQLITE_DONE, ecdb.nativeDb.getLastError());
        return row;
      });
    }

    result.insert.set(classFullName, insert);
    result.populate.set(classFullName, populate);
    result.update.set(classFullName, update);
    result.selectBinaries.set(classFullName, selectBinaries);
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
  const queryMap = await createPolymorphicEntityQueryMap(target, {
    extraBindings: {
      insert: {
        GeometryStream: {
          // it will be written with blob io...
          expr: (b) => `zeroblob(${b})`,
        },
      },
    },
  });

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const targetPath = target.pathName;
  target.saveChanges();
  target.close();
  const writeableTarget = new ECDb();
  writeableTarget.openDb(targetPath, ECDbOpenMode.ReadWrite);

  const remapTables = {
    element: new CompactRemapTable(),
    aspect: new CompactRemapTable(),
    codespec: new CompactRemapTable(),
    font: new CompactRemapTable(),
  };

  for (const name of ["element", "codespec", "aspect", "font"] as const) {
    // FIXME: compress this table into "runs"
    writeableTarget.withSqliteStatement(`
      CREATE TEMP TABLE ${name}_remap (
        SourceId INTEGER NOT NULL PRIMARY KEY,
        TargetId INTEGER NOT NULL,
        Length INTEGER NOT NULL
      )
    `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

    // always remap 0 to 0
    remapTables[name].remap(0, 0);
  }

  // fill already exported fonts
  for (const [sourceId, targetId] of fontRemaps) {
    remapTables.font.remap(sourceId, targetId);
  }

  writeableTarget.withSqliteStatement(`
    ATTACH DATABASE 'file://${source.pathName}?mode=ro' AS source
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  remapTables.element.remap(1, 1);
  remapTables.element.remap(0xe, 0xe);
  remapTables.element.remap(0x10, 0x10);

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

  const sourceCodeSpecSelect = `
    SELECT s.Id, t.Id, s.Name, s.JsonProperties
    FROM source.bis_CodeSpec s
    LEFT JOIN main.bis_CodeSpec t ON s.Name=t.Name
  `;

  console.log("insert codespecs");
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

      // FIXME: doesn't support briefcase ids > 2**13 - 1
      remapTables.codespec.remap(parseInt(sourceId, 16), parseInt(targetId, 16));
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

  // FIXME: can't handle float64 unsafe integers (e.g. >2**53 or briefcase id > 2**13)
  const numIdToId64 = (id: number | undefined): Id64String =>
    id === 0 || id === undefined ? "0" : `0x${id.toString(16)}`;
  const id64ToNumId = (id: Id64String): number => parseInt(id, 16);

  // FIXME: doesn't support high briefcase ids (> 2 << 13)!
  const useElemId = () => `0x${(_nextElemId++).toString(16)}`;
  const useInstanceId = () => `0x${(_nextInstanceId++).toString(16)}`;

  const sourceElemRemapSelect = `
    SELECT e.ECInstanceId,
    FROM bis.Element e
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    ORDER BY e.ECInstanceId ASC
  `;

  // first pass, populate the id map
  // FIXME: technically could do it all in one pass if we assume everything will have the same id moved
  // just offset all references by the count of rows in the source...
  //
  // Might be useful to still do two passes though in a filter-heavy transform... we can always
  // do the offsetting in the first pass, and then decide during the pass if there is too much sparsity
  // in the IDs and redo it?
  console.log("generate elements remap tables");
  const sourceElemRemapPassReader = source.createQueryReader(sourceElemRemapSelect, undefined, { abbreviateBlobs: true });
  while (await sourceElemRemapPassReader.step()) {
    const sourceId = sourceElemRemapPassReader.current[0] as Id64String;
    const targetId = useElemId();
    // FIXME: doesn't support briefcase ids > 2**13 - 1
    remapTables.element.remap(parseInt(sourceId, 16), parseInt(targetId, 16));
  }

  // give sqlite the tables
  for (const name of ["element", "codespec", "aspect", "font"] as const) {
    for (const run of remapTables[name].runs()) {
      writeableTarget.withPreparedSqliteStatement(`
        INSERT INTO temp.${name}_remap VALUES(?,?,?)
      `, (targetStmt) => {
        targetStmt.bindInteger(1, run.from);
        targetStmt.bindInteger(2, run.to);
        targetStmt.bindInteger(3, run.length);
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
      });
    }
  }

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

  const sourceGeomForHydrate = `
    SELECT CAST (RemapGeom(
        coalesce(g3d.GeometryStream, g2d.GeometryStream, gp.GeometryStream),
        'temp.font_remap', 'temp.element_remap'
      ) AS Binary)
    FROM bis.Element e
    LEFT JOIN bis.GeometricElement3d g3d ON e.ECInstanceId=g3d.ECInstanceId
    LEFT JOIN bis.GeometricElement2d g2d ON e.ECInstanceId=g2d.ECInstanceId
    LEFT JOIN bis.GeometryPart       gp ON e.ECInstanceId=gp.ECInstanceId
    -- NOTE: ORDER and WHERE must match the query above
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    ORDER BY e.ECInstanceId ASC
  `;
 
  // now insert everything now that we know ids
  console.log("insert elements");
  const sourceElemFirstPassReader = source.createQueryReader(sourceElemSelect, undefined, { abbreviateBlobs: true });
  await source.withPreparedStatement(sourceGeomForHydrate, async (geomStmt) => {
    while (await sourceElemFirstPassReader.step()) {
      assert(geomStmt.step() === DbResult.BE_SQLITE_ROW, source.nativeDb.getLastError());
      const geomStreamVal = geomStmt.getValue(0);
      const geomStream = geomStreamVal.isNull ? undefined : geomStreamVal.getBlob();

      const elemJsonString = sourceElemFirstPassReader.current[0] as string;
      const elemJson = JSON.parse(elemJsonString);
      const elemClass = sourceElemFirstPassReader.current[1];
      const sourceId = sourceElemFirstPassReader.current[2];
      const modelJsonString = sourceElemFirstPassReader.current[3];
      const modelJson = modelJsonString !== undefined && JSON.parse(modelJsonString);
      const modelClass = sourceElemFirstPassReader.current[4];

      const elemInsertQuery = queryMap.insert.get(elemClass);
      assert(elemInsertQuery, `couldn't find insert query for class '${elemClass}'`);
      const elemBinaryPropsQuery = queryMap.selectBinaries.get(elemClass);
      assert(elemBinaryPropsQuery, `couldn't find select binary props query for class '${elemClass}'`);

      const binaryValues = elemBinaryPropsQuery(source, sourceId);

      const targetId = numIdToId64(remapTables.element.get(id64ToNumId(sourceId)));
      elemJson.ECInstanceId = targetId;
      modelJson.ECInstanceId = targetId;

      elemInsertQuery(
        writeableTarget,
        targetId,
        elemJson,
        elemJsonString,
        {
          ...binaryValues,
          ...geomStream && { GeometryStream: geomStream },
        },
        { GeometryStream: geomStream?.byteLength ?? 0 },
        { id: sourceId, db: source },
      );

      if (modelJson) {
        const modelInsertQuery = queryMap.insert.get(modelClass);
        assert(modelInsertQuery, `couldn't find insert query for class '${modelClass}'`);

        // FIXME: not yet handling binary properties on these
        modelInsertQuery(writeableTarget, targetId, modelJson, modelJsonString);
      }

      // FIXME: doesn't support briefcase ids > 2**13 - 1
      remapTables.element.remap(parseInt(sourceId, 16), parseInt(targetId, 16));

      incrementStmtsExeced();
    }
  });

  const sourceAspectSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.ElementAspect
  `;

  console.log("insert aspects");
  const aspectReader = source.createQueryReader(sourceAspectSelect, undefined, { abbreviateBlobs: true });
  while (await aspectReader.step()) {
    const jsonString = aspectReader.current[0];
    const json = JSON.parse(jsonString);
    const classFullName = aspectReader.current[1];
    const sourceId = aspectReader.current[2];

    const selectBinariesQuery = queryMap.selectBinaries.get(classFullName);
    assert(selectBinariesQuery, `couldn't find select binary properties query for class '${classFullName}`);
    const insertQuery = queryMap.insert.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

    const binaryValues = selectBinariesQuery(source, sourceId);

    const targetId = insertQuery(writeableTarget, useInstanceId(), json, jsonString, binaryValues, { id: sourceId, db: source });

    // FIXME: do we even need aspect remap tables anymore? I don't remember
    // FIXME: doesn't support briefcase ids > 2**13 - 1
    remapTables.aspect.remap(parseInt(sourceId, 16), parseInt(targetId, 16));

    incrementStmtsExeced();
  }

  const elemRefersSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.ElementRefersToElements
  `;

  console.log("insert ElementRefersToElements");
  const elemRefersReader = source.createQueryReader(elemRefersSelect, undefined, { abbreviateBlobs: true });
  while (await elemRefersReader.step()) {
    const jsonString = elemRefersReader.current[0];
    const json = JSON.parse(jsonString);
    const classFullName = elemRefersReader.current[1];
    const sourceId = elemRefersReader.current[2];

    const insertQuery = queryMap.insert.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

    const selectBinariesQuery = queryMap.selectBinaries.get(classFullName);
    assert(selectBinariesQuery, `couldn't find select binary properties query for class '${classFullName}`);
    const binaryValues = selectBinariesQuery(source, sourceId);

    insertQuery(writeableTarget, useInstanceId(), json, jsonString, binaryValues, { id: sourceId, db: source });

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

  const makeGetter = (table: CompactRemapTable) => (id: Id64String) => {
    const targetIntId = table.get(parseInt(id, 16));
    return targetIntId === 0 || targetIntId === undefined ? "0" : `0x${targetIntId.toString(16)}`;
  };

  if (returnRemapper) {
    remapper = {
      findTargetElementId: makeGetter(remapTables.element),
      findTargetAspectId: makeGetter(remapTables.aspect),
      findTargetCodeSpecId: makeGetter(remapTables.codespec),
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
