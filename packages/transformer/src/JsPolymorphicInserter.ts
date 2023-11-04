import { BindParameter, ECDb, ECDbOpenMode, ECSqlStatement, IModelDb, SnapshotDb } from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { Property, PropertyType, RelationshipClass, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import { IModelTransformer } from "./IModelTransformer";

/* eslint-disable no-console */

// some high entropy string
const injectionString = "Inject_1243yu1";
const injectExpr = (s: string) => `(SELECT '${injectionString} ${escapeForSqlStr(s)}')`;

const getInjectedSqlite = (query: string, db: ECDb) => {
  try {
    return db.withPreparedStatement(query, (stmt) => {
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
    /** extra bindings are ignored if they do not exist in the class */
    extraBindings?: Record<keyof PopulateExtraBindings, any>
  ) => Id64String>;
  insert: Map<string, (db: ECDb, jsonString: string, source?: { id: Id64String, db: IModelDb }) => Id64String>;
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
    if (empty)
      return `coalesce(HexToId(JSON_EXTRACT(:x, '$.${accessStr ?? `${p.name}${navProp ? ".Id" : ""}`}')), ${empty})`;
    return `HexToId(JSON_EXTRACT(:x, '$.${accessStr ?? `${p.name}${navProp ? ".Id" : ""}`}'))`;
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

            // FIXME: how to handle this?
            //: p.propertyType === PropertyType.Binary
            //? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`

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

    const populateQuery = `
      INSERT INTO ${escapedClassFullName}
      (${properties
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
        )
        .concat(populateBindings.map(([name]) => name))
        .join(",\n  ")
      })
      VALUES
      (${properties
        .filter((p) => !(p.name in (options.extraBindings?.populate ?? {})))
        .map((p) =>
          // FIXME: do qualified check for exact schema of CodeValue prop
          p.name === "CodeValue"
          ? "NULL"
          : p.propertyType === PropertyType.DateTime
          ? `JSON_EXTRACT(:x, '$.${p.name}')`
          : p.propertyType === PropertyType.Navigation || p.propertyType === PropertyType.Long
          ? "0x1"
          // FIXME: need a sqlite extension for base64 decoding of binary...
          // : p.propertyType === PropertyType.Binary
          : p.propertyType === PropertyType.Point2d
          ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y')`
          : p.propertyType === PropertyType.Point3d
          ? `JSON_EXTRACT(:x, '$.${p.name}.x'), JSON_EXTRACT(:x, '$.${p.name}.y'), JSON_EXTRACT(:x, '$.${p.name}.z')`
          : `JSON_EXTRACT(:x, '$.${p.name}')`
        )
        // FIXME: use the names from the values of the binding object
        .concat(populateBindings.map(([name]) => `:b_${name}`))
        .join(",\n  ")
      })
    `;

    const insertQuery = `
      INSERT INTO ${escapedClassFullName}
      -- FIXME: getting SQLITE_MISMATCH... something weird going on in native
      (
        ${
          [
            { name: "ECInstanceId", propertyType: PropertyType.Long },
            ...properties,
          ].map((p) =>
          // FIXME: note that dynamic structs are completely unhandled
          p.propertyType === PropertyType.Navigation
          ? `[${p.name}].Id, [${p.name}].RelECClassId`
          : p.propertyType === PropertyType.Point2d
          ? `[${p.name}].x, [${p.name}].y`
          : p.propertyType === PropertyType.Point3d
          ? `[${p.name}].x, [${p.name}].y, [${p.name}].z`
          : p.name
        )
        .join(",\n  ")
      })
      VALUES (
      ${
        // could try incrementing from this (emulating briefcase behavior)
        //SELECT Val + 1
        //FROM be_Local
        //WHERE Name='bis_instanceidsequence'
        injectExpr(`
          /* FIXME: this obviously only works with empty targets */
          HexToId(JSON_EXTRACT(:x, '$.ECInstanceId'))
        `)}
      ${properties.length > 0 ? "," : "" /* FIXME: join instead */}
      ${properties
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
        )
        .join(",\n  ")
      })
    `;
    /* eslint-enable @typescript-eslint/indent */

    /* eslint-enable @typescript-eslint/indent */

    function populate(
      ecdb: ECDb,
      jsonString: string,
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

    function insert(ecdb: ECDb, jsonString: string, source?: { id: string, db: IModelDb }) {
      // NEXT FIXME: doesn't work on some relationships, need to explicitly know if it's a rel
      // class and then always add source/target to INSERT
      let hackedRemapInsertSql;
      try {
        // HACK: create hybrid sqlite/ecsql query
        hackedRemapInsertSql = getInjectedSqlite(insertQuery, ecdb);

        for (const sql of hackedRemapInsertSql.split(";")) {
          ecdb.withPreparedSqliteStatement(sql, (targetStmt) => {
            targetStmt.bindString(":x", jsonString);
            assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
          });
        }

        // FIXME: this doesn't work at all I believe, need to emulate how platform does it
        return ecdb.withPreparedSqliteStatement(`
          SELECT Val
          FROM be_Local
          WHERE Name='bis_elementidsequence'
        `, (s) => {
          assert(s.step() === DbResult.BE_SQLITE_ROW);
          return s.getValue(0).getId();
        });
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

    function update(
      ecdb: ECDb,
      jsonString: string,
      source?: { id: string, db: IModelDb },
      bindingValues: {[S in keyof UpdateExtraBindings]?: any} = {},
    ) {
      if (updateQuery === "")
        return; // ignore empty updates

      // HACK: create hybrid sqlite/ecsql query
      const hackedRemapUpdateSql = getInjectedSqlite(updateQuery, ecdb);

      try {
        for (const sql of hackedRemapUpdateSql.split(";")) {
          ecdb.withPreparedSqliteStatement(sql, (targetStmt) => {
            targetStmt.bindString(":x", jsonString);
            // FIXME: note that sometimes x_col1 is also defined...

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
  await schemaExporter.processSchemas();
  schemaExporter.processFonts();
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

  writeableTarget.withSqliteStatement(`
    ATTACH DATABASE 'file://${source.pathName}?mode=ro' AS source
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  writeableTarget.withPreparedSqliteStatement(`
    INSERT INTO temp.element_remap VALUES(0x1,0x1), (0xe,0xe), (0x10, 0x10)
  `, (targetStmt) => {
    assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
  });

  // FIXME: this doesn't work... using a workaround of setting all references to 0x0
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

  const sourceElemSelect = (needGeometry = false) => `
    SELECT e.$, ec_classname(e.ECClassId, 's.c'), e.ECInstanceId, CAST(e.FederationGuid AS Binary),
           m.$, ec_classname(m.ECClassId, 's.c')
    ${needGeometry ? `
          , coalesce(
              g3d.GeometryStream,
              g2d.GeometryStream,
              gp.GeometryStream
            )
    ` : ""}
    FROM bis.Element e
    -- FIXME: is it faster to use the new $->Blah syntax?
    LEFT JOIN bis.Model m ON e.ECInstanceId=m.ECInstanceId
    ${needGeometry ? `
    LEFT JOIN bis.GeometricElement3d g3d ON e.ECInstanceId=g3d.ECInstanceId
    LEFT JOIN bis.GeometricElement2d g2d ON e.ECInstanceId=g2d.ECInstanceId
    LEFT JOIN bis.GeometryPart       gp ON e.ECInstanceId=gp.ECInstanceId
    ` : ""}
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    -- FIXME: ordering by class *might* be faster due to less cache busting
    -- ORDER BY ECClassId, ECInstanceId ASC
    ORDER BY e.ECInstanceId ASC
  `;

  const startTime = performance.now();
  let stmtsExeced = 0;
  const incrementStmtsExeced = () => {
    stmtsExeced += 1;
    const elapsedMs = performance.now() - startTime;
    if (stmtsExeced % 1000 === 0)
      console.log(`executed ${stmtsExeced} statements at ${elapsedMs/1000}s`);
  };

  // first pass, update everything with trivial references (0 and null codes)
  // FIXME: technically could do it all in one pass if we preserve distances between rows and
  // just offset all references by the count of rows in the source...
  //
  // Might be useful to still do two passes though in a filter-heavy transform... we can always
  // do the offsetting in the first pass, and then decide during the pass if there is too much sparsity
  // in the IDs and redo it?
  console.log("populate elements");
  const sourceElemFirstPassReader = source.createQueryReader(sourceElemSelect(), undefined, { usePrimaryConn: true, abbreviateBlobs: false });
  while (await sourceElemFirstPassReader.step()) {
    const elemJson = sourceElemFirstPassReader.current[0];
    const elemClass = sourceElemFirstPassReader.current[1];
    const sourceId = sourceElemFirstPassReader.current[2];
    const federationGuid = sourceElemFirstPassReader.current[3];
    const modelJson = sourceElemFirstPassReader.current[4];
    const modelClass = sourceElemFirstPassReader.current[5];

    const elemPopulateQuery = queryMap.populate.get(elemClass);
    assert(elemPopulateQuery, `couldn't find insert query for class '${elemClass}'`);

    const targetId = elemPopulateQuery(writeableTarget, elemJson, { FederationGuid: federationGuid });

    if (modelJson) {
      const modelInsertQuery = queryMap.insert.get(modelClass);
      assert(modelInsertQuery, `couldn't find insert query for class '${modelClass}'`);

      // HACK: edit packaged json without parsing
      const modelJsonWithTargetId = modelJson.replace(/(?<="ECInstanceId":")[^"]+(?=")/, targetId);
      modelInsertQuery(writeableTarget, modelJsonWithTargetId);
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

  // second pass, update now that everything has been inserted
  console.log("hydrate elements");
  const sourceElemSecondPassReader = source.createQueryReader(sourceElemSelect(true), undefined, { usePrimaryConn: true });
  while (await sourceElemSecondPassReader.step()) {
    const jsonString = sourceElemSecondPassReader.current[0];
    const classFullName = sourceElemSecondPassReader.current[1];
    const sourceId = sourceElemSecondPassReader.current[2];
    const geometryStream = sourceElemSecondPassReader.current[6];

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
  const aspectReader = source.createQueryReader(sourceAspectSelect, undefined, { usePrimaryConn: true });
  while (await aspectReader.step()) {
    const jsonString = aspectReader.current[0];
    const classFullName = aspectReader.current[1];
    const sourceId = aspectReader.current[2];

    const insertQuery = queryMap.insert.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

    const _targetId = insertQuery(writeableTarget, jsonString, { id: sourceId, db: source });

    writeableTarget.withPreparedSqliteStatement(`
      INSERT INTO temp.aspect_remap VALUES(?,?)
    `, (targetStmt) => {
      // HACK: returned id is broken with sql_mismatch so just reusing source id
      targetStmt.bindId(1, sourceId);
      targetStmt.bindId(2, sourceId);
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
    });

    incrementStmtsExeced();
  }

  const elemRefersSelect = `
    SELECT $, ec_classname(ECClassId, 's.c'), ECInstanceId
    FROM bis.ElementRefersToElements
  `;

  console.log("insert ElementRefersToElements");
  const elemRefersReader = source.createQueryReader(elemRefersSelect, undefined, { usePrimaryConn: true });
  while (await elemRefersReader.step()) {
    const jsonString = elemRefersReader.current[0];
    const classFullName = elemRefersReader.current[1];
    const sourceId = elemRefersReader.current[2];

    const insertQuery = queryMap.insert.get(classFullName);
    assert(insertQuery, `couldn't find insert query for class '${classFullName}`);

    insertQuery(writeableTarget, jsonString, { id: sourceId, db: source });

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

  // FIXME: detach... readonly attached db gets write-locked for some reason
  writeableTarget.withSqliteStatement(`
    DETACH remaps
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_ERROR));


  writeableTarget.clearStatementCache(); // so we can detach attached db
  writeableTarget.saveChanges();
  writeableTarget.closeDb();

  return remapper;
}
