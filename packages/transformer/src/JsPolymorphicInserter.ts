import { ECDb, ECDbOpenMode, ECSqlStatement, IModelDb, SqliteStatement } from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { ECClass, ECClassModifier, PrimitiveOrEnumPropertyBase, Property, PropertyType, RelationshipClass, SchemaItemType, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import * as url from "url";
import * as path from "path";
import * as fs from "fs";
import { IModelTransformer } from "./IModelTransformer";
import { CompactRemapTable } from "./CompactRemapTable";

// NOTES:
// missing things:
// - arrays/struct properties
// - non-geometry binary

/* eslint-disable no-console, @itwin/no-internal */

// FIXME: injection not really used anymore, but the nativesql stealing is
// some high entropy string
const injectionString = "Inject_1243yu1";
const injectExpr = (s: string, type = "Integer") => `(CAST ((SELECT '${injectionString} ${escapeForSqlStr(s)}') AS ${type}))`;

const getInjectedSqlite = (db: ECDb | IModelDb, query: string) => {
  return db.withStatement(query, (stmt) => {
    const nativeSql = stmt.getNativeSql();
    return nativeSql.replace(
      new RegExp(`\\(SELECT '${injectionString} (.*?[^']('')*)'\\)`, "gs"),
      (_, p1) => unescapeSqlStr(p1),
    );
  });
};

// FIXME: note that SQLite doesn't seem to have types/statistics that would let it consider using
// an optimized binary search for our range query, so we should not do this via SQLite. Once we
// get around to designing how we'll pass a JavaScript object to RemapGeom, then we can fix that.
// That said, this should be pretty fast in our cases here regardless, since the table _should_
// scale with briefcase count well
const remapSql = (idExpr: string, remapType: "font" | "codespec" | "nonElemEntity" | "element") => `(
  SELECT TargetId + ((${idExpr}) - SourceId)
  FROM temp.${remapType}_remap
  WHERE ${idExpr} BETWEEN SourceId AND SourceId + Length - 1
)`;

const escapeForSqlStr = (s: string) => s.replace(/'/g, "''");
const unescapeSqlStr = (s: string) => s.replace(/''/g, "'");

interface PropInfo {
  name: Property["name"];
  propertyType: Property["propertyType"];
  extendedTypeName?: PrimitiveOrEnumPropertyBase["extendedTypeName"];
}

type CachedClassStatements = {
  remap: string;
  select: string;
  insert: string[];
  transform: string[];
} | "NON_TRANSFORMABLE_CLASS";

const statementCachePath = path.join(__dirname, ".transformer_statement_cache.json");

/**
 * not yet but eventually to be loaded from disk
 * key is `SchemaFullName@Version`
 * FIXME: dynamic schemas should not be cached
 */
const statementCache = new Map<string, Map<string, CachedClassStatements>>();

async function bulkInsertTransform(
  source: IModelDb,
  target: ECDb,
  propertyTransforms: {
    [propertyName: string]: (s: string) => string;
  } = {},
): Promise<void> {
  const schemaNamesReader = source.createQueryReader(`
    SELECT Name, VersionMajor, VersionWrite, VersionMinor
    FROM ECDbMeta.ECSchemaDef
  `);

  const schemas: { name: string, version: string }[] = [];
  while (await schemaNamesReader.step()) {
    schemas.push({
      name: schemaNamesReader.current[0],
      version: `${schemaNamesReader.current[1]}.${schemaNamesReader.current[2]}.${schemaNamesReader.current[3]}`,
    });
  }

  interface ClassData {
    /** `SchemaName@schemaVersion` */
    schemaKey: string;
    properties: PropInfo[];
    rootType: "element" | "aspect" | "codespec" | "relationship";
  }

  const schemaLoader = new SchemaLoader((name: string) => source.getSchemaProps(name));
  const classData = new Map<string, ClassData>();

  const bis = schemaLoader.getSchema("BisCore");
  const [multiAspect, uniqueAspect] = await Promise.all([
    bis.getItem("ElementMultiAspect") as Promise<ECClass>,
    bis.getItem("ElementUniqueAspect") as Promise<ECClass>,
  ]);
  assert(multiAspect && uniqueAspect);

  // NEXT: only do this on leaf classes so that we do not have id collisions

  for (const { name: schemaName, version } of schemas) {
    const schemaKey = `${schemaName}@${version}`;
    if (statementCache.has(schemaKey))
      continue;

    const schemaStmts = new Map<string, CachedClassStatements>();
    statementCache.set(schemaKey, schemaStmts);

    const schema = schemaLoader.getSchema(schemaName);

    for (const ecclass of schema.getClasses()) {
      // if transformable, overwritten in next loop
      schemaStmts.set(ecclass.name,  "NON_TRANSFORMABLE_CLASS");

      if (!(
        ecclass.schemaItemType === SchemaItemType.EntityClass
        || ecclass.schemaItemType === SchemaItemType.RelationshipClass
      )) {
        continue;
      }

      // FIXME: skip navigation property only relationships
      // FIXME: skip non-insertable custom mapped ec classes

      if (ecclass.modifier === ECClassModifier.Abstract)
        continue;

      const properties: PropInfo[] = [...await ecclass.getProperties()];

      if (ecclass instanceof RelationshipClass) {
        properties.push({
          name: "SourceECInstanceId",
          propertyType: PropertyType.Long,
        });
        properties.push({
          name: "TargetECInstanceId",
          propertyType: PropertyType.Long,
        });
      } else {
        properties.push({
          name: "ECInstanceId",
          propertyType: PropertyType.Long,
        });
      }

      // FIXME: remove this broken rule
      /* eslint-disable */
      classData.set(ecclass.fullName, {
        schemaKey,
        properties,
        rootType: ecclass.schemaItemType === SchemaItemType.RelationshipClass
          ? "relationship"
          : ecclass.fullName === "BisCore.CodeSpec"
          ? "codespec"
          // FIXME: async shortcircuit or?
          : ecclass.isSync(multiAspect) || ecclass.isSync(uniqueAspect)
          ? "aspect"
          : "element"
      });
      /* eslint-enable */
    }
  }

  const classInserters = new Map<string, {
    schemaName: string;
    className: string;
    run: () => void;
  }>();

  for (const [classFullName, { properties, rootType, schemaKey }] of classData) {
    try {
      const [schemaName, className] = classFullName.split(".");
      const escapedClassFullName = `[${schemaName}].[${className}]`;

      // TODO FIXME: support this
      const excludedPropertyTypes = new Set([
        PropertyType.Struct,
        PropertyType.Struct_Array,
        PropertyType.Binary_Array,
        PropertyType.Boolean_Array,
        PropertyType.DateTime_Array,
        PropertyType.Double_Array,
        PropertyType.Integer_Array,
        PropertyType.Integer_Enumeration_Array,
        PropertyType.Long_Array,
        PropertyType.Point2d_Array,
        PropertyType.Point3d_Array,
        PropertyType.String_Array,
        PropertyType.String_Enumeration_Array,
        PropertyType.IGeometry_Array,
      ]);

      const queryProps = properties.filter((p) => !excludedPropertyTypes.has(p.propertyType));

      /* eslint-disable @typescript-eslint/indent */
      const selectSql = getInjectedSqlite(source, `
        SELECT
          ${queryProps.map((p) =>
              p.propertyType === PropertyType.Navigation
              ? [`[${p.name}].Id`, `[${p.name}].RelECClassId`]
              : p.propertyType === PropertyType.Point2d
              ? [`[${p.name}].X`, `[${p.name}].Y`]
              : p.propertyType === PropertyType.Point3d
              ? [`[${p.name}].X`, `[${p.name}].Y`, `[${p.name}].Z`]
              : [`[${p.name}]`]
            )
          .flat()
          .map((expr, i) => `(${expr}) AS _${i + 1}`) // ecsql indexes are one-indexed
          .join(",\n  ")}
        FROM ONLY ${escapedClassFullName}
        -- TODO: add reality data dictionary
        WHERE ECInstanceId NOT IN (0x1, 0xe, 0x10)
      `);

      assert(!selectSql.includes(";"));

      // HACK: can increment this everytime we use it if we use it once per expression
      // also pre-increment for one-indexing
      let j = 0;
      const remappedSql = `
        SELECT
          ${queryProps.map((p) =>
              p.name in propertyTransforms
              ? [propertyTransforms[p.name](`_${++j}`)]
              : p.propertyType === PropertyType.Navigation
              ? [
                  // FIXME: need to use ECReferenceTypesCache to get type of reference, might not be an elem
                  // FIXME: detect exact property name for this, currently a hack
                  remapSql(`[_${++j}]`, p.name === "CodeSpec" ? "codespec" : "element"),
                  `(
                    -- FIXME: do this during remapping after schema processing!
                    SELECT tc.Id
                    FROM source.ec_Class sc
                    JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
                    JOIN main.ec_Schema ts ON ts.Name=ss.Name
                    JOIN main.ec_Class tc ON tc.Name=sc.Name
                    -- FIXME: need to derive the column containing the RelECClassId
                    WHERE sc.Id=[_${++j}]
                  )`,
                ]
              // FIXME: use ECReferenceTypesCache to determine which remap table to use
              : p.propertyType === PropertyType.Long // FIXME: check if Id subtype
              // FIXME: support relationships!
              ? [remapSql(`_${++j}`, rootType === "relationship" || rootType === "aspect" ? "nonElemEntity" : rootType)]
              : p.propertyType === PropertyType.Point2d
              ? [`[_${++j}]`, `[_${++j}]`]
              : p.propertyType === PropertyType.Point3d
              ? [`[_${++j}]`, `[_${++j}]`, `[_${++j}]`]
              : [`[_${++j}]`]
            )
          .flat()
          .map((expr, i) => `(${expr}) AS _r${i + 1}`) // ecsql indexes are one-indexed
          .join(",\n  ")}
        FROM (
          ${selectSql}
        )
      `;

      const allInsertSql = getInjectedSqlite(target, `
        INSERT INTO ${escapedClassFullName} (
          ${queryProps.map((p) =>
              p.propertyType === PropertyType.Navigation
                // FIXME: need to use ECReferenceTypesCache to get type of reference, might not be an elem
              ? [`[${p.name}].Id`, `[${p.name}].RelECClassId`]
              : p.propertyType === PropertyType.Point2d
              ? [`[${p.name}].X`, `[${p.name}].Y`]
              : p.propertyType === PropertyType.Point3d
              ? [`[${p.name}].X`, `[${p.name}].Y`, `[${p.name}].Z`]
              : [`[${p.name}]`]
            )
          .flat()
          .join(",\n  ")}
        ) VALUES (
          ${queryProps.map((p) =>
              p.propertyType === PropertyType.Navigation
                // FIXME: need to use ECReferenceTypesCache to get type of reference, might not be an elem
              ?  ["?","?"]
              : p.propertyType === PropertyType.Point2d
              ?  ["?","?"]
              : p.propertyType === PropertyType.Point3d
              ?  ["?","?","?"]
              : ["?"]
            )
          .flat()
          .join(",")}
        )
      `);
      /* eslint-enable @typescript-eslint/indent */

      const insertSqls = allInsertSql.split(";");

      const remappedFromAttached = remappedSql.replace(/\[main\]\./g, "[source].");

      const bulkInsertSqls = insertSqls.map((insertSql) => {
        // FIXME: just use ec_Table/ec_PropertyMap in the sqlite table!
        const [, insertHeader, _colNames, values] = /(^.*\(([^)]*)\))\s*VALUES\s*\(([^)]*)\)/.exec(insertSql)!;
        const mappedValues = values
          .split(",")
          .map((b) => [b, /_ix(\d+)_/.exec(b)?.[1]])
          // FIXME: is this even correct?
          .map(([b, maybeColIdx]) => maybeColIdx !== undefined ? `_r${maybeColIdx}` : b)
          // HACK: just a guess at how the ECInstance is laid out
          .map((b) => rootType !== "relationship" && b === ":_ecdb_ecsqlparam_id_col1" ? `_r${j} /*was: ${b}*/` : b)
          .join(",")
        ;

        const bulkInsertSql = `
          ${insertHeader}
          SELECT ${mappedValues}
          FROM (${remappedFromAttached})
          WHERE true -- necessary for sqlite's ON CONFLICT syntax
          ${
          /* FIXME: I think this can be removed after fixing the remapping */
          insertHeader.includes("[bis_CodeSpec]") ? `
            ON CONFLICT([main].[bis_CodeSpec].[Name]) DO NOTHING
          ` : ""}
        `;

        // FIXME: need to replace the last from with the attached source
        return bulkInsertSql;
      });

      statementCache.get(schemaKey)!.set(className, {
        select: selectSql,
        remap: remappedFromAttached,
        insert: insertSqls,
        transform: bulkInsertSqls,
      });

      let lastSql: string;
      const classInsert = () => {
        try {
          // eslint-disable-next-line
          for (const sql of bulkInsertSqls) {
            lastSql = sql;
            target.withPreparedSqliteStatement(sql, (targetStmt) => {
              assert(targetStmt.step() === DbResult.BE_SQLITE_DONE, target.nativeDb.getLastError());
            });
          }
        } catch (err) {
          console.log("lastSql:", lastSql);
          console.log("ERROR", target.nativeDb.getLastError());
          console.log("class:", classFullName);
          debugger;
          throw err;
        }
      };

      classInserters.set(classFullName, {
        schemaName,
        className,
        run: classInsert,
      });
    } catch (err: any) {
      if (/Use the respective navigation property to modify it\.$/.test(err.message))
        continue;
      if (/is mapped to an existing table not owned by ECDb/.test(err.message))
        continue;
      debugger;
      throw err;
    }
  }

  await fs.promises.writeFile(statementCachePath, JSON.stringify(statementCache, (_k, v) =>
    v instanceof Map
      ? Object.fromEntries(v.entries())
      : v
  ));

  {
    for (const [classFullName, { run, schemaName, className }] of classInserters) {
      const count = source.withStatement(`SELECT COUNT(*) FROM [${schemaName}].[${className}]`, (s) => {
        assert(s.step() === DbResult.BE_SQLITE_ROW);
        return s.getValue(0).getInteger();
      });
      console.log(`inserting ${count} instances of ${classFullName}`);
      run();
    }
  }
}

// FIXME: consolidate with assertIdentityTransform test, and maybe hide this return type
export interface Remapper {
  findTargetElementId(s: Id64String): Id64String;
  findTargetCodeSpecId(s: Id64String): Id64String;
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
  // HACK: avoid expensive initialize ECReferenceTypesCache
  schemaExporter.context.initialize = async () => {};
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

  source.withPreparedStatement("PRAGMA experimental_features_enabled = true", (s) => assert(s.step() !== DbResult.BE_SQLITE_ERROR));

  const targetPath = target.pathName;
  target.saveChanges();
  target.close();
  const writeableTarget = new ECDb();
  writeableTarget.openDb(targetPath, ECDbOpenMode.ReadWrite);

  const remapTables = {
    element: new CompactRemapTable(),
    nonElemEntity: new CompactRemapTable(),
    codespec: new CompactRemapTable(),
    font: new CompactRemapTable(),
  };

  for (const name of ["element", "codespec", "nonElemEntity", "font"] as const) {
    // FIXME: don't do it in both connections ! currently due to blobio we need the
    // remap table in both the source connection to use RemapGeom and the target
    // connection for our remapping queries
    for (const db of [source, writeableTarget]) {
      db.withSqliteStatement(`
        CREATE TEMP TABLE ${name}_remap (
          SourceId INTEGER NOT NULL PRIMARY KEY,
          TargetId INTEGER NOT NULL,
          Length INTEGER NOT NULL
        )
      `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));
    }

    // always remap 0 to 0
    remapTables[name].remap(0, 0);
  }

  // fill already exported fonts
  for (const [sourceId, targetId] of fontRemaps) {
    remapTables.font.remap(sourceId, targetId);
  }

  writeableTarget.withSqliteStatement(`
    ATTACH DATABASE '${url.pathToFileURL(source.pathName)}?mode=ro' AS source
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE));

  remapTables.element.remap(1, 1);
  remapTables.element.remap(0xe, 0xe);
  remapTables.element.remap(0x10, 0x10);

  writeableTarget.withPreparedSqliteStatement(`
    PRAGMA defer_foreign_keys = true;
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

  let [_nextElemId, _nextInstanceId] = ["bis_elementidsequence", "ec_instanceidsequence"]
    .map((seq) => writeableTarget.withSqliteStatement(`
      SELECT Val
      FROM be_Local
      WHERE Name='${seq}'
    `, (s) => {
      assert(s.step() === DbResult.BE_SQLITE_ROW, writeableTarget.nativeDb.getLastError());
      // FIXME: check if this needs to be + 1
      return parseInt(s.getValue(0).getId(), 16) + 1;
    }));

  // FIXME: doesn't support high briefcase ids (> 2 << 13)!
  const useElemIdRaw = () => _nextElemId++;
  const _useElemId = () => `0x${useElemIdRaw().toString(16)}`;

  // NOTE: I could do less querying if I LEFT JOIN the appropriate tables together
  const useInstanceIdRaw = () => _nextInstanceId++;
  const _useInstanceId = () => `0x${useInstanceIdRaw().toString(16)}`;

  {
    const sourceElemRemapSelect = `
      SELECT e.ECInstanceId
      FROM bis.Element e
      WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
      -- FIXME: CompactRemapTable is slow if this is not ordered
      ORDER BY e.ECInstanceId ASC
    `;

    console.log("generate element remap tables");
    const sourceElemRemapPassReader = source.createQueryReader(sourceElemRemapSelect);
    while (await sourceElemRemapPassReader.step()) {
      const sourceId = sourceElemRemapPassReader.current[0] as Id64String;
      const targetId = useElemIdRaw();
      remapTables.element.remap(parseInt(sourceId, 16), targetId);
    }
  }

  {
    const nonElemEntityRemapSelect = `
      SELECT ECInstanceId FROM (
        -- we know that aspects and entities (link table relationships such as ERtE)
        -- share an Id space, so this is valid
        SELECT ECInstanceId FROM bis.ElementAspect a
        UNION ALL
        SELECT ECInstanceId FROM bis.ElementRefersToElements r
      )
      WHERE ECInstanceId IS NOT NULL
      -- FIXME: CompactRemapTable is slow if this is not ordered
      -- FIXME: test if this can be faster, e.g. try ORDER BY a.ECInstanceId, r.ECInstanceId
      ORDER BY ECInstanceId
    `;

    console.log("generate Element*Aspect, ElementRefersToElements remap tables");
    const nonElemEntityRemapReader = source.createQueryReader(nonElemEntityRemapSelect);
    while (await nonElemEntityRemapReader.step()) {
      const sourceId = nonElemEntityRemapReader.current[0] as Id64String;
      const targetId = useInstanceIdRaw();
      remapTables.nonElemEntity.remap(parseInt(sourceId, 16), targetId);
    }
  }

  let _nextCodeSpecId = writeableTarget.withStatement(`
    SELECT Max(ECInstanceId) FROM Bis.CodeSpec
  `, (s) => {
    assert(s.step() === DbResult.BE_SQLITE_ROW, writeableTarget.nativeDb.getLastError());
    return parseInt(s.getValue(0).getId(), 16) + 1;
  });

  const useCodeSpecIdRaw = () => _nextCodeSpecId++;
  const _useCodeSpecId = () => `0x${useCodeSpecIdRaw().toString(16)}`;

  {
    const codeSpecRemapSelect = `
      SELECT s.Id, t.Id
      FROM bis_CodeSpec s
      LEFT JOIN main.bis_CodeSpec t ON s.Name=t.Name
    `;

    console.log("generate codespec remap tables");
    source.withPreparedSqliteStatement(codeSpecRemapSelect, (stmt) => {
      while (stmt.step() === DbResult.BE_SQLITE_ROW) {
        const sourceId = stmt.getValue(0).getId();
        const maybeTargetId = stmt.getValue(1).getId() as Id64String | undefined;
        const targetIdInt = maybeTargetId ? parseInt(maybeTargetId, 16) : useCodeSpecIdRaw();
        remapTables.codespec.remap(parseInt(sourceId, 16), targetIdInt);
      }
    });
  }

  // give sqlite the tables
  for (const name of ["element", "codespec", "nonElemEntity", "font"] as const) {
    for (const run of remapTables[name].runs()) {
      for (const db of [source, writeableTarget]) {
        db.withPreparedSqliteStatement(`
          INSERT INTO temp.${name}_remap VALUES(?,?,?)
        `, (targetStmt) => {
          targetStmt.bindInteger(1, run.from);
          targetStmt.bindInteger(2, run.to);
          targetStmt.bindInteger(3, run.length);
          assert(targetStmt.step() === DbResult.BE_SQLITE_DONE);
        });
      }
    }
  }

  await bulkInsertTransform(source, writeableTarget, {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    GeometryStream: (s) => `RemapGeom(${s}, 'temp.font_remap', 'temp.element_remap')`,
  });

  writeableTarget.withPreparedSqliteStatement(`
    PRAGMA defer_foreign_keys = false;
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
