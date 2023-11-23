import { ECDb, ECDbOpenMode, ECSqlStatement, IModelDb, IModelHost, IModelJsNative, SqliteStatement } from "@itwin/core-backend";
import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { ECClassModifier, PrimitiveOrEnumPropertyBase, Property, PropertyType, RelationshipClass, SchemaItemType, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import * as url from "url";
import { IModelTransformer } from "./IModelTransformer";
import { CompactRemapTable } from "./CompactRemapTable";
import { Mutex, Semaphore } from "async-mutex";

// NOTES:
// missing things:
// - arrays/struct properties
// - non-geometry binary

/* eslint-disable no-console, @itwin/no-internal */

// some high entropy string
const injectionString = "Inject_1243yu1";
const injectExpr = (s: string, type = "Integer") => `(CAST ((SELECT '${injectionString} ${escapeForSqlStr(s)}') AS ${type}))`;

const getInjectedSqlite = (db: ECDb | IModelDb, query: string) => {
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
    throw err;
  }
};

// FIXME: note that SQLite doesn't seem to have types/statistics that would let it consider using
// an optimized binary search for our range query, so we should not do this via SQLite. Once we
// get around to designing how we'll pass a JavaScript object to RemapGeom, then we can fix that.
// That said, this should be pretty fast in our cases here regardless, since the table _should_
// scale with briefcase count well
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
  ? [`:p_${p.name}_X`, `:p_${p.name}_Y`, `:p_${p.name}_Z`]
  : p.propertyType === PropertyType.Point2d
  ? [`:p_${p.name}_X`, `:p_${p.name}_Y`]
  : p.propertyType === PropertyType.Binary && p.extendedTypeName !== "BeGuid"
  ? [`:s_${p.name}_col1`]
  : p.propertyType === PropertyType.Navigation
  ? [`:p_${p.name}_Id`, `:p_${p.name}_RelECClassId`]
  : p.name === "SourceECInstanceId" || p.name === "TargetECInstanceId"
  ? [`:p_${p.name}`]
  : [`:p_${p.name}_col1`]
;
/* eslint-enable */

const sqlHasCache = new Map<string, Record<string, boolean>>();

function stmtBindProperty(
  stmt: SqliteStatement | ECSqlStatement,
  prop: { name: string, propertyType: PropertyType, extendedTypeName?: string },
  val: any,
) {
  const bindings = propBindings(prop);
  const binding = bindings[0];
  if (val === undefined)
    return;
  if (prop.propertyType === PropertyType.Long/* && prop.extendedTypeName === "Id"*/)
    return stmt.bindId(binding, val);
  if (prop.propertyType === PropertyType.Binary && prop.extendedTypeName === "BeGuid")
    // FIXME: avoid guid serialization
    return stmt.bindGuid(binding, val);
  if (prop.propertyType === PropertyType.Binary)
    return stmt.bindInteger(binding, (val as Uint8Array).byteLength);
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
  if (prop.propertyType === PropertyType.DateTime) {
    const date = new Date(val as string).valueOf();
    const julianDate = date / 86400000 + 2440587.5;
    return stmt.bindInteger(binding, julianDate); // FIXME: ecsql bindDateTime
  }
  if (prop.propertyType === PropertyType.Navigation) {
    stmt.bindId(bindings[0], val.Id);
    // FIXME: reuse binding detection in caller
    let stmtInfo = sqlHasCache.get(stmt.sql);
    if (stmtInfo === undefined) {
      stmtInfo = { [bindings[1]]: stmt.sql.includes(bindings[1]) };
      sqlHasCache.set(stmt.sql, stmtInfo);
    }
    if (stmtInfo[bindings[1]])
      stmt.bindId(bindings[1], val.RelECClassId);
    return;
  }
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

interface PropInfo {
  name: Property["name"];
  propertyType: Property["propertyType"];
  extendedTypeName?: PrimitiveOrEnumPropertyBase["extendedTypeName"];
}

async function bulkInsertTransform(
  source: IModelDb,
  target: ECDb,
  propertyTransforms: {
    [propertyName: string]: (s: string) => string;
  } = {},
): Promise<void> {
  const schemaNamesReader = source.createQueryReader("SELECT Name FROM ECDbMeta.ECSchemaDef", undefined, { usePrimaryConn: true });

  const schemaNames: string[] = [];
  while (await schemaNamesReader.step()) {
    schemaNames.push(schemaNamesReader.current[0]);
  }

  const schemaLoader = new SchemaLoader((name: string) => source.getSchemaProps(name));
  const classFullNameAndProps = new Map<string, PropInfo[]>();

  for (const schemaName of schemaNames) {
    const schema = schemaLoader.getSchema(schemaName);
    for (const ecclass of schema.getClasses()) {
      if (!(
        ecclass.schemaItemType === SchemaItemType.EntityClass
        || ecclass.schemaItemType === SchemaItemType.RelationshipClass
      )) continue;

      // FIXME: skip navigation property only relationships
      // FIXME: skip non-insertable custom mapped ec classes

      if (ecclass.modifier === ECClassModifier.Abstract)
        continue;

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
      } else {
        classProps.push({
          name: "ECInstanceId",
          propertyType: PropertyType.Long,
        });
      }

      classFullNameAndProps.set(ecclass.fullName, classProps);
    }
  }

  const classInserts = new Map<string, () => void>();

  for (const [classFullName, properties] of classFullNameAndProps) {
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
              p.name in propertyTransforms
              ? [propertyTransforms[p.name](p.name)]
              : p.propertyType === PropertyType.Navigation
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
        FROM ${escapedClassFullName}
      `);

      assert(!selectSql.includes(";"));

      // HACK: can increment this everytime we use it if we use it once per expression
      // also pre-increment for one-indexing
      let i = 0;
      const remappedSql = `
        SELECT
          ${queryProps.map((p) =>
              p.name in propertyTransforms
              ? [propertyTransforms[p.name](`_${++i}`)]
              : p.propertyType === PropertyType.Navigation
              ? [
                  // FIXME: need to use ECReferenceTypesCache to get type of reference, might not be an elem
                  // FIXME: detect exact property name for this, currently a hack
                  remapSql(`[_${++i}]`, p.name === "CodeSpec" ? "codespec" : "element"),
                  `(
                    -- FIXME: do this during remapping after schema processing!
                    SELECT tc.Id
                    FROM source.ec_Class sc
                    JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
                    JOIN main.ec_Schema ts ON ts.Name=ss.Name
                    JOIN main.ec_Class tc ON tc.Name=sc.Name
                    -- FIXME: need to derive the column containing the RelECClassId
                    WHERE sc.Id=[_${++i}]
                  )`,
                ]
              // FIXME: use ECReferenceTypesCache to determine which remap table to use
              : p.propertyType === PropertyType.Long // FIXME: check if Id subtype
              ? [remapSql(`_${++i}`, "element")]
              : p.propertyType === PropertyType.Point2d
              ? [`[_${++i}]`, `[_${++i}]`]
              : p.propertyType === PropertyType.Point3d
              ? [`[_${++i}]`, `[_${++i}]`, `[_${++i}]`]
              : [`[_${++i}]`]
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

      //const remappedFromAttached = remappedSql.replace(/FROM\s*(\[[^\]]\])/, (_full, table) => `source.${table}`);
      const remappedFromAttached = remappedSql.replace(/\[main\]\./g, "[source].");

      const bulkInsertSqls = insertSqls.map((insertSql) => {
        // FIXME: just use ec_Table/ec_PropertyMap in the sqlite table!
        const [, insertHeader, _colNames, values] = /(^.*\(([^)]*)\))\s*VALUES\s*\(([^)]*)\)/.exec(insertSql)!;
        const mappedValues = values
          .split(",")
          .map((b) => [b, /_ix(\d+)_/.exec(b)?.[1]])
          .map(([b, maybeColIdx]) => maybeColIdx !== undefined ? `_r${maybeColIdx}` : b)
        ;

        const bulkInsertSql = `
          ${insertHeader}
          SELECT ${mappedValues}
          FROM (${remappedFromAttached})
        `;

        // FIXME: need to replace the last from with the attached source
        return bulkInsertSql;
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
          console.log("ERROR", target.nativeDb.getLastError());
          console.log("lastSql:", lastSql);
          debugger;
          throw err;
        }
      };

      classInserts.set(classFullName, classInsert);
    } catch (err: any) {
      if (/Use the respective navigation property to modify it\.$/.test(err.message))
        continue;
      if (/is mapped to an existing table not owned by ECDb/.test(err.message))
        continue;
      debugger;
      throw err;
    }
  }

  let j = 0;
  for (const [className, inserter] of classInserts) {
    inserter();
    console.log("inserted all of className", j++);
  }
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
    aspect: new CompactRemapTable(),
    codespec: new CompactRemapTable(),
    font: new CompactRemapTable(),
  };

  for (const name of ["element", "codespec", "aspect", "font"] as const) {
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
      return parseInt(s.getValue(0).getId() + 1, 16);
    }));

  // FIXME: doesn't support high briefcase ids (> 2 << 13)!
  const useElemId = () => `0x${(_nextElemId++).toString(16)}`;

  const sourceElemRemapSelect = `
    SELECT e.ECInstanceId
    FROM bis.Element e
    WHERE e.ECInstanceId NOT IN (0x1, 0xe, 0x10)
    -- FIXME: CompactRemapTable is broken, this must be ordered
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
    remapTables.element.remap(parseInt(sourceId, 16), parseInt(targetId, 16));
  }

  // give sqlite the tables
  for (const name of ["element", "codespec", "aspect", "font"] as const) {
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
