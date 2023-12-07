import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String } from "@itwin/core-bentley";
import { ECClass, ECClassModifier, PrimitiveOrEnumPropertyBase, Property, PropertyType, RelationshipClass, SchemaItemType, SchemaLoader } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import * as url from "url";
import * as fs from "fs";
import { IModelTransformer } from "./IModelTransformer";
import { CompactRemapTable } from "./CompactRemapTable";

interface SourceColumnInfo {
  sqlite: {
    name: string;
    table: string;
  };
  ec: {
    type: PropertyType;
    rootType: "element" | "aspect" | "relationship" | "codespec";
    extendedType: string | undefined;
    schemaName: string;
    className: string;
    accessString: string;
  };
}


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

async function bulkInsertByTable(source: IModelDb, target: ECDb, {
  propertyTransforms = {},
}: {
  propertyTransforms?: {
    [propertyName: string]: (s: string) => string;
  };
} = {}): Promise<void> {

  /** key is table name in target */
  const classDatas = new Map<string, SourceColumnInfo[]>();

  const classRemapsReader = target.createQueryReader(`
    SELECT
        tepp.AccessString
      , teCol.Id
      , teCol.Name as ColumnName
      , tet.Name as TableName
      , teCls.Name as ClassName
      , tes.Name as SchemaName
      -- FIXME: use better, idiomatic root class check (see native extractChangeSets)
      , CASE
          WHEN tet.Name LIKE 'bis_ElementMultiAspect%'
          OR tet.Name LIKE 'bis_ElementUniqueAspect%'
            THEN 'aspect'
          WHEN tet.Name LIKE 'bis_ElementRefersToElements%'
          OR tet.Name LIKE 'bis_ElementDrivesElements%'
            THEN 'relationship'
          WHEN tet.Name='bis_CodeSpec'
            THEN 'codespec'
          ELSE 'element'
        END AS EntityType
    FROM ec_PropertyMap tepm
    JOIN ec_Column eCol ON teCol.Id=tepm.ColumnId
    JOIN ec_PropertyPath tepp ON tepp.Id=tepm.PropertyPathId
    JOIN ec_Class teCls ON teCls.Id=tepm.ClassId
    JOIN ec_Table tet ON tet.Id=teCol.TableId
    JOIN ec_Schema tes ON tes.Id=teCls.SchemaId
    WHERE NOT teCol.IsVirtual
      GROUP BY teCol.Id
  `);

  while (await classRemapsReader.step()) {
    const accessString = classRemapsReader.current[0];
    const sqliteColumnName = classRemapsReader.current[2];
    const sourceTableName = classRemapsReader.current[3];
    const sourceClassName = classRemapsReader.current[4];
    const sourceSchemaName = classRemapsReader.current[5];

    const propertyRootType = classRemapsReader.current[6];

    const targetTableName = classRemapsReader.current[7];
    const propertyType = classRemapsReader.current[8];
    const propertyExtendedType = classRemapsReader.current[9];

    let classData = classDatas.get(targetTableName);
    if (classData === undefined) {
      classData = [];
      classDatas.set(targetTableName, classData);
    }

    classData.push({
      sqlite: {
        name: sqliteColumnName,
        table: sourceTableName,
      },
      ec: {
        type: propertyType,
        rootType: propertyRootType,
        extendedType: propertyExtendedType,
        schemaName: sourceSchemaName,
        className:sourceClassName,
        accessString,
      },
    });
  }

  for (const [targetTableName, sourceColumns] of classDatas) {
    // FIXME: need to join these somehow
    const sourceTables = sourceColumns.reduce((set, c) => set.add(c.sqlite.table), new Set<string>());
    /* eslint-disable @typescript-eslint/indent */
    const transformSql = `
      INSERT INTO ${targetTableName}
      SELECT ${
        sourceColumns
          .map((c) => {
            const propQualifier = `${c.ec.schemaName}.${c.ec.className}.${c.ec.accessString}`;
            const propTransform = propertyTransforms[propQualifier];
            const sourceColumnQualifier = `${c.sqlite.table}.${c.sqlite.name}`;
            return propTransform
              ? propTransform(sourceColumnQualifier)
              : c.ec.type === PropertyType.Navigation
              ? remapSql(sourceColumnQualifier, c.ec.accessString === "CodeSpec" ? "codespec" : "element")
              : c.ec.type === "NavPropRelClassId"
              ? `(
                  -- FIXME: do this during remapping after schema processing!
                  SELECT tc.Id
                  FROM source.ec_Class sc
                  JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
                  JOIN main.ec_Schema ts ON ts.Name=ss.Name
                  JOIN main.ec_Class tc ON tc.Name=sc.Name
                  -- FIXME: need to derive the column containing the RelECClassId
                  WHERE sc.Id=[_${++j}]
                )`
              : remapSql(sourceColumnQualifier,
                c.ec.accessString === "ECInstanceId" // if it's 1, it's the ECInstanceId
                ? c.ec.rootType === "element"
                  ? "element"
                  : "nonElemEntity"
                // FIXME: support non-element-targeting navProps (using ECReferenceTypesCache)
                : "element")
            ;
          })
          .join(",")
      }
      FROM ${[...sourceTables].join(",")}
    `;

    // HACK: can increment this everytime we use it if we use it once per expression
    // also pre-increment for one-indexing
    let j = 0;
    const remappedSql = `
      SELECT
        ${classData.sourceColumns.map((c, i) =>
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
            : [`[_${++j}]`]
          )
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
  }

  return classDatas;
}

/** the trick is, sqlite calls in once per column, with every column
 * we transform the first time, and the rest are no-ops reusing that
 * result until the next row
 */
function _createTransformFunction(
  target: ECDb,
  classDatas: ClassDatas,
  transform: (row: Record<string, any>) => Record<string, any>,
) {
  let callCount = 0;
  let outputRow = {} as Record<string, any>;

  target.nativeDb.addJsDbFunc({
    name: "",
    deterministic: true,
    impl(key, classId, allValues) {
      const classData = classDatas.get(classId);
      assert(classData !== undefined);
      if (callCount === 0) {
        const inputRow = {} as Record<string, any>;
        for (let i = 0; i < allValues.length; ++i) {
          const prop = classData.properties[i];
          const val = allValues[i];
          inputRow[prop.name] = val;
        }
        outputRow = transform(inputRow);
      }

      callCount++;

      if (callCount >= classData.properties.length) {
        callCount = 0;
      }

      return outputRow[key];
    },
  });
}

async function bulkInsertTransform(
  source: IModelDb,
  target: ECDb,
  {
    propertyTransforms = {},
  }: {
    propertyTransforms?: {
      [propertyName: string]: (s: string) => string;
    };
  } = {},
): Promise<void> {
  assert(transformType === "process-all", "only transform 'process-all' supported for now");

  const classDatas = await getClassDatas(source, target);

  const classInserters = new Map<string, {
    schemaName: string;
    className: string;
    run: () => void;
  }>();

  for (const [classFullName, { properties, rootType, schemaKey }] of classDatas) {
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
              ? [remapSql(`_${++j}`,
                  j === 1 // if it's 1, it's the ECInstanceId
                  ? rootType === "element"
                    ? "element"
                    : "nonElemEntity"
                  // FIXME: support non-element-targeting navProps (using ECReferenceTypesCache)
                  : "element")]
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
          .map((b) => b === ":_ecdb_ecsqlparam_id_col1" ? `/*ECInstanceId*/ _r1` : b)
          .join(",")
        ;

        const bulkInsertSql = `
          ${insertHeader}
          SELECT ${mappedValues}
          FROM (${remappedFromAttached})
          WHERE ${transformType === "process-all"
            ? "true" // necessary for SQLite's ON CONFLICT clause
            : "InVirtualSet(?, ECInstanceId)"}
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

    console.log("generate element remap tables", performance.now());
    const sourceElemRemapPassReader = source.createQueryReader(sourceElemRemapSelect);
    while (await sourceElemRemapPassReader.step()) {
      const sourceId = sourceElemRemapPassReader.current[0] as Id64String;
      const targetId = useElemIdRaw();
      remapTables.element.remap(parseInt(sourceId, 16), targetId);
    }
  }

  {
    // use separate queries to prevent EC from unioning the tables, instead we manually do a sorted multiwalk
    const multiAspectRemapSelect = `
      SELECT CAST(ECInstanceId AS INTEGER)
      FROM bis.ElementMultiAspect
      ORDER BY ECInstanceId
    `;

    const uniqueAspectRemapSelect = `
      SELECT CAST(ECInstanceId AS INTEGER)
      FROM bis.ElementUniqueAspect
      ORDER BY ECInstanceId
    `;

    // HACK: will need to send high and low bits to support briefcase id > 13
    const relRemapSelect = `
      SELECT CAST(ECInstanceId AS INTEGER)
      FROM bis.ElementRefersToElements
      ORDER BY ECInstanceId
    `;

    console.log("generate Element*Aspect, ElementRefersToElements remap tables", performance.now());
    // we know that aspects and entities (link table relationships such as ERtE)
    // share an id space, so this is valid
    const multiAspectRemapReader = source.createQueryReader(multiAspectRemapSelect);
    const uniqueAspectRemapReader = source.createQueryReader(uniqueAspectRemapSelect);
    const relRemapReader = source.createQueryReader(relRemapSelect);

    const advanceMultiAspectId = async () => multiAspectRemapReader.step().then((has) => has ? multiAspectRemapReader.current[0] as number : undefined);
    const advanceUniqueAspectId = async () => uniqueAspectRemapReader.step().then((has) => has ? uniqueAspectRemapReader.current[0] as number : undefined);
    const advanceRelId = async () => relRemapReader.step().then((has) => has ? relRemapReader.current[0] as number : undefined);

    // I don't trust the implementation enough to parallelize this since I saw a deadlock
    await advanceMultiAspectId();
    await advanceUniqueAspectId();
    await advanceRelId();

    const remap = (sourceId: number) => {
      const targetId = useInstanceIdRaw();
      remapTables.nonElemEntity.remap(sourceId, targetId);
    };

    while (true) {
      // positive infinity so it's never the minimum
      const multiAspectId = multiAspectRemapReader.done ? Number.POSITIVE_INFINITY : multiAspectRemapReader.current[0] as number;
      const uniqueAspectId = uniqueAspectRemapReader.done ? Number.POSITIVE_INFINITY : uniqueAspectRemapReader.current[0] as number;
      const relId = relRemapReader.done ? Number.POSITIVE_INFINITY : relRemapReader.current[0] as number;

      if (multiAspectId === Number.POSITIVE_INFINITY
        && uniqueAspectId === Number.POSITIVE_INFINITY
        && relId === Number.POSITIVE_INFINITY
      )
        break;

      const ids = [uniqueAspectId, relId, multiAspectId];
      for (let i = 0; i < ids.length; ++i) {
        for (let j = i + 1; j < ids.length; ++j) {
          const id1 = ids[i];
          const id2 = ids[j];
          if (id1 === Number.POSITIVE_INFINITY || id2 === Number.POSITIVE_INFINITY)
            continue;
          assert(id1 !== id2);
        }
      }

      const min = Math.min(multiAspectId, uniqueAspectId, relId);
      if (multiAspectId === min) {
        remap(multiAspectId);
        await advanceMultiAspectId();
      } else if (uniqueAspectId === min) {
        remap(uniqueAspectId);
        await advanceUniqueAspectId();
      } else /* (relId === min) */ {
        remap(relId);
        await advanceRelId();
      }
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

    console.log("generate codespec remap tables", performance.now());
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

  await bulkInsertTransform(source, writeableTarget, "process-all", {
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
