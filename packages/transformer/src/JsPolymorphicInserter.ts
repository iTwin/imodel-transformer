import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String, StopWatch } from "@itwin/core-bentley";
import { PropertyType } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import * as url from "url";
import { IModelTransformer } from "./IModelTransformer";
import { CompactRemapTable } from "./CompactRemapTable";

interface SourceColumnInfo {
  sqlite: {
    name: string;
    table: string;
    sisterTables: string[];
  };
  ec: {
    type: PropertyType;
    rootType: "element" | "aspect" | "relationship" | "codespec";
    extendedType: string | undefined;
    schemaName: string;
    className: string;
    accessString: string;
    // FIXME: not used
    sourceClassId: Id64String;
    // FIXME: not used
    targetClassId: Id64String;
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

async function bulkInsertByTable(target: ECDb, {
  propertyTransforms = {},
}: {
  propertyTransforms?: {
    [propertyName: string]: (s: string) => string;
  };
} = {}): Promise<void> {

  /** table name -> sourceSqliteTable.sourceSqliteColName -> column info */
  const targetTableToSourceColumns = new Map<string, Map<string, SourceColumnInfo>>();

  const classRemapsSql = `
    WITH RECURSIVE srcRootTable(id, parent, root) AS (
      SELECT Id, ParentTableId, Id
      FROM source.ec_Table t

      UNION ALL

      SELECT t.Id, t.ParentTableId, p.root
      FROM srcRootTable p
      JOIN source.ec_Table t ON t.Id=p.parent
    )

    SELECT DISTINCT
        tepp.AccessString
      , teCol.Name AS TargetColumnName
      , tet.Name AS TargetTableName
      , teCls.Name AS ClassName
      , teCls.Id AS TargetClassId
      , tes.Name AS SchemaName
      -- FIXME: this probably doesn't work for structs?
      , tep.PrimitiveType AS PropertyType
      , tep.ExtendedTypeName AS ExtendedPropertyType
      -- FIXME: use better, idiomatic root class check (see native extractChangeSets)
      , CASE
          WHEN strt.Name='bis_ElementMultiAspect'
            OR strt.Name='bis_ElementUniqueAspect'
            THEN 'aspect'
          WHEN strt.Name='bis_ElementRefersToElements'
            OR strt.Name='bis_ElementDrivesElements'
            THEN 'relationship'
          WHEN strt.Name='bis_CodeSpec'
            THEN 'codespec'
          WHEN strt.Name='bis_Element'
            OR strt.Name='bis_Model'
            THEN 'element'
          ELSE
            'unknown'
        END AS EntityType
      , seCls.Id AS SourceClassId
      , set_.Name AS SourceTableName
      , strt.Name AS SourceRootTableName
      -- NOTE: aggregated in js
      , srcSisT.Name AS SisterTable
    FROM ec_PropertyMap tepm
    JOIN ec_Column teCol ON teCol.Id=tepm.ColumnId
    JOIN ec_PropertyPath tepp ON tepp.Id=tepm.PropertyPathId
    JOIN ec_Table tet ON tet.Id=teCol.TableId
    JOIN ec_Property tep ON tep.Id=tepp.RootPropertyId
                        AND tep.ClassId=teCls.id
    JOIN ec_Class teCls ON teCls.Id=tepm.ClassId
    JOIN ec_Schema tes ON tes.Id=teCls.SchemaId

    JOIN source.ec_Schema ses ON ses.Name=tes.Name
    JOIN source.ec_Class seCls ON seCls.Name=teCls.Name
    JOIN source.ec_PropertyPath sepp ON sepp.AccessString=tepp.AccessString
    JOIN source.ec_PropertyMap sepm ON sepm.PropertyPathId=sepp.Id
    JOIN source.ec_Property sep ON sep.Id=sepp.RootPropertyId
                                AND sep.ClassId=seCls.id
    JOIN source.ec_Column seCol ON seCol.Id=sepm.ColumnId
    JOIN source.ec_Table set_ ON set_.Id=seCol.TableId

    JOIN source.ec_cache_ClassHasTables seccht ON seccht.ClassId=seCls.Id
    JOIN source.ec_Table srcSisT ON srcSisT.Id=seccht.TableId

    -- FIXME: can probably remove
    JOIN srcRootTable srt ON srt.root=set_.Id
    JOIN source.ec_Table strt ON strt.Id=srt.root

    WHERE NOT teCol.IsVirtual
      AND srt.Parent IS NULL
      -- ignore metadata
      AND tes.Name != 'ECDbMeta'
  `;

  target.withPreparedSqliteStatement(classRemapsSql, (stmt) => {
    while (stmt.step() === DbResult.BE_SQLITE_ROW) {
      const accessString = stmt.getValue(0).getString();
      const sqliteColumnName = stmt.getValue(1).getString();
      const targetTableName = stmt.getValue(2).getString();
      const className = stmt.getValue(3).getString();
      const targetClassId = stmt.getValue(4).getId();
      const schemaName = stmt.getValue(5).getString();
      const propertyType = stmt.getValue(6).getInteger();
      const propertyExtendedType = stmt.getValue(7).getString();
      const propertyRootType = stmt.getValue(8).getString(); // FIXME: make this a numeric enum
      const sourceClassId = stmt.getValue(9).getId();
      const sourceTableName = stmt.getValue(10).getString();
      // FIXME: don't calculate this thing, it's expensive
      //const sourceRootTableName = stmt.getValue(11).getString();
      const sisterTable = stmt.getValue(12).getString();

      let columns = targetTableToSourceColumns.get(targetTableName);
      if (columns === undefined) {
        columns = new Map();
        targetTableToSourceColumns.set(targetTableName, columns);
      }

      const colPath = `${sourceTableName}.${sqliteColumnName}`;
      let colData = columns.get(colPath);

      if (colData === undefined) {
        colData = {
          sqlite: {
            name: sqliteColumnName,
            table: sourceTableName,
            // FIXME: remove from query
            sisterTables: [],
          },
          ec: {
            type: propertyType,
            rootType: propertyRootType as any,
            extendedType: propertyExtendedType,
            schemaName,
            className,
            accessString,
            sourceClassId,
            targetClassId,
          },
        };
        columns.set(colPath, colData);
      }

      colData.sqlite.sisterTables.push(sisterTable);

      /*
      // FIXME: handle unknown better
      assert(propertyRootType === "element"
        || propertyRootType === "aspect"
        || propertyRootType === "relationship"
        || propertyRootType === "codespec",
        `expected a rootType but got '${propertyRootType}'`
      );
      */

    }
  });

  for (const [targetTableName, sourceColumns] of targetTableToSourceColumns) {
    let rootTable: string | undefined;
    // FIXME: need to join these somehow
    const sourceTables = new Set<string>();

    for (const srcCol of sourceColumns) {
      sourceTables.add(srcCol.sqlite.table);
      try {
        assert(
          rootTable === undefined || srcCol.sqlite.rootTable === rootTable,
          `multiple root tables for ${srcCol.ec.schemaName}.${srcCol.ec.className}.${srcCol.ec.accessString}: ${
            rootTable
          },${srcCol.sqlite.rootTable}`
        );
      } catch (err) {
        // FIXME: temp ignore
        //console.log("sourceColumns", sourceColumns);
        //throw err;
      }
      rootTable = srcCol.sqlite.rootTable;
    }

    // FIXME
    //assert(rootTable !== undefined, "there was no root table");
    //sourceTables.delete(rootTable as any);

    const sourceColumnsWithId: SourceColumnInfo[] = [
      {
        sqlite: {
          ...sourceColumns[0].sqlite,
          name: "Id",
        },
        ec: {
          ...sourceColumns[0].ec,
          accessString: "ECInstanceId",
          type: PropertyType.Long,
          extendedType: "Id",
        },
      },
      ...sourceColumns,
    ];

    /* eslint-disable @typescript-eslint/indent */
    const transformSql = `
      INSERT INTO [${targetTableName}](
        ${sourceColumnsWithId.map((c) => c.sqlite.name).join(",")}
      )
      SELECT ${
        sourceColumnsWithId
          .map((c) => {
            const propQualifier = `${c.ec.schemaName}.${c.ec.className}.${c.ec.accessString}`;
            const propTransform = propertyTransforms[propQualifier];
            const sourceColumnQualifier = `[${c.sqlite.table}].[${c.sqlite.name}]`;

            return propTransform
              ? propTransform(sourceColumnQualifier)
              : c.ec.type === PropertyType.Navigation
              ? remapSql(sourceColumnQualifier, c.ec.rootType === "codespec" ? "codespec" : "element")
              : c.ec.type === PropertyType.Long && c.ec.extendedType === "Id"
              ? remapSql(sourceColumnQualifier, "element")
              // HACK
              : c.ec.accessString.endsWith(".RelECClassId")
              ? `(
                  -- FIXME: create a TEMP class remap cache!
                  SELECT tc.Id
                  FROM source.ec_Class sc
                  JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
                  JOIN main.ec_Schema ts ON ts.Name=ss.Name
                  JOIN main.ec_Class tc ON tc.Name=sc.Name
                  WHERE sc.Id=${sourceColumnQualifier}
                )`
              : remapSql(sourceColumnQualifier,
                c.ec.accessString === "ECInstanceId"
                ? c.ec.rootType === "element"
                  ? "element"
                  : "nonElemEntity"
                // FIXME: support non-element-targeting navProps (using ECReferenceTypesCache)
                : "element")
            ;
          })
          .join(",")
      }
      /*
      FROM ${[...sourceTables]
        .map((t) => `source.[${t}]`)
        .join(",")}
      */
      ${
      /*
        [...sourceTables]
          .map((sourceTable) => `JOIN [${sourceTable}] ON [${sourceTable}].${
            rootTable === "bis_Element" ? "ElementId" : "Id"
          }=[${rootTable}].Id`)
          .join("\n")
      */""
      }
    `;

    const timer = new StopWatch();
    timer.start();
    console.log(`filling table ${targetTableName}...`);

    target.withPreparedSqliteStatement(transformSql, (targetStmt) => {
      assert(targetStmt.step() === DbResult.BE_SQLITE_DONE, target.nativeDb.getLastError());
    });

    console.log(`done after ${timer.elapsedSeconds}s`);
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

  const geomStreamRemap = (s: string) => `RemapGeom(${s}, 'temp.font_remap', 'temp.element_remap')`;

  await bulkInsertByTable(writeableTarget, {
    propertyTransforms: {
      /* eslint-disable @typescript-eslint/naming-convention */
      "BisCore.GeometricElement3d.GeometryStream": geomStreamRemap,
      "BisCore.GeometricElement2d.GeometryStream": geomStreamRemap,
      "BisCore.GeometryPart.GeometryStream": geomStreamRemap,
      /* eslint-enable @typescript-eslint/naming-convention */
    },
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
