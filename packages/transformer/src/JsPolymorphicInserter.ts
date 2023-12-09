import { ECDb, ECDbOpenMode, IModelDb } from "@itwin/core-backend";
import { DbResult, Id64String, StopWatch } from "@itwin/core-bentley";
import { PropertyType } from "@itwin/ecschema-metadata";
import * as assert from "assert";
import * as url from "url";
import { IModelTransformer } from "./IModelTransformer";
import { CompactRemapTable } from "./CompactRemapTable";

type RootType = "element" | "aspect" | "model" | "codespec" | "relationship";

interface SourceColumnInfo {
  special: undefined | "ECInstanceId" | "ECClassId";
  sqlite: {
    name: string;
    table: string;
  };
  ec: {
    sourceClassId: Id64String;
    targetClassId: Id64String;
    /** maps target class id to source ec info */
    perClassData: Map<Id64String, {
      type: PropertyType;
      extendedType: string | undefined;
      rootType: RootType;
      schemaName: string;
      className: string;
      accessString: string;
    }>;
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

  /** tableName -> colName -> column info */
  const targetColToSourceCols = new Map<string, Map<string, SourceColumnInfo>>();

  const classRemapsSql = `
    WITH
    BisElementClassId(id) AS (
      SELECT c.Id FROM ec_Class c JOIN ec_Schema s WHERE s.Name='BisCore' AND c.Name='Element'
    ),
    BisModelClassId(id) AS (
      SELECT c.Id FROM ec_Class c JOIN ec_Schema s WHERE s.Name='BisCore' AND c.Name='Model'
    ),
    BisElementAspectClassId(id) AS (
      SELECT c.Id FROM ec_Class c JOIN ec_Schema s WHERE s.Name='BisCore' AND c.Name='ElementAspect'
    ),
    BisElementRefersToElementsClassId(id) AS (
      SELECT c.Id FROM ec_Class c JOIN ec_Schema s WHERE s.Name='BisCore' AND c.Name='ElementRefersToElements'
    ),
    BisElementDrivesElementClassId(id) AS (
      SELECT c.Id FROM ec_Class c JOIN ec_Schema s WHERE s.Name='BisCore' AND c.Name='ElementDrivesElement'
    ),
    BisCodeSpecClassId(id) AS (
      SELECT c.Id FROM ec_Class c JOIN ec_Schema s WHERE s.Name='BisCore' AND c.Name='CodeSpec'
    )

    SELECT
        tepp.AccessString
      , seCol.Name AS SourceColumnName
      , tet.Name AS TargetTableName
      , CASE
          WHEN teCls.Id IN (select ClassId FROM [ec_cache_ClassHierarchy] WHERE BaseClassId IN BisElementClassId)
            THEN 'element'
          WHEN teCls.Id IN (select ClassId FROM [ec_cache_ClassHierarchy] WHERE BaseClassId IN BisModelClassId)
            THEN 'model'
          WHEN teCls.Id IN (select ClassId FROM [ec_cache_ClassHierarchy] WHERE BaseClassId IN BisElementAspectClassId)
            THEN 'aspect'
          WHEN teCls.Id IN (select ClassId FROM [ec_cache_ClassHierarchy] WHERE BaseClassId IN BisCodeSpecClassId)
            THEN 'codespec'
          WHEN teCls.Id IN (select ClassId FROM [ec_cache_ClassHierarchy] WHERE BaseClassId IN BisElementRefersToElementsClassId)
            THEN 'relationship'
          WHEN teCls.Id IN (select ClassId FROM [ec_cache_ClassHierarchy] WHERE BaseClassId IN BisElementDrivesElementClassId)
            THEN 'ede'
          ELSE
            'unknown'
        END AS RootType
      , teCls.Name AS ClassName
      , teCls.Id AS TargetClassId
      , tes.Name AS SchemaName
      -- FIXME: this probably doesn't work for structs?
      , sep.PrimitiveType AS PropertyType
      , sep.ExtendedTypeName AS ExtendedPropertyType
      , seCls.Id AS SourceClassId
      , set_.Name AS SourceTableName
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

    WHERE NOT teCol.IsVirtual
      -- only transform bis data (for now)
      AND tet.Name like 'bis_%'
      -- FIXME: to do as much work as current transformer, ignore ElementDrivesElement
      AND RootType != 'ede'

    -- FIXME: is it possible for two columns in the source to write to 1 in the target?
    GROUP BY teCol.Name, TargetTableName, tep.PrimitiveType
  `;

  target.withPreparedSqliteStatement(classRemapsSql, (stmt) => {
    while (stmt.step() === DbResult.BE_SQLITE_ROW) {
      const accessString = stmt.getValue(0).getString();
      const sourceColumnName = stmt.getValue(1).getString();
      const targetTableName = stmt.getValue(2).getString();
      const rootType = stmt.getValue(3).getString();
      const className = stmt.getValue(4).getString();
      const targetClassId = stmt.getValue(5).getId();
      const schemaName = stmt.getValue(6).getString();
      const propertyType = stmt.getValue(7).getInteger();
      const propertyExtendedType = stmt.getValue(8).getString();
      const sourceClassId = stmt.getValue(9).getId();
      const sourceTableName = stmt.getValue(10).getString();

      let columns = targetColToSourceCols.get(targetTableName);
      if (columns === undefined) {
        columns = new Map();
        targetColToSourceCols.set(targetTableName, columns);
      }

      const colKey = sourceColumnName;

      let column = columns.get(colKey);
      if (column === undefined) {
        column = {
          special: undefined,
          sqlite: {
            name: sourceColumnName,
            table: sourceTableName,
          },
          ec: {
            sourceClassId,
            targetClassId,
            perClassData: new Map(),
          },
        };
        columns.set(colKey, column);
      }

      const thisEcClassData = {
        type: propertyType,
        extendedType: propertyExtendedType,
        rootType: rootType as any, // FIXME: handle unknown
        schemaName,
        className,
        accessString,
      };
      column.ec.perClassData.set(targetClassId, thisEcClassData);
    }
  });

  for (const [targetTableName, sourceColumnMap] of targetColToSourceCols) {
    const sourceColumns = [...sourceColumnMap.values()];

    // FIXME: can this be simplified
    const classJoinColumnsSql = `
      SELECT
        teCol.Name AS ColumnName
        , tet.Name AS TableName
      FROM ec_PropertyMap tepm
      JOIN ec_Column teCol ON teCol.Id=tepm.ColumnId
      JOIN ec_PropertyPath tepp ON tepp.Id=tepm.PropertyPathId
      JOIN ec_Table tet ON tet.Id=teCol.TableId
      JOIN ec_Class teCls ON teCls.Id=tepm.ClassId

      WHERE tepp.AccessString = 'ECInstanceId'
        AND teCls.Id = ?
    `;

    const joins = target.withPreparedSqliteStatement(classJoinColumnsSql, (stmt) => {
      const result: { colName: string, tableName: string }[] = [];

      // FIXME: this probably won't work because we need to join every possible source table...
      // probably need to aggregate for all sets in this class
      stmt.bindId(1, sourceColumns[0].ec.targetClassId);

      while (stmt.step() === DbResult.BE_SQLITE_ROW) {
        const colName = stmt.getValue(0).getString();
        const tableName = stmt.getValue(1).getString();
        result.push({ colName, tableName });
      }

      return result;
    });

    assert(joins.length > 0, "joins was empty");

    const idCol = joins.find((j) => j.tableName === targetTableName);

    assert(idCol, `couldn't find id column for ${targetTableName}`);

    // should not be possible to vary root type across possible classes
    const rootType = sourceColumns[0].ec.perClassData.values().next().value.rootType as RootType;

    const sourceColumnsWithId: SourceColumnInfo[] = [
      {
        special: "ECInstanceId",
        sqlite: {
          name: idCol.colName,
          table: idCol.tableName,
        },
        ec: {
          ...sourceColumns[0].ec,
          perClassData: new Map(),
        },
      },
      // FIXME HACK: how to tell if a table needs ECClassId?
      ...!["codespec", "unknown"].includes(rootType)
        ? [{
          special: "ECClassId" as const,
          sqlite: {
            name: idCol.colName,
            table: idCol.tableName,
          },
          ec: {
            ...sourceColumns[0].ec,
            perClassData: new Map(),
          },
        }] : [],
      ...sourceColumns,
    ];

    /* eslint-disable @typescript-eslint/indent */
    const handleColumn = (c: SourceColumnInfo) => {
      const sourceColumnQualifier = `source.[${c.sqlite.table}].[${c.sqlite.name}]`;

      if (c.special === "ECInstanceId") {
        return remapSql(
          sourceColumnQualifier,
          rootType === "aspect" || rootType === "relationship"
          ? "nonElemEntity"
          : rootType === "model"
          ? "element"
          : rootType
        );
      }

      const classRemapSql = `(
        -- FIXME: create a TEMP class remap cache!
        SELECT tc.Id
        FROM source.ec_Class sc
        JOIN source.ec_Schema ss ON ss.Id=sc.SchemaId
        JOIN main.ec_Schema ts ON ts.Name=ss.Name
        JOIN main.ec_Class tc ON tc.Name=sc.Name
        WHERE sc.Id=${sourceColumnQualifier}
      )`;

      if (c.special === "ECClassId") {
        return classRemapSql;
      }

      const cases = [...c.ec.perClassData.entries()].map(([classId, ec]) => {
        const propQualifier = `${ec.schemaName}.${ec.className}.${ec.accessString}`;
        const propTransform = propertyTransforms[propQualifier];

        const condition = `ECClassId=${classId}`;

        const consequent = propTransform
          ? propTransform(sourceColumnQualifier)
          : ec.accessString.endsWith(".RelECClassId")
          ? classRemapSql
          // @ts-ignore HACK: fix nav prop type in query, for now assuming null is navprop which is probably wrong for arrays
          : ec.type === PropertyType.Navigation || ec.type === 0
          // FIXME: use qualified name, correctly determine relationship end root type
          ? remapSql(sourceColumnQualifier, ec.accessString === "CodeSpec.Id" ? "codespec" : "element")
          // HACK
          : ec.type === PropertyType.Long && ec.extendedType === "Id"
          // FIXME: support non-element-targeting navProps (using something like ECReferenceTypesCache)
          ? remapSql(sourceColumnQualifier, "element")
          : sourceColumnQualifier
        ;

        return { condition, consequent };
      });

      if (cases.length === 0) {
        console.log(sourceColumns);
        console.log(c);
        throw Error("bad case size");
      }

      return `
        CASE
          ${cases
              .map((case_) => `
                WHEN ${case_.condition}
                  THEN ${case_.consequent}`.trim()
              )
              .join("\n  ")
          }
          ELSE RAISE (ABORT, 'ECClassId was not valid')
        END
      `.trim();
    };

    const transformSql = `
      INSERT INTO [${targetTableName}](
        ${sourceColumnsWithId.map((c) => c.sqlite.name).join(",\n  ")}
      )
      SELECT ${
        sourceColumnsWithId
          .map(handleColumn)
          .join(",\n  ")
      }
      FROM source.[${joins[0].tableName}]
      ${
        joins
          .slice(1)
          .map((join) => `
            JOIN source.[${join.tableName}]
              ON source.[${join.tableName}].[${join.colName}]
                = source.[${joins[0].tableName}].[${joins[0].colName}]`)
          .join("\n")
      }
      WHERE true -- required for syntax of INSERT INTO ... SELECT ... ON CONFLICT
      ${rootType === "codespec"
        ? `ON CONFLICT([main].[bis_CodeSpec].[Name]) DO NOTHING`
        : ""}
      ${rootType === "element" && targetTableName === "bis_Element"
        ? `ON CONFLICT([main].[${targetTableName}].[Id]) DO NOTHING`
        : rootType === "element" && targetTableName !== "bis_Element"
        ? `ON CONFLICT([main].[${targetTableName}].[ElementId]) DO NOTHING`
        : ""}
      ${rootType === "model" && targetTableName === "bis_Model"
        ? `ON CONFLICT([main].[${targetTableName}].[Id]) DO NOTHING`
        : rootType === "model" && targetTableName !== "bis_Model"
        ? `ON CONFLICT([main].[${targetTableName}].[ModelId]) DO NOTHING`
        : ""}
    `;

    const timer = new StopWatch();
    timer.start();
    console.log(`filling table ${targetTableName}...`);

    target.withPreparedSqliteStatement(transformSql, (targetStmt) => {
      try {
        assert(targetStmt.step() === DbResult.BE_SQLITE_DONE, target.nativeDb.getLastError());
      } catch (err) {
        console.log("SQL >>>>>>>>>>>>>>>>>>>>>", transformSql);
        throw err;
      }
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
