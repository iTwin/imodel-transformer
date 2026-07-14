/* eslint-disable no-console */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Quick performance benchmarks for imodel-transformer.
 *
 * These tests generate a synthetic iModel at runtime (no auth, no hub, no .env required)
 * with multiple custom schemas and 10k physical elements, then run a full identity
 * transformation while measuring schema processing and element processing times.
 *
 * Run with: pnpm test:quick-perf
 */

import {
  EditTxn,
  IModelDb,
  IModelHost,
  IModelHostOptions,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  StandaloneDb,
  withEditTxn,
} from "@itwin/core-backend";
import {
  Code,
  ColorDef,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { Logger, LogLevel } from "@itwin/core-bentley";
import { IModelTransformerTestUtils } from "@itwin/imodel-transformer/lib/cjs/test/IModelTransformerUtils";
import { ECReferenceTypesCache } from "@itwin/imodel-transformer/lib/cjs/ECReferenceTypesCache";
import { BenchmarkTransformer, printBenchmarkStats } from "./benchmarking";
import * as path from "path";
import * as fs from "fs";

const NUM_ELEMENTS = 10_000;
const NUM_CUSTOM_SCHEMAS = 15;
const outputDir = path.join(__dirname, ".output");

function ensureOutputDir(): void {
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
}

function initOutputFile(filename: string): string {
  ensureOutputDir();
  const filePath = path.join(outputDir, filename);
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
  return filePath;
}

/**
 * Generate synthetic ECSchema XML strings that reference BisCore.
 * Each schema has a couple of entity classes and properties to give
 * processSchemas() meaningful work.
 */
function generateSchemaStrings(count: number): string[] {
  return Array.from({ length: count }, (_, i) => {
    const schemaName = `PerfTestDomain${i}`;
    const alias = `ptd${i}`;
    return `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaName}" alias="${alias}" version="01.00.00"
  xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="BisCore" version="01.00.16" alias="bis"/>
  <ECSchemaReference name="CoreCustomAttributes" version="01.00.03" alias="CoreCA"/>
  <ECEntityClass typeName="TestPhysicalElement${i}">
    <BaseClass>bis:PhysicalElement</BaseClass>
    <ECProperty propertyName="StringProp" typeName="string"/>
    <ECProperty propertyName="DoubleProp" typeName="double"/>
    <ECProperty propertyName="IntProp" typeName="int"/>
  </ECEntityClass>
  <ECEntityClass typeName="TestInformationRecord${i}">
    <BaseClass>bis:InformationRecordElement</BaseClass>
    <ECProperty propertyName="RecordName" typeName="string"/>
    <ECProperty propertyName="RecordValue" typeName="double"/>
  </ECEntityClass>
</ECSchema>`;
  });
}

/**
 * Create a source iModel with custom schemas and elements.
 */
async function createSourceIModel(): Promise<StandaloneDb> {
  const sourceFileName = initOutputFile("quick_perf_source.bim");
  const sourceDb = StandaloneDb.createEmpty(sourceFileName, {
    rootSubject: { name: "QuickPerfBenchmark Source" },
  });

  // Import custom schemas
  const schemas = generateSchemaStrings(NUM_CUSTOM_SCHEMAS);
  await sourceDb.importSchemaStrings(schemas);

  // Insert elements
  const geom = IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1));
  withEditTxn(sourceDb, "insert benchmark elements", (txn) => {
    const categoryId = SpatialCategory.insert(
      txn,
      IModel.dictionaryId,
      "BenchmarkCategory",
      { color: ColorDef.blue.toJSON() }
    );
    const modelId = PhysicalModel.insert(
      txn,
      IModel.rootSubjectId,
      "BenchmarkPhysicalModel"
    );

    for (let i = 0; i < NUM_ELEMENTS; i++) {
      const elementProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        userLabel: `BenchElem_${i}`,
        geom,
        placement: {
          origin: new Point3d(i % 100, Math.floor(i / 100), 0),
          angles: YawPitchRollAngles.createDegrees(0, 0, 0),
        },
      };
      txn.insertElement(elementProps);
    }
  });

  return sourceDb;
}

before(async () => {
  Logger.initializeToConsole();
  Logger.setLevelDefault(LogLevel.Error);
  const cfg: IModelHostOptions = {};
  cfg.cacheDir = path.join(__dirname, ".cache");
  await IModelHost.startup(cfg);
});

after(async () => {
  await IModelHost.shutdown();
});

describe("Quick Performance Benchmarks", function () {
  this.timeout(120_000);

  it("should benchmark identity transform (10k elements, 15+ schemas)", async () => {
    // Generate source iModel
    console.log(
      "Generating source iModel with %d elements and %d custom schemas...",
      NUM_ELEMENTS,
      NUM_CUSTOM_SCHEMAS
    );
    const sourceDb = await createSourceIModel();
    console.log("Source iModel created: %s", sourceDb.pathName);

    // Create empty target
    const targetFileName = initOutputFile("quick_perf_target.bim");
    const targetDb = SnapshotDb.createEmpty(targetFileName, {
      rootSubject: { name: "QuickPerfBenchmark Target" },
    });

    // Set up benchmarked transformer
    const editTxn = new EditTxn(targetDb, "BenchmarkTransformer");
    editTxn.start();

    const transformer = new BenchmarkTransformer(
      { source: sourceDb, target: editTxn },
      { loadSourceGeometry: true, noProvenance: true }
    );

    // Run the full transformation
    console.log("Running processSchemas()...");
    await transformer.processSchemas();

    console.log("Running process()...");
    await transformer.process();

    editTxn.end();

    // Print results
    printBenchmarkStats(transformer.stats);

    // Cleanup
    transformer.dispose();
    sourceDb.close();
    targetDb.close();
  });

  it("should benchmark ECReferenceTypesCache initialization", async () => {
    // Generate a source iModel with schemas for cache init benchmarking
    console.log("Benchmarking ECReferenceTypesCache.initAllSchemasInIModel...");
    const sourceFileName = initOutputFile("quick_perf_cache_source.bim");
    const sourceDb = StandaloneDb.createEmpty(sourceFileName, {
      rootSubject: { name: "CacheBenchmark Source" },
    });
    const schemas = generateSchemaStrings(NUM_CUSTOM_SCHEMAS);
    await sourceDb.importSchemaStrings(schemas);

    const cache = new ECReferenceTypesCache();
    const start = performance.now();
    await cache.initAllSchemasInIModel(sourceDb);
    const elapsed = performance.now() - start;

    console.log("\n  ECReferenceTypesCache Init");
    console.log("  ─────────────────────────────────────");
    console.log("    Total init time:    %s ms", elapsed.toFixed(2));
    console.log("  ─────────────────────────────────────\n");

    sourceDb.close();
  });
});
