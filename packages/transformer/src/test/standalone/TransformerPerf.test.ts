/* eslint-disable no-console */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Performance tests for IModelTransformer.
 *
 * These tests are skipped by default. To run them, remove the `.skip` modifier
 * from individual tests or the entire describe block. Useful for:
 * - Profiling transformation performance
 * - Comparing performance before/after changes
 * - Identifying performance regressions
 */

import { assert } from "chai";
import {
  Code,
  ColorDef,
  GeometryStreamBuilder,
  GeometryStreamProps,
  IModel,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import {
  Box,
  Point3d,
  Range3d,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
import {
  IModelDb,
  IModelHost,
  IModelJsFs,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  StandaloneDb,
  withEditTxn,
} from "@itwin/core-backend";
import * as coreBackendPkgJson from "@itwin/core-backend/package.json";
import { IModelTransformer } from "../../IModelTransformer";
import {
  IModelTransformerTestUtils,
  createStartedEditTxn,
} from "../IModelTransformerUtils";

import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests
import * as path from "path";

const coreBackendVersion = coreBackendPkgJson.version;

const NUM_ELEMENTS = 10000;

function initOutputFile(filename: string): string {
  const outputDirName = path.join(__dirname, "output");
  if (!IModelJsFs.existsSync(outputDirName)) {
    IModelJsFs.mkdirSync(outputDirName);
  }
  const outputFileName = path.join(outputDirName, filename);
  if (IModelJsFs.existsSync(outputFileName)) {
    IModelJsFs.removeSync(outputFileName);
  }
  return outputFileName;
}

function createBoxGeometry(): GeometryStreamProps {
  const builder = new GeometryStreamBuilder();
  const box = Box.createRange(
    Range3d.create(Point3d.createZero(), new Point3d(1, 1, 1)),
    true
  );
  if (box) {
    builder.appendGeometry(box);
  }
  return builder.geometryStream;
}

interface SourceResult {
  db: StandaloneDb;
  insertDuration: number;
}

async function createSourceWithElements(
  numElements: number
): Promise<SourceResult> {
  const sourceFileName = initOutputFile("perftest_source.bim");

  const sourceDb = StandaloneDb.createEmpty(sourceFileName, {
    rootSubject: { name: "PerfTest Source" },
  });

  // Insert elements with geometry
  const geometry = createBoxGeometry();

  console.log(`Inserting ${numElements} elements into source iModel...`);

  const insertDuration = withEditTxn(
    sourceDb,
    "insert test elements",
    (txn) => {
      // Create a SpatialCategory
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "TestCategory",
        { color: ColorDef.blue.toJSON() }
      );

      // Create a PhysicalModel
      const physicalModelId = PhysicalModel.insert(
        txn,
        IModel.rootSubjectId,
        "TestPhysicalModel"
      );

      const insertStartTime = performance.now();

      for (let i = 0; i < numElements; i++) {
        const elementProps: PhysicalElementProps = {
          classFullName: "Generic:PhysicalObject",
          model: physicalModelId,
          category: categoryId,
          code: Code.createEmpty(),
          userLabel: `TestElement_${i}`,
          geom: geometry,
          placement: {
            origin: new Point3d(i * 2, 0, 0),
            angles: YawPitchRollAngles.createDegrees(0, 0, 0),
          },
        };
        txn.insertElement(elementProps);
      }

      return performance.now() - insertStartTime;
    }
  );

  return { db: sourceDb, insertDuration };
}

function createEmptyTarget(): StandaloneDb {
  const targetFileName = initOutputFile("perftest_target.bim");

  const targetDb = StandaloneDb.createEmpty(targetFileName, {
    rootSubject: { name: "PerfTest Target" },
  });

  return targetDb;
}

function printResults(results: {
  elementCount: number;
  insertionMs: number;
  schemaMs: number;
  processMs: number;
}): void {
  const separator = "===========================================";
  const avgPerElement = results.processMs / results.elementCount;

  console.log(separator);
  console.log(`  Results: core-backend ${coreBackendPkgJson.version}`);
  console.log(separator);
  console.log(`  Elements: ${results.elementCount}`);
  console.log(`  Element insertion: ${results.insertionMs.toFixed(2)} ms`);
  console.log(`  Schema processing: ${results.schemaMs.toFixed(2)} ms`);
  console.log(`  process() duration: ${results.processMs.toFixed(2)} ms`);
  console.log(`  Avg per element: ${avgPerElement.toFixed(4)} ms`);
  console.log(separator);
}

describe.skip("IModelTransformer Performance Tests", () => {
  it("should transform 10k elements", async function () {
    this.timeout(120000); // 2 minutes for large element count

    const elementCount = 10000;

    // Create source iModel
    const sourceDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      "Source10kElements.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbFile, {
      rootSubject: { name: "Source 10k Elements Test" },
    });

    // Set up category, model, and insert 10k elements
    const geom = IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1));
    const insertStartTime = performance.now();
    withEditTxn(sourceDb, "insert test elements", (txn) => {
      const categoryId = SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "TestCategory",
        { color: ColorDef.green.toJSON() }
      );
      const modelId = PhysicalModel.insert(
        txn,
        IModel.rootSubjectId,
        "TestPhysicalModel"
      );

      for (let i = 0; i < elementCount; i++) {
        const physicalObjectProps: PhysicalElementProps = {
          classFullName: PhysicalObject.classFullName,
          model: modelId,
          category: categoryId,
          code: Code.createEmpty(),
          userLabel: `Element-${i}`,
          geom,
          placement: Placement3d.fromJSON({
            origin: { x: i % 100, y: Math.floor(i / 100), z: 0 },
            angles: {},
          }),
        };
        txn.insertElement(physicalObjectProps);
      }
    });
    const insertEndTime = performance.now();

    // Verify source element count
    let sourceCount = 0;
    for await (const _row of sourceDb.createQueryReader(
      `SELECT COUNT(*) FROM ${PhysicalObject.classFullName}`
    )) {
      sourceCount = _row[0] as number;
    }
    assert.equal(sourceCount, elementCount, "Source should have 10k elements");

    // Create empty target iModel
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      "Target10kElements.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetDbFile, {
      rootSubject: { name: "Target 10k Elements Test" },
    });

    // Transform
    const transformer = new IModelTransformer(
      sourceDb,
      targetDb,
      createStartedEditTxn(targetDb),
      { loadSourceGeometry: true, noProvenance: true }
    );

    const schemasStartTime = performance.now();
    await transformer.processSchemas();
    const schemasEndTime = performance.now();

    const startTime = performance.now();
    await transformer.process();
    const endTime = performance.now();
    // eslint-disable-next-line @typescript-eslint/no-deprecated -- saving changes from transformer.process()
    targetDb.saveChanges();

    printResults({
      elementCount,
      insertionMs: insertEndTime - insertStartTime,
      schemaMs: schemasEndTime - schemasStartTime,
      processMs: endTime - startTime,
    });

    // Verify target element count
    let targetCount = 0;
    for await (const _row of targetDb.createQueryReader(
      `SELECT COUNT(*) FROM ${PhysicalObject.classFullName}`
    )) {
      targetCount = _row[0] as number;
    }
    assert.equal(targetCount, elementCount, "Target should have 10k elements");

    // Cleanup
    transformer.dispose();
    sourceDb.close();
    targetDb.close();
  });

  it("should transform 10k elements using hub", async function () {
    console.log("===========================================");
    console.log("  iModel Transformer Performance Test");
    console.log(`  Elements: ${NUM_ELEMENTS}`);
    console.log("===========================================\n");

    let hostOptions = {};
    if (coreBackendVersion === "5.6.1")
      hostOptions = { disableThinnedNativeInstanceWorkflow: true };

    await IModelHost.startup(hostOptions);

    try {
      // Create source iModel with elements
      const { db: sourceDb, insertDuration } =
        await createSourceWithElements(NUM_ELEMENTS);
      console.log(`Source iModel created: ${sourceDb.pathName}`);

      // Create empty target iModel
      const targetDb = createEmptyTarget();
      console.log(`Target iModel created: ${targetDb.pathName}`);

      // Create transformer
      const transformer = new IModelTransformer(
        sourceDb,
        targetDb,
        createStartedEditTxn(targetDb),
        { loadSourceGeometry: true, noProvenance: true }
      );

      // Time schema processing
      console.log("Processing schemas...");

      // Start CPU profiling for schemas
      // const schemaSession = new inspector.Session();
      // schemaSession.connect();
      // await schemaSession.post("Profiler.enable");
      // await schemaSession.post("Profiler.start");

      const schemaStartTime = performance.now();
      await transformer.processSchemas();
      const schemaEndTime = performance.now();
      const schemaDuration = schemaEndTime - schemaStartTime;

      // Stop CPU profiling and save
      // const { profile: schemaProfile } = await schemaSession.post("Profiler.stop");
      // const schemaProfilePath = path.join(__dirname, "output", `10kElemProcessSchemas${coreTransformerVersion}-${coreBackendVersion}.cpuprofile`);
      // fs.writeFileSync(schemaProfilePath, JSON.stringify(schemaProfile));
      // console.log(`Schema CPU profile saved to: ${schemaProfilePath}`);
      // schemaSession.disconnect();

      // Time the transformation process
      console.log("Running transformer.process()...");

      // Start CPU profiling
      // const session = new inspector.Session();
      // session.connect();
      // await session.post("Profiler.enable");
      // await session.post("Profiler.start");

      const processStartTime = performance.now();
      await transformer.process();
      const processEndTime = performance.now();
      const processDuration = processEndTime - processStartTime;

      // // Stop CPU profiling and save
      // const { profile } = await session.post("Profiler.stop");
      // const profilePath = path.join(__dirname, "output", `10kElemTransform${coreTransformerVersion}-${coreBackendVersion}.cpuprofile`);
      // fs.writeFileSync(profilePath, JSON.stringify(profile));
      // console.log(`CPU profile saved to: ${profilePath}`);
      // session.disconnect();

      // Cleanup
      transformer.dispose();
      // eslint-disable-next-line @typescript-eslint/no-deprecated -- saving changes from transformer.process()
      targetDb.saveChanges("Transformation complete");

      sourceDb.close();
      targetDb.close();

      console.log("\n===========================================");
      console.log(`  Results: core-backend: ${coreBackendVersion}`);
      console.log("===========================================");
      console.log(`  Elements: ${NUM_ELEMENTS}`);
      console.log(`  Element insertion: ${insertDuration.toFixed(2)} ms`);
      console.log(`  Schema processing: ${schemaDuration.toFixed(2)} ms`);
      console.log(`  process() duration: ${processDuration.toFixed(2)} ms`);
      console.log(
        `  Avg per element: ${(processDuration / NUM_ELEMENTS).toFixed(4)} ms`
      );
      console.log("===========================================");
    } catch (error) {
      console.error("Error during performance test:", error);
      throw error;
    } finally {
      await IModelHost.shutdown();
    }
  });
});
