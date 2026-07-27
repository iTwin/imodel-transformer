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

import { assert } from "vitest";
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
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

import * as path from "node:path";

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
  it("should transform 10k elements", async () => {
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

    const editTxn = createStartedEditTxn(targetDb);
    // Transform
    const transformer = new IModelTransformer(
      { source: sourceDb, target: editTxn },
      {
        loadSourceGeometry: true,
        noProvenance: true,
      }
    );

    const schemasStartTime = performance.now();
    await transformer.processSchemas();
    const schemasEndTime = performance.now();

    const startTime = performance.now();
    await transformer.process();
    const endTime = performance.now();
    editTxn.end();

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

      const editTxn = createStartedEditTxn(targetDb);
      // Create transformer
      const transformer = new IModelTransformer(
        { source: sourceDb, target: editTxn },
        {
          loadSourceGeometry: true,
          noProvenance: true,
        }
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
      editTxn.end("save", "Transformation complete");

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

describe.skip("Changeset scanning performance", () => {
  it("benchmarks changeset scanning across multiple files", async function () {
    this.timeout(10 * 60 * 1000);

    const elementCount = Number(
      process.env.TRANSFORMER_CHANGESET_SCAN_ELEMENTS ?? 2500
    );
    const changesetCount = Number(
      process.env.TRANSFORMER_CHANGESET_SCAN_CHANGESETS ?? 20
    );
    const measuredRuns = Number(
      process.env.TRANSFORMER_CHANGESET_SCAN_RUNS ?? 3
    );
    for (const [name, value] of [
      ["TRANSFORMER_CHANGESET_SCAN_ELEMENTS", elementCount],
      ["TRANSFORMER_CHANGESET_SCAN_CHANGESETS", changesetCount],
      ["TRANSFORMER_CHANGESET_SCAN_RUNS", measuredRuns],
    ] as const) {
      assert.isTrue(
        Number.isSafeInteger(value) && value > 0,
        `${name} must be a positive integer`
      );
    }

    HubMock.startup(
      "TransformerChangesetScanPerf",
      KnownTestLocations.outputDir
    );
    const accessToken = await HubWrappers.getAccessToken(TestUserType.Regular);
    let sourceDb: BriefcaseDb | undefined;
    try {
      const { db: seedDb } = await createSourceWithElements(elementCount);
      const seedPath = seedDb.pathName;
      seedDb.close();

      const iModelId = await IModelHost[_hubAccess].createNewIModel({
        accessToken,
        iTwinId: HubMock.iTwinId,
        iModelName: "Transformer changeset scan perf",
        description: "Changeset scanning performance fixture",
        version0: seedPath,
        noLocks: true,
      });
      sourceDb = await HubWrappers.downloadAndOpenBriefcase({
        accessToken,
        iTwinId: HubMock.iTwinId,
        iModelId,
      });

      const elementIds: string[] = [];
      for await (const row of sourceDb.createQueryReader(
        `SELECT ECInstanceId FROM ${PhysicalObject.classFullName}`
      )) {
        elementIds.push(row[0] as string);
      }
      assert.lengthOf(elementIds, elementCount);

      for (
        let changesetIndex = 1;
        changesetIndex <= changesetCount;
        ++changesetIndex
      ) {
        const elementsToDelete =
          changesetIndex === changesetCount
            ? Math.max(1, Math.floor(elementCount / 20))
            : 0;
        withEditTxn(
          sourceDb,
          `prepare performance changeset ${changesetIndex}`,
          (txn) => {
            for (
              let elementIndex = 0;
              elementIndex < elementIds.length;
              ++elementIndex
            ) {
              txn.updateElement({
                id: elementIds[elementIndex],
                classFullName: PhysicalObject.classFullName,
                userLabel: `Change_${changesetIndex}_Element_${elementIndex}`,
                placement: {
                  origin: {
                    x: elementIndex * 2,
                    y: changesetIndex,
                    z: 0,
                  },
                  angles: {},
                },
              } as Partial<PhysicalElementProps>);
            }
            if (elementsToDelete > 0)
              txn.deleteElement(elementIds.slice(0, elementsToDelete));
          }
        );
        await sourceDb.pushChanges({
          accessToken,
          description: `performance changeset ${changesetIndex}`,
        });
      }

      const csFileProps = await IModelHost[_hubAccess].downloadChangesets({
        accessToken,
        iModelId,
        range: { first: 1, end: changesetCount },
        targetDir: BriefcaseManager.getChangeSetsPath(iModelId),
      });
      assert.lengthOf(csFileProps, changesetCount);
      const deletedElementCount = Math.max(1, Math.floor(elementCount / 20));

      const measure = async (operation: () => Promise<void>) => {
        const cpuStart = process.cpuUsage();
        const elapsedStart = performance.now();
        await operation();
        const elapsedMs = performance.now() - elapsedStart;
        const cpuUsage = process.cpuUsage(cpuStart);
        return {
          elapsedMs,
          userCpuMs: cpuUsage.user / 1000,
          systemCpuMs: cpuUsage.system / 1000,
        };
      };
      const scanChangedInstanceIds = async () =>
        measure(async () => {
          const changedInstanceIds = await ChangedInstanceIds.initialize({
            iModel: sourceDb!,
            csFileProps,
          });
          assert.isDefined(changedInstanceIds);
          assert.equal(
            changedInstanceIds.element.updateIds.size,
            elementCount - deletedElementCount
          );
          assert.equal(
            changedInstanceIds.element.deleteIds.size,
            deletedElementCount
          );
        });
      let targetIndex = 0;
      const initializeTransformer = async () => {
        const targetFileName = initOutputFile(
          `changeset_scan_target_${targetIndex++}.bim`
        );
        fs.copyFileSync(seedPath, targetFileName);
        const targetDb = StandaloneDb.openFile(targetFileName);
        const targetEditTxn = createStartedEditTxn(targetDb);
        const transformer = new IModelTransformer(
          { source: sourceDb!, target: targetEditTxn },
          {
            argsForProcessChanges: { csFileProps },
            wasSourceIModelCopiedToTarget: true,
          }
        );
        try {
          return await measure(async () => transformer.initialize());
        } finally {
          transformer.dispose();
          targetEditTxn.end();
          targetDb.close();
        }
      };
      const summarize = (
        samples: Array<Awaited<ReturnType<typeof measure>>>
      ) => {
        const median = (values: number[]) => {
          const sortedValues = [...values].sort((lhs, rhs) => lhs - rhs);
          const middle = Math.floor(sortedValues.length / 2);
          return sortedValues.length % 2 === 0
            ? (sortedValues[middle - 1] + sortedValues[middle]) / 2
            : sortedValues[middle];
        };
        return {
          medianElapsedMs: median(samples.map((sample) => sample.elapsedMs)),
          medianUserCpuMs: median(samples.map((sample) => sample.userCpuMs)),
          medianSystemCpuMs: median(
            samples.map((sample) => sample.systemCpuMs)
          ),
          elapsedSamplesMs: samples.map((sample) => sample.elapsedMs),
        };
      };

      await scanChangedInstanceIds();
      const changedInstanceIdsSamples = [];
      for (let run = 0; run < measuredRuns; ++run)
        changedInstanceIdsSamples.push(await scanChangedInstanceIds());

      await initializeTransformer();
      const transformerInitializationSamples = [];
      for (let run = 0; run < measuredRuns; ++run)
        transformerInitializationSamples.push(await initializeTransformer());

      const changedInstanceIdsResult = summarize(changedInstanceIdsSamples);
      const transformerInitializationResult = summarize(
        transformerInitializationSamples
      );
      const printScanResult = (
        label: string,
        result: ReturnType<typeof summarize>
      ) => {
        console.log(`  ${label}:`);
        console.log(
          `    Median elapsed: ${result.medianElapsedMs.toFixed(2)} ms`
        );
        console.log(
          `    Median user CPU: ${result.medianUserCpuMs.toFixed(2)} ms`
        );
        console.log(
          `    Median system CPU: ${result.medianSystemCpuMs.toFixed(2)} ms`
        );
        console.log(
          `    Elapsed samples: ${result.elapsedSamplesMs
            .map((sample) => sample.toFixed(2))
            .join(", ")} ms`
        );
      };

      const separator = "===========================================";
      console.log(`\n${separator}`);
      console.log("  Changeset Scanning Performance Results");
      console.log(`  core-backend: ${coreBackendVersion}`);
      console.log(separator);
      console.log(`  Elements: ${elementCount}`);
      console.log(`  Changesets: ${changesetCount}`);
      console.log(`  Update operations: ${elementCount * changesetCount}`);
      console.log(`  Deleted elements: ${deletedElementCount}`);
      console.log(`  Measured runs: ${measuredRuns}\n`);
      printScanResult("ChangedInstanceIds scan", changedInstanceIdsResult);
      console.log("");
      printScanResult(
        "Transformer initialization",
        transformerInitializationResult
      );
      console.log(separator);
    } finally {
      sourceDb?.close();
      HubMock.shutdown();
    }
  });
});
