/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { expect } from "chai";
import {
  Code,
  IModel,
  PhysicalElementProps,
  QueryBinder,
} from "@itwin/core-common";
import {
  IModelHost,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { BenchmarkReporter } from "./BenchmarkReporter";
import { BenchmarkScenarioDefinition } from "./BenchmarkScenario";
import {
  benchmarkOutputMarkerName,
  BenchmarkRunner,
  prepareBenchmarkOutputDirectory,
} from "./BenchmarkRunner";
import { balancedIncrementalDescriptor } from "./FixtureCatalog";
import { materializeFixture } from "./FixtureMaterializer";
import {
  createStartedEditTxn,
  disposeReconstructedHub,
  ReconstructedHub,
  reconstructHub,
} from "./LocalHubFixture";
import {
  incrementalSynchronization,
  incrementalSynchronizationScenario,
} from "./scenarios/incrementalSynchronization";
import { assertSynchronizationProvenance } from "./validation/validateFixture";

function required<T>(value: T | undefined, name: string): T {
  if (value === undefined) throw new Error(`${name} was not initialized`);
  return value;
}

function insertPhysicalObject(
  db: SnapshotDb | ReconstructedHub["sourceDb"],
  modelId: string,
  categoryId: string,
  name: string
): string {
  return withEditTxn(db, `insert ${name}`, (txn) => {
    const props: PhysicalElementProps = {
      category: categoryId,
      classFullName: PhysicalObject.classFullName,
      code: new Code({
        scope: IModel.rootSubjectId,
        spec: IModel.rootSubjectId,
        value: name,
      }),
      model: modelId,
      userLabel: name,
    };
    return txn.insertElement(props);
  });
}

async function queryElementIdByLabel(
  db: ReconstructedHub["sourceDb"] | ReconstructedHub["targetDb"],
  label: string
): Promise<string | undefined> {
  const reader = db.createQueryReader(
    "SELECT ECInstanceId id FROM bis.Element WHERE UserLabel=?",
    QueryBinder.from([label]),
    { usePrimaryConn: true }
  );
  return (await reader.step()) ? (reader.current.id as string) : undefined;
}

describe("LocalHubFixture reconstruction", () => {
  let outputDir: string;

  before(async () => {
    outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-reconstruct-")
    );
    await IModelHost.startup();
  });

  after(async () => {
    await IModelHost.shutdown();
    fs.rmSync(outputDir, { recursive: true, force: true });
  });

  it("shuts down HubMock when reconstruction fails", async () => {
    let failure: unknown;
    try {
      await reconstructHub(outputDir, "expected-failure", () => {
        throw new Error("expected seed failure");
      });
    } catch (error) {
      failure = error;
    }
    expect(failure).to.be.instanceOf(Error);
    expect(HubMock.isValid).to.be.false;
  });

  it("rejects an invalid HubMock output path before startup", async () => {
    const invalidOutput = path.join(outputDir, "not-a-directory");
    fs.writeFileSync(invalidOutput, "file");
    let failure: unknown;
    try {
      await reconstructHub(invalidOutput, "invalid-output", () => undefined);
    } catch (error) {
      failure = error;
    }
    expect(failure).to.be.instanceOf(Error);
    expect(HubMock.isValid).to.be.false;
  });

  it("disposes a reconstructed hub when materialization fails", async () => {
    const invalidDescriptor = {
      ...balancedIncrementalDescriptor,
      distribution: {
        ...balancedIncrementalDescriptor.distribution,
        operations: {
          ...balancedIncrementalDescriptor.distribution.operations,
          sourceChangesets: 9,
        },
      },
    };
    let failure: unknown;
    try {
      await materializeFixture(
        invalidDescriptor,
        path.join(outputDir, "materialize-failure"),
        "materialize-failure"
      );
    } catch (error) {
      failure = error;
    }
    expect(failure).to.be.instanceOf(Error);
    expect(HubMock.isValid).to.be.false;
  });

  it("does not delete an unowned output directory", async () => {
    const unsafeOutput = path.join(outputDir, "unowned-output");
    const sentinel = path.join(unsafeOutput, "sentinel.txt");
    fs.mkdirSync(unsafeOutput, { recursive: true });
    fs.writeFileSync(sentinel, "preserve");
    let failure: unknown;
    try {
      await new BenchmarkRunner(
        balancedIncrementalDescriptor,
        unsafeOutput,
        incrementalSynchronizationScenario
      ).run(1);
    } catch (error) {
      failure = error;
    }
    expect(failure).to.be.instanceOf(Error);
    expect(fs.readFileSync(sentinel, "utf8")).to.equal("preserve");
  });

  it("rejects an arbitrary empty output directory", () => {
    const unsafeOutput = fs.mkdtempSync(
      path.join(process.cwd(), "unsafe-quick-output-")
    );
    try {
      expect(() => prepareBenchmarkOutputDirectory(unsafeOutput)).to.throw(
        "Quick performance output must be below"
      );
      expect(fs.existsSync(path.join(unsafeOutput, benchmarkOutputMarkerName)))
        .to.be.false;
    } finally {
      fs.rmSync(unsafeOutput, { recursive: true, force: true });
    }
  });

  it("rejects an empty measured sample set", async () => {
    let failure: unknown;
    try {
      await new BenchmarkRunner(
        balancedIncrementalDescriptor,
        path.join(outputDir, "zero-samples"),
        incrementalSynchronizationScenario
      ).run(0);
    } catch (error) {
      failure = error;
    }
    expect(failure).to.be.instanceOf(Error);
    expect((failure as Error).message).to.equal(
      "Quick performance requires at least one measured sample"
    );
  });

  it("reconstructs an offline hub and processes insert, update, and delete changesets", async () => {
    let hub: ReconstructedHub | undefined;
    let modelId: string | undefined;
    let categoryId: string | undefined;
    try {
      hub = await reconstructHub(outputDir, "phase-zero", (sourceSeed) => {
        const db = SnapshotDb.createEmpty(sourceSeed, {
          rootSubject: { name: "phase-zero-source" },
        });
        ({ categoryId, modelId } = withEditTxn(
          db,
          "create base model and category",
          (txn) => ({
            modelId: PhysicalModel.insert(
              txn,
              IModel.rootSubjectId,
              "PhysicalModel"
            ),
            categoryId: SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              "SpatialCategory",
              {}
            ),
          })
        ));
        insertPhysicalObject(db, modelId, categoryId, "update-me");
        insertPhysicalObject(db, modelId, categoryId, "delete-me");
        db.close();
      });

      const initialTxn = createStartedEditTxn(hub.targetDb);
      const initialTransformer = new IModelTransformer({
        source: hub.sourceDb,
        target: initialTxn,
      });
      await initialTransformer.process();
      initialTransformer.dispose();
      initialTxn.end();
      await hub.targetDb.pushChanges({
        accessToken: hub.accessToken,
        description: "establish base provenance",
      });

      insertPhysicalObject(
        hub.sourceDb,
        required(modelId, "modelId"),
        required(categoryId, "categoryId"),
        "inserted"
      );
      await hub.sourceDb.pushChanges({
        accessToken: hub.accessToken,
        description: "insert element",
      });

      const updateId = await queryElementIdByLabel(hub.sourceDb, "update-me");
      expect(updateId).not.to.be.undefined;
      const sourceDb = hub.sourceDb;
      withEditTxn(sourceDb, "update element", (txn) => {
        const props = sourceDb.elements.getElementProps(
          required(updateId, "updateId")
        );
        txn.updateElement({ ...props, userLabel: "updated" });
      });
      await hub.sourceDb.pushChanges({
        accessToken: hub.accessToken,
        description: "update element",
      });

      const deleteId = await queryElementIdByLabel(hub.sourceDb, "delete-me");
      expect(deleteId).not.to.be.undefined;
      withEditTxn(hub.sourceDb, "delete element", (txn) =>
        txn.deleteElement(required(deleteId, "deleteId"))
      );
      await hub.sourceDb.pushChanges({
        accessToken: hub.accessToken,
        description: "delete element",
      });

      const incrementalTxn = createStartedEditTxn(hub.targetDb);
      const incrementalTransformer = new IModelTransformer(
        { source: hub.sourceDb, target: incrementalTxn },
        { argsForProcessChanges: {} }
      );
      await incrementalTransformer.process();
      incrementalTransformer.dispose();
      incrementalTxn.end();
      await assertSynchronizationProvenance(hub.sourceDb, hub.targetDb);

      expect(await queryElementIdByLabel(hub.targetDb, "inserted")).not.to.be
        .undefined;
      expect(await queryElementIdByLabel(hub.targetDb, "updated")).not.to.be
        .undefined;
      expect(await queryElementIdByLabel(hub.targetDb, "delete-me")).to.be
        .undefined;
    } finally {
      if (hub) await disposeReconstructedHub(hub);
    }
  });
});

describe("BenchmarkRunner scenario injection", function () {
  this.timeout(5 * 60 * 1000);

  const testDescriptor = {
    ...balancedIncrementalDescriptor,
    id: "balanced-incremental-runner-test",
    distribution: {
      base: {
        aspects: 480,
        elements: 240,
        geometricElements: 120,
        relationships: 120,
      },
      operations: {
        aspects: { deletes: 48, inserts: 24, updates: 24 },
        elements: { deletes: 24, inserts: 24, updates: 24 },
        geometryUpdates: 6,
        relationships: { deletes: 33, inserts: 12, updates: 12 },
        sourceChangesets: 8,
      },
    },
  };

  it("uses the injected factory, propagates its identity, and cleans every sample", async () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-injected-")
    );
    const calls = { abort: 0, factory: 0, finish: 0, measure: 0 };
    const scenario: BenchmarkScenarioDefinition = {
      id: "injected-scenario",
      factory: (dataset) => {
        calls.factory++;
        const delegate = incrementalSynchronization(dataset);
        return {
          abort() {
            calls.abort++;
            delegate.abort();
          },
          async finish() {
            calls.finish++;
            return delegate.finish();
          },
          async measure() {
            calls.measure++;
            await delegate.measure();
          },
        };
      },
    };
    try {
      const samples = await new BenchmarkRunner(
        testDescriptor,
        outputDir,
        scenario
      ).run(1);
      expect(calls).to.deep.equal({
        abort: 2,
        factory: 2,
        finish: 2,
        measure: 2,
      });
      expect(samples.map((sample) => sample.scenarioId)).to.deep.equal([
        scenario.id,
        scenario.id,
      ]);
      expect(HubMock.isValid).to.be.false;
      expect(
        fs.readdirSync(outputDir).filter((entry) => entry.startsWith("sample-"))
      ).to.be.empty;

      BenchmarkReporter.write(outputDir, samples);
      const jsonLines = fs
        .readFileSync(path.join(outputDir, "samples.jsonl"), "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { scenarioId: string });
      expect(jsonLines.map((sample) => sample.scenarioId)).to.deep.equal([
        scenario.id,
        scenario.id,
      ]);
      const summary = JSON.parse(
        fs.readFileSync(path.join(outputDir, "summary.json"), "utf8")
      ) as { scenarioId: string };
      expect(summary.scenarioId).to.equal(scenario.id);
      expect(
        fs.readFileSync(path.join(outputDir, "summary.csv"), "utf8")
      ).to.match(/^scenario,fixture,.+\ninjected-scenario,/);
      expect(() =>
        BenchmarkReporter.write(outputDir, [
          samples[0],
          { ...samples[1], scenarioId: "different-scenario" },
        ])
      ).to.throw("Cannot mix quick performance scenarios");
    } finally {
      fs.rmSync(outputDir, { recursive: true, force: true });
    }
  });

  it("aborts and tears down the reconstructed hub after a scenario failure", async () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-failure-")
    );
    let aborts = 0;
    const scenario: BenchmarkScenarioDefinition = {
      id: "failing-scenario",
      factory: () => ({
        abort() {
          aborts++;
        },
        async finish() {
          throw new Error("finish must not run");
        },
        async measure() {
          throw new Error("expected scenario failure");
        },
      }),
    };
    let failure: unknown;
    try {
      await new BenchmarkRunner(testDescriptor, outputDir, scenario).run(1);
    } catch (error) {
      failure = error;
    }
    try {
      expect(failure).to.be.instanceOf(Error);
      expect((failure as Error).message).to.equal("expected scenario failure");
      expect(aborts).to.equal(1);
      expect(HubMock.isValid).to.be.false;
      expect(
        fs.readdirSync(outputDir).filter((entry) => entry.startsWith("sample-"))
      ).to.be.empty;
    } finally {
      fs.rmSync(outputDir, { recursive: true, force: true });
    }
  });
});
