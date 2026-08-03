/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
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
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock.js";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { BenchmarkReporter } from "../../src/reporting/BenchmarkReporter.js";
import { BenchmarkScenarioDefinition } from "../../src/framework/BenchmarkScenario.js";
import {
  benchmarkOutputMarkerName,
  BenchmarkRunner,
  prepareBenchmarkOutputDirectory,
} from "../../src/framework/BenchmarkRunner.js";
import {
  balancedIncrementalDescriptor,
  balancedIncrementalFixture,
  balancedIncrementalRecipe,
} from "../../src/fixtures/recipes/balancedIncremental.js";
import { configureFixture } from "../../src/fixtures/FixtureRecipe.js";
import { materializeLiveHubFixture } from "../../src/fixtures/providers/liveHubProvider.js";
import {
  createStartedEditTxn,
  disposeReconstructedHub,
  ReconstructedHub,
  reconstructHub,
} from "../../src/fixtures/LocalHubFixture.js";
import {
  incrementalSynchronization,
  incrementalSynchronizationScenario,
} from "../../src/scenarios/incrementalSynchronization.js";
import { assertSynchronizationProvenance } from "../../src/fixtures/validation/validateFixture.js";

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

  beforeAll(async () => {
    outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-reconstruct-")
    );
    await IModelHost.startup();
  });

  afterAll(async () => {
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
    const invalidFixture = {
      ...balancedIncrementalFixture,
      async validate() {
        throw new Error("expected fixture validation failure");
      },
    };
    let failure: unknown;
    try {
      await materializeLiveHubFixture(
        invalidFixture,
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
        balancedIncrementalFixture,
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
        balancedIncrementalFixture,
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

describe("BenchmarkRunner scenario injection", () => {
  const testFixture = configureFixture(balancedIncrementalRecipe, {
    id: "balanced-incremental-runner-test",
    version: 1,
    label: "balanced incremental runner test",
    scenarioClaims: balancedIncrementalDescriptor.scenarioClaims,
    topology: "source-and-empty-target",
    seed: 328,
    parameters: { scale: 1 },
  });
  const { descriptor: testDescriptor } = testFixture;

  it("uses the injected factory, propagates its identity, and cleans every sample", async () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-injected-")
    );
    const calls = { abort: 0, factory: 0, finish: 0, measure: 0 };
    const scenario: BenchmarkScenarioDefinition = {
      id: "injected-scenario",
      defaultFixtureId: balancedIncrementalDescriptor.id,
      capabilities: { topology: "source-and-empty-target" },
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
        testFixture,
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

      BenchmarkReporter.write(outputDir, samples, 1234);
      const jsonLines = fs
        .readFileSync(path.join(outputDir, "samples.jsonl"), "utf8")
        .trim()
        .split("\n")
        .map(
          (line) =>
            JSON.parse(line) as {
              fixtureRecipeHash: string;
              fixtureVersion: number;
              reportSchemaVersion: number;
              scenarioId: string;
            }
        );
      expect(jsonLines.map((sample) => sample.scenarioId)).to.deep.equal([
        scenario.id,
        scenario.id,
      ]);
      expect(jsonLines[0]).to.include({
        fixtureRecipeHash: testDescriptor.recipeHash,
        fixtureVersion: testDescriptor.version,
        reportSchemaVersion: 1,
      });
      const summary = JSON.parse(
        fs.readFileSync(path.join(outputDir, "summary.json"), "utf8")
      ) as {
        fixtureGenerator: typeof testDescriptor.generator;
        fixtureRecipeHash: string;
        fixtureVersion: number;
        jobMilliseconds: number;
        reportSchemaVersion: number;
        scenarioId: string;
      };
      expect(summary.scenarioId).to.equal(scenario.id);
      expect(summary).to.include({
        fixtureRecipeHash: testDescriptor.recipeHash,
        fixtureVersion: testDescriptor.version,
        jobMilliseconds: 1234,
        reportSchemaVersion: 1,
      });
      expect(summary.fixtureGenerator).to.deep.equal(testDescriptor.generator);
      expect(
        fs.readFileSync(path.join(outputDir, "summary.csv"), "utf8")
      ).to.match(
        /^reportSchemaVersion,scenario,fixture,.+\n1,injected-scenario,/
      );
      expect(() =>
        BenchmarkReporter.write(outputDir, [
          samples[0],
          { ...samples[1], scenarioId: "different-scenario" },
        ])
      ).to.throw("Cannot mix quick performance scenarios");
      expect(() =>
        BenchmarkReporter.write(outputDir, [
          samples[0],
          { ...samples[1], fixtureRecipeHash: "different-recipe" },
        ])
      ).to.throw("Cannot mix quick performance fixture identities");
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
      defaultFixtureId: balancedIncrementalDescriptor.id,
      capabilities: { topology: "source-and-empty-target" },
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
      await new BenchmarkRunner(testFixture, outputDir, scenario).run(1);
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

  it("preserves scenario and cleanup failures", async () => {
    const outputDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "quick-perf-cleanup-failure-")
    );
    const scenario: BenchmarkScenarioDefinition = {
      id: "operation-and-cleanup-failure",
      defaultFixtureId: balancedIncrementalDescriptor.id,
      capabilities: { topology: "source-and-empty-target" },
      factory: () => ({
        abort() {
          throw new Error("expected abort failure");
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
      await new BenchmarkRunner(testFixture, outputDir, scenario).run(1);
    } catch (error) {
      failure = error;
    }
    try {
      expect(failure).to.be.instanceOf(AggregateError);
      const errors = (failure as AggregateError).errors as Error[];
      expect(errors[0].message).to.equal("expected scenario failure");
      expect(errors[1].message).to.equal(
        "Cleanup failed: abort quick performance scenario"
      );
      expect((errors[1].cause as Error).message).to.equal(
        "expected abort failure"
      );
      expect(HubMock.isValid).to.be.false;
      expect(
        fs.readdirSync(outputDir).filter((entry) => entry.startsWith("sample-"))
      ).to.be.empty;
    } finally {
      fs.rmSync(outputDir, { recursive: true, force: true });
    }
  });
});
