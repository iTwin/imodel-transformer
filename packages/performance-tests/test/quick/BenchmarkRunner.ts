/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { IModelHost } from "@itwin/core-backend";
import { CleanupTask, runWithCleanup } from "../Cleanup.js";
import {
  assertScenarioSupportsFixture,
  defaultQuickPerformanceMeasuredSamples,
} from "./BenchmarkResolution.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "./BenchmarkScenario.js";
import { DatasetDescriptor, FixtureTopology } from "./DatasetDescriptor.js";
import { PreparedDataset } from "./FixtureMaterializer.js";
import { getFixtureProvider } from "./FixtureProvider.js";
import { quickSourcePath } from "./quickPaths.js";

export const benchmarkOutputMarkerName =
  ".imodel-transformer-quick-performance";
export const benchmarkReportSchemaVersion = 1 as const;

/** Where stage 1 writes its artifact, relative to the run output directory. */
export const fixtureArtifactDirectoryName = "fixture-artifact";

function resolveThroughExistingAncestor(fileName: string): string {
  let ancestor = path.resolve(fileName);
  const suffix: string[] = [];
  while (!fs.existsSync(ancestor)) {
    const parent = path.dirname(ancestor);
    if (parent === ancestor)
      throw new Error(`Cannot resolve benchmark output path: ${fileName}`);
    suffix.unshift(path.basename(ancestor));
    ancestor = parent;
  }
  return path.join(fs.realpathSync(ancestor), ...suffix);
}

function isStrictDescendant(candidate: string, root: string): boolean {
  const relative = path.relative(root, candidate);
  return (
    relative.length > 0 &&
    relative !== ".." &&
    !relative.startsWith(`..${path.sep}`) &&
    !path.isAbsolute(relative)
  );
}

export function assertSafeBenchmarkOutputPath(outputDir: string): void {
  const candidate = resolveThroughExistingAncestor(outputDir);
  const allowedRoots = [
    quickSourcePath(".quick-output"),
    os.tmpdir(),
    process.platform === "win32" ? undefined : "/tmp",
    process.env.RUNNER_TEMP,
  ]
    .filter((root): root is string => root !== undefined)
    .map(resolveThroughExistingAncestor);
  if (!allowedRoots.some((root) => isStrictDescendant(candidate, root)))
    throw new Error(
      `Quick performance output must be below the package output or temporary directory: ${outputDir}`
    );
}

export function prepareBenchmarkOutputDirectory(outputDir: string): void {
  assertSafeBenchmarkOutputPath(outputDir);
  fs.mkdirSync(outputDir, { recursive: true });
  const marker = path.join(outputDir, benchmarkOutputMarkerName);
  const entries = fs.readdirSync(outputDir);
  if (!fs.existsSync(marker) && entries.length > 0)
    throw new Error(
      `Refusing to use non-empty unowned output directory: ${outputDir}`
    );
  fs.writeFileSync(
    marker,
    "Owned by iModel Transformer quick performance tests.\n"
  );
  for (const entry of entries) {
    if (
      /^sample-\d+$/.test(entry) ||
      [
        fixtureArtifactDirectoryName,
        "manifest.json",
        "samples.jsonl",
        "summary.csv",
        "summary.json",
      ].includes(entry)
    )
      fs.rmSync(path.join(outputDir, entry), {
        recursive: true,
        force: true,
      });
  }
}

export interface BenchmarkSample {
  readonly cpuSystemMilliseconds: number;
  readonly cpuUserMilliseconds: number;
  readonly fixtureId: string;
  readonly fixtureGenerator: DatasetDescriptor["generator"];
  readonly fixtureRecipeHash: string;
  readonly fixtureVersion: number;
  readonly measured: boolean;
  readonly operations: DatasetDescriptor["distribution"]["operations"];
  /** Stage-1 cost, identical on every sample: the fixture is built once per run. */
  readonly fixtureBuildMilliseconds: number;
  /** Stage-2 cost for this sample: producing its pristine working copy. */
  readonly reconstructionMilliseconds: number;
  readonly rssDeltaBytes: number;
  readonly reportSchemaVersion: typeof benchmarkReportSchemaVersion;
  readonly sample: number;
  readonly scenarioId: string;
  readonly semanticDigest: string;
  readonly teardownMilliseconds: number;
  readonly topology: FixtureTopology;
  readonly verificationMilliseconds: number;
  readonly wallMilliseconds: number;
}

export class BenchmarkRunner {
  public constructor(
    private readonly _descriptor: DatasetDescriptor,
    private readonly _outputDir: string,
    private readonly _scenario: BenchmarkScenarioDefinition
  ) {
    assertScenarioSupportsFixture(_scenario, _descriptor);
  }

  public async run(
    measuredSamples = defaultQuickPerformanceMeasuredSamples
  ): Promise<BenchmarkSample[]> {
    if (!Number.isInteger(measuredSamples) || measuredSamples < 1)
      throw new Error(
        "Quick performance requires at least one measured sample"
      );
    prepareBenchmarkOutputDirectory(this._outputDir);
    const provider = getFixtureProvider(this._descriptor);
    const samples: BenchmarkSample[] = [];
    await runWithCleanup(async () => {
      await IModelHost.startup();
      // Stage 1: build the fixture exactly once, outside the sample loop.
      const built = await provider.build(
        this._descriptor,
        path.join(this._outputDir, fixtureArtifactDirectoryName)
      );
      await runWithCleanup(async () => {
        for (let sample = 0; sample <= measuredSamples; sample++) {
          const sampleDir = path.join(this._outputDir, `sample-${sample}`);
          let dataset: PreparedDataset | undefined;
          let scenario: BenchmarkScenario | undefined;
          let teardownStart: bigint | undefined;
          const beginTeardown = () => {
            teardownStart ??= process.hrtime.bigint();
          };
          const cleanupTasks: CleanupTask[] = [
            {
              name: "abort quick performance scenario",
              run: () => {
                beginTeardown();
                scenario?.abort();
              },
            },
            {
              name: "dispose quick performance sample",
              run: async () => {
                beginTeardown();
                if (dataset) await provider.disposeSample(dataset);
              },
            },
            {
              name: "remove quick performance sample directory",
              run: () => {
                beginTeardown();
                fs.rmSync(sampleDir, { recursive: true, force: true });
              },
            },
          ];
          const completedSample = await runWithCleanup(async () => {
            // Stage 2: a pristine working copy per sample. Mutation is the scenario's business.
            dataset = await provider.materialize(
              built,
              sampleDir,
              `quick-sample-${sample}`
            );
            scenario = this._scenario.factory(dataset);
            const rssBefore = process.memoryUsage().rss;
            const cpuBefore = process.cpuUsage();
            const wallStart = process.hrtime.bigint();
            await scenario.measure();
            const wallMilliseconds =
              Number(process.hrtime.bigint() - wallStart) / 1_000_000;
            const cpu = process.cpuUsage(cpuBefore);
            const rssDeltaBytes = process.memoryUsage().rss - rssBefore;
            const verificationStart = process.hrtime.bigint();
            const semanticDigest = await scenario.finish();
            const verificationMilliseconds =
              Number(process.hrtime.bigint() - verificationStart) / 1_000_000;
            return {
              cpuSystemMilliseconds: cpu.system / 1000,
              cpuUserMilliseconds: cpu.user / 1000,
              fixtureBuildMilliseconds: built.buildMilliseconds,
              fixtureGenerator: this._descriptor.generator,
              fixtureId: this._descriptor.id,
              fixtureRecipeHash: this._descriptor.recipeHash,
              fixtureVersion: this._descriptor.version,
              measured: sample !== 0,
              operations: this._descriptor.distribution.operations,
              reconstructionMilliseconds: dataset.reconstructionMilliseconds,
              reportSchemaVersion: benchmarkReportSchemaVersion,
              rssDeltaBytes,
              sample,
              scenarioId: this._scenario.id,
              semanticDigest,
              topology: this._descriptor.layout.topology,
              verificationMilliseconds,
              wallMilliseconds,
            };
          }, cleanupTasks);
          if (teardownStart === undefined)
            throw new Error("Quick performance sample cleanup did not start");
          const teardownMilliseconds =
            Number(process.hrtime.bigint() - teardownStart) / 1_000_000;
          const sampleResult = {
            ...completedSample,
            teardownMilliseconds,
          };
          samples.push(sampleResult);
          fs.appendFileSync(
            path.join(this._outputDir, "samples.jsonl"),
            `${JSON.stringify(sampleResult)}\n`
          );
        }
      }, [
        {
          name: "dispose quick performance fixture build",
          run: async () => {
            await provider.disposeBuild(built);
          },
        },
      ]);
    }, [
      {
        name: "shut down IModelHost after quick performance run",
        run: async () => {
          if (IModelHost.isValid) await IModelHost.shutdown();
        },
      },
    ]);
    return samples;
  }
}
