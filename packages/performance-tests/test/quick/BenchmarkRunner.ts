/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { IModelHost } from "@itwin/core-backend";
import { DatasetDescriptor } from "./DatasetDescriptor";
import { materializeFixture, PreparedDataset } from "./FixtureMaterializer";
import { disposeReconstructedHub } from "./LocalHubFixture";
import { incrementalSynchronization } from "./scenarios/incrementalSynchronization";

export const benchmarkOutputMarkerName =
  ".imodel-transformer-quick-performance";

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

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
    path.join(__dirname, ".quick-output"),
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

async function cleanupSample(
  scenario: ReturnType<typeof incrementalSynchronization> | undefined,
  dataset: PreparedDataset | undefined,
  sampleDir: string
): Promise<unknown[]> {
  const errors: unknown[] = [];
  try {
    scenario?.abort();
  } catch (error) {
    errors.push(error);
  }
  try {
    if (dataset) await disposeReconstructedHub(dataset.hub);
  } catch (error) {
    errors.push(error);
  }
  try {
    fs.rmSync(sampleDir, { recursive: true, force: true });
  } catch (error) {
    errors.push(error);
  }
  return errors;
}

export interface BenchmarkSample {
  readonly cpuSystemMilliseconds: number;
  readonly cpuUserMilliseconds: number;
  readonly fixtureId: string;
  readonly measured: boolean;
  readonly operations: DatasetDescriptor["distribution"]["operations"];
  readonly reconstructionMilliseconds: number;
  readonly rssDeltaBytes: number;
  readonly sample: number;
  readonly semanticDigest: string;
  readonly teardownMilliseconds: number;
  readonly verificationMilliseconds: number;
  readonly wallMilliseconds: number;
}

export class BenchmarkRunner {
  public constructor(
    private readonly _descriptor: DatasetDescriptor,
    private readonly _outputDir: string
  ) {}

  public async run(measuredSamples = 8): Promise<BenchmarkSample[]> {
    if (!Number.isInteger(measuredSamples) || measuredSamples < 1)
      throw new Error(
        "Quick performance requires at least one measured sample"
      );
    prepareBenchmarkOutputDirectory(this._outputDir);
    const samples: BenchmarkSample[] = [];
    await IModelHost.startup();
    try {
      for (let sample = 0; sample <= measuredSamples; sample++) {
        const sampleDir = path.join(this._outputDir, `sample-${sample}`);
        let dataset: PreparedDataset | undefined;
        let scenario: ReturnType<typeof incrementalSynchronization> | undefined;
        let operationError: Error | undefined;
        let completedSample:
          | Omit<BenchmarkSample, "teardownMilliseconds">
          | undefined;
        try {
          dataset = await materializeFixture(
            this._descriptor,
            sampleDir,
            `quick-sample-${sample}`
          );
          scenario = incrementalSynchronization(dataset);
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
          completedSample = {
            cpuSystemMilliseconds: cpu.system / 1000,
            cpuUserMilliseconds: cpu.user / 1000,
            fixtureId: this._descriptor.id,
            measured: sample !== 0,
            operations: this._descriptor.distribution.operations,
            reconstructionMilliseconds: dataset.reconstructionMilliseconds,
            rssDeltaBytes,
            sample,
            semanticDigest,
            verificationMilliseconds,
            wallMilliseconds,
          };
        } catch (error) {
          operationError = normalizeError(error);
        }
        const teardownStart = process.hrtime.bigint();
        const cleanupErrors = await cleanupSample(scenario, dataset, sampleDir);
        const teardownMilliseconds =
          Number(process.hrtime.bigint() - teardownStart) / 1_000_000;
        if (operationError && cleanupErrors.length === 0) throw operationError;
        if (cleanupErrors.length > 0)
          throw new AggregateError(
            operationError ? [operationError, ...cleanupErrors] : cleanupErrors,
            "Quick performance sample cleanup failed"
          );
        if (!completedSample)
          throw new Error(
            "Quick performance sample completed without a result"
          );
        const sampleResult = { ...completedSample, teardownMilliseconds };
        samples.push(sampleResult);
        fs.appendFileSync(
          path.join(this._outputDir, "samples.jsonl"),
          `${JSON.stringify(sampleResult)}\n`
        );
      }
    } finally {
      await IModelHost.shutdown();
    }
    return samples;
  }
}
