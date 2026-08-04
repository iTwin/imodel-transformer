/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  BenchmarkSample,
  fixtureArtifactDirectoryName,
  prepareBenchmarkOutputDirectory,
} from "../framework/BenchmarkRunner.js";
import {
  FixtureArtifactManifest,
  readFixtureArtifact,
} from "../fixtures/FixtureArtifact.js";
import {
  ComparisonArm,
  ComparisonReporter,
  ComparisonSummary,
} from "./ComparisonReport.js";

export const defaultComparisonMeasuredSamples = 3;
export const defaultInformationalThresholdPercent = 5;
export const defaultComparisonWorkerTimeoutMilliseconds = 10 * 60 * 1000;
const maximumWorkerTimeoutMilliseconds = 2_147_483_647;
const defaultWorkerTerminationGraceMilliseconds = 5_000;

export interface ComparisonArmConfiguration {
  readonly revision: string;
  readonly rootDirectory: string;
}

export interface ComparisonRunOptions {
  readonly baseline: ComparisonArmConfiguration;
  readonly candidate: ComparisonArmConfiguration;
  readonly fixtureId?: string;
  readonly informationalThresholdPercent?: number;
  readonly measuredSamplesPerArm?: number;
  readonly outputDir: string;
  readonly scenarioId?: string;
  readonly workerTimeoutMilliseconds?: number;
}

export interface ArmExecutionRequest {
  readonly arm: ComparisonArm;
  readonly fixtureArtifactDirectory: string;
  readonly fixtureId?: string;
  readonly harnessRootDirectory: string;
  readonly measured: boolean;
  readonly outputDir: string;
  readonly revision: string;
  readonly rootDirectory: string;
  readonly sample: number;
  readonly scenarioId?: string;
  readonly workerTimeoutMilliseconds?: number;
}

export type ArmExecutor = (
  request: ArmExecutionRequest
) => Promise<BenchmarkSample>;

export interface FixtureArtifactBuildRequest {
  readonly artifactDirectory: string;
  readonly fixtureId?: string;
  readonly harnessRootDirectory: string;
  readonly rootDirectory: string;
  readonly scenarioId?: string;
  readonly workerTimeoutMilliseconds?: number;
}

export type FixtureArtifactBuilder = (
  request: FixtureArtifactBuildRequest
) => Promise<FixtureArtifactManifest>;

export interface ScheduledExecution {
  readonly arm: ComparisonArm;
  readonly measured: boolean;
  readonly sample: number;
}

export function createExecutionSchedule(
  measuredSamplesPerArm: number
): ScheduledExecution[] {
  if (!Number.isSafeInteger(measuredSamplesPerArm) || measuredSamplesPerArm < 1)
    throw new Error(
      "A/B comparison requires at least one measured sample per arm"
    );
  const schedule: ScheduledExecution[] = [
    { arm: "candidate", measured: false, sample: 0 },
    { arm: "baseline", measured: false, sample: 0 },
  ];
  for (let sample = 1; sample <= measuredSamplesPerArm; sample++) {
    const pair: readonly ComparisonArm[] =
      sample % 2 === 1 ? ["baseline", "candidate"] : ["candidate", "baseline"];
    schedule.push(...pair.map((arm) => ({ arm, measured: true, sample })));
  }
  return schedule;
}

export function comparisonArmWorkerPath(rootDirectory: string): string {
  return path.join(
    rootDirectory,
    "packages",
    "performance-tests",
    "test",
    "quick",
    "runtime",
    ".compiled",
    "quick",
    "src",
    "cli",
    "comparisonArmCli.js"
  );
}

function isBenchmarkSample(value: unknown): value is BenchmarkSample {
  if (value === null || typeof value !== "object") return false;
  const sample = value as Record<string, unknown>;
  return (
    typeof sample.scenarioId === "string" &&
    typeof sample.fixtureId === "string" &&
    typeof sample.fixtureContentHash === "string" &&
    typeof sample.fixtureRecipeHash === "string" &&
    typeof sample.semanticDigest === "string" &&
    typeof sample.measured === "boolean" &&
    typeof sample.sample === "number" &&
    typeof sample.wallMilliseconds === "number" &&
    typeof sample.fixtureVersion === "number" &&
    typeof sample.reportSchemaVersion === "number" &&
    typeof sample.topology === "string" &&
    sample.fixtureGenerator !== null &&
    typeof sample.fixtureGenerator === "object" &&
    sample.operations !== null &&
    typeof sample.operations === "object" &&
    sample.transformerProvenance !== null &&
    typeof sample.transformerProvenance === "object" &&
    typeof (sample.transformerProvenance as Record<string, unknown>)
      .contentHash === "string" &&
    typeof (sample.transformerProvenance as Record<string, unknown>)
      .entryPoint === "string" &&
    typeof (sample.transformerProvenance as Record<string, unknown>).version ===
      "string"
  );
}

interface WorkerProcessResult {
  readonly exitCode: number | null;
  readonly stderr: string;
  readonly stdout: string;
}

async function runWorkerProcess(
  rootDirectory: string,
  harnessRootDirectory: string,
  serializedRequest: string,
  description: string,
  timeoutMilliseconds: number,
  terminationGraceMilliseconds: number
): Promise<WorkerProcessResult> {
  const workerPath = comparisonArmWorkerPath(rootDirectory);
  if (!fs.existsSync(workerPath))
    throw new Error(`Compiled A/B arm worker does not exist: ${workerPath}`);
  return new Promise<WorkerProcessResult>((resolve, reject) => {
    const child = spawn(process.execPath, [workerPath], {
      cwd: rootDirectory,
      env: {
        ...process.env,
        QUICK_PERF_ARM_REQUEST: serializedRequest,
        QUICK_PERF_HARNESS_ROOT: harnessRootDirectory,
      },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    let stdout = "";
    let stderr = "";
    let settled = false;
    let timedOut = false;
    let forceTermination: NodeJS.Timeout | undefined;
    const timeoutError = () => {
      const output = stderr || stdout;
      return new Error(
        `${description} process timed out after ${timeoutMilliseconds} ms${
          output.length === 0 ? "" : `: ${output}`
        }`
      );
    };
    const settle = (action: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (forceTermination !== undefined) clearTimeout(forceTermination);
      action();
    };
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => (stdout += chunk));
    child.stderr.on("data", (chunk: string) => (stderr += chunk));
    child.once("error", (error) =>
      settle(() =>
        reject(
          new Error(`${description} process failed to start`, { cause: error })
        )
      )
    );
    child.once("close", (exitCode) =>
      settle(() =>
        timedOut
          ? reject(timeoutError())
          : resolve({ exitCode, stderr, stdout })
      )
    );
    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      forceTermination = setTimeout(() => {
        child.kill("SIGKILL");
        child.stdout.destroy();
        child.stderr.destroy();
        child.unref();
        settle(() => reject(timeoutError()));
      }, terminationGraceMilliseconds);
    }, timeoutMilliseconds);
  });
}

export async function executeArmProcess(
  request: ArmExecutionRequest,
  terminationGraceMilliseconds = defaultWorkerTerminationGraceMilliseconds
): Promise<BenchmarkSample> {
  const resultFile = path.join(request.outputDir, "sample-result.json");
  fs.mkdirSync(request.outputDir, { recursive: true });
  const processResult = await runWorkerProcess(
    request.rootDirectory,
    request.harnessRootDirectory,
    JSON.stringify({
      ...request,
      expectedTransformerRootDirectory: request.rootDirectory,
      kind: "run-sample",
      resultFile,
    }),
    `${request.arm} sample ${request.sample}`,
    request.workerTimeoutMilliseconds ??
      defaultComparisonWorkerTimeoutMilliseconds,
    terminationGraceMilliseconds
  );
  if (processResult.exitCode !== 0)
    throw new Error(
      `${request.arm} sample ${request.sample} process failed with exit code ${processResult.exitCode}: ${processResult.stderr || processResult.stdout}`
    );
  if (!fs.existsSync(resultFile))
    throw new Error(
      `${request.arm} sample ${request.sample} process did not write a result`
    );
  const parsed: unknown = JSON.parse(fs.readFileSync(resultFile, "utf8"));
  if (!isBenchmarkSample(parsed))
    throw new Error(
      `${request.arm} sample ${request.sample} process wrote an invalid result`
    );
  return parsed;
}

export async function buildFixtureArtifactProcess(
  request: FixtureArtifactBuildRequest
): Promise<FixtureArtifactManifest> {
  const resultFile = path.join(
    path.dirname(request.artifactDirectory),
    "fixture-build-result.json"
  );
  fs.rmSync(resultFile, { force: true });
  const processResult = await runWorkerProcess(
    request.rootDirectory,
    request.harnessRootDirectory,
    JSON.stringify({
      ...request,
      expectedTransformerRootDirectory: request.rootDirectory,
      kind: "build-fixture",
      resultFile,
    }),
    "candidate fixture build",
    request.workerTimeoutMilliseconds ??
      defaultComparisonWorkerTimeoutMilliseconds,
    defaultWorkerTerminationGraceMilliseconds
  );
  if (processResult.exitCode !== 0)
    throw new Error(
      `candidate fixture build process failed with exit code ${processResult.exitCode}: ${processResult.stderr || processResult.stdout}`
    );
  if (!fs.existsSync(resultFile))
    throw new Error("Candidate fixture build process did not write a result");
  const artifact = readFixtureArtifact(request.artifactDirectory);
  const parsed: unknown = JSON.parse(fs.readFileSync(resultFile, "utf8"));
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    (parsed as Partial<FixtureArtifactManifest>).contentHash !==
      artifact.manifest.contentHash
  )
    throw new Error("Candidate fixture build process wrote an invalid result");
  return artifact.manifest;
}

export async function runComparison(
  options: ComparisonRunOptions,
  execute: ArmExecutor = executeArmProcess,
  buildFixture: FixtureArtifactBuilder = buildFixtureArtifactProcess
): Promise<ComparisonSummary> {
  const measuredSamplesPerArm =
    options.measuredSamplesPerArm ?? defaultComparisonMeasuredSamples;
  const informationalThresholdPercent =
    options.informationalThresholdPercent ??
    defaultInformationalThresholdPercent;
  const workerTimeoutMilliseconds =
    options.workerTimeoutMilliseconds ??
    defaultComparisonWorkerTimeoutMilliseconds;
  if (
    !Number.isSafeInteger(workerTimeoutMilliseconds) ||
    workerTimeoutMilliseconds < 1 ||
    workerTimeoutMilliseconds > maximumWorkerTimeoutMilliseconds
  )
    throw new Error(
      `A/B comparison worker timeout must be an integer between 1 and ${maximumWorkerTimeoutMilliseconds} milliseconds`
    );
  const schedule = createExecutionSchedule(measuredSamplesPerArm);
  prepareBenchmarkOutputDirectory(options.outputDir);
  for (const reportFile of [
    "comparison.json",
    "comparison.md",
    "comparison-samples.jsonl",
  ])
    fs.rmSync(path.join(options.outputDir, reportFile), { force: true });
  const executionRoot = path.join(options.outputDir, "executions");
  fs.rmSync(executionRoot, { recursive: true, force: true });
  fs.mkdirSync(executionRoot, { recursive: true });
  const harnessRootDirectory = path.join(
    options.candidate.rootDirectory,
    "packages",
    "performance-tests",
    "test",
    "quick"
  );
  const fixtureArtifactDirectory = path.join(
    options.outputDir,
    fixtureArtifactDirectoryName
  );
  fs.rmSync(fixtureArtifactDirectory, { recursive: true, force: true });
  const fixtureManifest = await buildFixture({
    artifactDirectory: fixtureArtifactDirectory,
    fixtureId: options.fixtureId,
    harnessRootDirectory,
    rootDirectory: options.candidate.rootDirectory,
    scenarioId: options.scenarioId,
    workerTimeoutMilliseconds,
  });
  const samples: Record<ComparisonArm, BenchmarkSample[]> = {
    baseline: [],
    candidate: [],
  };

  for (const [index, execution] of schedule.entries()) {
    const arm = options[execution.arm];
    const outputDir = path.join(
      executionRoot,
      `${String(index + 1).padStart(2, "0")}-${execution.arm}-sample-${execution.sample}`
    );
    const sample = await execute({
      ...execution,
      fixtureArtifactDirectory,
      fixtureId: options.fixtureId,
      harnessRootDirectory,
      outputDir,
      revision: arm.revision,
      rootDirectory: arm.rootDirectory,
      scenarioId: options.scenarioId,
      workerTimeoutMilliseconds,
    });
    if (sample.fixtureContentHash !== fixtureManifest.contentHash)
      throw new Error(
        `${execution.arm} sample ${execution.sample} did not consume the candidate-authored fixture artifact`
      );
    samples[execution.arm].push(sample);
  }

  return ComparisonReporter.write(options.outputDir, {
    baseline: {
      revision: options.baseline.revision,
      samples: samples.baseline,
    },
    candidate: {
      revision: options.candidate.revision,
      samples: samples.candidate,
    },
    informationalThresholdPercent,
    measuredSamplesPerArm,
    ordering: schedule.map((execution) => execution.arm),
  });
}
