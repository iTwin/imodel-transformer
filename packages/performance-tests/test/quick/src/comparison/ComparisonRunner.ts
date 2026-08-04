/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  BenchmarkSample,
  prepareBenchmarkOutputDirectory,
} from "../framework/BenchmarkRunner.js";
import {
  ComparisonArm,
  ComparisonReporter,
  ComparisonSummary,
} from "./ComparisonReport.js";

export const defaultComparisonMeasuredSamples = 3;
export const defaultInformationalThresholdPercent = 5;

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
}

export interface ArmExecutionRequest {
  readonly arm: ComparisonArm;
  readonly fixtureId?: string;
  readonly harnessRootDirectory: string;
  readonly measured: boolean;
  readonly outputDir: string;
  readonly revision: string;
  readonly rootDirectory: string;
  readonly sample: number;
  readonly scenarioId?: string;
}

export type ArmExecutor = (
  request: ArmExecutionRequest
) => Promise<BenchmarkSample>;

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
    typeof sample.operations === "object"
  );
}

export async function executeArmProcess(
  request: ArmExecutionRequest
): Promise<BenchmarkSample> {
  const workerPath = comparisonArmWorkerPath(request.rootDirectory);
  if (!fs.existsSync(workerPath))
    throw new Error(
      `Compiled A/B arm worker does not exist for ${request.arm}: ${workerPath}`
    );
  const resultFile = path.join(request.outputDir, "sample-result.json");
  fs.mkdirSync(request.outputDir, { recursive: true });
  const serializedRequest = JSON.stringify({ ...request, resultFile });
  const processResult = await new Promise<{
    exitCode: number | null;
    stderr: string;
    stdout: string;
  }>((resolve, reject) => {
    const child = spawn(process.execPath, [workerPath], {
      cwd: request.rootDirectory,
      env: {
        ...process.env,
        QUICK_PERF_ARM_REQUEST: serializedRequest,
        QUICK_PERF_HARNESS_ROOT: request.harnessRootDirectory,
      },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => (stdout += chunk));
    child.stderr.on("data", (chunk: string) => (stderr += chunk));
    child.once("error", reject);
    child.once("close", (exitCode) => resolve({ exitCode, stderr, stdout }));
  });
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

export async function runComparison(
  options: ComparisonRunOptions,
  execute: ArmExecutor = executeArmProcess
): Promise<ComparisonSummary> {
  const measuredSamplesPerArm =
    options.measuredSamplesPerArm ?? defaultComparisonMeasuredSamples;
  const informationalThresholdPercent =
    options.informationalThresholdPercent ??
    defaultInformationalThresholdPercent;
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
      fixtureId: options.fixtureId,
      harnessRootDirectory: path.join(
        options.candidate.rootDirectory,
        "packages",
        "performance-tests",
        "test",
        "quick"
      ),
      outputDir,
      revision: arm.revision,
      rootDirectory: arm.rootDirectory,
      scenarioId: options.scenarioId,
    });
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
