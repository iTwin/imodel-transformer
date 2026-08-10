/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import {
  defaultComparisonMeasuredSamples,
  defaultComparisonWorkerTimeoutMilliseconds,
  defaultInformationalThresholdPercent,
  runComparison,
} from "../comparison/ComparisonRunner.js";
import { quickPath, quickRootDirectory } from "../support/paths.js";

function trimmed(value: string | undefined): string | undefined {
  const result = value?.trim();
  return result === undefined || result.length === 0 ? undefined : result;
}

function requiredPath(
  env: NodeJS.ProcessEnv,
  name: string,
  defaultValue?: string
): string {
  const value = trimmed(env[name]) ?? defaultValue;
  if (value === undefined) throw new Error(`${name} is required`);
  return path.resolve(value);
}

function positiveInteger(
  value: string | undefined,
  name: string,
  defaultValue: number
): number {
  const configured = trimmed(value);
  if (configured === undefined) return defaultValue;
  if (
    !/^[1-9]\d*$/.test(configured) ||
    !Number.isSafeInteger(Number(configured))
  )
    throw new Error(`${name} must be a positive safe integer`);
  return Number(configured);
}

function nonNegativeNumber(
  value: string | undefined,
  name: string,
  defaultValue: number
): number {
  const configured = trimmed(value);
  if (configured === undefined) return defaultValue;
  const parsed = Number(configured);
  if (!Number.isFinite(parsed) || parsed < 0)
    throw new Error(`${name} must be a non-negative number`);
  return parsed;
}

async function main(): Promise<void> {
  const candidateDefault = path.resolve(
    quickRootDirectory,
    "..",
    "..",
    "..",
    ".."
  );
  const outputDir = requiredPath(
    process.env,
    "QUICK_PERF_COMPARISON_OUTPUT",
    quickPath(".quick-output", "comparison")
  );
  const summary = await runComparison({
    baseline: {
      revision: trimmed(process.env.QUICK_PERF_BASELINE_REVISION) ?? "baseline",
      rootDirectory: requiredPath(process.env, "QUICK_PERF_BASELINE_ROOT"),
    },
    candidate: {
      revision:
        trimmed(process.env.QUICK_PERF_CANDIDATE_REVISION) ?? "candidate",
      rootDirectory: requiredPath(
        process.env,
        "QUICK_PERF_CANDIDATE_ROOT",
        candidateDefault
      ),
    },
    fixtureId: trimmed(process.env.QUICK_PERF_FIXTURE),
    informationalThresholdPercent: nonNegativeNumber(
      process.env.QUICK_PERF_COMPARISON_THRESHOLD_PERCENT,
      "QUICK_PERF_COMPARISON_THRESHOLD_PERCENT",
      defaultInformationalThresholdPercent
    ),
    measuredSamplesPerArm: positiveInteger(
      process.env.QUICK_PERF_COMPARISON_SAMPLES,
      "QUICK_PERF_COMPARISON_SAMPLES",
      defaultComparisonMeasuredSamples
    ),
    outputDir,
    scenarioId: trimmed(process.env.QUICK_PERF_SCENARIO),
    workerTimeoutMilliseconds:
      positiveInteger(
        process.env.QUICK_PERF_COMPARISON_WORKER_TIMEOUT_SECONDS,
        "QUICK_PERF_COMPARISON_WORKER_TIMEOUT_SECONDS",
        defaultComparisonWorkerTimeoutMilliseconds / 1000
      ) * 1000,
  });
  process.stdout.write(
    `${fs.readFileSync(path.join(outputDir, "comparison.md"), "utf8")}\n`
  );
  if (!summary.informationalOnly)
    throw new Error("A/B comparison report must remain informational");
}

void main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`
  );
  process.exitCode = 1;
});
