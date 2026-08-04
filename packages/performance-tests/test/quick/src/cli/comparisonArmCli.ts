/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import {
  BenchmarkRunner,
  BenchmarkSample,
} from "../framework/BenchmarkRunner.js";
import { resolveBenchmarkRun } from "../framework/BenchmarkResolution.js";

interface ArmWorkerRequest {
  readonly fixtureId?: string;
  readonly measured: boolean;
  readonly outputDir: string;
  readonly resultFile: string;
  readonly sample: number;
  readonly scenarioId?: string;
}

function parseRequest(value: string | undefined): ArmWorkerRequest {
  if (value === undefined)
    throw new Error("QUICK_PERF_ARM_REQUEST is required");
  const parsed: unknown = JSON.parse(value);
  if (parsed === null || typeof parsed !== "object")
    throw new Error("QUICK_PERF_ARM_REQUEST must be an object");
  const request = parsed as Partial<ArmWorkerRequest>;
  if (
    typeof request.measured !== "boolean" ||
    typeof request.outputDir !== "string" ||
    typeof request.resultFile !== "string" ||
    typeof request.sample !== "number" ||
    (request.fixtureId !== undefined &&
      typeof request.fixtureId !== "string") ||
    (request.scenarioId !== undefined && typeof request.scenarioId !== "string")
  )
    throw new Error("QUICK_PERF_ARM_REQUEST has an invalid shape");
  return request as ArmWorkerRequest;
}

async function main(): Promise<BenchmarkSample> {
  const request = parseRequest(process.env.QUICK_PERF_ARM_REQUEST);
  const { fixture, scenario } = resolveBenchmarkRun(
    request.scenarioId,
    request.fixtureId
  );
  const sample = await new BenchmarkRunner(
    fixture,
    request.outputDir,
    scenario
  ).runSample(request.sample, request.measured);
  fs.writeFileSync(
    request.resultFile,
    `${JSON.stringify(sample, undefined, 2)}\n`
  );
  return sample;
}

void main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`
  );
  process.exitCode = 1;
});
