/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import {
  BenchmarkRunner,
  BenchmarkSample,
} from "../framework/BenchmarkRunner.js";
import { resolveBenchmarkRun } from "../framework/BenchmarkResolution.js";
import { resolveTransformerProvenance } from "../comparison/TransformerProvenance.js";
import { FixtureArtifactManifest } from "../fixtures/FixtureArtifact.js";

interface ArmWorkerRequestBase {
  readonly expectedTransformerRootDirectory: string;
  readonly fixtureId?: string;
  readonly resultFile: string;
  readonly scenarioId?: string;
}

interface FixtureBuildWorkerRequest extends ArmWorkerRequestBase {
  readonly artifactDirectory: string;
  readonly kind: "build-fixture";
}

interface SampleWorkerRequest extends ArmWorkerRequestBase {
  readonly fixtureArtifactDirectory: string;
  readonly kind: "run-sample";
  readonly measured: boolean;
  readonly outputDir: string;
  readonly sample: number;
}

type ArmWorkerRequest = FixtureBuildWorkerRequest | SampleWorkerRequest;

function parseRequest(value: string | undefined): ArmWorkerRequest {
  if (value === undefined)
    throw new Error("QUICK_PERF_ARM_REQUEST is required");
  const parsed: unknown = JSON.parse(value);
  if (parsed === null || typeof parsed !== "object")
    throw new Error("QUICK_PERF_ARM_REQUEST must be an object");
  const request = parsed as Partial<ArmWorkerRequest>;
  if (
    typeof request.expectedTransformerRootDirectory !== "string" ||
    typeof request.resultFile !== "string" ||
    (request.fixtureId !== undefined &&
      typeof request.fixtureId !== "string") ||
    (request.scenarioId !== undefined && typeof request.scenarioId !== "string")
  )
    throw new Error("QUICK_PERF_ARM_REQUEST has an invalid shape");
  if (
    request.kind === "build-fixture" &&
    typeof request.artifactDirectory === "string"
  )
    return request as FixtureBuildWorkerRequest;
  if (
    request.kind === "run-sample" &&
    typeof request.fixtureArtifactDirectory === "string" &&
    typeof request.measured === "boolean" &&
    typeof request.outputDir === "string" &&
    typeof request.sample === "number"
  )
    return request as SampleWorkerRequest;
  throw new Error("QUICK_PERF_ARM_REQUEST has an invalid operation");
}

async function main(): Promise<BenchmarkSample | FixtureArtifactManifest> {
  const request = parseRequest(process.env.QUICK_PERF_ARM_REQUEST);
  const transformerProvenance = resolveTransformerProvenance(
    request.expectedTransformerRootDirectory
  );
  const { fixture, scenario } = resolveBenchmarkRun(
    request.scenarioId,
    request.fixtureId
  );
  const runner = new BenchmarkRunner(
    fixture,
    request.kind === "run-sample"
      ? request.outputDir
      : path.dirname(request.artifactDirectory),
    scenario
  );
  const result =
    request.kind === "build-fixture"
      ? await runner.buildReusableFixtureArtifact(request.artifactDirectory)
      : await runner.runSample(
          request.sample,
          request.measured,
          request.fixtureArtifactDirectory,
          transformerProvenance
        );
  fs.writeFileSync(
    request.resultFile,
    `${JSON.stringify(result, undefined, 2)}\n`
  );
  return result;
}

void main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`
  );
  process.exitCode = 1;
});
