/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { spawn } from "node:child_process";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import { BriefcaseDb, IModelHost } from "@itwin/core-backend";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
  artifactBriefcasePath,
  artifactManifestFileName,
  FixtureArtifact,
  readChangesetFileProps,
  readFixtureArtifact,
  readFixtureRecipeData,
} from "../fixtures/FixtureArtifact.js";
import type { PreparedDetachedDataset } from "../fixtures/FixtureProvider.js";
import { createChangesetScanningBenchmark } from "../scenarios/changesetScanning.js";
import {
  ArmRuntimeIdentity,
  ArmSpec,
  assertArmRuntimeComparable,
  assertArmSpecsComparable,
  loadArmModule,
  resolveArmSpec,
} from "./ArmModule.js";
import { classifyEnvironment, EnvironmentClass } from "./EnvironmentClass.js";
import {
  ExecutionFingerprint,
  executionFingerprintKey,
} from "./ExecutionFingerprint.js";
import {
  CollapsedPair,
  collapsePair,
  logRatioToPercent,
  PairObservation,
  PairOrder,
} from "./logRatio.js";
import { deriveNoiseBand, NoiseBand, NoiseBandPool } from "./NoiseBand.js";

export const comparisonScenarioId = "changeset-scanning";
export const comparisonWarmups = 1;
export const comparisonMeasuredSamples = 3;
export const comparisonExecutionsPerPair = 8;
export const comparisonExecution: ExecutionFingerprint = {
  warmupSamplesPerArm: comparisonWarmups,
  measuredSamplesPerArm: comparisonMeasuredSamples,
  processPolicy: {
    kind: "one-process-per-arm",
    restartBetweenPairs: true,
  },
  pairPolicy: { kind: "paired", pairsPerJob: 1 },
  orderPolicy: { kind: "alternating", first: "AB" },
};

export interface ComparisonFingerprint {
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly recipeHash: string;
  readonly workloadHash: string;
  readonly execution: ExecutionFingerprint;
}

export interface ArmSource {
  readonly ref: string;
  readonly sha: string;
}

export interface ArmRunRequest {
  readonly arm: ArmSpec;
  readonly source: ArmSource;
  readonly scenarioId: string;
  readonly fixtureDirectory: string;
  readonly fixtureArtifactHash: string;
  readonly fingerprint: ComparisonFingerprint;
  readonly outputDirectory: string;
}

export interface RawArmSample {
  readonly sample: number;
  readonly measured: boolean;
  readonly wallMilliseconds: number;
  readonly semanticDigest: string;
  readonly reconstructionMilliseconds: number;
  readonly verificationMilliseconds: number;
  readonly teardownMilliseconds: number;
}

export interface ArmRunResult {
  readonly arm: ArmSpec;
  readonly source: ArmSource;
  readonly runtime: ArmRuntimeIdentity;
  readonly fingerprint: ComparisonFingerprint;
  readonly fixtureArtifactHash: string;
  readonly samples: readonly RawArmSample[];
  readonly generatedAt: string;
}

export interface PairRunArtifact {
  readonly jobId: string;
  readonly pair: number;
  readonly order: PairOrder;
  readonly fingerprint: ComparisonFingerprint;
  readonly environment: EnvironmentClass;
  readonly fixtureArtifactHash: string;
  readonly armA?: ArmRunResult;
  readonly armB?: ArmRunResult;
  readonly observation?: PairObservation;
  readonly collapsed?: CollapsedPair;
  readonly discardedReason?: string;
  readonly generatedAt: string;
}

export interface CalibrationArtifact {
  readonly fingerprint: ComparisonFingerprint;
  readonly environment: EnvironmentClass;
  readonly pool: NoiseBandPool;
  readonly band: NoiseBand;
  readonly refs: readonly ArmSource[];
  readonly orders: readonly PairOrder[];
  readonly generatedAt: string;
}

export type ArmLauncher = (
  request: ArmRunRequest,
  timeoutMilliseconds: number
) => Promise<unknown>;

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value))
    return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  return `{${Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
}

function sha256(value: string | Buffer): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export function comparisonFingerprintKey(
  fingerprint: ComparisonFingerprint
): string {
  return canonicalJson({
    scenarioId: fingerprint.scenarioId,
    fixtureId: fingerprint.fixtureId,
    recipeHash: fingerprint.recipeHash,
    workloadHash: fingerprint.workloadHash,
    execution: executionFingerprintKey(fingerprint.execution),
  });
}

export function assertComparisonFingerprintMatches(
  actual: ComparisonFingerprint,
  expected: ComparisonFingerprint
): void {
  const actualKey = comparisonFingerprintKey(actual);
  const expectedKey = comparisonFingerprintKey(expected);
  if (actualKey !== expectedKey)
    throw new Error(
      `Comparison fingerprint does not match: ${actualKey} != ${expectedKey}`
    );
}

export function fingerprintForArtifact(
  fixtureDirectory: string
): ComparisonFingerprint {
  const { descriptor } = readFixtureArtifact(fixtureDirectory).manifest;
  return {
    scenarioId: comparisonScenarioId,
    fixtureId: descriptor.id,
    recipeHash: descriptor.recipeHash,
    workloadHash: sha256(canonicalJson(descriptor.distribution)),
    execution: comparisonExecution,
  };
}

export function hashFixtureArtifact(directory: string): string {
  const files: string[] = [];
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) visit(absolute);
      else if (entry.isFile()) files.push(absolute);
    }
  };
  visit(directory);
  const hash = crypto.createHash("sha256");
  for (const file of files.sort()) {
    hash.update(path.relative(directory, file).split(path.sep).join("/"));
    hash.update("\0");
    hash.update(fs.readFileSync(file));
    hash.update("\0");
  }
  return hash.digest("hex");
}

async function withHost<T>(
  profileName: string,
  operation: () => Promise<T>
): Promise<T> {
  await IModelHost.startup({ profileName });
  const profileDir = IModelHost.profileDir;
  try {
    return await operation();
  } finally {
    await IModelHost.shutdown();
    if (profileDir.includes(profileName))
      fs.rmSync(profileDir, { recursive: true, force: true });
  }
}

async function materializeArtifact(
  artifact: FixtureArtifact,
  sampleDirectory: string
): Promise<PreparedDetachedDataset> {
  const start = process.hrtime.bigint();
  fs.rmSync(sampleDirectory, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(sampleDirectory), { recursive: true });
  fs.cpSync(artifact.directory, sampleDirectory, { recursive: true });
  const sourceDb = await BriefcaseDb.open({
    fileName: artifactBriefcasePath(sampleDirectory),
    readonly: true,
  });
  return {
    topology: "source-only",
    descriptor: artifact.manifest.descriptor,
    directory: sampleDirectory,
    sourceDb,
    csFileProps: readChangesetFileProps(sampleDirectory),
    manifest: artifact.manifest,
    recipe: readFixtureRecipeData(sampleDirectory, artifact.manifest),
    reconstructionMilliseconds:
      Number(process.hrtime.bigint() - start) / 1_000_000,
  };
}

function validateArmResult(
  value: unknown,
  request: ArmRunRequest
): ArmRunResult {
  if (value === null || typeof value !== "object")
    throw new Error(`Arm "${request.arm.id}" returned malformed output`);
  const result = value as Partial<ArmRunResult>;
  if (
    !Array.isArray(result.samples) ||
    result.samples.length !== comparisonWarmups + comparisonMeasuredSamples ||
    result.samples.filter((sample) => sample.measured).length !==
      comparisonMeasuredSamples ||
    result.samples[0]?.measured !== false
  )
    throw new Error(
      `Arm "${request.arm.id}" did not return exactly one warm-up and three measured samples`
    );
  for (const [index, sample] of result.samples.entries()) {
    if (
      sample.sample !== index ||
      typeof sample.wallMilliseconds !== "number" ||
      !Number.isFinite(sample.wallMilliseconds) ||
      sample.wallMilliseconds <= 0 ||
      typeof sample.semanticDigest !== "string"
    )
      throw new Error(
        `Arm "${request.arm.id}" returned malformed sample ${index}`
      );
  }
  if (
    !result.runtime ||
    result.arm?.id !== request.arm.id ||
    result.runtime.armId !== request.arm.id ||
    result.source?.ref !== request.source.ref ||
    result.source.sha !== request.source.sha ||
    result.fixtureArtifactHash !== request.fixtureArtifactHash ||
    !result.fingerprint
  )
    throw new Error(`Arm "${request.arm.id}" returned malformed identity data`);
  assertComparisonFingerprintMatches(result.fingerprint, request.fingerprint);
  return result as ArmRunResult;
}

export async function runArm(request: ArmRunRequest): Promise<ArmRunResult> {
  if (request.scenarioId !== comparisonScenarioId)
    throw new Error(
      `Unsupported comparison scenario "${request.scenarioId}"; only "${comparisonScenarioId}" is implemented`
    );
  const artifact = readFixtureArtifact(request.fixtureDirectory);
  const artifactHash = hashFixtureArtifact(request.fixtureDirectory);
  if (artifactHash !== request.fixtureArtifactHash)
    throw new Error(
      `Fixture artifact hash changed: ${artifactHash} != ${request.fixtureArtifactHash}`
    );
  const actualFingerprint = fingerprintForArtifact(request.fixtureDirectory);
  assertComparisonFingerprintMatches(actualFingerprint, request.fingerprint);
  const resolvedArm = resolveArmSpec(request.arm);
  const loaded = loadArmModule(resolvedArm);
  const samples: RawArmSample[] = [];
  await withHost(
    `quick-compare-${request.arm.id.replace(/[^A-Za-z0-9_-]/g, "-")}-${
      process.pid
    }`,
    async () => {
      for (
        let sample = 0;
        sample < comparisonWarmups + comparisonMeasuredSamples;
        sample++
      ) {
        const sampleDirectory = path.join(
          request.outputDirectory,
          `sample-${sample}`
        );
        const dataset = await materializeArtifact(artifact, sampleDirectory);
        const scenario = createChangesetScanningBenchmark(
          loaded.changedInstanceIds
        ).scenario.factory(dataset);
        let operationError: unknown;
        let completed: Omit<RawArmSample, "teardownMilliseconds"> | undefined;
        try {
          const start = process.hrtime.bigint();
          await scenario.measure();
          const wallMilliseconds =
            Number(process.hrtime.bigint() - start) / 1_000_000;
          const verificationStart = process.hrtime.bigint();
          const semanticDigest = await scenario.finish();
          const verificationMilliseconds =
            Number(process.hrtime.bigint() - verificationStart) / 1_000_000;
          completed = {
            sample,
            measured: sample >= comparisonWarmups,
            wallMilliseconds,
            semanticDigest,
            reconstructionMilliseconds: dataset.reconstructionMilliseconds,
            verificationMilliseconds,
          };
        } catch (error) {
          operationError = error;
        }
        const teardownStart = process.hrtime.bigint();
        let cleanupError: unknown;
        try {
          if (operationError) scenario.abort();
          dataset.sourceDb.close();
          fs.rmSync(sampleDirectory, { recursive: true, force: true });
        } catch (error) {
          cleanupError = error;
        }
        const teardownMilliseconds =
          Number(process.hrtime.bigint() - teardownStart) / 1_000_000;
        if (operationError && cleanupError)
          throw new AggregateError(
            [operationError, cleanupError],
            `Arm "${request.arm.id}" sample ${sample} and cleanup both failed`
          );
        if (operationError) throw normalizeError(operationError);
        if (cleanupError) throw normalizeError(cleanupError);
        if (!completed)
          throw new Error(
            `Arm "${request.arm.id}" sample ${sample} produced no result`
          );
        samples.push({ ...completed, teardownMilliseconds });
      }
    }
  );
  const digests = new Set(samples.map((sample) => sample.semanticDigest));
  if (digests.size !== 1)
    throw new Error(`Arm "${request.arm.id}" produced inconsistent semantics`);
  return {
    arm: request.arm,
    source: request.source,
    runtime: loaded.runtime,
    fingerprint: actualFingerprint,
    fixtureArtifactHash: artifactHash,
    samples,
    generatedAt: new Date().toISOString(),
  };
}

export async function spawnArmProcess(
  armProcessPath: string,
  request: ArmRunRequest,
  timeoutMilliseconds: number
): Promise<unknown> {
  fs.mkdirSync(request.outputDirectory, { recursive: true });
  const requestFile = path.join(request.outputDirectory, "request.json");
  const resultFile = path.join(request.outputDirectory, "arm-result.json");
  fs.writeFileSync(requestFile, `${JSON.stringify(request, undefined, 2)}\n`);
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [armProcessPath, "--request", requestFile, "--output", resultFile],
      { stdio: ["ignore", "inherit", "inherit"], shell: false }
    );
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill();
    }, timeoutMilliseconds);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      if (timedOut)
        reject(
          new Error(
            `Arm "${request.arm.id}" timed out after ${timeoutMilliseconds}ms`
          )
        );
      else if (code !== 0)
        reject(
          new Error(
            `Arm "${request.arm.id}" exited with code ${String(
              code
            )} and signal ${String(signal)}`
          )
        );
      else if (!fs.existsSync(resultFile))
        reject(new Error(`Arm "${request.arm.id}" produced no result file`));
      else {
        try {
          resolve(JSON.parse(fs.readFileSync(resultFile, "utf8")));
        } catch (error) {
          reject(
            new Error(`Arm "${request.arm.id}" produced malformed JSON`, {
              cause: error,
            })
          );
        }
      }
    });
  });
}

export async function runPair(options: {
  readonly jobId: string;
  readonly pair: number;
  readonly order: PairOrder;
  readonly scenarioId: string;
  readonly fixtureDirectory: string;
  readonly armA: ArmSpec;
  readonly armASource: ArmSource;
  readonly armB: ArmSpec;
  readonly armBSource: ArmSource;
  readonly outputDirectory: string;
  readonly armProcessPath?: string;
  readonly timeoutMilliseconds?: number;
  readonly launcher?: ArmLauncher;
}): Promise<PairRunArtifact> {
  const generatedAt = new Date().toISOString();
  const environment = classifyEnvironment();
  const fixtureArtifactHash = hashFixtureArtifact(options.fixtureDirectory);
  const fingerprint = fingerprintForArtifact(options.fixtureDirectory);
  if (options.scenarioId !== comparisonScenarioId)
    throw new Error(
      `Unsupported comparison scenario "${options.scenarioId}"; only "${comparisonScenarioId}" is implemented`
    );
  if (options.order !== "AB" && options.order !== "BA")
    throw new Error(
      `Pair order must be AB or BA, received "${String(options.order)}"`
    );
  if (options.jobId.trim().length === 0)
    throw new Error("Pair job id cannot be empty");
  const scheduledOrder: PairOrder = options.pair % 2 === 0 ? "AB" : "BA";
  if (options.order !== scheduledOrder)
    throw new Error(
      `Pair ${options.pair} must use ${scheduledOrder}, received ${options.order}`
    );
  const resolvedA = resolveArmSpec(options.armA);
  const resolvedB = resolveArmSpec(options.armB);
  assertArmSpecsComparable(resolvedA, resolvedB);
  const requests: Record<"A" | "B", ArmRunRequest> = {
    A: {
      arm: options.armA,
      source: options.armASource,
      scenarioId: options.scenarioId,
      fixtureDirectory: options.fixtureDirectory,
      fixtureArtifactHash,
      fingerprint,
      outputDirectory: path.join(options.outputDirectory, "arm-a"),
    },
    B: {
      arm: options.armB,
      source: options.armBSource,
      scenarioId: options.scenarioId,
      fixtureDirectory: options.fixtureDirectory,
      fixtureArtifactHash,
      fingerprint,
      outputDirectory: path.join(options.outputDirectory, "arm-b"),
    },
  };
  const launcher =
    options.launcher ??
    (async (request, timeout) =>
      spawnArmProcess(
        options.armProcessPath ??
          fileURLToPath(new URL("../cli/armProcessCli.js", import.meta.url)),
        request,
        timeout
      ));
  const results: Partial<Record<"A" | "B", ArmRunResult>> = {};
  try {
    for (const label of options.order.split("") as ("A" | "B")[]) {
      const request = requests[label];
      results[label] = validateArmResult(
        await launcher(request, options.timeoutMilliseconds ?? 10 * 60 * 1000),
        request
      );
    }
    const armA = results.A;
    const armB = results.B;
    if (!armA || !armB) throw new Error("Pair did not execute both arms");
    assertArmRuntimeComparable(armA.runtime, armB.runtime);
    if (armA.fixtureArtifactHash !== armB.fixtureArtifactHash)
      throw new Error("Arms consumed different fixture artifact bytes");
    if (
      new Set([
        ...armA.samples.map((sample) => sample.semanticDigest),
        ...armB.samples.map((sample) => sample.semanticDigest),
      ]).size !== 1
    )
      throw new Error("Arms produced different semantic results");
    const armASamples = armA.samples
      .filter((sample) => sample.measured)
      .map((sample) => sample.wallMilliseconds);
    const armBSamples = armB.samples
      .filter((sample) => sample.measured)
      .map((sample) => sample.wallMilliseconds);
    const observation: PairObservation = {
      pair: options.pair,
      order: options.order,
      armASamples,
      armBSamples,
    };
    return {
      jobId: options.jobId,
      pair: options.pair,
      order: options.order,
      fingerprint,
      environment,
      fixtureArtifactHash,
      armA,
      armB,
      observation,
      collapsed: collapsePair(observation),
      generatedAt,
    };
  } catch (error) {
    return {
      jobId: options.jobId,
      pair: options.pair,
      order: options.order,
      fingerprint,
      environment,
      fixtureArtifactHash,
      armA: results.A,
      armB: results.B,
      discardedReason: error instanceof Error ? error.message : String(error),
      generatedAt,
    };
  }
}

export function aggregateCalibration(
  artifacts: readonly PairRunArtifact[]
): CalibrationArtifact {
  if (artifacts.length < 3)
    throw new Error(
      "Calibration requires at least three independent job artifacts"
    );
  const first = artifacts[0];
  const jobs = new Set<string>();
  const observations: number[] = [];
  const refs: ArmSource[] = [];
  for (const artifact of artifacts) {
    if (jobs.has(artifact.jobId))
      throw new Error(`Duplicate calibration job id: ${artifact.jobId}`);
    jobs.add(artifact.jobId);
    assertComparisonFingerprintMatches(artifact.fingerprint, first.fingerprint);
    if (artifact.environment.id !== first.environment.id)
      throw new Error(
        `Calibration environment mismatch: ${artifact.environment.id} != ${first.environment.id}`
      );
    if (artifact.discardedReason || !artifact.collapsed)
      throw new Error(
        `Calibration pair ${artifact.jobId} was discarded: ${
          artifact.discardedReason ?? "missing collapsed observation"
        }`
      );
    const expectedOrder: PairOrder = artifact.pair % 2 === 0 ? "AB" : "BA";
    if (artifact.order !== expectedOrder)
      throw new Error(
        `Calibration job ${artifact.jobId} used ${artifact.order}; pair ${artifact.pair} requires ${expectedOrder}`
      );
    if (
      !artifact.armA ||
      !artifact.armB ||
      artifact.armA.source.sha !== artifact.armB.source.sha ||
      artifact.armA.runtime.transformerVersion !==
        artifact.armB.runtime.transformerVersion ||
      artifact.armA.runtime.transformerPackageHash !==
        artifact.armB.runtime.transformerPackageHash
    )
      throw new Error(
        `Calibration pair ${artifact.jobId} did not use the same build for both labeled arms`
      );
    observations.push(artifact.collapsed.logRatio);
    refs.push(artifact.armA.source);
  }
  if (refs.some((source) => source.sha !== refs[0].sha))
    throw new Error(
      "Calibration jobs did not use one identical calibration build"
    );
  const pool: NoiseBandPool = {
    scenarioId: first.fingerprint.scenarioId,
    fixtureId: first.fingerprint.fixtureId,
    recipeHash: first.fingerprint.recipeHash,
    environmentClass: first.environment.id,
    execution: first.fingerprint.execution,
    kind: "paired",
    observations,
    independentJobs: jobs.size,
    updatedAt: new Date().toISOString(),
  };
  return {
    fingerprint: first.fingerprint,
    environment: first.environment,
    pool,
    band: deriveNoiseBand(pool, 1),
    refs,
    orders: artifacts.map((artifact) => artifact.order),
    generatedAt: new Date().toISOString(),
  };
}

export function renderCalibration(calibration: CalibrationArtifact): string {
  return [
    "# Quick performance A/A calibration",
    "",
    `**Status:** ${calibration.band.status}`,
    "",
    `**Quality:** ${calibration.band.quality} (informational)`,
    "",
    "| Property | Value |",
    "|---|---|",
    `| Scenario | ${calibration.fingerprint.scenarioId} |`,
    `| Fixture | ${calibration.fingerprint.fixtureId} |`,
    `| Recipe hash | ${calibration.fingerprint.recipeHash} |`,
    `| Fingerprint key | \`${sha256(
      comparisonFingerprintKey(calibration.fingerprint)
    )}\` |`,
    `| Independent jobs | ${calibration.pool.independentJobs} |`,
    `| Calibration ref / SHA | ${calibration.refs[0]?.ref ?? "n/a"} / ${
      calibration.refs[0]?.sha ?? "n/a"
    } |`,
    `| Pair orders | ${calibration.orders.join(", ")} |`,
    `| Pair log-ratios | ${calibration.pool.observations
      .map((value) => value.toFixed(6))
      .join(", ")} |`,
    `| A/A band | ${calibration.band.bandPercent.toFixed(2)}% |`,
    "| Target | <=5% actionable; 5-10% marginal; >=10% unresolvable |",
    "",
    "This calibration is informational and does not create a merge-blocking gate.",
    "",
  ].join("\n");
}

export function renderPairSummary(artifact: PairRunArtifact): string {
  const lines = [
    "# Quick performance pair",
    "",
    `**Status:** ${artifact.discardedReason ? "DISCARDED" : "VALID"}`,
    "",
    "| Property | Value |",
    "|---|---|",
    `| Job | ${artifact.jobId} |`,
    `| Scenario | ${artifact.fingerprint.scenarioId} |`,
    `| Fixture | ${artifact.fingerprint.fixtureId} |`,
    `| Fixture artifact hash | ${artifact.fixtureArtifactHash} |`,
    `| Recipe hash | ${artifact.fingerprint.recipeHash} |`,
    `| Fingerprint key | \`${sha256(
      comparisonFingerprintKey(artifact.fingerprint)
    )}\` |`,
    `| Order | ${artifact.order} |`,
    `| Arm A ref / SHA | ${artifact.armA?.source.ref ?? "n/a"} / ${
      artifact.armA?.source.sha ?? "n/a"
    } |`,
    `| Arm B ref / SHA | ${artifact.armB?.source.ref ?? "n/a"} / ${
      artifact.armB?.source.sha ?? "n/a"
    } |`,
    `| Arm A median | ${artifact.collapsed?.armA.toFixed(3) ?? "n/a"} ms |`,
    `| Arm B median | ${artifact.collapsed?.armB.toFixed(3) ?? "n/a"} ms |`,
    `| Delta | ${
      artifact.collapsed
        ? `${logRatioToPercent(artifact.collapsed.logRatio).toFixed(2)}%`
        : "n/a"
    } |`,
  ];
  if (artifact.discardedReason)
    lines.push("", `Discarded pair: ${artifact.discardedReason}`);
  lines.push(
    "",
    "This result is informational and does not create a merge-blocking gate.",
    ""
  );
  return lines.join("\n");
}

export function readPairArtifact(fileName: string): PairRunArtifact {
  return JSON.parse(fs.readFileSync(fileName, "utf8")) as PairRunArtifact;
}

export function readCalibrationArtifact(fileName: string): CalibrationArtifact {
  return JSON.parse(fs.readFileSync(fileName, "utf8")) as CalibrationArtifact;
}

export function writeJson(fileName: string, value: unknown): void {
  fs.mkdirSync(path.dirname(fileName), { recursive: true });
  fs.writeFileSync(fileName, `${JSON.stringify(value, undefined, 2)}\n`);
}

export { artifactManifestFileName };
