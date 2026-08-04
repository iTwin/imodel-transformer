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
import { createChangesetScanningBenchmark } from "../scenarios/changesetScanningFactory.js";
import {
  ArmRuntimeIdentity,
  ArmSpec,
  assertArmRuntimeComparable,
  assertArmSpecsComparable,
  loadArmModule,
  resolveArmSpec,
} from "./ArmModule.js";
import {
  classifyEnvironment,
  EnvironmentClass,
  validateEnvironmentClass,
} from "./EnvironmentClass.js";
import {
  ExecutionFingerprint,
  executionFingerprintKey,
  expectedPairOrder,
  expectedPairOrders,
  validateExecutionFingerprint,
} from "./ExecutionFingerprint.js";
import {
  CollapsedPair,
  collapsePair,
  logRatioToPercent,
  PairObservation,
  PairOrder,
  validateCollapsedPair,
} from "./logRatio.js";
import {
  assertBandApplies,
  assertBandDerivedFromPool,
  assertPoolApplies,
  CalibrationRequirements,
  deriveNoiseBand,
  NoiseBand,
  NoiseBandKey,
  NoiseBandPool,
  validateNoiseBand,
  validateNoiseBandPool,
} from "./NoiseBand.js";
import {
  ComparisonFixtureIdentity,
  readComparisonFixtureIdentity,
  validateComparisonFixtureIdentity,
} from "./ComparisonFixtureIdentity.js";

export const comparisonScenarioId = "changeset-scanning";
export const comparisonArtifactVersion = 2;
export const comparisonHarnessVersion = "changeset-scanning-isolated-v3";
const comparisonImplementationExtension = path.extname(
  fileURLToPath(import.meta.url)
);
const quickSourceRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  ".."
);

function comparisonImplementationFiles(): string[] {
  const files: string[] = [];
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) visit(absolute);
      else if (
        entry.isFile() &&
        path.extname(entry.name) === comparisonImplementationExtension
      )
        files.push(absolute);
    }
  };
  visit(quickSourceRoot);
  files.push(
    path.resolve(
      quickSourceRoot,
      `../../Cleanup${comparisonImplementationExtension}`
    )
  );
  return files.sort();
}

function hashComparisonImplementation(): string {
  const hash = crypto.createHash("sha256");
  for (const file of comparisonImplementationFiles()) {
    hash.update(path.relative(quickSourceRoot, file).split(path.sep).join("/"));
    hash.update("\0");
    hash.update(fs.readFileSync(file));
    hash.update("\0");
  }
  return hash.digest("hex");
}

export const comparisonHarnessHash = hashComparisonImplementation();
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
  readonly artifactVersion: number;
  readonly harnessVersion: string;
  readonly harnessHash: string;
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
  readonly fixtureIdentity: ComparisonFixtureIdentity;
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
  readonly fixtureIdentity: ComparisonFixtureIdentity;
  readonly samples: readonly RawArmSample[];
  readonly generatedAt: string;
}

export interface PairRunArtifact {
  readonly artifactVersion: number;
  readonly jobId: string;
  readonly pair: number;
  readonly order: PairOrder;
  readonly fingerprint: ComparisonFingerprint;
  readonly environment: EnvironmentClass;
  readonly fixtureArtifactHash: string;
  readonly fixtureIdentity: ComparisonFixtureIdentity;
  readonly semanticDigest?: string;
  readonly armASource: ArmSource;
  readonly armBSource: ArmSource;
  readonly armA?: ArmRunResult;
  readonly armB?: ArmRunResult;
  readonly observation?: PairObservation;
  readonly collapsed?: CollapsedPair;
  readonly discardedReason?: string;
  readonly generatedAt: string;
}

export interface CalibrationJobArtifact {
  readonly jobId: string;
  readonly pair: number;
  readonly order: PairOrder;
  readonly source: ArmSource;
  readonly fixtureContentDigest: string;
  readonly semanticDigest?: string;
  readonly logRatio?: number;
  readonly discardedReason?: string;
}

export interface CalibrationArtifact {
  readonly artifactVersion: number;
  readonly fingerprint: ComparisonFingerprint;
  readonly environment: EnvironmentClass;
  readonly pool: NoiseBandPool;
  readonly band: NoiseBand;
  readonly refs: readonly ArmSource[];
  readonly orders: readonly PairOrder[];
  readonly jobs: readonly CalibrationJobArtifact[];
  readonly fixtureIdentity: ComparisonFixtureIdentity;
  readonly semanticDigest: string;
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
    artifactVersion: fingerprint.artifactVersion,
    harnessVersion: fingerprint.harnessVersion,
    harnessHash: fingerprint.harnessHash,
    scenarioId: fingerprint.scenarioId,
    fixtureId: fingerprint.fixtureId,
    recipeHash: fingerprint.recipeHash,
    workloadHash: fingerprint.workloadHash,
    execution: executionFingerprintKey(fingerprint.execution),
  });
}

function assertNonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0)
    throw new Error(`${label} cannot be empty`);
}

function assertSha256(value: string, label: string): void {
  if (!/^[a-f0-9]{64}$/i.test(value))
    throw new Error(`${label} must be a SHA-256 hex digest`);
}

function validateArmSource(source: ArmSource, label: string): void {
  if (!source || typeof source !== "object")
    throw new Error(`${label} must be an object`);
  assertNonEmpty(source.ref, `${label} ref`);
  if (!/^[a-f0-9]{40}$|^[a-f0-9]{64}$/i.test(source.sha))
    throw new Error(`${label} SHA must be a 40- or 64-character hex object id`);
}

export function validateComparisonFingerprint(
  fingerprint: ComparisonFingerprint
): void {
  if (!fingerprint || typeof fingerprint !== "object")
    throw new Error("Comparison fingerprint must be an object");
  if (fingerprint.artifactVersion !== comparisonArtifactVersion)
    throw new Error(
      `Unsupported comparison artifact version ${String(
        fingerprint.artifactVersion
      )}; expected ${comparisonArtifactVersion}`
    );
  if (fingerprint.harnessVersion !== comparisonHarnessVersion)
    throw new Error(
      `Unsupported comparison harness version "${String(
        fingerprint.harnessVersion
      )}"; expected "${comparisonHarnessVersion}"`
    );
  assertSha256(fingerprint.harnessHash, "Comparison harness hash");
  if (fingerprint.harnessHash !== comparisonHarnessHash)
    throw new Error(
      `Comparison harness hash ${fingerprint.harnessHash} does not match the current implementation ${comparisonHarnessHash}`
    );
  assertNonEmpty(fingerprint.scenarioId, "Comparison scenario id");
  assertNonEmpty(fingerprint.fixtureId, "Comparison fixture id");
  assertSha256(fingerprint.recipeHash, "Comparison recipe hash");
  assertSha256(fingerprint.workloadHash, "Comparison workload hash");
  validateExecutionFingerprint(fingerprint.execution);
}

export function assertComparisonFingerprintMatches(
  actual: ComparisonFingerprint,
  expected: ComparisonFingerprint
): void {
  validateComparisonFingerprint(actual);
  validateComparisonFingerprint(expected);
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
    artifactVersion: comparisonArtifactVersion,
    harnessVersion: comparisonHarnessVersion,
    harnessHash: comparisonHarnessHash,
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

function validateArmSpecValue(arm: ArmSpec, label: string): void {
  if (!arm || typeof arm !== "object")
    throw new Error(`${label} specification must be an object`);
  assertNonEmpty(arm.id, `${label} id`);
  assertNonEmpty(arm.packageRoot, `${label} package root`);
  if (
    arm.operation !== "identity" &&
    arm.operation !== "fork-init" &&
    arm.operation !== "change-processing"
  )
    throw new Error(`${label} operation is invalid`);
  if (arm.modulePath !== undefined)
    assertNonEmpty(arm.modulePath, `${label} module path`);
}

function validateRawArmSample(
  sample: RawArmSample,
  expectedIndex: number,
  warmups: number
): void {
  if (!sample || typeof sample !== "object")
    throw new Error(`Arm sample ${expectedIndex} must be an object`);
  if (sample.sample !== expectedIndex)
    throw new Error(`Arm sample index ${sample.sample} != ${expectedIndex}`);
  if (sample.measured !== expectedIndex >= warmups)
    throw new Error(
      `Arm sample ${expectedIndex} measured flag is inconsistent`
    );
  for (const [label, value, strictlyPositive] of [
    ["wall time", sample.wallMilliseconds, true],
    ["reconstruction time", sample.reconstructionMilliseconds, false],
    ["verification time", sample.verificationMilliseconds, false],
    ["teardown time", sample.teardownMilliseconds, false],
  ] as const)
    if (!Number.isFinite(value) || (strictlyPositive ? value <= 0 : value < 0))
      throw new Error(`Arm sample ${expectedIndex} ${label} is invalid`);
  assertNonEmpty(sample.semanticDigest, `Arm sample ${expectedIndex} digest`);
}

function validateArmRunResult(
  result: ArmRunResult,
  fingerprint: ComparisonFingerprint,
  expectedSource: ArmSource,
  expectedFixtureHash: string,
  expectedFixtureIdentity: ComparisonFixtureIdentity
): void {
  if (!result || typeof result !== "object")
    throw new Error("Arm result must be an object");
  validateArmSpecValue(result.arm, "Arm result");
  validateArmSource(result.source, "Arm result source");
  if (
    result.source.ref !== expectedSource.ref ||
    result.source.sha !== expectedSource.sha
  )
    throw new Error("Arm result source does not match its pair source");
  if (!result.runtime || typeof result.runtime !== "object")
    throw new Error("Arm runtime identity must be an object");
  if (result.runtime.armId !== result.arm.id)
    throw new Error("Arm runtime id does not match its specification");
  assertNonEmpty(result.runtime.transformerVersion, "Arm transformer version");
  assertSha256(
    result.runtime.transformerPackageHash,
    "Arm transformer package hash"
  );
  assertNonEmpty(result.runtime.coreBackendVersion, "Arm core-backend version");
  assertSha256(
    result.runtime.coreBackendPackageHash,
    "Arm core-backend package hash"
  );
  assertComparisonFingerprintMatches(result.fingerprint, fingerprint);
  if (result.fixtureArtifactHash !== expectedFixtureHash)
    throw new Error("Arm fixture artifact hash does not match its pair");
  if (
    canonicalJson(result.fixtureIdentity) !==
    canonicalJson(expectedFixtureIdentity)
  )
    throw new Error("Arm fixture content identity does not match its pair");
  validateComparisonFixtureIdentity(result.fixtureIdentity);
  if (!Array.isArray(result.samples))
    throw new Error("Arm samples must be an array");
  const expectedSamples =
    fingerprint.execution.warmupSamplesPerArm +
    fingerprint.execution.measuredSamplesPerArm;
  if (result.samples.length !== expectedSamples)
    throw new Error(`Arm result requires exactly ${expectedSamples} samples`);
  result.samples.forEach((sample, index) =>
    validateRawArmSample(
      sample,
      index,
      fingerprint.execution.warmupSamplesPerArm
    )
  );
  if (!Number.isFinite(Date.parse(result.generatedAt)))
    throw new Error("Arm result generatedAt must be a valid timestamp");
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
  validateArmRunResult(
    result as ArmRunResult,
    request.fingerprint,
    request.source,
    request.fixtureArtifactHash,
    request.fixtureIdentity
  );
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
  const fixtureIdentity = readComparisonFixtureIdentity(
    request.fixtureDirectory
  );
  if (canonicalJson(fixtureIdentity) !== canonicalJson(request.fixtureIdentity))
    throw new Error("Fixture content identity changed before arm execution");
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
    fixtureIdentity,
    samples,
    generatedAt: new Date().toISOString(),
  };
}

export async function spawnArmProcess(
  armProcessPath: string,
  request: ArmRunRequest,
  timeoutMilliseconds: number,
  terminationGraceMilliseconds = 5_000
): Promise<unknown> {
  if (!Number.isSafeInteger(timeoutMilliseconds) || timeoutMilliseconds < 1)
    throw new Error("Arm timeout must be a positive integer");
  if (
    !Number.isSafeInteger(terminationGraceMilliseconds) ||
    terminationGraceMilliseconds < 1
  )
    throw new Error("Arm termination grace must be a positive integer");
  fs.mkdirSync(request.outputDirectory, { recursive: true });
  const requestFile = path.join(request.outputDirectory, "request.json");
  const resultFile = path.join(request.outputDirectory, "arm-result.json");
  fs.writeFileSync(requestFile, `${JSON.stringify(request, undefined, 2)}\n`);
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [armProcessPath, "--request", requestFile, "--output", resultFile],
      {
        detached: process.platform !== "win32",
        stdio: ["ignore", "inherit", "inherit"],
        shell: false,
      }
    );
    let timedOut = false;
    let forced = false;
    let forceTimer: NodeJS.Timeout | undefined;
    const signalChild = (signal: NodeJS.Signals): void => {
      if (child.pid === undefined) return;
      try {
        if (process.platform === "win32") child.kill(signal);
        else process.kill(-child.pid, signal);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      }
    };
    const timer = setTimeout(() => {
      timedOut = true;
      signalChild("SIGTERM");
      forceTimer = setTimeout(() => {
        forced = true;
        signalChild("SIGKILL");
      }, terminationGraceMilliseconds);
    }, timeoutMilliseconds);
    child.once("error", (error) => {
      clearTimeout(timer);
      if (forceTimer) clearTimeout(forceTimer);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      if (forceTimer) clearTimeout(forceTimer);
      if (timedOut)
        reject(
          new Error(
            `Arm "${request.arm.id}" timed out after ${timeoutMilliseconds}ms${
              forced ? " and required forced termination" : ""
            }`
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
  const fixtureIdentity = readComparisonFixtureIdentity(
    options.fixtureDirectory
  );
  const fingerprint = fingerprintForArtifact(options.fixtureDirectory);
  validateComparisonFingerprint(fingerprint);
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
  validateArmSource(options.armASource, "Arm A source");
  validateArmSource(options.armBSource, "Arm B source");
  const scheduledOrder = expectedPairOrder(fingerprint.execution, options.pair);
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
      fixtureIdentity,
      fingerprint,
      outputDirectory: path.join(options.outputDirectory, "arm-a"),
    },
    B: {
      arm: options.armB,
      source: options.armBSource,
      scenarioId: options.scenarioId,
      fixtureDirectory: options.fixtureDirectory,
      fixtureArtifactHash,
      fixtureIdentity,
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
    const semanticDigests = new Set([
      ...armA.samples.map((sample) => sample.semanticDigest),
      ...armB.samples.map((sample) => sample.semanticDigest),
    ]);
    if (semanticDigests.size !== 1)
      throw new Error("Arms produced different semantic results");
    const [semanticDigest] = semanticDigests;
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
      artifactVersion: comparisonArtifactVersion,
      jobId: options.jobId,
      pair: options.pair,
      order: options.order,
      fingerprint,
      environment,
      fixtureArtifactHash,
      fixtureIdentity,
      semanticDigest,
      armASource: options.armASource,
      armBSource: options.armBSource,
      armA,
      armB,
      observation,
      collapsed: collapsePair(observation),
      generatedAt,
    };
  } catch (error) {
    return {
      artifactVersion: comparisonArtifactVersion,
      jobId: options.jobId,
      pair: options.pair,
      order: options.order,
      fingerprint,
      environment,
      fixtureArtifactHash,
      fixtureIdentity,
      armASource: options.armASource,
      armBSource: options.armBSource,
      armA: results.A,
      armB: results.B,
      discardedReason: error instanceof Error ? error.message : String(error),
      generatedAt,
    };
  }
}

function assertFiniteJsonNumbers(value: unknown, label = "JSON value"): void {
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error(`${label} contains a non-finite number`);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      assertFiniteJsonNumbers(entry, `${label}[${index}]`)
    );
    return;
  }
  if (value && typeof value === "object")
    for (const [key, entry] of Object.entries(value))
      assertFiniteJsonNumbers(entry, `${label}.${key}`);
}

function sameFiniteNumber(actual: number, expected: number): boolean {
  const tolerance =
    Number.EPSILON * Math.max(1, Math.abs(actual), Math.abs(expected)) * 32;
  return Number.isFinite(actual) && Math.abs(actual - expected) <= tolerance;
}

export function validatePairRunArtifact(artifact: PairRunArtifact): void {
  if (!artifact || typeof artifact !== "object")
    throw new Error("Pair artifact must be an object");
  if (artifact.artifactVersion !== comparisonArtifactVersion)
    throw new Error(
      `Unsupported pair artifact version ${String(
        artifact.artifactVersion
      )}; expected ${comparisonArtifactVersion}`
    );
  assertNonEmpty(artifact.jobId, "Pair job id");
  if (!Number.isSafeInteger(artifact.pair) || artifact.pair < 0)
    throw new Error("Pair index must be a non-negative integer");
  validateComparisonFingerprint(artifact.fingerprint);
  const expectedOrder = expectedPairOrder(
    artifact.fingerprint.execution,
    artifact.pair
  );
  if (artifact.order !== expectedOrder)
    throw new Error(
      `Pair ${artifact.pair} order ${artifact.order} does not match execution plan ${expectedOrder}`
    );
  validateEnvironmentClass(artifact.environment);
  assertSha256(artifact.fixtureArtifactHash, "Fixture artifact hash");
  validateComparisonFixtureIdentity(artifact.fixtureIdentity);
  validateArmSource(artifact.armASource, "Arm A source");
  validateArmSource(artifact.armBSource, "Arm B source");
  if (!Number.isFinite(Date.parse(artifact.generatedAt)))
    throw new Error("Pair artifact generatedAt must be a valid timestamp");
  if (artifact.armA)
    validateArmRunResult(
      artifact.armA,
      artifact.fingerprint,
      artifact.armASource,
      artifact.fixtureArtifactHash,
      artifact.fixtureIdentity
    );
  if (artifact.armB)
    validateArmRunResult(
      artifact.armB,
      artifact.fingerprint,
      artifact.armBSource,
      artifact.fixtureArtifactHash,
      artifact.fixtureIdentity
    );
  if (artifact.armA && artifact.armB)
    assertArmRuntimeComparable(artifact.armA.runtime, artifact.armB.runtime);
  if (artifact.discardedReason !== undefined) {
    assertNonEmpty(artifact.discardedReason, "Discarded pair reason");
    if (artifact.observation || artifact.collapsed || artifact.semanticDigest)
      throw new Error("A discarded pair cannot contain a valid observation");
  } else {
    if (
      !artifact.armA ||
      !artifact.armB ||
      !artifact.observation ||
      !artifact.collapsed
    )
      throw new Error(
        "A valid pair requires both arms and a collapsed observation"
      );
    if (
      artifact.observation.pair !== artifact.pair ||
      artifact.observation.order !== artifact.order
    )
      throw new Error("Pair observation identity is inconsistent");
    assertSha256(artifact.semanticDigest as string, "Pair semantic digest");
    if (
      artifact.armA.samples.some(
        (sample) => sample.semanticDigest !== artifact.semanticDigest
      ) ||
      artifact.armB.samples.some(
        (sample) => sample.semanticDigest !== artifact.semanticDigest
      )
    )
      throw new Error(
        "Pair semantic digest does not match the raw arm results"
      );
    for (const [label, observed, arm] of [
      ["A", artifact.observation.armASamples, artifact.armA],
      ["B", artifact.observation.armBSamples, artifact.armB],
    ] as const) {
      const measured = arm.samples
        .filter((sample) => sample.measured)
        .map((sample) => sample.wallMilliseconds);
      if (
        observed.length !== measured.length ||
        observed.some((value, index) => value !== measured[index])
      )
        throw new Error(
          `Pair observation arm ${label} samples do not match the raw measured samples`
        );
    }
    const expectedCollapsed = collapsePair(artifact.observation);
    validateCollapsedPair(artifact.collapsed);
    if (
      artifact.collapsed.pair !== expectedCollapsed.pair ||
      artifact.collapsed.order !== expectedCollapsed.order ||
      !sameFiniteNumber(artifact.collapsed.armA, expectedCollapsed.armA) ||
      !sameFiniteNumber(artifact.collapsed.armB, expectedCollapsed.armB) ||
      !sameFiniteNumber(artifact.collapsed.logRatio, expectedCollapsed.logRatio)
    )
      throw new Error(
        "Collapsed pair does not match its raw measured observations"
      );
  }
  assertFiniteJsonNumbers(artifact, "Pair artifact");
}

export function aggregateCalibration(
  artifacts: readonly PairRunArtifact[],
  options: {
    readonly expectedPairs?: number;
    readonly requirements?: CalibrationRequirements;
  } = {}
): CalibrationArtifact {
  const expectedPairs = options.expectedPairs ?? 3;
  if (!Number.isSafeInteger(expectedPairs) || expectedPairs < 1)
    throw new Error("Calibration expected pairs must be a positive integer");
  if (artifacts.length !== expectedPairs)
    throw new Error(
      `Calibration contains ${artifacts.length} job artifacts; expected ${expectedPairs}`
    );
  artifacts.forEach(validatePairRunArtifact);
  const ordered = [...artifacts].sort((left, right) => left.pair - right.pair);
  const first = ordered[0];
  const jobs = new Set<string>();
  const observations: number[] = [];
  const refs: ArmSource[] = [];
  const jobRecords: CalibrationJobArtifact[] = [];
  for (const [expectedPair, artifact] of ordered.entries()) {
    if (artifact.pair !== expectedPair)
      throw new Error(
        `Calibration pair indexes must be complete and zero-based; expected ${expectedPair}, received ${artifact.pair}`
      );
    if (jobs.has(artifact.jobId))
      throw new Error(`Duplicate calibration job id: ${artifact.jobId}`);
    jobs.add(artifact.jobId);
    assertComparisonFingerprintMatches(artifact.fingerprint, first.fingerprint);
    if (
      canonicalJson(artifact.environment) !== canonicalJson(first.environment)
    )
      throw new Error(
        `Calibration environment mismatch: ${artifact.environment.id} != ${first.environment.id}`
      );
    if (
      canonicalJson(artifact.fixtureIdentity) !==
      canonicalJson(first.fixtureIdentity)
    )
      throw new Error(
        `Calibration pair ${artifact.jobId} used different fixture content`
      );
    if (
      artifact.armASource.sha !== artifact.armBSource.sha ||
      artifact.armASource.ref !== artifact.armBSource.ref
    )
      throw new Error(
        `Calibration pair ${artifact.jobId} did not use the same build for both labeled arms`
      );
    if (
      artifact.armA &&
      artifact.armB &&
      (artifact.armA.runtime.transformerVersion !==
        artifact.armB.runtime.transformerVersion ||
        artifact.armA.runtime.transformerPackageHash !==
          artifact.armB.runtime.transformerPackageHash)
    )
      throw new Error(
        `Calibration pair ${artifact.jobId} did not load identical transformer packages`
      );
    const source = artifact.armASource;
    refs.push(source);
    if (artifact.collapsed) observations.push(artifact.collapsed.logRatio);
    jobRecords.push({
      jobId: artifact.jobId,
      pair: artifact.pair,
      order: artifact.order,
      source,
      fixtureContentDigest: artifact.fixtureIdentity.contentDigest,
      semanticDigest: artifact.semanticDigest,
      logRatio: artifact.collapsed?.logRatio,
      discardedReason: artifact.discardedReason,
    });
  }
  if (observations.length === 0)
    throw new Error("Calibration contains no valid independent observations");
  const semanticDigests = new Set(
    ordered
      .map((artifact) => artifact.semanticDigest)
      .filter((digest): digest is string => digest !== undefined)
  );
  if (semanticDigests.size !== 1)
    throw new Error(
      "Calibration jobs produced different semantic result digests"
    );
  const semanticDigest = [...semanticDigests][0];
  if (!semanticDigest)
    throw new Error("Calibration contains no semantic result digest");
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
    independentJobs: observations.length,
    updatedAt: new Date().toISOString(),
  };
  const calibration: CalibrationArtifact = {
    artifactVersion: comparisonArtifactVersion,
    fingerprint: first.fingerprint,
    environment: first.environment,
    pool,
    band: deriveNoiseBand(pool, 1, {
      requirements: options.requirements,
    }),
    refs,
    orders: ordered.map((artifact) => artifact.order),
    jobs: jobRecords,
    fixtureIdentity: first.fixtureIdentity,
    semanticDigest,
    generatedAt: new Date().toISOString(),
  };
  validateCalibrationArtifact(calibration);
  return calibration;
}

export function renderCalibration(calibration: CalibrationArtifact): string {
  validateCalibrationArtifact(calibration);
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
    `| Harness version | ${calibration.fingerprint.harnessVersion} |`,
    `| Harness SHA-256 | \`${calibration.fingerprint.harnessHash}\` |`,
    `| Recipe hash | ${calibration.fingerprint.recipeHash} |`,
    `| Fixture content SHA-256 | \`${calibration.fixtureIdentity.contentDigest}\` |`,
    `| Fixture semantic SHA-256 | \`${calibration.fixtureIdentity.baseSemanticDigest}\` |`,
    `| Changeset semantic SHA-256 | \`${calibration.fixtureIdentity.changesetSemanticDigest}\` |`,
    `| Scan result SHA-256 | \`${calibration.semanticDigest}\` |`,
    `| Fingerprint key | \`${sha256(
      comparisonFingerprintKey(calibration.fingerprint)
    )}\` |`,
    `| Planned independent jobs | ${calibration.jobs.length} |`,
    `| Valid independent observations | ${calibration.pool.independentJobs} |`,
    `| Discarded jobs | ${
      calibration.jobs.length - calibration.pool.independentJobs
    } |`,
    `| Calibration ref / SHA | ${calibration.refs[0]?.ref ?? "n/a"} / ${
      calibration.refs[0]?.sha ?? "n/a"
    } |`,
    `| Pair orders | ${calibration.orders.join(", ")} |`,
    `| Pair log-ratios | ${calibration.pool.observations
      .map((value) => value.toFixed(6))
      .join(", ")} |`,
    `| A/A band | ${calibration.band.bandPercent.toFixed(2)}% |`,
    `| Source pool SHA-256 | \`${calibration.band.derivation.poolDigest}\` |`,
    "| Target | <=5% actionable; 5-10% marginal; >=10% unresolvable |",
    "",
    "This calibration is informational and does not create a merge-blocking gate.",
    "",
  ].join("\n");
}

export function renderPairSummary(artifact: PairRunArtifact): string {
  validatePairRunArtifact(artifact);
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
    `| Harness version | ${artifact.fingerprint.harnessVersion} |`,
    `| Harness SHA-256 | \`${artifact.fingerprint.harnessHash}\` |`,
    `| Fixture artifact hash | ${artifact.fixtureArtifactHash} |`,
    `| Fixture content SHA-256 | ${artifact.fixtureIdentity.contentDigest} |`,
    `| Fixture semantic SHA-256 | ${artifact.fixtureIdentity.baseSemanticDigest} |`,
    `| Changeset semantic SHA-256 | ${artifact.fixtureIdentity.changesetSemanticDigest} |`,
    `| Scan result SHA-256 | ${artifact.semanticDigest ?? "n/a"} |`,
    `| Recipe hash | ${artifact.fingerprint.recipeHash} |`,
    `| Fingerprint key | \`${sha256(
      comparisonFingerprintKey(artifact.fingerprint)
    )}\` |`,
    `| Order | ${artifact.order} |`,
    `| Arm A ref / SHA | ${artifact.armASource.ref} / ${artifact.armASource.sha} |`,
    `| Arm B ref / SHA | ${artifact.armBSource.ref} / ${artifact.armBSource.sha} |`,
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

export function validateCalibrationArtifact(
  calibration: CalibrationArtifact
): void {
  if (!calibration || typeof calibration !== "object")
    throw new Error("Calibration artifact must be an object");
  if (calibration.artifactVersion !== comparisonArtifactVersion)
    throw new Error(
      `Unsupported calibration artifact version ${String(
        calibration.artifactVersion
      )}; expected ${comparisonArtifactVersion}`
    );
  validateComparisonFingerprint(calibration.fingerprint);
  validateEnvironmentClass(calibration.environment);
  validateComparisonFixtureIdentity(calibration.fixtureIdentity);
  assertSha256(calibration.semanticDigest, "Calibration semantic digest");
  validateNoiseBandPool(calibration.pool);
  validateNoiseBand(calibration.band);
  if (!Array.isArray(calibration.refs) || !Array.isArray(calibration.orders))
    throw new Error("Calibration refs and orders must be arrays");
  if (!Array.isArray(calibration.jobs) || calibration.jobs.length === 0)
    throw new Error("Calibration jobs must be a non-empty array");
  if (
    calibration.refs.length !== calibration.jobs.length ||
    calibration.orders.length !== calibration.jobs.length
  )
    throw new Error(
      "Calibration refs, orders, and jobs must have equal lengths"
    );
  const expectedOrders = expectedPairOrders(
    calibration.fingerprint.execution,
    calibration.jobs.length
  );
  const validLogRatios: number[] = [];
  const jobIds = new Set<string>();
  calibration.jobs.forEach((job, index) => {
    if (!job || typeof job !== "object")
      throw new Error(`Calibration job ${index} must be an object`);
    assertNonEmpty(job.jobId, `Calibration job ${index} id`);
    if (jobIds.has(job.jobId))
      throw new Error(`Duplicate calibration job id: ${job.jobId}`);
    jobIds.add(job.jobId);
    if (job.pair !== index)
      throw new Error(
        `Calibration pair indexes must be complete and zero-based; expected ${index}, received ${job.pair}`
      );
    if (job.order !== expectedOrders[index])
      throw new Error(
        `Calibration pair ${index} order ${job.order} does not match execution plan ${expectedOrders[index]}`
      );
    validateArmSource(job.source, `Calibration job ${index} source`);
    assertSha256(
      job.fixtureContentDigest,
      `Calibration job ${index} fixture content digest`
    );
    if (job.fixtureContentDigest !== calibration.fixtureIdentity.contentDigest)
      throw new Error(
        `Calibration job ${index} fixture content digest does not match the calibration`
      );
    validateArmSource(calibration.refs[index], `Calibration ref ${index}`);
    if (
      job.source.ref !== calibration.refs[index].ref ||
      job.source.sha !== calibration.refs[index].sha
    )
      throw new Error(`Calibration job ${index} source does not match its ref`);
    if (calibration.orders[index] !== job.order)
      throw new Error(`Calibration job ${index} order does not match orders`);
    const valid = job.logRatio !== undefined;
    const discarded = job.discardedReason !== undefined;
    if (valid === discarded)
      throw new Error(
        `Calibration job ${index} must contain exactly one of logRatio or discardedReason`
      );
    if (valid) {
      if (!Number.isFinite(job.logRatio))
        throw new Error(`Calibration job ${index} log ratio is not finite`);
      if (job.semanticDigest !== calibration.semanticDigest)
        throw new Error(
          `Calibration job ${index} semantic digest does not match the calibration`
        );
      validLogRatios.push(job.logRatio);
    } else {
      if (job.semanticDigest !== undefined)
        throw new Error(
          `Discarded calibration job ${index} cannot contain a semantic digest`
        );
      assertNonEmpty(
        job.discardedReason as string,
        `Calibration job ${index} discarded reason`
      );
    }
  });
  if (
    calibration.refs.some(
      (source) =>
        source.sha !== calibration.refs[0].sha ||
        source.ref !== calibration.refs[0].ref
    )
  )
    throw new Error("Calibration jobs did not use one identical A/A build");
  const expectedKey: NoiseBandKey = {
    scenarioId: calibration.fingerprint.scenarioId,
    fixtureId: calibration.fingerprint.fixtureId,
    recipeHash: calibration.fingerprint.recipeHash,
    environmentClass: calibration.environment.id,
    execution: calibration.fingerprint.execution,
    kind: "paired",
  };
  assertPoolApplies(calibration.pool, expectedKey);
  if (
    calibration.pool.kind !== "paired" ||
    calibration.pool.independentJobs !== validLogRatios.length ||
    calibration.pool.observations.length !== validLogRatios.length ||
    calibration.pool.observations.some(
      (value, index) => !sameFiniteNumber(value, validLogRatios[index])
    )
  )
    throw new Error(
      "Calibration pool does not match the valid independent job observations"
    );
  assertBandApplies(
    calibration.band,
    expectedKey,
    calibration.band.statisticSampleSize
  );
  assertBandDerivedFromPool(calibration.band, calibration.pool);
  if (!Number.isFinite(Date.parse(calibration.generatedAt)))
    throw new Error("Calibration generatedAt must be a valid timestamp");
  assertFiniteJsonNumbers(calibration, "Calibration artifact");
}

export function assertCalibrationMatchesPair(
  pair: PairRunArtifact,
  calibration: CalibrationArtifact
): void {
  validatePairRunArtifact(pair);
  validateCalibrationArtifact(calibration);
  assertComparisonFingerprintMatches(pair.fingerprint, calibration.fingerprint);
  if (
    canonicalJson(pair.environment) !== canonicalJson(calibration.environment)
  )
    throw new Error(
      `Comparison environment ${pair.environment.id} does not match calibration ${calibration.environment.id}`
    );
  if (
    canonicalJson(pair.fixtureIdentity) !==
    canonicalJson(calibration.fixtureIdentity)
  )
    throw new Error(
      "Comparison fixture content identity does not match calibration"
    );
  if (pair.semanticDigest !== calibration.semanticDigest)
    throw new Error(
      "Comparison semantic result digest does not match calibration"
    );
}

export function readPairArtifact(fileName: string): PairRunArtifact {
  const artifact = JSON.parse(
    fs.readFileSync(fileName, "utf8")
  ) as PairRunArtifact;
  validatePairRunArtifact(artifact);
  return artifact;
}

export function readCalibrationArtifact(fileName: string): CalibrationArtifact {
  const calibration = JSON.parse(
    fs.readFileSync(fileName, "utf8")
  ) as CalibrationArtifact;
  validateCalibrationArtifact(calibration);
  return calibration;
}

export function writeJson(fileName: string, value: unknown): void {
  assertFiniteJsonNumbers(value);
  fs.mkdirSync(path.dirname(fileName), { recursive: true });
  fs.writeFileSync(fileName, `${JSON.stringify(value, undefined, 2)}\n`);
}

export { artifactManifestFileName };
