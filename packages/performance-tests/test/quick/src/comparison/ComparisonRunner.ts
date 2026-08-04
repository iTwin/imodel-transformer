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
  ComparisonFixtureIdentity,
  readComparisonFixtureIdentity,
  validateComparisonFixtureIdentity,
} from "./ComparisonFixtureIdentity.js";

export const comparisonScenarioId = "changeset-scanning";
export const comparisonArtifactVersion = 3;
export const comparisonHarnessVersion = "isolated-ab-v1";
export const comparisonWarmups = 1;
export const comparisonMeasuredSamples = 3;
export const comparisonExecutionsPerPair = 8;
export const informationalThresholdPercent = 10;

const implementationExtension = path.extname(fileURLToPath(import.meta.url));
const quickSourceRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  ".."
);

function implementationFiles(): string[] {
  const files: string[] = [];
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) visit(absolute);
      else if (
        entry.isFile() &&
        path.extname(entry.name) === implementationExtension
      )
        files.push(absolute);
    }
  };
  visit(quickSourceRoot);
  files.push(
    path.resolve(quickSourceRoot, `../../Cleanup${implementationExtension}`)
  );
  return files.sort();
}

function hashImplementation(): string {
  const hash = crypto.createHash("sha256");
  for (const file of implementationFiles()) {
    hash.update(path.relative(quickSourceRoot, file).split(path.sep).join("/"));
    hash.update("\0");
    hash.update(fs.readFileSync(file));
    hash.update("\0");
  }
  return hash.digest("hex");
}

export const comparisonHarnessHash = hashImplementation();

export type PairOrder = "AB" | "BA";

export interface ComparisonFingerprint {
  readonly artifactVersion: number;
  readonly harnessVersion: string;
  readonly harnessHash: string;
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly recipeHash: string;
  readonly workloadHash: string;
  readonly warmupsPerArm: number;
  readonly measuredSamplesPerArm: number;
  readonly processPolicy: "one-isolated-process-per-arm";
  readonly samplePolicy: "median";
  readonly orderPolicy: "declared-per-job";
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

export type InformationalComparison =
  | "candidate-faster"
  | "candidate-slower"
  | "within-threshold";

export interface PairSummary {
  readonly armAMedianMilliseconds: number;
  readonly armBMedianMilliseconds: number;
  /** Candidate (arm B) relative to baseline (arm A); negative is faster. */
  readonly percentDelta: number;
  readonly informationalThresholdPercent: number;
  readonly classification: InformationalComparison;
}

export interface PairRunArtifact {
  readonly artifactVersion: number;
  readonly jobId: string;
  readonly pair: number;
  readonly order: PairOrder;
  readonly fingerprint: ComparisonFingerprint;
  readonly fixtureArtifactHash: string;
  readonly fixtureIdentity: ComparisonFixtureIdentity;
  readonly semanticDigest?: string;
  readonly armASource: ArmSource;
  readonly armBSource: ArmSource;
  readonly armA?: ArmRunResult;
  readonly armB?: ArmRunResult;
  readonly summary?: PairSummary;
  readonly discardedReason?: string;
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

export function comparisonFingerprintKey(
  fingerprint: ComparisonFingerprint
): string {
  return canonicalJson(fingerprint);
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
  if (
    fingerprint.warmupsPerArm !== comparisonWarmups ||
    fingerprint.measuredSamplesPerArm !== comparisonMeasuredSamples ||
    fingerprint.processPolicy !== "one-isolated-process-per-arm" ||
    fingerprint.samplePolicy !== "median" ||
    fingerprint.orderPolicy !== "declared-per-job"
  )
    throw new Error("Comparison execution policy is unsupported");
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
    warmupsPerArm: comparisonWarmups,
    measuredSamplesPerArm: comparisonMeasuredSamples,
    processPolicy: "one-isolated-process-per-arm",
    samplePolicy: "median",
    orderPolicy: "declared-per-job",
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
  expectedIndex: number
): void {
  if (!sample || typeof sample !== "object")
    throw new Error(`Arm sample ${expectedIndex} must be an object`);
  if (sample.sample !== expectedIndex)
    throw new Error(`Arm sample index ${sample.sample} != ${expectedIndex}`);
  if (sample.measured !== expectedIndex >= comparisonWarmups)
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
  assertSha256(sample.semanticDigest, `Arm sample ${expectedIndex} digest`);
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
  if (result.samples.length !== comparisonWarmups + comparisonMeasuredSamples)
    throw new Error(
      `Arm result requires exactly ${
        comparisonWarmups + comparisonMeasuredSamples
      } samples`
    );
  result.samples.forEach(validateRawArmSample);
  if (!Number.isFinite(Date.parse(result.generatedAt)))
    throw new Error("Arm result generatedAt must be a valid timestamp");
}

function validateArmResult(
  value: unknown,
  request: ArmRunRequest
): ArmRunResult {
  if (!value || typeof value !== "object")
    throw new Error(`Arm "${request.arm.id}" returned malformed output`);
  const result = value as ArmRunResult;
  validateArmRunResult(
    result,
    request.fingerprint,
    request.source,
    request.fixtureArtifactHash,
    request.fixtureIdentity
  );
  if (result.arm.id !== request.arm.id)
    throw new Error(`Arm "${request.arm.id}" returned malformed identity data`);
  return result;
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
  const loaded = loadArmModule(resolveArmSpec(request.arm));
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
  if (new Set(samples.map((sample) => sample.semanticDigest)).size !== 1)
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

function median(values: readonly number[]): number {
  if (values.length === 0) throw new Error("Cannot calculate an empty median");
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function measuredSamples(result: ArmRunResult): number[] {
  return result.samples
    .filter((sample) => sample.measured)
    .map((sample) => sample.wallMilliseconds);
}

function buildPairSummary(armA: ArmRunResult, armB: ArmRunResult): PairSummary {
  const armAMedianMilliseconds = median(measuredSamples(armA));
  const armBMedianMilliseconds = median(measuredSamples(armB));
  const percentDelta =
    ((armBMedianMilliseconds - armAMedianMilliseconds) /
      armAMedianMilliseconds) *
    100;
  const classification: InformationalComparison =
    percentDelta <= -informationalThresholdPercent
      ? "candidate-faster"
      : percentDelta >= informationalThresholdPercent
        ? "candidate-slower"
        : "within-threshold";
  return {
    armAMedianMilliseconds,
    armBMedianMilliseconds,
    percentDelta,
    informationalThresholdPercent,
    classification,
  };
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
  if (!Number.isSafeInteger(options.pair) || options.pair < 0)
    throw new Error("Pair index must be a non-negative integer");
  if (options.jobId.trim().length === 0)
    throw new Error("Pair job id cannot be empty");
  validateArmSource(options.armASource, "Arm A source");
  validateArmSource(options.armBSource, "Arm B source");
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
    const artifact: PairRunArtifact = {
      artifactVersion: comparisonArtifactVersion,
      jobId: options.jobId,
      pair: options.pair,
      order: options.order,
      fingerprint,
      fixtureArtifactHash,
      fixtureIdentity,
      semanticDigest,
      armASource: options.armASource,
      armBSource: options.armBSource,
      armA,
      armB,
      summary: buildPairSummary(armA, armB),
      generatedAt,
    };
    validatePairRunArtifact(artifact);
    return artifact;
  } catch (error) {
    const artifact: PairRunArtifact = {
      artifactVersion: comparisonArtifactVersion,
      jobId: options.jobId,
      pair: options.pair,
      order: options.order,
      fingerprint,
      fixtureArtifactHash,
      fixtureIdentity,
      armASource: options.armASource,
      armBSource: options.armBSource,
      armA: results.A,
      armB: results.B,
      discardedReason: normalizeError(error).message,
      generatedAt,
    };
    validatePairRunArtifact(artifact);
    return artifact;
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

export function validatePairRunArtifact(
  artifact: PairRunArtifact
): PairRunArtifact {
  assertFiniteJsonNumbers(artifact, "Pair artifact");
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
    throw new Error("Pair artifact index must be a non-negative integer");
  if (artifact.order !== "AB" && artifact.order !== "BA")
    throw new Error("Pair artifact order must be AB or BA");
  validateComparisonFingerprint(artifact.fingerprint);
  validateComparisonFixtureIdentity(artifact.fixtureIdentity);
  assertSha256(artifact.fixtureArtifactHash, "Fixture artifact hash");
  validateArmSource(artifact.armASource, "Pair arm A source");
  validateArmSource(artifact.armBSource, "Pair arm B source");
  if (!Number.isFinite(Date.parse(artifact.generatedAt)))
    throw new Error("Pair generatedAt must be a valid timestamp");
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
  if (artifact.discardedReason !== undefined) {
    assertNonEmpty(artifact.discardedReason, "Pair discard reason");
    if (artifact.summary !== undefined || artifact.semanticDigest !== undefined)
      throw new Error("Discarded pair cannot contain a summary or digest");
    return artifact;
  }
  if (!artifact.armA || !artifact.armB || !artifact.summary)
    throw new Error("Valid pair requires both arms and a summary");
  if (artifact.semanticDigest === undefined)
    throw new Error("Valid pair requires a semantic digest");
  assertSha256(artifact.semanticDigest, "Pair semantic digest");
  const semanticDigests = new Set([
    ...artifact.armA.samples.map((sample) => sample.semanticDigest),
    ...artifact.armB.samples.map((sample) => sample.semanticDigest),
  ]);
  if (
    semanticDigests.size !== 1 ||
    !semanticDigests.has(artifact.semanticDigest)
  )
    throw new Error("Pair semantic digest does not match arm samples");
  const expected = buildPairSummary(artifact.armA, artifact.armB);
  if (
    !sameFiniteNumber(
      artifact.summary.armAMedianMilliseconds,
      expected.armAMedianMilliseconds
    ) ||
    !sameFiniteNumber(
      artifact.summary.armBMedianMilliseconds,
      expected.armBMedianMilliseconds
    ) ||
    !sameFiniteNumber(artifact.summary.percentDelta, expected.percentDelta) ||
    artifact.summary.informationalThresholdPercent !==
      informationalThresholdPercent ||
    artifact.summary.classification !== expected.classification
  )
    throw new Error("Pair summary does not match measured arm samples");
  return artifact;
}

export function readPairArtifact(fileName: string): PairRunArtifact {
  return validatePairRunArtifact(
    JSON.parse(fs.readFileSync(fileName, "utf8")) as PairRunArtifact
  );
}

export function writeJson(fileName: string, value: unknown): void {
  assertFiniteJsonNumbers(value);
  fs.mkdirSync(path.dirname(fileName), { recursive: true });
  fs.writeFileSync(fileName, `${JSON.stringify(value, undefined, 2)}\n`);
}

export function renderPairSummary(artifact: PairRunArtifact): string {
  validatePairRunArtifact(artifact);
  const lines = [
    "# Isolated quick A/B comparison",
    "",
    "| Field | Value |",
    "| --- | --- |",
    `| Scenario | ${artifact.fingerprint.scenarioId} |`,
    `| Fixture | ${artifact.fingerprint.fixtureId} |`,
    `| Order | ${artifact.order} |`,
    `| Baseline | ${artifact.armASource.ref} (\`${artifact.armASource.sha}\`) |`,
    `| Candidate | ${artifact.armBSource.ref} (\`${artifact.armBSource.sha}\`) |`,
    `| Harness SHA-256 | \`${artifact.fingerprint.harnessHash}\` |`,
    `| Fixture artifact SHA-256 | \`${artifact.fixtureArtifactHash}\` |`,
    `| Fixture content SHA-256 | \`${artifact.fixtureIdentity.contentDigest}\` |`,
    `| Changeset semantic SHA-256 | \`${artifact.fixtureIdentity.changesetSemanticDigest}\` |`,
  ];
  if (artifact.discardedReason) {
    lines.push(
      `| Result | Discarded: ${artifact.discardedReason.replaceAll("|", "\\|")} |`
    );
  } else {
    const summary = artifact.summary;
    if (!summary) throw new Error("Valid pair is missing its summary");
    lines.push(
      `| Result semantic SHA-256 | \`${artifact.semanticDigest}\` |`,
      `| Baseline median | ${summary.armAMedianMilliseconds.toFixed(3)} ms |`,
      `| Candidate median | ${summary.armBMedianMilliseconds.toFixed(3)} ms |`,
      `| Candidate delta | ${summary.percentDelta.toFixed(2)}% |`,
      `| Informational classification | ${summary.classification} |`,
      `| Informational threshold | ±${summary.informationalThresholdPercent}% |`
    );
  }
  lines.push(
    "",
    "This is a single informational A/B observation. It is not a statistical confidence claim and never blocks merging.",
    ""
  );
  return lines.join("\n");
}
