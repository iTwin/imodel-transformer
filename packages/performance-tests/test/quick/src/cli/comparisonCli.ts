/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { spawnSync } from "node:child_process";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildComparisonReport,
  writeComparisonReport,
} from "../comparison/ComparisonReport.js";
import {
  aggregateCalibration,
  ArmRunRequest,
  assertCalibrationMatchesPair,
  comparisonExecutionsPerPair,
  comparisonFingerprintKey,
  comparisonMeasuredSamples,
  comparisonScenarioId,
  comparisonWarmups,
  readCalibrationArtifact,
  readPairArtifact,
  renderCalibration,
  renderPairSummary,
  runPair,
  spawnArmProcess,
  validatePairRunArtifact,
  writeJson,
} from "../comparison/ComparisonRunner.js";
import { prepareComparisonFixture } from "../comparison/ComparisonFixture.js";
import { ArmOperation, ArmSpec } from "../comparison/ArmModule.js";

const cliRequire = createRequire(import.meta.url);

function parseArguments(args: readonly string[]): Map<string, string> {
  const parsed = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index];
    const value = args[index + 1];
    if (!key?.startsWith("--") || value === undefined)
      throw new Error(
        `Expected --name value, received "${key ?? "<missing>"}"`
      );
    if (parsed.has(key)) throw new Error(`Duplicate argument: ${key}`);
    parsed.set(key, value);
  }
  return parsed;
}

function required(args: Map<string, string>, name: string): string {
  const value = args.get(`--${name}`);
  if (!value) throw new Error(`Missing required argument --${name}`);
  return value;
}

function optional(args: Map<string, string>, name: string): string | undefined {
  return args.get(`--${name}`);
}

function integer(args: Map<string, string>, name: string): number {
  const raw = required(args, name);
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0)
    throw new Error(`--${name} must be a non-negative integer`);
  return value;
}

function positiveInteger(args: Map<string, string>, name: string): number {
  const value = integer(args, name);
  if (value < 1) throw new Error(`--${name} must be a positive integer`);
  return value;
}

function assertScenario(args: Map<string, string>): string {
  const scenario = required(args, "scenario");
  if (scenario !== comparisonScenarioId)
    throw new Error(
      `Unsupported comparison scenario "${scenario}"; only "${comparisonScenarioId}" is implemented`
    );
  return scenario;
}

function armSpec(
  args: Map<string, string>,
  prefix: "arm-a" | "arm-b"
): ArmSpec {
  const operation: ArmOperation = "change-processing";
  return {
    id: required(args, `${prefix}-id`),
    label: optional(args, `${prefix}-label`),
    packageRoot: required(args, `${prefix}-package`),
    modulePath: optional(args, `${prefix}-module`),
    operation,
  };
}

function findFiles(directory: string, name: string): string[] {
  const files: string[] = [];
  const visit = (current: string): void => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) visit(absolute);
      else if (entry.isFile() && entry.name === name) files.push(absolute);
    }
  };
  visit(directory);
  return files.sort();
}

function createSmokeArmPackage(outputRoot: string): string {
  const sourceRoot = path.dirname(
    cliRequire.resolve("@itwin/imodel-transformer/package.json")
  );
  const sourceRequire = createRequire(path.join(sourceRoot, "package.json"));
  const manifest = JSON.parse(
    fs.readFileSync(path.join(sourceRoot, "package.json"), "utf8")
  ) as {
    readonly dependencies?: Readonly<Record<string, string>>;
    readonly main?: string;
    readonly peerDependencies?: Readonly<Record<string, string>>;
  };
  const manifestMain = manifest.main ?? "index.js";
  const sourceModule = path.join(sourceRoot, manifestMain);
  if (!fs.existsSync(sourceModule))
    throw new Error(
      `Smoke arm transformer must be built before comparison: ${sourceModule}`
    );
  const armRoot = path.join(outputRoot, "selected-arm");
  fs.mkdirSync(path.dirname(path.join(armRoot, manifestMain)), {
    recursive: true,
  });
  fs.copyFileSync(
    path.join(sourceRoot, "package.json"),
    path.join(armRoot, "package.json")
  );
  fs.cpSync(
    path.dirname(sourceModule),
    path.dirname(path.join(armRoot, manifestMain)),
    { recursive: true }
  );
  for (const dependency of Object.keys({
    ...manifest.dependencies,
    ...manifest.peerDependencies,
  })) {
    const dependencyRoot = fs.realpathSync(
      path.dirname(sourceRequire.resolve(`${dependency}/package.json`))
    );
    const dependencyLink = path.join(
      armRoot,
      "node_modules",
      ...dependency.split("/")
    );
    fs.mkdirSync(path.dirname(dependencyLink), { recursive: true });
    fs.symlinkSync(
      dependencyRoot,
      dependencyLink,
      process.platform === "win32" ? "junction" : "dir"
    );
  }
  return armRoot;
}

async function prepareFixture(args: Map<string, string>): Promise<void> {
  assertScenario(args);
  const output = required(args, "output");
  const result = await prepareComparisonFixture(
    output,
    optional(args, "smoke") === "true"
  );
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

function executeArm(args: Map<string, string>): void {
  const result = spawnSync(
    process.execPath,
    [
      fileURLToPath(new URL("./armProcessCli.js", import.meta.url)),
      "--request",
      required(args, "request"),
      "--output",
      required(args, "output"),
    ],
    { shell: false, stdio: "inherit" }
  );
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(
      `Isolated arm process exited with code ${String(result.status)}`
    );
}

async function executePair(args: Map<string, string>): Promise<void> {
  const output = required(args, "output");
  const artifact = await runPair({
    jobId: required(args, "job-id"),
    pair: integer(args, "pair"),
    order: required(args, "order") as "AB" | "BA",
    scenarioId: assertScenario(args),
    fixtureDirectory: required(args, "fixture"),
    armA: armSpec(args, "arm-a"),
    armASource: {
      ref: required(args, "arm-a-ref"),
      sha: required(args, "arm-a-sha"),
    },
    armB: armSpec(args, "arm-b"),
    armBSource: {
      ref: required(args, "arm-b-ref"),
      sha: required(args, "arm-b-sha"),
    },
    outputDirectory: output,
    timeoutMilliseconds: optional(args, "timeout-ms")
      ? integer(args, "timeout-ms")
      : undefined,
  });
  writeJson(path.join(output, "pair-observation.json"), artifact);
  fs.writeFileSync(
    path.join(output, "pair-summary.md"),
    renderPairSummary(artifact)
  );
  if (artifact.discardedReason)
    throw new Error(`Pair discarded: ${artifact.discardedReason}`);
}

async function smoke(args: Map<string, string>): Promise<void> {
  assertScenario(args);
  const requestedOutput = optional(args, "output");
  const outputRoot =
    requestedOutput ??
    fs.mkdtempSync(path.join(os.tmpdir(), "quick-comparison-smoke-"));
  const fixture = path.join(outputRoot, "fixture");
  const pairOutput = path.join(outputRoot, "pair");
  const startedAt = Date.now();
  try {
    const prepared = await prepareComparisonFixture(fixture, true);
    const armPackage =
      optional(args, "arm-package") ?? createSmokeArmPackage(outputRoot);
    const timeoutMarker = path.join(outputRoot, "timeout-marker.txt");
    const timeoutChild = path.join(outputRoot, "timeout-child.cjs");
    fs.writeFileSync(
      timeoutChild,
      [
        'const fs = require("node:fs");',
        `const marker = ${JSON.stringify(timeoutMarker)};`,
        "fs.writeFileSync(marker, String(process.pid));",
        'process.on("SIGTERM", () => fs.appendFileSync(marker, "\\nSIGTERM"));',
        "setInterval(() => {}, 1_000);",
      ].join("\n")
    );
    const timeoutRequest: ArmRunRequest = {
      arm: {
        id: "timeout-probe",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      source: { ref: "smoke", sha: "0".repeat(40) },
      scenarioId: comparisonScenarioId,
      fixtureDirectory: fixture,
      fixtureArtifactHash: prepared.artifactHash,
      fixtureIdentity: prepared.fixtureIdentity,
      fingerprint: prepared.fingerprint,
      outputDirectory: path.join(outputRoot, "timeout-probe"),
    };
    const timeoutStartedAt = Date.now();
    let timeoutMessage: string | undefined;
    try {
      await spawnArmProcess(timeoutChild, timeoutRequest, 500, 150);
    } catch (error) {
      timeoutMessage = error instanceof Error ? error.message : String(error);
    }
    const timeoutProbeMilliseconds = Date.now() - timeoutStartedAt;
    if (!timeoutMessage?.includes("timed out"))
      throw new Error(
        "Compiled timeout probe did not reach the timeout boundary"
      );
    if (timeoutProbeMilliseconds > 5_000)
      throw new Error(
        `Compiled timeout probe took ${timeoutProbeMilliseconds}ms to settle`
      );
    if (
      process.platform !== "win32" &&
      !fs.readFileSync(timeoutMarker, "utf8").includes("SIGTERM")
    )
      throw new Error("Compiled timeout probe did not receive SIGTERM");
    if (
      process.platform !== "win32" &&
      !timeoutMessage.includes("required forced termination")
    )
      throw new Error("Compiled timeout probe did not escalate to SIGKILL");
    const artifact = await runPair({
      jobId: "compiled-isolation-smoke",
      pair: 0,
      order: "AB",
      scenarioId: comparisonScenarioId,
      fixtureDirectory: fixture,
      armA: {
        id: "smoke-a",
        label: "smoke",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "smoke", sha: "0".repeat(40) },
      armB: {
        id: "smoke-b",
        label: "smoke",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armBSource: { ref: "smoke", sha: "0".repeat(40) },
      outputDirectory: pairOutput,
      timeoutMilliseconds: optional(args, "timeout-ms")
        ? positiveInteger(args, "timeout-ms")
        : 120_000,
    });
    validatePairRunArtifact(artifact);
    if (artifact.discardedReason || !artifact.armA || !artifact.armB)
      throw new Error(
        `Compiled comparison smoke discarded its pair: ${
          artifact.discardedReason ?? "missing arm results"
        }`
      );
    const samples = [...artifact.armA.samples, ...artifact.armB.samples];
    const measured = samples.filter((sample) => sample.measured).length;
    const warmups = samples.length - measured;
    if (
      samples.length !== comparisonExecutionsPerPair ||
      measured !== comparisonMeasuredSamples * 2 ||
      warmups !== comparisonWarmups * 2
    )
      throw new Error(
        `Compiled comparison smoke executed ${samples.length} samples (${measured} measured, ${warmups} warm-ups)`
      );
    process.stdout.write(
      `${JSON.stringify({
        smokeOnly: true,
        totalExecutions: samples.length,
        measuredExecutions: measured,
        warmups,
        order: artifact.order,
        fixtureId: artifact.fingerprint.fixtureId,
        fixtureContentDigest: artifact.fixtureIdentity.contentDigest,
        changesetSemanticDigest:
          artifact.fixtureIdentity.changesetSemanticDigest,
        semanticDigest: artifact.semanticDigest,
        timeoutProbeMilliseconds,
        elapsedMilliseconds: Date.now() - startedAt,
      })}\n`
    );
  } finally {
    if (!requestedOutput)
      fs.rmSync(outputRoot, { recursive: true, force: true });
  }
}

function aggregate(args: Map<string, string>): void {
  const input = required(args, "input");
  const output = required(args, "output");
  const files = findFiles(input, "pair-observation.json");
  const calibration = aggregateCalibration(files.map(readPairArtifact), {
    expectedPairs: positiveInteger(args, "expected-pairs"),
  });
  fs.mkdirSync(output, { recursive: true });
  writeJson(path.join(output, "calibration.json"), calibration);
  writeJson(path.join(output, "noise-pool.json"), calibration.pool);
  writeJson(path.join(output, "noise-band.json"), calibration.band);
  fs.writeFileSync(
    path.join(output, "calibration-summary.md"),
    renderCalibration(calibration)
  );
}

function compare(args: Map<string, string>): void {
  const pair = readPairArtifact(required(args, "observation"));
  const calibration = readCalibrationArtifact(required(args, "calibration"));
  const output = required(args, "output");
  if (pair.discardedReason || !pair.collapsed || !pair.armA || !pair.armB)
    throw new Error(
      `Cannot compare a discarded pair: ${
        pair.discardedReason ?? "missing arm or collapsed result"
      }`
    );
  assertCalibrationMatchesPair(pair, calibration);
  const baseReport = buildComparisonReport({
    scenarioId: pair.fingerprint.scenarioId,
    fixtureId: pair.fingerprint.fixtureId,
    recipeHash: pair.fingerprint.recipeHash,
    mode: "paired",
    environment: pair.environment,
    execution: pair.fingerprint.execution,
    armA: {
      id: pair.armA.arm.id,
      label: pair.armA.arm.label,
      ref: pair.armA.source.ref,
      sha: pair.armA.source.sha,
      transformerVersion: pair.armA.runtime.transformerVersion,
      transformerPackageHash: pair.armA.runtime.transformerPackageHash,
      coreBackendVersion: pair.armA.runtime.coreBackendVersion,
      coreBackendPackageHash: pair.armA.runtime.coreBackendPackageHash,
    },
    armB: {
      id: pair.armB.arm.id,
      label: pair.armB.arm.label,
      ref: pair.armB.source.ref,
      sha: pair.armB.source.sha,
      transformerVersion: pair.armB.runtime.transformerVersion,
      transformerPackageHash: pair.armB.runtime.transformerPackageHash,
      coreBackendVersion: pair.armB.runtime.coreBackendVersion,
      coreBackendPackageHash: pair.armB.runtime.coreBackendPackageHash,
    },
    pairs: [pair.collapsed],
    independentJobs: 1,
    pool: calibration.pool,
    band: calibration.band,
  });
  const report = {
    ...baseReport,
    verdict: {
      ...baseReport.verdict,
      evidence: "informational" as const,
      reason: `${baseReport.verdict.reason} This workflow is informational-only.`,
    },
  };
  writeComparisonReport(output, report);
  fs.appendFileSync(
    path.join(output, "comparison.md"),
    [
      "",
      "## Runner identity",
      "",
      `- Fingerprint key: \`${crypto
        .createHash("sha256")
        .update(comparisonFingerprintKey(pair.fingerprint))
        .digest("hex")}\``,
      `- Fixture artifact hash: \`${pair.fixtureArtifactHash}\``,
      `- Fixture content SHA-256: \`${pair.fixtureIdentity.contentDigest}\``,
      `- Fixture semantic SHA-256: \`${pair.fixtureIdentity.baseSemanticDigest}\``,
      `- Changeset semantic SHA-256: \`${pair.fixtureIdentity.changesetSemanticDigest}\``,
      `- Scan result SHA-256: \`${pair.semanticDigest}\``,
      `- Harness SHA-256: \`${pair.fingerprint.harnessHash}\``,
      `- Pair order: ${pair.order}`,
      `- Arm A median: ${pair.collapsed.armA.toFixed(3)} ms`,
      `- Arm B median: ${pair.collapsed.armB.toFixed(3)} ms`,
      "",
      "This workflow is informational-only and never creates a merge-blocking performance gate.",
      "",
    ].join("\n")
  );
  writeJson(path.join(output, "summary.json"), {
    report,
    pair,
    calibration: {
      fingerprint: calibration.fingerprint,
      environment: calibration.environment,
      pool: calibration.pool,
      band: calibration.band,
      fixtureIdentity: calibration.fixtureIdentity,
      semanticDigest: calibration.semanticDigest,
    },
  });
}

async function main(): Promise<void> {
  const command = process.argv[2];
  const args = parseArguments(process.argv.slice(3));
  switch (command) {
    case "prepare-fixture":
      await prepareFixture(args);
      return;
    case "run-arm":
      executeArm(args);
      return;
    case "run-pair":
      await executePair(args);
      return;
    case "smoke":
      await smoke(args);
      return;
    case "aggregate-calibration":
      aggregate(args);
      return;
    case "compare":
      compare(args);
      return;
    default:
      throw new Error(`Unknown comparison command: ${command ?? "<missing>"}`);
  }
}

void main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`
  );
  process.exitCode = 1;
});
