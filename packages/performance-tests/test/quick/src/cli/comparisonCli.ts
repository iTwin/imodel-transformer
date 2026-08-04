/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { ArmOperation, ArmSpec } from "../comparison/ArmModule.js";
import { prepareComparisonFixture } from "../comparison/ComparisonFixture.js";
import {
  ArmRunRequest,
  comparisonExecutionsPerPair,
  comparisonMeasuredSamples,
  comparisonScenarioId,
  comparisonWarmups,
  PairRunArtifact,
  renderPairSummary,
  runPair,
  spawnArmProcess,
  validatePairRunArtifact,
  writeJson,
} from "../comparison/ComparisonRunner.js";

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
  if (!Number.isSafeInteger(value) || value < 0)
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
  const result = await prepareComparisonFixture(
    required(args, "output"),
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

function writePairArtifacts(
  outputDirectory: string,
  artifact: PairRunArtifact
): void {
  validatePairRunArtifact(artifact);
  writeJson(path.join(outputDirectory, "pair-result.json"), artifact);
  if (artifact.armA)
    writeJson(path.join(outputDirectory, "baseline-arm.json"), artifact.armA);
  if (artifact.armB)
    writeJson(path.join(outputDirectory, "candidate-arm.json"), artifact.armB);
  writeJson(path.join(outputDirectory, "summary.json"), {
    artifactVersion: artifact.artifactVersion,
    jobId: artifact.jobId,
    pair: artifact.pair,
    order: artifact.order,
    baseline: artifact.armASource,
    candidate: artifact.armBSource,
    fingerprint: artifact.fingerprint,
    fixtureArtifactHash: artifact.fixtureArtifactHash,
    fixtureIdentity: artifact.fixtureIdentity,
    semanticDigest: artifact.semanticDigest,
    summary: artifact.summary,
    discardedReason: artifact.discardedReason,
  });
  fs.writeFileSync(
    path.join(outputDirectory, "comparison.md"),
    renderPairSummary(artifact)
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
      ? positiveInteger(args, "timeout-ms")
      : undefined,
  });
  writePairArtifacts(output, artifact);
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
        label: "baseline",
        packageRoot: armPackage,
        operation: "change-processing",
      },
      armASource: { ref: "smoke", sha: "0".repeat(40) },
      armB: {
        id: "smoke-b",
        label: "candidate",
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
        percentDelta: artifact.summary?.percentDelta,
        timeoutProbeMilliseconds,
        elapsedMilliseconds: Date.now() - startedAt,
      })}\n`
    );
  } finally {
    if (!requestedOutput)
      fs.rmSync(outputRoot, { recursive: true, force: true });
  }
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
