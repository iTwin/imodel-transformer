/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { spawnSync } from "node:child_process";
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildComparisonReport,
  writeComparisonReport,
} from "../comparison/ComparisonReport.js";
import {
  aggregateCalibration,
  assertComparisonFingerprintMatches,
  comparisonFingerprintKey,
  comparisonScenarioId,
  readCalibrationArtifact,
  readPairArtifact,
  renderCalibration,
  renderPairSummary,
  runPair,
  writeJson,
} from "../comparison/ComparisonRunner.js";
import { prepareComparisonFixture } from "../comparison/ComparisonFixture.js";
import { ArmOperation, ArmSpec } from "../comparison/ArmModule.js";

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

function aggregate(args: Map<string, string>): void {
  const input = required(args, "input");
  const output = required(args, "output");
  const files = findFiles(input, "pair-observation.json");
  const calibration = aggregateCalibration(files.map(readPairArtifact));
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
  assertComparisonFingerprintMatches(pair.fingerprint, calibration.fingerprint);
  if (pair.environment.id !== calibration.environment.id)
    throw new Error(
      `Comparison environment ${pair.environment.id} does not match calibration ${calibration.environment.id}`
    );
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
      band: calibration.band,
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
