/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { EnvironmentClass, validateEnvironmentClass } from "./EnvironmentClass";
import {
  ExecutionFingerprint,
  validateExecutionFingerprint,
} from "./ExecutionFingerprint";
import {
  aggregateLogRatios,
  CollapsedPair,
  LogRatioAggregate,
  logRatioToPercent,
  PairOrder,
  percentToLogRatio,
  validateCollapsedPair,
  validateLogRatioAggregate,
} from "./logRatio";
import {
  assertBandApplies,
  assertBandDerivedFromPool,
  assertPoolApplies,
  NoiseBand,
  NoiseBandPool,
  powerPoint,
} from "./NoiseBand";
import { SeededRandom } from "./SeededRandom";
import {
  ComparisonVerdict,
  decideVerdict,
  defaultEquivalenceMarginPercent,
  VerdictInput,
  VerdictResult,
} from "./verdict";

export type ComparisonMode = "paired";

export interface ArmDescription {
  readonly id: string;
  readonly label?: string;
  readonly transformerVersion: string;
  readonly coreBackendVersion: string;
}

export interface DetectabilitySummary {
  readonly noiseBandPercent: number;
  readonly eightyPercentMagnitudePowerPercent?: number;
  readonly status: NoiseBand["status"];
  readonly quality: NoiseBand["quality"];
  readonly observations: number;
  readonly independentJobs: number;
  readonly individualObservation95Percent: number;
  readonly observedMaximumPercent: number;
}

export interface ComparisonReport {
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly recipeHash: string;
  readonly mode: ComparisonMode;
  readonly environment: EnvironmentClass;
  readonly execution: ExecutionFingerprint;
  readonly armA: ArmDescription;
  readonly armB: ArmDescription;
  readonly signConvention: string;
  readonly observations: number;
  readonly independentJobs: number;
  readonly discardedObservations: readonly DiscardedObservation[];
  readonly aggregate: LogRatioAggregate;
  readonly verdict: VerdictResult;
  readonly detectability?: DetectabilitySummary;
  readonly collapsedObservations: readonly CollapsedPair[];
  readonly generatedAt: string;
}

export interface DiscardedObservation {
  readonly index: number;
  readonly order: PairOrder;
  readonly reason: string;
}

export interface BuildComparisonReportOptions {
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly recipeHash: string;
  readonly mode: ComparisonMode;
  readonly environment: EnvironmentClass;
  readonly execution: ExecutionFingerprint;
  readonly armA: ArmDescription;
  readonly armB: ArmDescription;
  readonly pairs: readonly CollapsedPair[];
  readonly independentJobs: number;
  readonly band?: NoiseBand;
  readonly pool?: NoiseBandPool;
  readonly equivalenceMarginPercent?: number;
  readonly discardedObservations?: readonly DiscardedObservation[];
  readonly validityFailures?: VerdictInput["validityFailures"];
  readonly minimumObservations?: number;
  readonly minimumEquivalenceObservations?: number;
  readonly equivalenceConfidenceLevel?: number;
}

function expectedPairOrders(
  execution: ExecutionFingerprint,
  count: number
): PairOrder[] {
  const policy = execution.orderPolicy;
  if (policy.kind === "fixed")
    return Array.from({ length: count }, () => policy.order);
  if (policy.kind === "alternating")
    return Array.from({ length: count }, (_, index) =>
      index % 2 === 0 ? policy.first : policy.first === "AB" ? "BA" : "AB"
    );
  const random = new SeededRandom(policy.seed);
  return Array.from({ length: count }, () =>
    random.next() < 0.5 ? "AB" : "BA"
  );
}

function validateObservationPlan(
  execution: ExecutionFingerprint,
  pairs: readonly CollapsedPair[],
  discarded: readonly DiscardedObservation[],
  expectedCount: number
): void {
  const expectedOrders = expectedPairOrders(execution, expectedCount);
  const observed = new Map<number, PairOrder>();
  const add = (index: number, order: PairOrder, label: string) => {
    if (!Number.isSafeInteger(index) || index < 0 || index >= expectedCount)
      throw new Error(
        `${label} index ${index} is outside the execution plan 0..${expectedCount - 1}`
      );
    if (order !== "AB" && order !== "BA")
      throw new Error(`${label} order must be AB or BA`);
    if (observed.has(index))
      throw new Error(`Execution plan contains duplicate pair index ${index}`);
    if (order !== expectedOrders[index])
      throw new Error(
        `Pair ${index} order ${order} does not match execution plan ${expectedOrders[index]}`
      );
    observed.set(index, order);
  };
  for (const pair of pairs) {
    validateCollapsedPair(pair);
    add(pair.pair, pair.order, "Pair");
  }
  for (const discardedObservation of discarded) {
    if (
      typeof discardedObservation.reason !== "string" ||
      discardedObservation.reason.trim().length === 0
    )
      throw new Error("Discarded observation reason cannot be empty");
    add(
      discardedObservation.index,
      discardedObservation.order,
      "Discarded observation"
    );
  }
  if (observed.size !== expectedCount)
    throw new Error(
      `Execution plan covers ${observed.size} pair indexes; expected ${expectedCount}`
    );
}

function assertFiniteJsonNumbers(
  value: unknown,
  pathLabel = "comparison report"
): void {
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error(`${pathLabel} contains a non-finite number`);
    return;
  }

  if (Array.isArray(value)) {
    value.forEach((entry, index) =>
      assertFiniteJsonNumbers(entry, `${pathLabel}[${index}]`)
    );
    return;
  }
  if (value && typeof value === "object")
    for (const [key, entry] of Object.entries(value))
      assertFiniteJsonNumbers(entry, `${pathLabel}.${key}`);
}

function assertFiniteNumber(value: number, label: string): void {
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new Error(`${label} must be a finite number`);
}

function assertNonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0)
    throw new Error(`${label} cannot be empty`);
}

function summarizeDetectability(
  band: NoiseBand,
  pool: NoiseBandPool | undefined,
  observations: number,
  decisionThreshold: number
): DetectabilitySummary {
  return {
    noiseBandPercent: band.bandPercent,
    eightyPercentMagnitudePowerPercent: pool
      ? logRatioToPercent(
          powerPoint(pool.observations, observations, decisionThreshold, 0.8)
        )
      : undefined,
    status: band.status,
    quality: band.quality,
    observations: band.observations,
    independentJobs: band.independentJobs,
    individualObservation95Percent: band.individualObservation95Percent,
    observedMaximumPercent: band.observedMaximumPercent,
  };
}

export function buildComparisonReport(
  options: BuildComparisonReportOptions
): ComparisonReport {
  validateExecutionFingerprint(options.execution);
  if (!Number.isInteger(options.independentJobs) || options.independentJobs < 1)
    throw new Error("Comparison requires at least one independent job");
  const discardedObservations = options.discardedObservations ?? [];
  const discardedCount = discardedObservations.length;
  const totalObservations = options.pairs.length + discardedCount;
  if (options.execution.pairPolicy.kind !== "paired")
    throw new Error(
      "Unpaired comparison requires a distinct estimator and is not implemented"
    );
  const expected =
    options.execution.pairPolicy.pairsPerJob * options.independentJobs;
  if (totalObservations !== expected)
    throw new Error(
      `Comparison contains ${totalObservations} paired observations; execution fingerprint requires ${expected}`
    );
  validateObservationPlan(
    options.execution,
    options.pairs,
    discardedObservations,
    expected
  );
  const calibrationKey = {
    scenarioId: options.scenarioId,
    fixtureId: options.fixtureId,
    recipeHash: options.recipeHash,
    environmentClass: options.environment.id,
    execution: options.execution,
    kind: options.mode,
  };
  if (options.pool) assertPoolApplies(options.pool, calibrationKey);
  if (options.band) {
    if (!options.pool)
      throw new Error(
        "A comparison calibration band requires its source A/A pool"
      );
    assertBandApplies(options.band, calibrationKey, options.pairs.length);
    assertBandDerivedFromPool(options.band, options.pool);
  }
  const aggregate = aggregateLogRatios(
    options.pairs.map((pair) => pair.logRatio)
  );
  const verdict = decideVerdict({
    aggregate,
    band: options.band,
    equivalenceMarginPercent: options.equivalenceMarginPercent,
    mode: options.mode,
    validityFailures: options.validityFailures,
    minimumObservations: options.minimumObservations,
    minimumEquivalenceObservations: options.minimumEquivalenceObservations,
    equivalenceConfidenceLevel: options.equivalenceConfidenceLevel,
  });
  const report: ComparisonReport = {
    scenarioId: options.scenarioId,
    fixtureId: options.fixtureId,
    recipeHash: options.recipeHash,
    mode: options.mode,
    environment: options.environment,
    execution: options.execution,
    armA: options.armA,
    armB: options.armB,
    signConvention: "Positive percent change means arm B is slower than arm A.",
    observations: options.pairs.length,
    independentJobs: options.independentJobs,
    discardedObservations,
    aggregate,
    verdict,
    detectability: options.band
      ? summarizeDetectability(
          options.band,
          options.pool,
          options.pairs.length,
          Math.max(
            options.band.band,
            percentToLogRatio(
              options.equivalenceMarginPercent ??
                defaultEquivalenceMarginPercent
            )
          )
        )
      : undefined,
    collapsedObservations: options.pairs,
    generatedAt: new Date().toISOString(),
  };
  validateComparisonReport(report);
  return report;
}

export function validateComparisonReport(report: ComparisonReport): void {
  if (!report || typeof report !== "object")
    throw new Error("Comparison report must be an object");
  if (!Array.isArray(report.collapsedObservations))
    throw new Error("Comparison report observations must be an array");
  if (!Array.isArray(report.discardedObservations))
    throw new Error(
      "Comparison report discarded observations must be an array"
    );
  assertNonEmpty(report.scenarioId, "Comparison scenario id");
  assertNonEmpty(report.fixtureId, "Comparison fixture id");
  assertNonEmpty(report.recipeHash, "Comparison recipe hash");
  if (report.mode !== "paired")
    throw new Error("Comparison report mode must be paired");
  validateEnvironmentClass(report.environment);
  for (const [label, arm] of [
    ["Arm A", report.armA],
    ["Arm B", report.armB],
  ] as const) {
    if (!arm || typeof arm !== "object")
      throw new Error(`${label} description must be an object`);
    assertNonEmpty(arm.id, `${label} id`);
    assertNonEmpty(arm.transformerVersion, `${label} transformer version`);
    assertNonEmpty(arm.coreBackendVersion, `${label} core-backend version`);
  }
  validateExecutionFingerprint(report.execution);
  validateLogRatioAggregate(report.aggregate);
  if (
    !Number.isSafeInteger(report.observations) ||
    report.observations < 1 ||
    report.observations !== report.collapsedObservations.length ||
    report.aggregate.pairs !== report.observations
  )
    throw new Error("Comparison report observation counts are inconsistent");
  if (
    !Number.isSafeInteger(report.independentJobs) ||
    report.independentJobs < 1
  )
    throw new Error("Comparison report independent jobs must be positive");
  if (report.execution.pairPolicy.kind !== "paired")
    throw new Error("Comparison report execution policy must be paired");
  const expected =
    report.execution.pairPolicy.pairsPerJob * report.independentJobs;
  validateObservationPlan(
    report.execution,
    report.collapsedObservations,
    report.discardedObservations,
    expected
  );
  if (!Number.isFinite(Date.parse(report.generatedAt)))
    throw new Error("Comparison report generatedAt must be a valid timestamp");
  if (!report.verdict || typeof report.verdict !== "object")
    throw new Error("Comparison report verdict must be an object");
  if (
    ![
      "regressed",
      "improved",
      "unchanged",
      "inconclusive",
      "uncalibrated",
      "insufficient-observations",
      "invalid",
    ].includes(report.verdict.verdict)
  )
    throw new Error("Unknown comparison verdict");
  if (
    !["actionable", "informational", "descriptive"].includes(
      report.verdict.evidence
    )
  )
    throw new Error("Unknown comparison evidence");
  assertNonEmpty(report.verdict.reason, "Comparison verdict reason");
  if (report.verdict.magnitudeGate) {
    if (typeof report.verdict.magnitudeGate.passed !== "boolean")
      throw new Error("Magnitude gate result must be boolean");
    for (const [label, value] of Object.entries(
      report.verdict.magnitudeGate
    ).filter((entry) => entry[0] !== "passed"))
      assertFiniteNumber(value as number, `Magnitude gate ${label}`);
  }
  if (report.verdict.signDiagnostic)
    for (const [label, value] of Object.entries(
      report.verdict.signDiagnostic
    ).filter((entry) => entry[0] !== "nearEven"))
      assertFiniteNumber(value as number, `Sign diagnostic ${label}`);
  if (
    report.verdict.signDiagnostic &&
    typeof report.verdict.signDiagnostic.nearEven !== "boolean"
  )
    throw new Error("Sign diagnostic nearEven must be boolean");
  if (report.verdict.equivalenceDiagnostic)
    for (const [label, value] of Object.entries(
      report.verdict.equivalenceDiagnostic
    ))
      assertFiniteNumber(value, `Equivalence diagnostic ${label}`);
  if (report.detectability) {
    for (const [label, value] of [
      ["noise band percent", report.detectability.noiseBandPercent],
      ["observations", report.detectability.observations],
      ["independent jobs", report.detectability.independentJobs],
      [
        "individual observation percentile",
        report.detectability.individualObservation95Percent,
      ],
      ["observed maximum", report.detectability.observedMaximumPercent],
    ] as const)
      assertFiniteNumber(value, `Detectability ${label}`);
    if (report.detectability.eightyPercentMagnitudePowerPercent !== undefined)
      assertFiniteNumber(
        report.detectability.eightyPercentMagnitudePowerPercent,
        "Detectability power point"
      );
  }
  assertFiniteJsonNumbers(report);
}

export function parseComparisonReportJson(json: string): ComparisonReport {
  const value = JSON.parse(json) as ComparisonReport;
  validateComparisonReport(value);
  return value;
}

export function readComparisonReport(inputPath: string): ComparisonReport {
  return parseComparisonReportJson(fs.readFileSync(inputPath, "utf8"));
}

function formatPercent(value: number | undefined): string {
  return value === undefined
    ? "n/a"
    : `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

export function renderComparisonReport(report: ComparisonReport): string {
  validateComparisonReport(report);
  const lines = [
    `# Quick performance comparison: ${report.scenarioId}`,
    "",
    `**Verdict:** ${report.verdict.verdict.toUpperCase()} (${report.verdict.evidence})`,
    "",
    report.verdict.reason,
    "",
    "| Property | Value |",
    "|---|---|",
    `| Mode | ${report.mode} |`,
    `| Fixture | ${report.fixtureId} |`,
    `| Environment | ${report.environment.id} |`,
    `| Arm A | ${report.armA.id} (${report.armA.transformerVersion}) |`,
    `| Arm B | ${report.armB.id} (${report.armB.transformerVersion}) |`,
    `| Median change | ${formatPercent(report.aggregate.percentChange)} |`,
    `| Geometric-mean change | ${formatPercent(
      report.aggregate.geometricMeanPercentChange
    )} |`,
    `| Valid observations | ${report.observations} |`,
    `| Independent jobs | ${report.independentJobs} |`,
    `| Discarded observations | ${report.discardedObservations.length} |`,
    "",
    report.signConvention,
    "",
    `Bootstrap ${(report.aggregate.bootstrap.level * 100).toFixed(
      0
    )}% interval: [${formatPercent(
      logRatioToPercent(report.aggregate.bootstrap.lower)
    )}, ${formatPercent(
      logRatioToPercent(report.aggregate.bootstrap.upper)
    )}] (indicative, not a change gate).`,
    "",
  ];
  if (report.verdict.signDiagnostic)
    lines.push(
      `Signs: ${report.verdict.signDiagnostic.positive} slower / ${report.verdict.signDiagnostic.negative} faster / ${report.verdict.signDiagnostic.ties} tied; imbalance ${report.verdict.signDiagnostic.imbalance.toFixed(
        3
      )}.`,
      ""
    );
  if (report.verdict.equivalenceDiagnostic)
    lines.push(
      `Exact median interval (${(
        report.verdict.equivalenceDiagnostic.coverage * 100
      ).toFixed(2)}% coverage): [${formatPercent(
        logRatioToPercent(report.verdict.equivalenceDiagnostic.lower)
      )}, ${formatPercent(
        logRatioToPercent(report.verdict.equivalenceDiagnostic.upper)
      )}].`,
      ""
    );
  if (report.detectability)
    lines.push(
      `Calibration: **${report.detectability.quality}** (${report.detectability.status}); ${report.detectability.noiseBandPercent.toFixed(
        2
      )}% band from ${report.detectability.observations} observations across ${
        report.detectability.independentJobs
      } independent jobs.`,
      `Magnitude-only 80% power point: ${
        report.detectability.eightyPercentMagnitudePowerPercent === undefined
          ? "unknown"
          : `${report.detectability.eightyPercentMagnitudePowerPercent.toFixed(
              2
            )}%`
      }.`,
      ""
    );
  else
    lines.push(
      "Calibration: **unknown**. No matching A/A pool exists; output is descriptive only.",
      ""
    );
  lines.push(
    "Execution fingerprint:",
    "",
    "```json",
    JSON.stringify(report.execution, undefined, 2),
    "```",
    ""
  );
  return lines.join("\n");
}

export function writeComparisonReport(
  outputDir: string,
  report: ComparisonReport
): void {
  validateComparisonReport(report);
  fs.mkdirSync(outputDir, { recursive: true });
  fs.writeFileSync(
    path.join(outputDir, "comparison.json"),
    `${JSON.stringify(report, undefined, 2)}\n`
  );
  fs.writeFileSync(
    path.join(outputDir, "comparison.md"),
    renderComparisonReport(report)
  );
}

export type { ComparisonVerdict };
