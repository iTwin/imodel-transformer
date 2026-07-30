/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { EnvironmentClass } from "./EnvironmentClass";
import { ExecutionFingerprint } from "./ExecutionFingerprint";
import {
  aggregateLogRatios,
  CollapsedPair,
  LogRatioAggregate,
  logRatioToPercent,
  percentToLogRatio,
} from "./logRatio";
import {
  assertBandApplies,
  assertPoolApplies,
  NoiseBand,
  NoiseBandPool,
  powerPoint,
} from "./NoiseBand";
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
  readonly discardedObservations: readonly { index: number; reason: string }[];
  readonly aggregate: LogRatioAggregate;
  readonly verdict: VerdictResult;
  readonly detectability?: DetectabilitySummary;
  readonly collapsedObservations: readonly CollapsedPair[];
  readonly generatedAt: string;
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
  readonly discardedObservations?: readonly {
    readonly index: number;
    readonly reason: string;
  }[];
  readonly validityFailures?: VerdictInput["validityFailures"];
  readonly minimumObservations?: number;
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
  if (!Number.isInteger(options.independentJobs) || options.independentJobs < 1)
    throw new Error("Comparison requires at least one independent job");
  const discardedCount = options.discardedObservations?.length ?? 0;
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
  const calibrationKey = {
    scenarioId: options.scenarioId,
    fixtureId: options.fixtureId,
    recipeHash: options.recipeHash,
    environmentClass: options.environment.id,
    execution: options.execution,
    kind: options.mode,
  };
  if (options.pool) assertPoolApplies(options.pool, calibrationKey);
  if (options.band)
    assertBandApplies(options.band, calibrationKey, options.pairs.length);
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
  });
  return {
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
    discardedObservations: options.discardedObservations ?? [],
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
}

function formatPercent(value: number | undefined): string {
  return value === undefined
    ? "n/a"
    : `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

export function renderComparisonReport(report: ComparisonReport): string {
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
