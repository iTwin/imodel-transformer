/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { EnvironmentClass } from "./EnvironmentClass";
import {
  aggregateLogRatios,
  CollapsedPair,
  LogRatioAggregate,
  logRatioToPercent,
} from "./logRatio";
import { NoiseBand, NoiseBandPool, powerPoint } from "./NoiseBand";
import {
  ComparisonVerdict,
  decideVerdict,
  VerdictInput,
  VerdictResult,
} from "./verdict";

/**
 * Comparison report assembly.
 *
 * One report shape serves both comparison modes, but the two are not equally strong and the report
 * says so rather than letting a reader assume otherwise.
 */

export type ComparisonMode = "paired" | "unpaired";

export interface ArmDescription {
  readonly id: string;
  readonly label?: string;
  readonly transformerVersion: string;
  readonly coreBackendVersion: string;
}

export interface DetectabilitySummary {
  /**
   * The band, as a percentage. This is roughly the 50%-POWER point: at a true effect exactly equal
   * to the threshold, the estimator lands above it about half the time. It is reported with that
   * label because "MDE 4%" reads as "catches 4% regressions", which overstates detection by about
   * a factor of two.
   */
  readonly fiftyPercentPowerPercent: number;
  /** Effect size at which the magnitude gate reaches 80% power. The honest "we will catch it" number. */
  readonly eightyPercentPowerPercent?: number;
  readonly bandStatus: NoiseBand["status"];
  readonly bandKind: NoiseBand["kind"];
  readonly pairsAccumulated: number;
  readonly runsAccumulated: number;
  /** Diagnostic only, never the gate. */
  readonly individualPair95Percent: number;
  readonly observedMaximumPercent: number;
}

export interface ComparisonReport {
  readonly scenarioId: string;
  readonly recipeHash: string;
  readonly mode: ComparisonMode;
  readonly look: 1 | 2;
  readonly environment: EnvironmentClass;
  readonly armA: ArmDescription;
  readonly armB: ArmDescription;
  readonly signConvention: string;
  readonly pairs: number;
  readonly discardedPairs: readonly { pair: number; reason: string }[];
  readonly samplesPerArmPerPair: number;
  readonly aggregate: LogRatioAggregate;
  readonly verdict: VerdictResult;
  readonly detectability?: DetectabilitySummary;
  readonly observations: readonly CollapsedPair[];
  readonly generatedAt: string;
}

export interface BuildComparisonReportOptions {
  readonly scenarioId: string;
  readonly recipeHash: string;
  readonly mode: ComparisonMode;
  readonly look: 1 | 2;
  readonly environment: EnvironmentClass;
  readonly armA: ArmDescription;
  readonly armB: ArmDescription;
  readonly pairs: readonly CollapsedPair[];
  readonly samplesPerArmPerPair: number;
  readonly band?: NoiseBand;
  readonly pool?: NoiseBandPool;
  readonly equivalenceMargin?: number;
  readonly discardedPairs?: readonly { pair: number; reason: string }[];
  readonly validityFailures?: VerdictInput["validityFailures"];
  readonly minimumPairs?: number;
}

function summarizeDetectability(
  band: NoiseBand,
  pool: NoiseBandPool | undefined,
  pairs: number
): DetectabilitySummary {
  return {
    fiftyPercentPowerPercent: band.bandPercent,
    eightyPercentPowerPercent: pool
      ? logRatioToPercent(powerPoint(pool.observations, pairs, band.band, 0.8))
      : undefined,
    bandStatus: band.status,
    bandKind: band.kind,
    pairsAccumulated: band.pairsAccumulated,
    runsAccumulated: band.runsAccumulated,
    individualPair95Percent: band.individualPair95Percent,
    observedMaximumPercent: band.observedMaximumPercent,
  };
}

export function buildComparisonReport(
  options: BuildComparisonReportOptions
): ComparisonReport {
  const aggregate = aggregateLogRatios(
    options.pairs.map((pair) => pair.logRatio)
  );
  const verdict = decideVerdict({
    aggregate,
    band: options.band,
    equivalenceMargin: options.equivalenceMargin,
    look: options.look,
    mode: options.mode,
    validityFailures: options.validityFailures,
    minimumPairs: options.minimumPairs,
  });
  return {
    scenarioId: options.scenarioId,
    recipeHash: options.recipeHash,
    mode: options.mode,
    look: options.look,
    environment: options.environment,
    armA: options.armA,
    armB: options.armB,
    signConvention: "Positive percent change means arm B is SLOWER than arm A.",
    pairs: options.pairs.length,
    discardedPairs: options.discardedPairs ?? [],
    samplesPerArmPerPair: options.samplesPerArmPerPair,
    aggregate,
    verdict,
    detectability: options.band
      ? summarizeDetectability(options.band, options.pool, options.pairs.length)
      : undefined,
    observations: options.pairs,
    generatedAt: new Date().toISOString(),
  };
}

function formatPercent(value: number | undefined): string {
  return value === undefined
    ? "n/a"
    : `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

/**
 * Human-readable rendering.
 *
 * Detectability is printed on EVERY report, including uncalibrated ones, where it prints as
 * unknown rather than being silently omitted -- an absent noise floor is the single most important
 * thing a reader needs to know about a comparison.
 */
export function renderComparisonReport(report: ComparisonReport): string {
  const lines: string[] = [];
  lines.push(`Quick performance comparison: ${report.scenarioId}`);
  lines.push(
    `Mode: ${report.mode}${report.mode === "unpaired" ? " (baseline; no per-pair signs, wider band)" : ""}  Look: ${report.look}`
  );
  lines.push(
    `Arm A: ${report.armA.id}${report.armA.label ? ` (${report.armA.label})` : ""} transformer ${report.armA.transformerVersion}`
  );
  lines.push(
    `Arm B: ${report.armB.id}${report.armB.label ? ` (${report.armB.label})` : ""} transformer ${report.armB.transformerVersion}`
  );
  lines.push(
    `core-backend: ${report.armA.coreBackendVersion} (identical in both arms)`
  );
  lines.push(
    `Environment: ${report.environment.id} (${report.environment.descriptor.platform}/${report.environment.descriptor.arch}, ${report.environment.descriptor.runner})`
  );
  lines.push("");
  lines.push(`VERDICT: ${report.verdict.verdict.toUpperCase()}`);
  lines.push(`  ${report.verdict.reason}`);
  if (report.verdict.provisionalBand)
    lines.push(
      "  NOTE: band is PROVISIONAL (below the established pair/run minimum)."
    );
  if (report.verdict.escalationRecommended)
    lines.push(
      "  Escalation recommended: magnitude cleared but signs did not agree. This is the only " +
        "inconclusive shape that more pairs actually fixes."
    );
  lines.push("");
  lines.push(report.signConvention);
  lines.push(
    `Median change: ${formatPercent(report.aggregate.percentChange)}  (geometric mean ${formatPercent(report.aggregate.geometricMeanPercentChange)})`
  );
  lines.push(
    `Bootstrap ${(report.aggregate.bootstrap.level * 100).toFixed(0)}% interval: [${formatPercent(logRatioToPercent(report.aggregate.bootstrap.lower))}, ${formatPercent(logRatioToPercent(report.aggregate.bootstrap.upper))}] (indicative, not a gate)`
  );
  lines.push(
    `Pairs: ${report.pairs} valid, ${report.discardedPairs.length} discarded, k = ${report.samplesPerArmPerPair}`
  );
  if (report.mode === "paired") {
    const signRequirement = report.verdict.signGate
      ? `  requirement ${report.verdict.signGate.requirement.requiredAgreeing} of ${report.verdict.signGate.effectivePairs} (exact p = ${report.verdict.signGate.exactP.toFixed(5)})`
      : "";
    lines.push(
      `Signs: ${report.aggregate.signs.positive} slower / ${report.aggregate.signs.negative} faster / ${report.aggregate.signs.ties} tied${signRequirement}`
    );
  }
  lines.push("");
  if (report.detectability) {
    lines.push(
      `Detectability (${report.detectability.bandKind} band, ${report.detectability.bandStatus}; ${report.detectability.pairsAccumulated} A/A pairs over ${report.detectability.runsAccumulated} runs):`
    );
    lines.push(
      `  ~50% power at ${report.detectability.fiftyPercentPowerPercent.toFixed(2)}%  <- this is the band itself`
    );
    lines.push(
      `  ~80% power at ${report.detectability.eightyPercentPowerPercent !== undefined ? `${report.detectability.eightyPercentPowerPercent.toFixed(2)}%` : "unknown"}  <- what is actually caught reliably`
    );
    lines.push(
      `  diagnostics: individual-pair 95th pct ${report.detectability.individualPair95Percent.toFixed(2)}%, observed max ${report.detectability.observedMaximumPercent.toFixed(2)}%`
    );
  } else {
    lines.push(
      "Detectability: UNKNOWN (uncalibrated -- no A/A band for this environment)."
    );
    lines.push(
      `  Within-run bootstrap half-width is ${((logRatioToPercent(report.aggregate.bootstrap.upper) - logRatioToPercent(report.aggregate.bootstrap.lower)) / 2).toFixed(2)}%. ` +
        "That is within-run spread, NOT a noise floor and NOT an MDE."
    );
  }
  return `${lines.join("\n")}\n`;
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
    path.join(outputDir, "comparison.txt"),
    renderComparisonReport(report)
  );
}

export type { ComparisonVerdict };
