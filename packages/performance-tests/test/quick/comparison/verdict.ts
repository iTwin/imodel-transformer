/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  LogRatioAggregate,
  logRatioToPercent,
  percentToLogRatio,
} from "./logRatio";
import {
  CalibrationQuality,
  meaningfulRegressionPercent,
  NoiseBand,
} from "./NoiseBand";

export type ComparisonVerdict =
  | "regressed"
  | "improved"
  | "unchanged"
  | "inconclusive"
  | "uncalibrated"
  | "insufficient-observations"
  | "invalid";

export type EvidenceLevel = "actionable" | "informational" | "descriptive";

export const defaultEquivalenceMarginPercent = meaningfulRegressionPercent;
/** A 50/50 or 5/3 split at eight observations is treated as near-even. */
export const defaultMaximumSignImbalance = 0.25;

export interface ValidityFailure {
  readonly check: string;
  readonly detail: string;
}

export interface MagnitudeGate {
  readonly passed: boolean;
  readonly statisticPercent: number;
  readonly lowerThreshold: number;
  readonly upperThreshold: number;
  readonly lowerThresholdPercent: number;
  readonly upperThresholdPercent: number;
  readonly noiseThreshold: number;
  readonly meaningfulLowerThreshold: number;
  readonly meaningfulUpperThreshold: number;
}

export interface SignDiagnostic {
  readonly positive: number;
  readonly negative: number;
  readonly ties: number;
  readonly effectiveObservations: number;
  readonly imbalance: number;
  readonly nearEven: boolean;
  readonly exactP: number;
}

export interface VerdictInput {
  readonly aggregate: LogRatioAggregate;
  readonly band?: NoiseBand;
  readonly equivalenceMarginPercent?: number;
  readonly validityFailures?: readonly ValidityFailure[];
  readonly mode: "paired";
  readonly minimumObservations?: number;
  readonly maximumSignImbalance?: number;
}

export interface VerdictResult {
  readonly verdict: ComparisonVerdict;
  readonly evidence: EvidenceLevel;
  readonly reason: string;
  readonly calibrationQuality?: CalibrationQuality;
  readonly magnitudeGate?: MagnitudeGate;
  readonly signDiagnostic?: SignDiagnostic;
}

function evidenceFor(quality: CalibrationQuality): EvidenceLevel {
  return quality === "target" ? "actionable" : "informational";
}

function evaluateMagnitude(
  aggregate: LogRatioAggregate,
  band: NoiseBand,
  equivalenceMarginPercent: number
): MagnitudeGate {
  const meaningfulLowerThreshold = percentToLogRatio(-equivalenceMarginPercent);
  const meaningfulUpperThreshold = percentToLogRatio(equivalenceMarginPercent);
  const lowerThreshold = Math.min(-band.band, meaningfulLowerThreshold);
  const upperThreshold = Math.max(band.band, meaningfulUpperThreshold);
  return {
    passed:
      aggregate.medianLogRatio < lowerThreshold ||
      aggregate.medianLogRatio > upperThreshold,
    statisticPercent: aggregate.percentChange,
    lowerThreshold,
    upperThreshold,
    lowerThresholdPercent: logRatioToPercent(lowerThreshold),
    upperThresholdPercent: logRatioToPercent(upperThreshold),
    noiseThreshold: band.band,
    meaningfulLowerThreshold,
    meaningfulUpperThreshold,
  };
}

function diagnoseSigns(
  aggregate: LogRatioAggregate,
  maximumImbalance: number
): SignDiagnostic {
  const { positive, negative, ties, effectivePairs, exactP } = aggregate.signs;
  const imbalance =
    effectivePairs === 0 ? 0 : Math.abs(positive - negative) / effectivePairs;
  return {
    positive,
    negative,
    ties,
    effectiveObservations: effectivePairs,
    imbalance,
    nearEven: effectivePairs >= 2 && imbalance <= maximumImbalance,
    exactP,
  };
}

export function decideVerdict(input: VerdictInput): VerdictResult {
  const failures = input.validityFailures ?? [];
  if (failures.length > 0)
    return {
      verdict: "invalid",
      evidence: "descriptive",
      reason: `Harness validity failed: ${failures
        .map((failure) => `${failure.check} (${failure.detail})`)
        .join("; ")}`,
    };

  const requiredObservations = input.minimumObservations ?? 1;
  if (input.aggregate.pairs < requiredObservations)
    return {
      verdict: "insufficient-observations",
      evidence: "descriptive",
      reason: `${input.aggregate.pairs} valid observations is below the configured minimum of ${requiredObservations}`,
    };

  const signDiagnostic = diagnoseSigns(
    input.aggregate,
    input.maximumSignImbalance ?? defaultMaximumSignImbalance
  );

  if (input.band && input.band.kind !== input.mode)
    return {
      verdict: "invalid",
      evidence: "descriptive",
      reason: `A ${input.band.kind} calibration band cannot be used for a ${input.mode} comparison.`,
      calibrationQuality: input.band.quality,
      signDiagnostic,
    };

  if (!input.band || input.band.status !== "established")
    return {
      verdict: "uncalibrated",
      evidence: "descriptive",
      reason: input.band
        ? `A/A calibration is ${input.band.status}; independent calibration jobs must satisfy the configured establishment requirements before a verdict is issued.`
        : "No matching A/A calibration exists. Output is descriptive only.",
      calibrationQuality: input.band?.quality,
      signDiagnostic,
    };

  const marginPercent =
    input.equivalenceMarginPercent ?? defaultEquivalenceMarginPercent;
  const lowerMargin = percentToLogRatio(-marginPercent);
  const upperMargin = percentToLogRatio(marginPercent);
  const magnitudeGate = evaluateMagnitude(
    input.aggregate,
    input.band,
    marginPercent
  );
  const evidence = evidenceFor(input.band.quality);

  if (magnitudeGate.passed) {
    if (signDiagnostic?.nearEven)
      return {
        verdict: "inconclusive",
        evidence: "informational",
        reason: `Magnitude clears the declared ${marginPercent.toFixed(
          2
        )}% threshold and A/A band, but signs are near-even (${signDiagnostic.positive} slower / ${signDiagnostic.negative} faster), indicating disagreement, outliers, or bimodality.`,
        calibrationQuality: input.band.quality,
        magnitudeGate,
        signDiagnostic,
      };
    const direction =
      input.aggregate.medianLogRatio > 0 ? "regressed" : "improved";
    return {
      verdict: direction,
      evidence,
      reason: `Median change ${input.aggregate.percentChange.toFixed(
        2
      )}% exceeds both the ${input.band.bandPercent.toFixed(
        2
      )}% A/A noise band and the ${marginPercent.toFixed(
        2
      )}% meaningful-change threshold.`,
      calibrationQuality: input.band.quality,
      magnitudeGate,
      signDiagnostic,
    };
  }

  if (input.band.band >= upperMargin || -input.band.band <= lowerMargin)
    return {
      verdict: "inconclusive",
      evidence: "informational",
      reason: `The ${input.band.bandPercent.toFixed(
        2
      )}% A/A noise band cannot resolve the declared ${marginPercent.toFixed(
        2
      )}% meaningful-change threshold.`,
      calibrationQuality: input.band.quality,
      magnitudeGate,
      signDiagnostic,
    };

  const { lower, upper } = input.aggregate.bootstrap;
  if (lower > lowerMargin && upper < upperMargin)
    return {
      verdict: "unchanged",
      evidence,
      reason: `The bootstrap interval [${logRatioToPercent(lower).toFixed(
        2
      )}%, ${logRatioToPercent(upper).toFixed(
        2
      )}%] lies inside the declared +/-${marginPercent.toFixed(
        2
      )}% equivalence margin.`,
      calibrationQuality: input.band.quality,
      magnitudeGate,
      signDiagnostic,
    };

  return {
    verdict: "inconclusive",
    evidence,
    reason: `Median change does not clear the A/A band and declared ${marginPercent.toFixed(
      2
    )}% threshold, and equivalence was not established.`,
    calibrationQuality: input.band.quality,
    magnitudeGate,
    signDiagnostic,
  };
}
