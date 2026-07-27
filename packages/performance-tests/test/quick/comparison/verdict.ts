/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { signGateRequirement, SignGateRequirement } from "./binomial";
import { LogRatioAggregate, logRatioToPercent } from "./logRatio";
import { NoiseBand } from "./NoiseBand";

/**
 * Verdict rule.
 *
 * A change requires BOTH gates. Magnitude alone is a number that could be noise; consistency
 * alone is a direction with no size. `unchanged` is a separate claim from `inconclusive` and
 * requires its own evidence -- "we showed it did not move" and "we could not tell" are different
 * statements, and reporting the second as the first is how a regression ships.
 */

export type ComparisonVerdict =
  | "regressed"
  | "improved"
  | "unchanged"
  | "inconclusive"
  | "uncalibrated"
  | "insufficient-pairs"
  | "invalid";

/** One declared target level per look; the agreement COUNT is derived from it exactly. */
export const signGateTargetLevel = 0.01;
export const defaultPairs = 8;
export const escalatedPairs = 16;
export const minimumPairs = 6;

/**
 * Practical equivalence margin: the change below which nobody would act, declared from domain
 * relevance rather than derived from the measurement.
 *
 * This is deliberately NOT the noise floor. The floor says what the machine can resolve; the margin
 * says what is worth caring about. Deriving one from the other makes `unchanged` hostage to how
 * quiet the machine happened to be, and reduces a statement a reader wants ("smaller than we care
 * about") to one they do not ("smaller than today's noise").
 *
 * When the measured floor exceeds this margin, the environment cannot resolve what we care about
 * and the honest output is `inconclusive` saying exactly that. The margin is never widened to make
 * `unchanged` reachable.
 */
export const defaultEquivalenceMarginPercent = 5;

export interface ValidityFailure {
  readonly check: string;
  readonly detail: string;
}

export interface MagnitudeGate {
  readonly passed: boolean;
  readonly statistic: number;
  readonly threshold: number;
  readonly statisticPercent: number;
  readonly thresholdPercent: number;
}

export interface SignGate {
  readonly passed: boolean;
  readonly agreeing: number;
  readonly effectivePairs: number;
  readonly requirement: SignGateRequirement;
  readonly exactP: number;
}

export interface VerdictInput {
  readonly aggregate: LogRatioAggregate;
  /** Absent when the environment has no A/A calibration; the run is then descriptive only. */
  readonly band?: NoiseBand;
  /**
   * Equivalence margin on the log scale, declared from domain relevance -- the size below which
   * nobody would act. Absent until declared, in which case `unchanged` cannot be established and
   * the run resolves to `inconclusive` with that stated as the reason.
   */
  readonly equivalenceMargin?: number;
  readonly look: 1 | 2;
  readonly validityFailures?: readonly ValidityFailure[];
  /**
   * Paired comparisons have per-pair signs; baseline comparisons do not, so the sign gate is
   * unavailable and the verdict rests on the magnitude gate alone against the wider unpaired band.
   */
  readonly mode: "paired" | "unpaired";
  readonly minimumPairs?: number;
}

export interface VerdictResult {
  readonly verdict: ComparisonVerdict;
  readonly reason: string;
  readonly magnitudeGate?: MagnitudeGate;
  readonly signGate?: SignGate;
  /**
   * True only when the sign gate failed while the magnitude gate passed. The sign gate is the
   * binding constraint across the usable effect range, and escalation is what relaxes it -- from
   * unanimity at 8 pairs to 14/16. Escalating on a magnitude failure instead spends a second full
   * run on the gate that was not the obstacle.
   *
   * Note the tempting but invalid check: magnitude power measured at the band is ~50% at every
   * pair count. That is an identity -- the band is the 95th percentile of the null median and the
   * median is centred on the true effect -- not evidence that escalation achieves nothing. Each
   * pair count is being evaluated at a different effect size there. At a FIXED effect, escalation
   * roughly doubles detection.
   */
  readonly escalationRecommended: boolean;
  readonly provisionalBand: boolean;
}

function evaluateMagnitude(
  aggregate: LogRatioAggregate,
  band: NoiseBand
): MagnitudeGate {
  const statistic = Math.abs(aggregate.medianLogRatio);
  return {
    passed: statistic > band.band,
    statistic,
    threshold: band.band,
    statisticPercent: logRatioToPercent(statistic),
    thresholdPercent: band.bandPercent,
  };
}

function evaluateSign(aggregate: LogRatioAggregate): SignGate {
  const requirement = signGateRequirement(
    aggregate.signs.effectivePairs,
    signGateTargetLevel
  );
  return {
    passed:
      !requirement.unachievable &&
      aggregate.signs.agreeing >= requirement.requiredAgreeing,
    agreeing: aggregate.signs.agreeing,
    effectivePairs: aggregate.signs.effectivePairs,
    requirement,
    exactP: aggregate.signs.exactP,
  };
}

export function decideVerdict(input: VerdictInput): VerdictResult {
  const failures = input.validityFailures ?? [];
  if (failures.length > 0)
    return {
      verdict: "invalid",
      reason: `Harness validity failed: ${failures
        .map((failure) => `${failure.check} (${failure.detail})`)
        .join("; ")}`,
      escalationRecommended: false,
      provisionalBand: false,
    };

  const requiredPairs = input.minimumPairs ?? minimumPairs;
  if (input.aggregate.pairs < requiredPairs)
    return {
      verdict: "insufficient-pairs",
      reason: `${input.aggregate.pairs} valid pairs is below the minimum of ${requiredPairs}`,
      escalationRecommended: false,
      provisionalBand: false,
    };

  const signGate =
    input.mode === "paired" ? evaluateSign(input.aggregate) : undefined;

  if (!input.band)
    return {
      verdict: "uncalibrated",
      reason:
        "No A/A noise band exists for this environment class, so no verdict may be issued. Output is descriptive only.",
      signGate,
      escalationRecommended: false,
      provisionalBand: false,
    };

  const provisionalBand = input.band.status === "provisional";
  if (input.band.status === "uncalibrated")
    return {
      verdict: "uncalibrated",
      reason: `A/A pool has ${input.band.pairsAccumulated} pairs across ${input.band.runsAccumulated} runs, below the calibration minimum. Output is descriptive only.`,
      signGate,
      escalationRecommended: false,
      provisionalBand: false,
    };

  const magnitudeGate = evaluateMagnitude(input.aggregate, input.band);
  // Unpaired baseline comparison has no pairs, so no sign gate exists to satisfy.
  const signSatisfied = signGate === undefined || signGate.passed;

  if (magnitudeGate.passed && signSatisfied) {
    const direction =
      input.aggregate.medianLogRatio > 0 ? "regressed" : "improved";
    return {
      verdict: direction,
      reason: `Median log-ratio ${magnitudeGate.statisticPercent.toFixed(
        2
      )}% exceeds the ${input.band.kind} A/A band of ${magnitudeGate.thresholdPercent.toFixed(
        2
      )}%${
        signGate
          ? `, with ${signGate.agreeing}/${signGate.effectivePairs} pairs agreeing (requirement ${signGate.requirement.requiredAgreeing})`
          : " (unpaired: no sign evidence available)"
      }`,
      magnitudeGate,
      signGate,
      escalationRecommended: false,
      provisionalBand,
    };
  }

  if (input.equivalenceMargin === undefined)
    return {
      verdict: "inconclusive",
      reason:
        "No equivalence margin has been declared, so `unchanged` cannot be established. Declare the size below which no action would be taken.",
      magnitudeGate,
      signGate,
      escalationRecommended: Boolean(
        input.look === 1 && magnitudeGate.passed && signGate && !signGate.passed
      ),
      provisionalBand,
    };

  const margin = Math.abs(input.equivalenceMargin);
  if (margin < input.band.band)
    return {
      verdict: "inconclusive",
      reason: `The declared equivalence margin of ${logRatioToPercent(
        margin
      ).toFixed(
        2
      )}% is below the measured noise floor of ${input.band.bandPercent.toFixed(
        2
      )}%, so this environment cannot resolve it.`,
      magnitudeGate,
      signGate,
      escalationRecommended: false,
      provisionalBand,
    };

  const { lower, upper } = input.aggregate.bootstrap;
  if (lower > -margin && upper < margin)
    return {
      verdict: "unchanged",
      reason: `The bootstrap interval [${logRatioToPercent(lower).toFixed(
        2
      )}%, ${logRatioToPercent(upper).toFixed(
        2
      )}%] lies entirely inside the declared equivalence margin of +/-${logRatioToPercent(
        margin
      ).toFixed(2)}%`,
      magnitudeGate,
      signGate,
      escalationRecommended: false,
      provisionalBand,
    };

  return {
    verdict: "inconclusive",
    reason: magnitudeGate.passed
      ? `Median log-ratio ${magnitudeGate.statisticPercent.toFixed(
          2
        )}% clears the band, but only ${signGate?.agreeing ?? 0}/${
          signGate?.effectivePairs ?? 0
        } pairs agree in sign (requirement ${
          signGate?.requirement.requiredAgreeing ?? "n/a"
        })`
      : `Median log-ratio ${magnitudeGate.statisticPercent.toFixed(
          2
        )}% does not clear the ${input.band.kind} A/A band of ${magnitudeGate.thresholdPercent.toFixed(
          2
        )}%, and equivalence was not established`,
    magnitudeGate,
    signGate,
    escalationRecommended: Boolean(
      input.look === 1 && magnitudeGate.passed && signGate && !signGate.passed
    ),
    provisionalBand,
  };
}
