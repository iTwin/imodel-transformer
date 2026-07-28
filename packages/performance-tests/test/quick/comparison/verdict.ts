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
 *
 * WHY THE CONSISTENCY GATE SURVIVED ITS REVIEW, AND WHY IT WAS LOOSENED
 *
 * The gate was challenged on the grounds that resampling the real A/A pool already makes the
 * magnitude gate nonparametric, so the consistency gate's distribution-free justification is
 * redundant and is costing minimum detectable effect for nothing.
 *
 * Half right, and the half that is wrong matters more. The magnitude gate is nonparametric in the
 * SHAPE of the per-pair distribution, but it is not robust to a change in SCALE between the run
 * that produced the band and the run being judged against it. Bands are persisted and reused --
 * that is the entire point of storing them -- so scale drift is the expected operating condition,
 * not an edge case. Measured false-positive rate at zero true effect, band calibrated at one
 * spread and the comparison run drawn at a multiple of it (20,000 trials per cell):
 *
 *   run spread     magnitude only     + 8/8     + 7/8     + 6/8
 *   1.0x                  4.88%       0.24%     1.48%     3.68%
 *   1.5x                 18.75%       0.66%     3.96%    11.67%
 *   3.0x                 50.97%       0.83%     6.53%    23.96%
 *
 * The consistency gate is scale-free: its null is 50/50 under any distribution with zero median,
 * whatever the spread. That is what holds the top-right of that table flat while the magnitude
 * gate alone degrades to a coin flip. Local runs have already been observed spanning 2.17% to
 * 6.43% CV on one machine class, so the 3.0x row is roughly the observed range, not a worst case.
 *
 * The obvious alternative -- detect staleness from the run's own spread and reject the band -- was
 * measured and is too weak to replace this: at 1.5x drift it flags only 34% of runs, while already
 * false-flagging 6.7% of well-calibrated ones. Useful as a supplement, not as a substitute.
 *
 * What the challenge got right is the price. Unanimity is over-insurance: it bought a 0.24%
 * false-positive rate against a 5% budget and cost 2.38x band for 80% power. Loosening to the
 * level below (7/8 at P = 8, 12/16 at P = 16) holds the combined rate at or near the 5% budget
 * across the whole drift range while moving the 80% point to 1.67x band -- recovering most of the
 * distance to the 1.42x an unprotected magnitude gate would give, at a fraction of the exposure.
 *
 * Note the level is declared on the CONJUNCTION, which is the rule that actually ships. Per-gate
 * levels are not the operative quantity and are reported only as diagnostics.
 */

export type ComparisonVerdict =
  | "regressed"
  | "improved"
  | "unchanged"
  | "inconclusive"
  | "uncalibrated"
  | "insufficient-pairs"
  | "invalid";

/**
 * One declared target level per look; the agreement COUNT is derived from it exactly.
 *
 * CHANGING THIS MOVES EVERY POWER STATEMENT IN EVERY REPORT. The operative 80%-power point is a
 * property of the CONJUNCTION of both gates, so it is a function of this constant:
 *
 * | consistency requirement | level | 80% power point | FP calibrated | FP at 3x band drift |
 * |---|---|---|---|---|
 * | none (magnitude only)   |   --  | 1.42x band | 4.88% | 50.97% |
 * | 6/8                     | 0.30  | 1.46x band | 3.68% | 23.96% |
 * | 7/8 (current)           | 0.10  | 1.67x band | 1.48% |  6.53% |
 * | 8/8                     | 0.01  | 2.38x band | 0.24% |  0.83% |
 *
 * The coupling is invisible at the call sites, which is why it is written here. Quoting `1.42x` is
 * correct ONLY if the consistency gate is removed entirely; under any AND rule it understates the
 * detectable effect. Reports must derive the power point from the shipped rule rather than carry a
 * literal, and `comparison.quick-unit.ts` pins the current value so a silent revert fails loudly.
 *
 * The rightmost column is why the gate is a gate and not a diagnostic: the magnitude gate is
 * nonparametric in the SHAPE of the per-pair distribution but not in its SCALE, and bands are
 * persisted and reused, so drift between calibration and comparison is the normal operating
 * condition rather than an edge case. See COMPARISON_STATISTICS.md §4.5.
 */
export const signGateTargetLevel = 0.1;
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
 *
 * Declared at 10% on the grounds that a margin is an ACTION threshold: `unchanged` asserts "we
 * looked, and any real change is below what we would act on". The number therefore has to be the
 * size of change this suite exists to catch, and a transformer regression small enough to sit under
 * 10% is not one anyone would open work for. Set lower, `unchanged` stops being reachable on real
 * hardware and every quiet week reports `inconclusive`, which trains readers to ignore the output.
 *
 * Note this shares no derivation with the 5% in `classifyVariance` (BenchmarkReporter.ts) -- that is
 * a coefficient-of-variation threshold on within-run stability, a different quantity on a different
 * scale. The numerals were a coincidence, not a lineage, and the two must not be reconciled.
 */
export const defaultEquivalenceMarginPercent = 10;

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
      // An unresolvable margin blocks `unchanged`, but it does not block a change verdict -- that
      // is decided against the band above, never against the margin. So the ordinary escalation
      // condition still applies here, and escalating helps twice over: it relaxes unanimity to
      // 14/16, and the band itself tightens at the larger pair count, which can lift the floor
      // back below the margin. Suppressing escalation in this branch would discard a detectable
      // regression precisely in the noisy environments that produced it.
      escalationRecommended: Boolean(
        input.look === 1 && magnitudeGate.passed && signGate && !signGate.passed
      ),
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
        )}% clears the band, but the per-pair directions are split ${
          signGate?.agreeing ?? 0
        }/${signGate?.effectivePairs ?? 0} (requirement ${
          signGate?.requirement.requiredAgreeing ?? "n/a"
        }), so the effect is driven by a few extreme pairs rather than a consistent shift`
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
