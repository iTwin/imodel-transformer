/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Exact binomial machinery for the sign test.
 *
 * Gates are expressed in COUNTS, never by comparing against a decimal transcription of a
 * p-value. `2 * 0.5 ** 8` is `0.0078125`, which is not `<= 0.0078`; a threshold written that way
 * makes the gate unreachable at every effect size. Counts are derived from a declared target
 * level here, once, and the achieved level is carried alongside for reporting only.
 */

/** Probability mass of `Binomial(trials, 0.5)` at `successes`, by stable multiplicative recurrence. */
export function binomialPmf(trials: number, successes: number): number {
  if (!Number.isInteger(trials) || trials < 0)
    throw new Error("Binomial trials must be a non-negative integer");
  if (!Number.isInteger(successes) || successes < 0 || successes > trials)
    throw new Error(
      "Binomial successes must be within zero and the trial count"
    );
  let mass = 0.5 ** trials;
  for (let index = 0; index < successes; index++)
    mass = (mass * (trials - index)) / (index + 1);
  return mass;
}

/**
 * Two-sided exact p-value for a symmetric sign test: the probability of an agreement count at
 * least as extreme as the one observed, in either direction.
 */
export function twoSidedSignTestP(trials: number, successes: number): number {
  if (trials === 0) return 1;
  const extreme = Math.max(successes, trials - successes);
  let tail = 0;
  for (let count = extreme; count <= trials; count++)
    tail += binomialPmf(trials, count);
  return Math.min(1, 2 * tail);
}

export interface SignGateRequirement {
  /** Number of pairs that must agree in sign for the gate to pass. */
  readonly requiredAgreeing: number;
  /** Exact two-sided p at `requiredAgreeing`. Reported, never compared against. */
  readonly achievedLevel: number;
  /** The level that was asked for. */
  readonly targetLevel: number;
  /**
   * True when no achievable count reaches `targetLevel` at this pair count, so the gate can never
   * pass. Surfaced explicitly rather than silently never firing.
   */
  readonly unachievable: boolean;
}

/**
 * Smallest agreement count whose exact two-sided p is at or below `targetLevel`.
 *
 * At eight pairs and a target of 0.01 this returns unanimity (p = 0.0078125); seven of eight is
 * 0.0703125 and does not qualify. At sixteen pairs it returns fourteen (p = 0.0041809).
 */
export function signGateRequirement(
  pairs: number,
  targetLevel: number
): SignGateRequirement {
  if (!Number.isInteger(pairs) || pairs < 1)
    throw new Error("Sign gate requires at least one pair");
  if (targetLevel <= 0 || targetLevel >= 1)
    throw new Error(
      "Sign gate target level must lie strictly between zero and one"
    );
  for (let count = Math.ceil(pairs / 2); count <= pairs; count++) {
    const achievedLevel = twoSidedSignTestP(pairs, count);
    if (achievedLevel <= targetLevel)
      return {
        requiredAgreeing: count,
        achievedLevel,
        targetLevel,
        unachievable: false,
      };
  }
  return {
    requiredAgreeing: pairs + 1,
    achievedLevel: twoSidedSignTestP(pairs, pairs),
    targetLevel,
    unachievable: true,
  };
}
