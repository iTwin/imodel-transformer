/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Exact binomial machinery for the reported sign diagnostic.
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
