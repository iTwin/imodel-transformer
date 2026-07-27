/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { median, percentile } from "../validation/statistics";
import { logRatioToPercent } from "./logRatio";
import {
  defaultResampleCount,
  defaultSeed,
  SeededRandom,
} from "./SeededRandom";

/**
 * A/A noise calibration.
 *
 * Running the comparison with identical code in both arms makes the true log-ratio exactly zero,
 * so the observed spread IS the noise floor for that environment, measured on real hardware with
 * the real process structure.
 *
 * The gate compares `median(d)` over P pairs, so the null it must be compared against is the
 * distribution of `median(d)` -- NOT the distribution of an individual pair's `d`. Those live on
 * different scales: the median's spread shrinks with P, an individual-pair quantile does not
 * shrink at all. Using the individual-pair quantile as the gate throws away most of the
 * resolution the pairs already paid for.
 *
 * Because the median-null depends on P, a band is DERIVED for a given pair count rather than
 * stored as a single number. Escalating from 8 to 16 pairs legitimately tightens the band, and a
 * band frozen at P = 8 would silently under-detect at P = 16.
 */

export type NoiseBandKind = "paired" | "unpaired";
export type NoiseBandStatus = "established" | "provisional" | "uncalibrated";

/** Minimum accumulated pairs before any band may be derived. */
export const provisionalBandMinimumPairs = 16;
/** Minimum accumulated pairs for an established band. */
export const establishedBandMinimumPairs = 24;
/** Minimum distinct A/A runs for an established band. */
export const establishedBandMinimumRuns = 3;
/** Minimum distinct A/A runs for a provisional band. */
export const provisionalBandMinimumRuns = 2;
/** Quantile of the null distribution used as the band. */
export const bandQuantile = 0.95;

/**
 * Accumulated A/A observations for one environment class.
 *
 * The raw per-pair log-ratios are retained rather than a summary, because the band must be
 * re-derived for whichever pair count is in play.
 */
export interface NoiseBandPool {
  readonly environmentClass: string;
  readonly scenarioId: string;
  readonly kind: NoiseBandKind;
  /**
   * Samples per arm per pair the pool was collected under. A band describes a whole process
   * structure, not an estimator, so a pool collected at a different `k` does not apply.
   */
  readonly samplesPerArmPerPair: number;
  /** Per-pair log-ratios from A/A runs, in collection order. */
  readonly observations: readonly number[];
  /** Number of distinct A/A runs contributing, needed to capture between-run drift. */
  readonly runs: number;
  readonly updatedAt: string;
}

export interface NoiseBand {
  readonly kind: NoiseBandKind;
  readonly status: NoiseBandStatus;
  /** Pair count this band was derived for. */
  readonly pairs: number;
  /**
   * Gate threshold on the log scale: the 95th percentile of `|median(d)|` under the A/A null,
   * resampled at this pair count.
   */
  readonly band: number;
  readonly bandPercent: number;
  /**
   * 95th percentile of `|d|` for INDIVIDUAL pairs. A useful diagnostic reported next to the
   * observed maximum -- it is not the gate.
   */
  readonly individualPair95: number;
  readonly individualPair95Percent: number;
  readonly observedMaximum: number;
  readonly observedMaximumPercent: number;
  readonly pairsAccumulated: number;
  readonly runsAccumulated: number;
  readonly samplesPerArmPerPair: number;
}

export function classifyBandStatus(
  pairsAccumulated: number,
  runsAccumulated: number
): NoiseBandStatus {
  if (
    pairsAccumulated >= establishedBandMinimumPairs &&
    runsAccumulated >= establishedBandMinimumRuns
  )
    return "established";
  if (
    pairsAccumulated >= provisionalBandMinimumPairs &&
    runsAccumulated >= provisionalBandMinimumRuns
  )
    return "provisional";
  return "uncalibrated";
}

/**
 * Distribution of `|median(d)|` under the A/A null at a given pair count, by resampling the
 * accumulated pool with replacement.
 *
 * The pool is used uncentred. A systematic A/A offset is an ordering or warm-up defect and is
 * caught by the validity checks; absorbing it into a wider band would hide the defect.
 */
export function medianNullThreshold(
  observations: readonly number[],
  pairs: number,
  quantile = bandQuantile,
  resamples = defaultResampleCount,
  seed = defaultSeed
): number {
  if (observations.length === 0)
    throw new Error("Cannot derive a band from an empty pool");
  if (!Number.isInteger(pairs) || pairs < 1)
    throw new Error("Band derivation requires at least one pair");
  const random = new SeededRandom(seed);
  const magnitudes = new Array<number>(resamples);
  for (let index = 0; index < resamples; index++)
    magnitudes[index] = Math.abs(median(random.resample(observations, pairs)));
  return percentile(magnitudes, quantile);
}

/** Derive the band applicable to `pairs` pairs from an accumulated pool. */
export function deriveNoiseBand(
  pool: NoiseBandPool,
  pairs: number,
  options: { resamples?: number; seed?: number } = {}
): NoiseBand {
  const status = classifyBandStatus(pool.observations.length, pool.runs);
  const magnitudes = pool.observations.map(Math.abs);
  const band = medianNullThreshold(
    pool.observations,
    pairs,
    bandQuantile,
    options.resamples,
    options.seed
  );
  const individualPair95 = percentile(magnitudes, bandQuantile);
  const observedMaximum = Math.max(...magnitudes);
  return {
    kind: pool.kind,
    status,
    pairs,
    band,
    bandPercent: logRatioToPercent(band),
    individualPair95,
    individualPair95Percent: logRatioToPercent(individualPair95),
    observedMaximum,
    observedMaximumPercent: logRatioToPercent(observedMaximum),
    pairsAccumulated: pool.observations.length,
    runsAccumulated: pool.runs,
    samplesPerArmPerPair: pool.samplesPerArmPerPair,
  };
}

/**
 * Reject a pool that does not describe the process structure actually in use.
 *
 * A band is a property of the whole structure. Silently reusing one collected at a different `k`,
 * scenario or environment would produce a confidently wrong threshold.
 */
export function assertPoolApplies(
  pool: NoiseBandPool,
  expected: {
    environmentClass: string;
    scenarioId: string;
    samplesPerArmPerPair: number;
    kind: NoiseBandKind;
  }
): void {
  const mismatches: string[] = [];
  if (pool.environmentClass !== expected.environmentClass)
    mismatches.push(
      `environment class ${pool.environmentClass} != ${expected.environmentClass}`
    );
  if (pool.scenarioId !== expected.scenarioId)
    mismatches.push(`scenario ${pool.scenarioId} != ${expected.scenarioId}`);
  if (pool.samplesPerArmPerPair !== expected.samplesPerArmPerPair)
    mismatches.push(
      `samples per arm per pair ${pool.samplesPerArmPerPair} != ${expected.samplesPerArmPerPair}`
    );
  if (pool.kind !== expected.kind)
    mismatches.push(`band kind ${pool.kind} != ${expected.kind}`);
  if (mismatches.length > 0)
    throw new Error(
      `Noise band pool does not apply to this comparison: ${mismatches.join(
        "; "
      )}`
    );
}

/**
 * Effect size, on the log scale, at which the magnitude gate alone reaches `targetPower`.
 *
 * The band itself is roughly the 50%-power point, because at an effect exactly equal to the
 * threshold the estimator lands above it about half the time. Reporting the band as "the MDE"
 * invites reading it as "regressions this size are caught", which overstates it by about a factor
 * of two in detection rate.
 */
export function powerPoint(
  observations: readonly number[],
  pairs: number,
  band: number,
  targetPower: number,
  options: { resamples?: number; seed?: number } = {}
): number {
  const resamples = options.resamples ?? 2_000;
  const seed = options.seed ?? defaultSeed;
  const powerAt = (shift: number): number => {
    const random = new SeededRandom(seed);
    let detected = 0;
    for (let index = 0; index < resamples; index++) {
      const drawn = random
        .resample(observations, pairs)
        .map((value) => value + shift);
      if (Math.abs(median(drawn)) > band) detected++;
    }
    return detected / resamples;
  };
  let low = 0;
  let high = Math.max(band * 8, Number.EPSILON);
  for (let iteration = 0; iteration < 40; iteration++) {
    const mid = (low + high) / 2;
    if (powerAt(mid) < targetPower) low = mid;
    else high = mid;
  }
  return high;
}
