/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { median } from "../reporting/statistics";
import { twoSidedSignTestP } from "./binomial";
import {
  defaultResampleCount,
  defaultSeed,
  SeededRandom,
} from "./SeededRandom";

/**
 * Paired log-ratio estimator.
 *
 * The unit of independence is one PAIR: one arm-A process and one arm-B process run adjacent in
 * time. Analysis is on `ln(B / A)` because performance noise is multiplicative -- a slow machine
 * inflates both arms proportionally -- so the log scale makes noise roughly symmetric, is free of
 * the scenario's absolute duration, and converts directly to a percentage.
 *
 * Sign convention: POSITIVE MEANS ARM B IS SLOWER THAN ARM A.
 */

export type PairOrder = "AB" | "BA";

export interface PairObservation {
  readonly pair: number;
  /**
   * Execution order within the pair. Carried as a covariate for the order-effect validity check;
   * it never flips the sign of the log-ratio, which is defined by arm identity alone.
   */
  readonly order: PairOrder;
  /** Measured samples for arm A within this pair, before collapse. */
  readonly armASamples: readonly number[];
  /** Measured samples for arm B within this pair, before collapse. */
  readonly armBSamples: readonly number[];
}

export interface CollapsedPair {
  readonly pair: number;
  readonly order: PairOrder;
  readonly armA: number;
  readonly armB: number;
  readonly logRatio: number;
}

function assertPositiveDuration(value: number, label: string): void {
  if (!Number.isFinite(value) || value <= 0)
    throw new Error(
      `${label} must be a finite positive duration, received ${value}`
    );
}

/**
 * Collapse the within-process samples of one arm to a single value.
 *
 * Median, deliberately: it is robust to a single within-process spike (GC, JIT, page fault),
 * which is the failure mode within-process replication can actually fix.
 */
export function collapseArmSamples(samples: readonly number[]): number {
  if (samples.length === 0)
    throw new Error("An arm requires at least one measured sample");
  for (const sample of samples) assertPositiveDuration(sample, "Arm sample");
  return median(samples);
}

export function logRatio(armA: number, armB: number): number {
  assertPositiveDuration(armA, "Arm A value");
  assertPositiveDuration(armB, "Arm B value");
  return Math.log(armB / armA);
}

export function collapsePair(observation: PairObservation): CollapsedPair {
  const armA = collapseArmSamples(observation.armASamples);
  const armB = collapseArmSamples(observation.armBSamples);
  return {
    pair: observation.pair,
    order: observation.order,
    armA,
    armB,
    logRatio: logRatio(armA, armB),
  };
}

export function collapsePairs(
  observations: readonly PairObservation[]
): CollapsedPair[] {
  return observations.map(collapsePair);
}

/** Convert a log-scale quantity to a percentage change. */
export function logRatioToPercent(value: number): number {
  return (Math.exp(value) - 1) * 100;
}

/** Convert a percentage change back to the log scale. */
export function percentToLogRatio(percent: number): number {
  return Math.log(1 + percent / 100);
}

export interface BootstrapInterval {
  readonly lower: number;
  readonly upper: number;
  readonly level: number;
  readonly resamples: number;
  readonly seed: number;
}

/**
 * Percentile bootstrap of the median log-ratio.
 *
 * Indicative only. At eight pairs the bootstrap distribution of a median is chunky, so this
 * conveys the spread of a single run and participates in the equivalence test, but it never by
 * itself establishes that something changed.
 */
export function bootstrapMedianInterval(
  logRatios: readonly number[],
  level = 0.95,
  resamples = defaultResampleCount,
  seed = defaultSeed
): BootstrapInterval {
  if (logRatios.length === 0)
    throw new Error("Bootstrap requires at least one pair");
  if (level <= 0 || level >= 1)
    throw new Error("Bootstrap level must lie strictly between zero and one");
  const random = new SeededRandom(seed);
  const medians = new Array<number>(resamples);
  for (let index = 0; index < resamples; index++)
    medians[index] = median(random.resample(logRatios, logRatios.length));
  medians.sort((left, right) => left - right);
  const tail = (1 - level) / 2;
  const at = (probability: number) =>
    medians[
      Math.min(
        medians.length - 1,
        Math.max(0, Math.round(probability * (medians.length - 1)))
      )
    ];
  return { lower: at(tail), upper: at(1 - tail), level, resamples, seed };
}

export interface SignSummary {
  /** Pairs whose log-ratio is strictly positive (arm B slower). */
  readonly positive: number;
  /** Pairs whose log-ratio is strictly negative (arm B faster). */
  readonly negative: number;
  /**
   * Pairs whose log-ratio is exactly zero. Ties carry no directional information and are excluded
   * from the test, reducing its effective sample size.
   */
  readonly ties: number;
  /** Effective sample size after excluding ties. */
  readonly effectivePairs: number;
  /** Agreement count on the majority side. */
  readonly agreeing: number;
  /** Exact two-sided p at the observed agreement count. Reported, never used as a gate. */
  readonly exactP: number;
}

export function summarizeSigns(logRatios: readonly number[]): SignSummary {
  const positive = logRatios.filter((value) => value > 0).length;
  const negative = logRatios.filter((value) => value < 0).length;
  const ties = logRatios.length - positive - negative;
  const effectivePairs = positive + negative;
  const agreeing = Math.max(positive, negative);
  return {
    positive,
    negative,
    ties,
    effectivePairs,
    agreeing,
    exactP: twoSidedSignTestP(effectivePairs, agreeing),
  };
}

export interface LogRatioAggregate {
  readonly pairs: number;
  /** Headline point estimate: median of the per-pair log-ratios. */
  readonly medianLogRatio: number;
  /**
   * Mean of the per-pair log-ratios, i.e. the log of the geometric mean ratio. Reported for
   * reference; a large divergence from the median is itself a signal that one pair misbehaved.
   */
  readonly meanLogRatio: number;
  readonly percentChange: number;
  readonly geometricMeanPercentChange: number;
  readonly signs: SignSummary;
  readonly bootstrap: BootstrapInterval;
  readonly logRatios: readonly number[];
}

export function aggregateLogRatios(
  logRatios: readonly number[],
  options: { level?: number; resamples?: number; seed?: number } = {}
): LogRatioAggregate {
  if (logRatios.length === 0)
    throw new Error("Cannot aggregate an empty set of pairs");
  const medianLogRatio = median(logRatios);
  const meanLogRatio =
    logRatios.reduce((sum, value) => sum + value, 0) / logRatios.length;
  return {
    pairs: logRatios.length,
    medianLogRatio,
    meanLogRatio,
    percentChange: logRatioToPercent(medianLogRatio),
    geometricMeanPercentChange: logRatioToPercent(meanLogRatio),
    signs: summarizeSigns(logRatios),
    bootstrap: bootstrapMedianInterval(
      logRatios,
      options.level,
      options.resamples,
      options.seed
    ),
    logRatios: [...logRatios],
  };
}

export function aggregatePairs(
  pairs: readonly CollapsedPair[],
  options: { level?: number; resamples?: number; seed?: number } = {}
): LogRatioAggregate {
  return aggregateLogRatios(
    pairs.map((pair) => pair.logRatio),
    options
  );
}

/**
 * Per-pair `ln(second / first)`, for the order-effect validity check.
 *
 * A systematic order effect is a warm-up or ordering defect in the harness, not noise.
 */
export function orderEffectLogRatios(
  pairs: readonly CollapsedPair[]
): number[] {
  return pairs.map((pair) =>
    pair.order === "AB"
      ? logRatio(pair.armA, pair.armB)
      : logRatio(pair.armB, pair.armA)
  );
}
