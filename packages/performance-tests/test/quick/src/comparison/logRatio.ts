/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { median } from "../reporting/statistics";
import { binomialPmf, twoSidedSignTestP } from "./binomial";
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

export function validateCollapsedPair(pair: CollapsedPair): void {
  if (!pair || typeof pair !== "object")
    throw new Error("Collapsed pair must be an object");
  if (!Number.isSafeInteger(pair.pair) || pair.pair < 0)
    throw new Error("Pair index must be a non-negative integer");
  if (pair.order !== "AB" && pair.order !== "BA")
    throw new Error(
      `Pair order must be AB or BA, received ${String(pair.order)}`
    );
  assertPositiveDuration(pair.armA, "Collapsed arm A value");
  assertPositiveDuration(pair.armB, "Collapsed arm B value");
  if (!Number.isFinite(pair.logRatio))
    throw new Error("Collapsed pair log ratio must be finite");
  const expected = logRatio(pair.armA, pair.armB);
  const tolerance =
    Number.EPSILON *
    Math.max(1, Math.abs(expected), Math.abs(pair.logRatio)) *
    16;
  if (Math.abs(pair.logRatio - expected) > tolerance)
    throw new Error("Collapsed pair log ratio does not match its arm values");
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
  const value = Math.log(armB) - Math.log(armA);
  const ratio = Math.exp(value);
  if (!Number.isFinite(value) || !Number.isFinite(ratio) || ratio === 0)
    throw new Error("Arm duration ratio must be finite and representable");
  return value;
}

export function collapsePair(observation: PairObservation): CollapsedPair {
  if (!Number.isSafeInteger(observation.pair) || observation.pair < 0)
    throw new Error("Pair index must be a non-negative integer");
  if (observation.order !== "AB" && observation.order !== "BA")
    throw new Error(
      `Pair order must be AB or BA, received ${String(observation.order)}`
    );
  const armA = collapseArmSamples(observation.armASamples);
  const armB = collapseArmSamples(observation.armBSamples);
  const pair = {
    pair: observation.pair,
    order: observation.order,
    armA,
    armB,
    logRatio: logRatio(armA, armB),
  };
  validateCollapsedPair(pair);
  return pair;
}

export function collapsePairs(
  observations: readonly PairObservation[]
): CollapsedPair[] {
  return observations.map(collapsePair);
}

/** Convert a log-scale quantity to a percentage change. */
export function logRatioToPercent(value: number): number {
  if (!Number.isFinite(value)) throw new Error("Log ratio must be finite");
  const ratio = Math.exp(value);
  const percent = (ratio - 1) * 100;
  if (!Number.isFinite(ratio) || ratio === 0 || !Number.isFinite(percent))
    throw new Error("Log ratio cannot be represented as a finite percentage");
  return percent;
}

/** Convert a percentage change back to the log scale. */
export function percentToLogRatio(percent: number): number {
  if (!Number.isFinite(percent) || percent <= -100)
    throw new Error("Percentage change must be finite and greater than -100");
  const value = Math.log1p(percent / 100);
  if (!Number.isFinite(value))
    throw new Error("Percentage change cannot be represented as a log ratio");
  return value;
}

export interface BootstrapInterval {
  readonly lower: number;
  readonly upper: number;
  readonly level: number;
  readonly resamples: number;
  readonly seed: number;
}

export interface MedianConfidenceInterval {
  readonly lower: number;
  readonly upper: number;
  readonly requestedLevel: number;
  readonly coverage: number;
  readonly lowerOrderStatistic: number;
  readonly upperOrderStatistic: number;
}

function assertFiniteValues(values: readonly number[], label: string): void {
  for (const value of values)
    if (!Number.isFinite(value))
      throw new Error(`${label} must contain only finite values`);
}

function assertProbability(value: number, label: string): void {
  if (!Number.isFinite(value) || value <= 0 || value >= 1)
    throw new Error(`${label} must lie strictly between zero and one`);
}

function medianIntervalCoverage(
  observations: number,
  excludedFromEachTail: number
): number {
  let tailProbability = 0;
  for (let count = 0; count <= excludedFromEachTail; count++)
    tailProbability += binomialPmf(observations, count);
  return 1 - 2 * tailProbability;
}

export function minimumObservationsForMedianConfidence(level = 0.95): number {
  assertProbability(level, "Median confidence level");
  for (let observations = 1; observations < 10_000; observations++)
    if (medianIntervalCoverage(observations, 0) >= level) return observations;
  throw new Error(
    `No practical observation count supports confidence ${level}`
  );
}

/**
 * Exact distribution-free confidence interval for the population median.
 *
 * The bounds are observed order statistics, not interpolated percentiles. The widest interval
 * [minimum, maximum] first reaches at least 95% coverage with six independent observations.
 */
export function exactMedianConfidenceInterval(
  logRatios: readonly number[],
  level = 0.95
): MedianConfidenceInterval {
  if (logRatios.length === 0)
    throw new Error("Median confidence interval requires at least one pair");
  assertFiniteValues(logRatios, "Median confidence interval");
  assertProbability(level, "Median confidence level");
  const ordered = [...logRatios].sort((left, right) => left - right);
  let excludedFromEachTail = 0;
  while (
    excludedFromEachTail + 1 < ordered.length / 2 &&
    medianIntervalCoverage(ordered.length, excludedFromEachTail + 1) >= level
  )
    excludedFromEachTail++;
  return {
    lower: ordered[excludedFromEachTail],
    upper: ordered[ordered.length - excludedFromEachTail - 1],
    requestedLevel: level,
    coverage: medianIntervalCoverage(ordered.length, excludedFromEachTail),
    lowerOrderStatistic: excludedFromEachTail + 1,
    upperOrderStatistic: ordered.length - excludedFromEachTail,
  };
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
  assertFiniteValues(logRatios, "Bootstrap input");
  assertProbability(level, "Bootstrap level");
  if (!Number.isSafeInteger(resamples) || resamples < 1)
    throw new Error("Bootstrap resamples must be a positive integer");
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
  assertFiniteValues(logRatios, "Sign summary");
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

export function validateLogRatioAggregate(aggregate: LogRatioAggregate): void {
  if (!aggregate || typeof aggregate !== "object")
    throw new Error("Log-ratio aggregate must be an object");
  if (!Array.isArray(aggregate.logRatios))
    throw new Error("Log-ratio aggregate observations must be an array");
  if (!aggregate.bootstrap || typeof aggregate.bootstrap !== "object")
    throw new Error("Log-ratio aggregate bootstrap must be an object");
  if (!aggregate.signs || typeof aggregate.signs !== "object")
    throw new Error("Log-ratio aggregate sign summary must be an object");
  if (
    !Number.isSafeInteger(aggregate.pairs) ||
    aggregate.pairs < 1 ||
    aggregate.pairs !== aggregate.logRatios.length
  )
    throw new Error(
      "Log-ratio aggregate pair count must match its non-empty observations"
    );
  assertFiniteValues(aggregate.logRatios, "Log-ratio aggregate");
  for (const [label, value] of [
    ["median log ratio", aggregate.medianLogRatio],
    ["mean log ratio", aggregate.meanLogRatio],
    ["percent change", aggregate.percentChange],
    ["geometric mean percent change", aggregate.geometricMeanPercentChange],
    ["bootstrap lower bound", aggregate.bootstrap.lower],
    ["bootstrap upper bound", aggregate.bootstrap.upper],
    ["bootstrap level", aggregate.bootstrap.level],
  ] as const)
    if (!Number.isFinite(value))
      throw new Error(`Aggregate ${label} must be finite`);
  assertProbability(aggregate.bootstrap.level, "Bootstrap level");
  if (
    !Number.isSafeInteger(aggregate.bootstrap.resamples) ||
    aggregate.bootstrap.resamples < 1
  )
    throw new Error("Bootstrap resamples must be a positive integer");
  new SeededRandom(aggregate.bootstrap.seed);
  if (aggregate.bootstrap.lower > aggregate.bootstrap.upper)
    throw new Error("Bootstrap lower bound cannot exceed upper bound");
  const signs = aggregate.signs;
  for (const [label, value] of [
    ["positive signs", signs.positive],
    ["negative signs", signs.negative],
    ["ties", signs.ties],
    ["effective pairs", signs.effectivePairs],
    ["agreeing signs", signs.agreeing],
  ] as const)
    if (!Number.isSafeInteger(value) || value < 0)
      throw new Error(`Aggregate ${label} must be a non-negative integer`);
  if (
    signs.positive + signs.negative + signs.ties !== aggregate.pairs ||
    signs.positive + signs.negative !== signs.effectivePairs ||
    Math.max(signs.positive, signs.negative) !== signs.agreeing ||
    !Number.isFinite(signs.exactP) ||
    signs.exactP < 0 ||
    signs.exactP > 1
  )
    throw new Error("Aggregate sign summary is inconsistent");
}

export function aggregateLogRatios(
  logRatios: readonly number[],
  options: { level?: number; resamples?: number; seed?: number } = {}
): LogRatioAggregate {
  if (logRatios.length === 0)
    throw new Error("Cannot aggregate an empty set of pairs");
  assertFiniteValues(logRatios, "Log-ratio aggregate");
  const medianLogRatio = median(logRatios);
  const meanLogRatio = logRatios.reduce(
    (sum, value) => sum + value / logRatios.length,
    0
  );
  if (!Number.isFinite(meanLogRatio))
    throw new Error("Mean log ratio must be finite");
  const aggregate = {
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
  validateLogRatioAggregate(aggregate);
  return aggregate;
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
