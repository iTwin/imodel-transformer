/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as crypto from "crypto";
import { median, percentile } from "../validation/statistics";
import {
  assertExecutionFingerprintMatches,
  ExecutionFingerprint,
  executionFingerprintKey,
} from "./ExecutionFingerprint";
import { logRatioToPercent } from "./logRatio";
import {
  defaultResampleCount,
  defaultSeed,
  SeededRandom,
} from "./SeededRandom";

export type NoiseBandKind = "paired" | "unpaired";
export type NoiseBandStatus = "established" | "provisional" | "uncalibrated";
export type CalibrationQuality =
  | "target"
  | "marginal"
  | "unresolvable"
  | "uncalibrated";

export const targetNoiseBandPercent = 5;
export const meaningfulRegressionPercent = 10;
export const bandQuantile = 0.95;

export interface CalibrationRequirements {
  readonly provisionalIndependentJobs: number;
  readonly establishedIndependentJobs: number;
  readonly provisionalObservations: number;
  readonly establishedObservations: number;
}

/**
 * Starting requirements, deliberately expressed as configuration rather than pair policy.
 * Calibration is expected to accumulate across independent CI jobs.
 */
export const defaultCalibrationRequirements: CalibrationRequirements = {
  provisionalIndependentJobs: 1,
  establishedIndependentJobs: 3,
  provisionalObservations: 1,
  establishedObservations: 3,
};

export interface NoiseBandKey {
  readonly scenarioId: string;
  readonly fixtureId: string;
  readonly recipeHash: string;
  readonly environmentClass: string;
  readonly execution: ExecutionFingerprint;
  readonly kind: NoiseBandKind;
}

export function noiseBandKey(key: NoiseBandKey): string {
  const executionHash = crypto
    .createHash("sha256")
    .update(executionFingerprintKey(key.execution))
    .digest("hex")
    .slice(0, 16);
  return [
    key.scenarioId,
    key.fixtureId,
    key.recipeHash,
    key.environmentClass,
    executionHash,
    key.kind,
  ]
    .map(encodeURIComponent)
    .join("/");
}

/** Serializable A/A observations for one exact calibration identity. */
export interface NoiseBandPool extends NoiseBandKey {
  readonly observations: readonly number[];
  readonly independentJobs: number;
  readonly updatedAt: string;
}

export interface NoiseBand {
  readonly key: NoiseBandKey;
  readonly kind: NoiseBandKind;
  readonly status: NoiseBandStatus;
  readonly quality: CalibrationQuality;
  readonly observations: number;
  readonly independentJobs: number;
  readonly statisticSampleSize: number;
  readonly band: number;
  readonly bandPercent: number;
  readonly individualObservation95: number;
  readonly individualObservation95Percent: number;
  readonly observedMaximum: number;
  readonly observedMaximumPercent: number;
}

export function classifyBandStatus(
  observations: number,
  independentJobs: number,
  requirements: CalibrationRequirements = defaultCalibrationRequirements
): NoiseBandStatus {
  if (
    observations >= requirements.establishedObservations &&
    independentJobs >= requirements.establishedIndependentJobs
  )
    return "established";
  if (
    observations >= requirements.provisionalObservations &&
    independentJobs >= requirements.provisionalIndependentJobs
  )
    return "provisional";
  return "uncalibrated";
}

export function classifyCalibrationQuality(
  status: NoiseBandStatus,
  bandPercent: number
): CalibrationQuality {
  if (status !== "established") return "uncalibrated";
  const tolerance = Number.EPSILON * Math.max(1, Math.abs(bandPercent)) * 16;
  if (bandPercent <= targetNoiseBandPercent + tolerance) return "target";
  if (bandPercent < meaningfulRegressionPercent) return "marginal";
  return "unresolvable";
}

export function medianNullThreshold(
  observations: readonly number[],
  statisticSampleSize: number,
  quantile = bandQuantile,
  resamples = defaultResampleCount,
  seed = defaultSeed
): number {
  if (observations.length === 0)
    throw new Error("Cannot derive a band from an empty pool");
  if (!Number.isInteger(statisticSampleSize) || statisticSampleSize < 1)
    throw new Error("Band derivation requires at least one observation");
  const random = new SeededRandom(seed);
  const magnitudes = new Array<number>(resamples);
  for (let index = 0; index < resamples; index++)
    magnitudes[index] = Math.abs(
      median(random.resample(observations, statisticSampleSize))
    );
  return percentile(magnitudes, quantile);
}

export function deriveNoiseBand(
  pool: NoiseBandPool,
  statisticSampleSize: number,
  options: {
    readonly resamples?: number;
    readonly seed?: number;
    readonly requirements?: CalibrationRequirements;
  } = {}
): NoiseBand {
  const expectedKind =
    pool.execution.pairPolicy.kind === "paired" ? "paired" : "unpaired";
  if (pool.kind !== expectedKind)
    throw new Error(
      `Noise band kind ${pool.kind} does not match ${pool.execution.pairPolicy.kind} execution policy`
    );
  const status = classifyBandStatus(
    pool.observations.length,
    pool.independentJobs,
    options.requirements
  );
  const magnitudes = pool.observations.map(Math.abs);
  const band = medianNullThreshold(
    pool.observations,
    statisticSampleSize,
    bandQuantile,
    options.resamples,
    options.seed
  );
  const bandPercent = logRatioToPercent(band);
  const individualObservation95 = percentile(magnitudes, bandQuantile);
  const observedMaximum = Math.max(...magnitudes);
  return {
    key: {
      scenarioId: pool.scenarioId,
      fixtureId: pool.fixtureId,
      recipeHash: pool.recipeHash,
      environmentClass: pool.environmentClass,
      execution: pool.execution,
      kind: pool.kind,
    },
    kind: pool.kind,
    status,
    quality: classifyCalibrationQuality(status, bandPercent),
    observations: pool.observations.length,
    independentJobs: pool.independentJobs,
    statisticSampleSize,
    band,
    bandPercent,
    individualObservation95,
    individualObservation95Percent: logRatioToPercent(individualObservation95),
    observedMaximum,
    observedMaximumPercent: logRatioToPercent(observedMaximum),
  };
}

export function assertBandApplies(
  band: NoiseBand,
  expected: NoiseBandKey,
  statisticSampleSize: number
): void {
  const syntheticPool: NoiseBandPool = {
    ...band.key,
    observations: [0],
    independentJobs: 1,
    updatedAt: "",
  };
  assertPoolApplies(syntheticPool, expected);
  if (band.statisticSampleSize !== statisticSampleSize)
    throw new Error(
      `Noise band statistic sample size ${band.statisticSampleSize} != ${statisticSampleSize}`
    );
}

export function assertPoolApplies(
  pool: NoiseBandPool,
  expected: NoiseBandKey
): void {
  const mismatches: string[] = [];
  if (pool.environmentClass !== expected.environmentClass)
    mismatches.push(
      `environment class ${pool.environmentClass} != ${expected.environmentClass}`
    );
  if (pool.scenarioId !== expected.scenarioId)
    mismatches.push(`scenario ${pool.scenarioId} != ${expected.scenarioId}`);
  if (pool.fixtureId !== expected.fixtureId)
    mismatches.push(`fixture ${pool.fixtureId} != ${expected.fixtureId}`);
  if (pool.recipeHash !== expected.recipeHash)
    mismatches.push(`recipe hash ${pool.recipeHash} != ${expected.recipeHash}`);
  if (pool.kind !== expected.kind)
    mismatches.push(`band kind ${pool.kind} != ${expected.kind}`);
  try {
    assertExecutionFingerprintMatches(pool.execution, expected.execution);
  } catch (error) {
    mismatches.push((error as Error).message);
  }
  if (mismatches.length > 0)
    throw new Error(
      `Noise band pool does not apply to this comparison: ${mismatches.join(
        "; "
      )}`
    );
}

export function powerPoint(
  observations: readonly number[],
  statisticSampleSize: number,
  band: number,
  targetPower: number,
  options: { readonly resamples?: number; readonly seed?: number } = {}
): number {
  const resamples = options.resamples ?? 2_000;
  const seed = options.seed ?? defaultSeed;
  const powerAt = (shift: number): number => {
    const random = new SeededRandom(seed);
    let detected = 0;
    for (let index = 0; index < resamples; index++) {
      const drawn = random
        .resample(observations, statisticSampleSize)
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
