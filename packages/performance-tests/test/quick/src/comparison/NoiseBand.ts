/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as crypto from "node:crypto";
import { median, percentile } from "../reporting/statistics.js";
import {
  assertExecutionFingerprintMatches,
  ExecutionFingerprint,
  executionFingerprintKey,
  validateExecutionFingerprint,
} from "./ExecutionFingerprint.js";
import { logRatioToPercent } from "./logRatio.js";
import {
  defaultResampleCount,
  defaultSeed,
  SeededRandom,
} from "./SeededRandom.js";

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
  readonly provisionalObservations: number;
  readonly establishedIndependentJobs?: number;
  readonly establishedObservations?: number;
}

/**
 * Calibration starts provisionally. Establishment thresholds must come from an explicit,
 * empirically justified policy rather than a small arbitrary default.
 */
export const defaultCalibrationRequirements: CalibrationRequirements = {
  provisionalIndependentJobs: 1,
  provisionalObservations: 1,
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

export interface NoiseBandDerivation {
  readonly poolDigest: string;
  readonly quantile: number;
  readonly resamples: number;
  readonly seed: number;
  readonly requirements: CalibrationRequirements;
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
  readonly derivation: NoiseBandDerivation;
}

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(`${label} must be a positive integer`);
}

function assertNonNegativeInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error(`${label} must be a non-negative integer`);
}

function assertProbability(value: number, label: string): void {
  if (!Number.isFinite(value) || value <= 0 || value >= 1)
    throw new Error(`${label} must lie strictly between zero and one`);
}

function assertNonEmpty(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0)
    throw new Error(`${label} cannot be empty`);
}

export function validateCalibrationRequirements(
  requirements: CalibrationRequirements
): void {
  assertPositiveInteger(
    requirements.provisionalIndependentJobs,
    "Provisional independent jobs"
  );
  assertPositiveInteger(
    requirements.provisionalObservations,
    "Provisional observations"
  );
  const establishedJobs = requirements.establishedIndependentJobs;
  const establishedObservations = requirements.establishedObservations;
  const hasEstablishedJobs = establishedJobs !== undefined;
  const hasEstablishedObservations = establishedObservations !== undefined;
  if (hasEstablishedJobs !== hasEstablishedObservations)
    throw new Error(
      "Established calibration requires both independent-job and observation thresholds"
    );
  if (hasEstablishedJobs && hasEstablishedObservations) {
    assertPositiveInteger(establishedJobs, "Established independent jobs");
    assertPositiveInteger(establishedObservations, "Established observations");
    if (
      establishedJobs < requirements.provisionalIndependentJobs ||
      establishedObservations < requirements.provisionalObservations
    )
      throw new Error(
        "Established calibration thresholds cannot be below provisional thresholds"
      );
  }
}

function canonicalRequirements(
  requirements: CalibrationRequirements
): Record<string, number | null> {
  return {
    provisionalIndependentJobs: requirements.provisionalIndependentJobs,
    provisionalObservations: requirements.provisionalObservations,
    establishedIndependentJobs: requirements.establishedIndependentJobs ?? null,
    establishedObservations: requirements.establishedObservations ?? null,
  };
}

export function validateNoiseBandPool(pool: NoiseBandPool): void {
  if (!pool || typeof pool !== "object")
    throw new Error("Noise band pool must be an object");
  if (!Array.isArray(pool.observations))
    throw new Error("Noise band pool observations must be an array");
  assertNonEmpty(pool.scenarioId, "Scenario id");
  assertNonEmpty(pool.fixtureId, "Fixture id");
  assertNonEmpty(pool.recipeHash, "Recipe hash");
  assertNonEmpty(pool.environmentClass, "Environment class");
  validateExecutionFingerprint(pool.execution);
  const expectedKind =
    pool.execution.pairPolicy.kind === "paired" ? "paired" : "unpaired";
  if (pool.kind !== expectedKind)
    throw new Error(
      `Noise band kind ${pool.kind} does not match ${pool.execution.pairPolicy.kind} execution policy`
    );
  if (pool.observations.length === 0)
    throw new Error("Noise band pool requires at least one observation");
  for (const observation of pool.observations)
    if (!Number.isFinite(observation))
      throw new Error("Noise band observations must be finite");
  assertPositiveInteger(pool.independentJobs, "Calibration independent jobs");
  if (
    typeof pool.updatedAt !== "string" ||
    !Number.isFinite(Date.parse(pool.updatedAt))
  )
    throw new Error("Noise band pool updatedAt must be a valid timestamp");
}

export function noiseBandPoolDigest(pool: NoiseBandPool): string {
  validateNoiseBandPool(pool);
  return crypto
    .createHash("sha256")
    .update(
      JSON.stringify({
        scenarioId: pool.scenarioId,
        fixtureId: pool.fixtureId,
        recipeHash: pool.recipeHash,
        environmentClass: pool.environmentClass,
        execution: executionFingerprintKey(pool.execution),
        kind: pool.kind,
        observations: pool.observations,
        independentJobs: pool.independentJobs,
      })
    )
    .digest("hex");
}

export function classifyBandStatus(
  observations: number,
  independentJobs: number,
  requirements: CalibrationRequirements = defaultCalibrationRequirements
): NoiseBandStatus {
  assertNonNegativeInteger(observations, "Calibration observations");
  assertNonNegativeInteger(independentJobs, "Calibration independent jobs");
  validateCalibrationRequirements(requirements);
  if (
    requirements.establishedObservations !== undefined &&
    requirements.establishedIndependentJobs !== undefined &&
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
  if (!Number.isFinite(bandPercent) || bandPercent < 0)
    throw new Error("Noise band percentage must be finite and non-negative");
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
  for (const observation of observations)
    if (!Number.isFinite(observation))
      throw new Error("Noise band observations must be finite");
  if (!Number.isInteger(statisticSampleSize) || statisticSampleSize < 1)
    throw new Error("Band derivation requires at least one observation");
  assertProbability(quantile, "Noise band quantile");
  assertPositiveInteger(resamples, "Noise band resamples");
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
  validateNoiseBandPool(pool);
  assertPositiveInteger(statisticSampleSize, "Band statistic sample size");
  const quantile = bandQuantile;
  const resamples = options.resamples ?? defaultResampleCount;
  const seed = options.seed ?? defaultSeed;
  const requirements = options.requirements ?? defaultCalibrationRequirements;
  assertProbability(quantile, "Noise band quantile");
  assertPositiveInteger(resamples, "Noise band resamples");
  new SeededRandom(seed);
  validateCalibrationRequirements(requirements);
  const status = classifyBandStatus(
    pool.observations.length,
    pool.independentJobs,
    requirements
  );
  const magnitudes = pool.observations.map(Math.abs);
  const band = medianNullThreshold(
    pool.observations,
    statisticSampleSize,
    quantile,
    resamples,
    seed
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
    derivation: {
      poolDigest: noiseBandPoolDigest(pool),
      quantile,
      resamples,
      seed,
      requirements,
    },
  };
}

function canonicalBand(band: NoiseBand): string {
  return JSON.stringify({
    key: noiseBandKey(band.key),
    kind: band.kind,
    status: band.status,
    quality: band.quality,
    observations: band.observations,
    independentJobs: band.independentJobs,
    statisticSampleSize: band.statisticSampleSize,
    band: band.band,
    bandPercent: band.bandPercent,
    individualObservation95: band.individualObservation95,
    individualObservation95Percent: band.individualObservation95Percent,
    observedMaximum: band.observedMaximum,
    observedMaximumPercent: band.observedMaximumPercent,
    derivation: {
      poolDigest: band.derivation.poolDigest,
      quantile: band.derivation.quantile,
      resamples: band.derivation.resamples,
      seed: band.derivation.seed,
      requirements: canonicalRequirements(band.derivation.requirements),
    },
  });
}

export function validateNoiseBand(band: NoiseBand): void {
  if (!band || typeof band !== "object")
    throw new Error("Noise band must be an object");
  if (!band.key || typeof band.key !== "object")
    throw new Error("Noise band key must be an object");
  if (!band.derivation || typeof band.derivation !== "object")
    throw new Error("Noise band derivation must be an object");
  assertNonEmpty(band.key.scenarioId, "Noise band scenario id");
  assertNonEmpty(band.key.fixtureId, "Noise band fixture id");
  assertNonEmpty(band.key.recipeHash, "Noise band recipe hash");
  assertNonEmpty(band.key.environmentClass, "Noise band environment class");
  validateExecutionFingerprint(band.key.execution);
  if (band.kind !== band.key.kind)
    throw new Error("Noise band kind must match its calibration key");
  const expectedKind =
    band.key.execution.pairPolicy.kind === "paired" ? "paired" : "unpaired";
  if (band.kind !== expectedKind)
    throw new Error("Noise band kind must match its execution policy");
  if (!["established", "provisional", "uncalibrated"].includes(band.status))
    throw new Error("Unknown noise band status");
  if (
    !["target", "marginal", "unresolvable", "uncalibrated"].includes(
      band.quality
    )
  )
    throw new Error("Unknown calibration quality");
  if (!/^[a-f0-9]{64}$/i.test(band.derivation.poolDigest))
    throw new Error("Noise band pool digest must be a SHA-256 hex digest");
  assertPositiveInteger(band.observations, "Noise band observations");
  assertPositiveInteger(band.independentJobs, "Noise band independent jobs");
  assertPositiveInteger(
    band.statisticSampleSize,
    "Noise band statistic sample size"
  );
  assertProbability(band.derivation.quantile, "Noise band quantile");
  if (band.derivation.quantile !== bandQuantile)
    throw new Error(`Noise band quantile must be ${bandQuantile}`);
  assertPositiveInteger(band.derivation.resamples, "Noise band resamples");
  new SeededRandom(band.derivation.seed);
  validateCalibrationRequirements(band.derivation.requirements);
  const expectedStatus = classifyBandStatus(
    band.observations,
    band.independentJobs,
    band.derivation.requirements
  );
  if (band.status !== expectedStatus)
    throw new Error(
      "Noise band status does not match its derivation requirements"
    );
  if (
    band.quality !== classifyCalibrationQuality(band.status, band.bandPercent)
  )
    throw new Error(
      "Noise band quality does not match its status and magnitude"
    );
  for (const [label, value] of [
    ["band", band.band],
    ["band percent", band.bandPercent],
    ["individual observation percentile", band.individualObservation95],
    [
      "individual observation percentile percent",
      band.individualObservation95Percent,
    ],
    ["observed maximum", band.observedMaximum],
    ["observed maximum percent", band.observedMaximumPercent],
  ] as const)
    if (!Number.isFinite(value) || value < 0)
      throw new Error(`Noise ${label} must be finite and non-negative`);
  for (const [value, expected, label] of [
    [band.bandPercent, logRatioToPercent(band.band), "band percentage"],
    [
      band.individualObservation95Percent,
      logRatioToPercent(band.individualObservation95),
      "individual observation percentage",
    ],
    [
      band.observedMaximumPercent,
      logRatioToPercent(band.observedMaximum),
      "observed maximum percentage",
    ],
  ] as const) {
    const tolerance = Number.EPSILON * Math.max(1, Math.abs(expected)) * 32;
    if (Math.abs(value - expected) > tolerance)
      throw new Error(`Noise ${label} does not match its log-ratio value`);
  }
  if (band.individualObservation95 > band.observedMaximum)
    throw new Error(
      "Noise individual-observation percentile cannot exceed the observed maximum"
    );
}

export function parseNoiseBandPoolJson(json: string): NoiseBandPool {
  const value = JSON.parse(json) as NoiseBandPool;
  validateNoiseBandPool(value);
  return value;
}

export function parseNoiseBandJson(json: string): NoiseBand {
  const value = JSON.parse(json) as NoiseBand;
  validateNoiseBand(value);
  return value;
}

export function assertBandDerivedFromPool(
  band: NoiseBand,
  pool: NoiseBandPool
): void {
  validateNoiseBand(band);
  validateNoiseBandPool(pool);
  if (band.derivation.poolDigest !== noiseBandPoolDigest(pool))
    throw new Error("Noise band was not derived from the supplied pool");
  const expected = deriveNoiseBand(pool, band.statisticSampleSize, {
    resamples: band.derivation.resamples,
    seed: band.derivation.seed,
    requirements: band.derivation.requirements,
  });
  if (canonicalBand(band) !== canonicalBand(expected))
    throw new Error(
      "Noise band values do not match the supplied pool and derivation settings"
    );
}

export function assertBandApplies(
  band: NoiseBand,
  expected: NoiseBandKey,
  statisticSampleSize: number
): void {
  validateNoiseBand(band);
  const syntheticPool: NoiseBandPool = {
    ...band.key,
    observations: [0],
    independentJobs: 1,
    updatedAt: new Date(0).toISOString(),
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
  validateNoiseBandPool(pool);
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
