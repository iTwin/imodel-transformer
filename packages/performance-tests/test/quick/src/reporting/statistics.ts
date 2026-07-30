/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

function sorted(values: readonly number[]): number[] {
  if (values.length === 0) throw new Error("At least one sample is required");
  return [...values].sort((left, right) => left - right);
}

export function percentile(
  values: readonly number[],
  probability: number
): number {
  if (probability < 0 || probability > 1)
    throw new Error("Probability must be between zero and one");
  const ordered = sorted(values);
  const index = (ordered.length - 1) * probability;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower);
}

export function median(values: readonly number[]): number {
  return percentile(values, 0.5);
}

export function medianAbsoluteDeviation(values: readonly number[]): number {
  const center = median(values);
  return median(values.map((value) => Math.abs(value - center)));
}

export function coefficientOfVariation(values: readonly number[]): number {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance) / mean;
}
