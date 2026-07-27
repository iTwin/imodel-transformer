/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Deterministic pseudo-random source.
 *
 * Every resampling procedure in the comparison layer is seeded, so a report is reproducible from
 * its own sample file. A bootstrap interval that moves when the analysis is re-run is not
 * evidence of anything.
 */
export class SeededRandom {
  private _state: number;

  public constructor(seed: number) {
    if (!Number.isInteger(seed)) throw new Error("Seed must be an integer");
    // Avoid the fixed point at zero.
    this._state = seed >>> 0 || 0x9e3779b9;
  }

  /** Uniform on [0, 1). mulberry32. */
  public next(): number {
    this._state = (this._state + 0x6d2b79f5) >>> 0;
    let value = this._state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  }

  /** Uniform integer on [0, exclusiveUpperBound). */
  public nextIndex(exclusiveUpperBound: number): number {
    return Math.floor(this.next() * exclusiveUpperBound);
  }

  /** A resample of `size` elements drawn from `values` with replacement. */
  public resample(values: readonly number[], size: number): number[] {
    if (values.length === 0)
      throw new Error("Cannot resample an empty collection");
    const drawn: number[] = new Array(size);
    for (let index = 0; index < size; index++)
      drawn[index] = values[this.nextIndex(values.length)];
    return drawn;
  }
}

export const defaultResampleCount = 10_000;
export const defaultSeed = 0x5eed;
