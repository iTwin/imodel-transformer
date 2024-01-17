/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { rangesFromRangeAndSkipped } from "../../Algo";
import { expect } from "chai";
/** given a discrete inclusive range [start, end] e.g. [-10, 12] and several "skipped" values", e.g.
 * (-10, 1, -3, 5, 15), return the ordered set of subranges of the original range that exclude
 * those values
 */
// function rangesFromRangeAndSkipped(start: number, end: number, skipped: number[]): [number, number][]

describe("Test rangesFromRangeAndSkipped", async () => {
  it("should return proper ranges with skipped at beginning of range", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [-10, 1, -3, 5, 15]);
    expect(ranges).to.eql([
      [-9, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two consecutive values skipped at beginning of range ascending order", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [-10, -9, 1, -3, 5, 15]);
    expect(ranges).to.eql([
      [-8, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two consecutive values skipped at beginning of range descending order", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, -8, [-9, -10]);
    expect(ranges).to.eql([[-8, -8]]);
  });

  it("should return proper ranges with two consecutive values skipped at beginning of range descending order and more skips", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [-9, -10, 1, -3, 5, 15]);
    expect(ranges).to.eql([
      [-8, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two consecutive values skipped at beginning of range but at the end of the skipped array", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [1, -3, 5, -9, -10, 15]);
    expect(ranges).to.eql([
      [-8, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two consecutive values skipped at beginning of range but the two values are duplicated in middle and end of skipped array", async () => {
    const ranges = rangesFromRangeAndSkipped(
      -10,
      12,
      [1, -3, 5, -9, -10, 15, -9, -10]
    );
    expect(ranges).to.eql([
      [-8, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two non-consecutive values skipped at beginning of range", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [-8, -10, 1, -3, 5, 15]);
    expect(ranges).to.eql([
      [-9, -9],
      [-7, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two consecutive values skipped at middle of range", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [-10, 1, -2, -3, 5, 15]);
    expect(ranges).to.eql([
      [-9, -4],
      [-1, 0],
      [2, 4],
      [6, 12],
    ]);
  });

  it("should return proper ranges with two consecutive values skipped at end of range ascending order", async () => {
    const ranges = rangesFromRangeAndSkipped(8, 10, [9, 10]);
    expect(ranges).to.eql([[8, 8]]);
  });

  it("should return proper ranges with two consecutive values skipped at end of range descending order", async () => {
    const ranges = rangesFromRangeAndSkipped(8, 10, [10, 9]);
    expect(ranges).to.eql([[8, 8]]);
  });

  it("should return proper ranges with skipped array being sorted in descending order", async () => {
    const ranges = rangesFromRangeAndSkipped(-10, 12, [15, 5, 1, -3, -9, -10]);
    expect(ranges).to.eql([
      [-8, -4],
      [-2, 0],
      [2, 4],
      [6, 12],
    ]);
  });
});
