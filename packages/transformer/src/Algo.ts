/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

// FIXME: tests
/** given a discrete inclusive range [start, end] e.g. [-10, 12] and several "skipped" values", e.g.
 * (-10, 1, -3, 5, 15), return the ordered set of subranges of the original range that exclude
 * those values
 */
export function rangesFromRangeAndSkipped(start: number, end: number, skipped: number[]): [number, number][] {
  function validRange(range: [number,number]): boolean {
    return range[0] < range[1];
  }

  const firstRange = [start, end] as [number, number];

  if (!validRange(firstRange))
    throw RangeError(`invalid range: [${start}, ${end}]`);

  const ranges = [firstRange];

  function findRangeContaining(pt: number, inRanges: [number, number][]): number {
    // TODO: binary search
    return inRanges.findIndex((r) => (pt >= r[0] && pt <= r[1]));
  }

  for (const skip of skipped) {
    const rangeIndex = findRangeContaining(skip, ranges);
    if (rangeIndex === -1)
      continue;
    const range = ranges[rangeIndex];
    const leftRange = [range[0], skip - 1] as [number, number];
    const rightRange = [skip + 1, range[1]] as [number, number];
    if (validRange(leftRange) && validRange(rightRange))
      ranges.splice(rangeIndex, 1, leftRange, rightRange);
    else if (validRange(leftRange))
      ranges.splice(rangeIndex, 1, leftRange);
    else if (validRange(rightRange))
      ranges.splice(rangeIndex, 1, rightRange);
  }

  return ranges;
}

export function renderRanges(ranges: [number, number][]): number[] {
  const result = [];
  for (const range of ranges)
    for (let i = range[0]; i <= range[1]; ++i)
      result.push(i);
}

