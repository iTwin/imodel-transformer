export interface Run {
  from: number;
  to: number;
  length: number;
}

interface IndexInfo {
  /** if not in a run, this is the index where it would be if added */
  index: number;
  /** true if in a run, false if outside one */
  inRun: boolean;
  hasTarget: boolean;
  /** not valid unless @see hasTarget is true
   * only exists when the operation could determine a target and succeeded */
  target: number;
}

/**
 * A compact remap table, a mutable remap table that is dynamically run-length compressed
 * and allow binary searching
 */
export class CompactRemapTable {
  /** an array of inlined @see Run 'from's constituting Runs */
  private _froms: number[] = [];
  /** an array of inlined @see Run 'to's constituting Runs */
  private _tos: number[] = [];
  /** an array of inlined @see Run 'length'es constituting Runs */
  private _lengths: number[] = [];

  private get _size() { return this._froms.length; }

  private _tryMergeLeft(index: number, inFrom: number, inTo: number): boolean {
    const prevIndex = index - 1;
    if (prevIndex < 0)
      return false;

    const prevFrom = this._froms[prevIndex];
    const prevTo = this._tos[prevIndex];
    const prevLength = this._lengths[prevIndex];
    const needsMerge = inFrom === prevFrom + prevLength && inTo === prevTo + prevLength;
    if (!needsMerge)
      return false;

    this._lengths[prevIndex] = prevLength + this._lengths[index];
    this._froms.splice(index, 1);
    this._tos.splice(index, 1);
    this._lengths.splice(index, 1);

    return true;
  }

  private _tryMergeRight(index: number, inFrom: number, inTo: number): boolean {
    const nextIndex = index + 1;
    if (nextIndex >= this._size)
      return false;

    const nextFrom = this._froms[nextIndex];
    const nextTo = this._tos[nextIndex];
    const nextLength = this._lengths[nextIndex];
    const needsMerge = inFrom === nextFrom - 1 && inTo === nextTo - 1;
    if (!needsMerge)
      return false;

    this._lengths[index] = this._lengths[index] + nextLength;
    this._froms.splice(nextIndex, 1);
    this._tos.splice(nextIndex, 1);
    this._lengths.splice(nextIndex, 1);

    return true;
  }

  public remap(inFrom: number, inTo: number) {
    const info = this._getInfo(inFrom);

    if (info.inRun) {
      if (info.target === inTo)
        return;

      const from = this._froms[info.index];
      const to = this._tos[info.index];
      const length = this._lengths[info.index];

      const isFirstElem = inFrom === from;
      const isLastElem = inFrom === from + length - 1;
      const isOnlyElem = isFirstElem && isLastElem;

      if (isOnlyElem) {
        this._froms[info.index] = inFrom;
        this._tos[info.index] = inTo;
        const mergedLeft = this._tryMergeLeft(info.index, inFrom, inTo);
        this._tryMergeRight(info.index + (mergedLeft ? -1 : 0), inFrom, inTo);

      } else if (isFirstElem) {
        // move up old
        this._froms[info.index] += 1;
        this._tos[info.index] += 1;
        this._lengths[info.index] -= 1;
        // insert new
        this._froms.splice(info.index, 0, inFrom);
        this._tos.splice(info.index, 0, inTo);
        this._lengths.splice(info.index, 0, 1);
        // merge left if necessary
        this._tryMergeLeft(info.index, inFrom, inTo);

      // is last element of run
      } else if (isLastElem) {
        // shrink old
        this._lengths[info.index] -= 1;
        // insert new
        this._froms.splice(info.index + 1, 0, inFrom);
        this._tos.splice(info.index + 1, 0, inTo);
        this._lengths.splice(info.index + 1, 0, 1);
        // merge right if necessary
        this._tryMergeRight(info.index + 1, inFrom, inTo);

      // in the middle of the run
      } else {
        const splitDistance = inFrom - from;
        // cut off old
        this._lengths[info.index] = splitDistance;
        // insert splitter and remainder
        this._froms.splice(info.index + 1, 0, inFrom, from + splitDistance + 1);
        this._tos.splice(info.index + 1, 0, inTo, to + splitDistance + 1);
        this._lengths.splice(info.index + 1, 0, 1, length - splitDistance - 1);
      }

    } else {
      this._froms.splice(info.index, 0, inFrom);
      this._tos.splice(info.index, 0, inTo);
      this._lengths.splice(info.index, 0, 1);
      const mergedLeft = this._tryMergeLeft(info.index, inFrom, inTo);
      this._tryMergeRight(info.index + (mergedLeft ? -1 : 0), inFrom, inTo);
    }
  }

  private _indexContains(index: number, inFrom: number): { target: number, hasTarget: boolean } {
    const from = this._froms[index];
    const to = this._tos[index];
    const length = this._lengths[index];

    const runOffset = inFrom - from;

    if (inFrom >= from && inFrom < from + length)
      return { target: to + runOffset, hasTarget: true };

    return { target: 0, hasTarget: false };
  }

  /**
   * binary search for an id in the table
   */
  private _getInfo(inFrom: number): IndexInfo {
    if (this._size === 0) {
      return { hasTarget: false, target: 0, inRun: false, index: 0 };
    }

    const firstFrom = this._froms[0];
    const lastFrom = this._froms[this._froms.length - 1];
    const lastLength = this._lengths[this._lengths.length - 1];

    if (inFrom >= lastFrom + lastLength) {
      return { hasTarget: false, target: 0, inRun: false, index: this._size };
    }

    if (inFrom < firstFrom) {
      return { hasTarget: false, target: 0, inRun: false, index: 0 };
    }

    let left = 0;
    let right = this._size - 1;

    while (true) {
      const curr = left + Math.floor((right - left) / 2);

      const from = this._froms[curr];
      const length = this._lengths[curr];

      if (right <= left + 1) {
        const leftFrom = this._froms[left];
        const leftLength = this._lengths[left];
        const rightFrom = this._froms[right];
        const rightLength = this._lengths[right];
          

        // before
        if (inFrom < leftFrom) {
          return { hasTarget: false, target: 0, inRun: false, index: left - 1 };

        // after
        } else if (inFrom >= rightFrom + rightLength) {
          return { hasTarget: false, target: 0, inRun: false, index: right + 1 };

        // in left
        } else if (inFrom < leftFrom + leftLength) {
          const match = this._indexContains(left, inFrom);
          return { hasTarget: match.hasTarget, target: match.target, inRun: true, index: left };

        // in right
        } else if (inFrom >= rightFrom) {
          const match = this._indexContains(right, inFrom);
          return { hasTarget: match.hasTarget, target: match.target, inRun: true, index: right };

        // in middle
        } else {
          return { hasTarget: false, target: 0, inRun: false, index: left + 1 };
        }
      }

      if (inFrom < from) {
        right = curr - 1;
      } else if (inFrom >= from + length) {
        left = curr + 1;
      } else {
        const currMatch = this._indexContains(curr, inFrom);
        return {
          hasTarget: currMatch.hasTarget, target: currMatch.target,
          inRun: true,
          index: curr,
        };
      }
    }
  }

  public get(from: number): number | undefined {
    const info = this._getInfo(from);
    return info.hasTarget ? info.target : undefined;
  }

  public *runs() {
    for (let i = 0; i < this._size; ++i) {
      yield { from: this._froms[i], to: this._tos[i], length: this._lengths[i] };
    }
  }

  public clone(): CompactRemapTable {
    const cloned = new CompactRemapTable();
    /* eslint-disable @typescript-eslint/dot-notation */
    cloned["_froms"] = this._froms.slice();
    cloned["_tos"] = this._tos.slice();
    cloned["_lengths"] = this._lengths.slice();
    /* eslint-enable @typescript-eslint/dot-notation */
    return cloned;
  }
}
