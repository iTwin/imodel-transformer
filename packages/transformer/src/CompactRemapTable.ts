import assert from "node:assert";

interface Run {
  from: number;
  to: number;
  length: number;
}

enum RunField {
  from = 0,
  to = 1,
  length = 2,
}

interface IndexInfo {
  index: number;
  place: "before" | "in" | "after";
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

  public remap(inFrom: number, inTo: number) {
    const info = this._getInfo(inFrom);

    if (process.env.DEBUG)
      console.log({ info });

    // FIXME: must handle splitting existing ones!
    if (info.place === "in") {
      if (info.target === inTo)
        return;

      // split the run
      const from = this._froms[info.index];
      const to = this._tos[info.index];
      const length = this._lengths[info.index];

      // is first element of run
      if (inFrom === from) {
        // move up old
        this._froms[info.index] += 1;
        this._tos[info.index] += 1;
        this._lengths[info.index] -= 1;
        // insert new
        this._froms.splice(info.index, 0, inFrom);
        this._tos.splice(info.index, 0, inTo);
        this._lengths.splice(info.index, 0, 1);

      // is last element of run
      } else if (inFrom === from + length - 1) {
        // shrink old
        this._lengths[info.index] -= 1;
        // insert new
        this._froms.splice(info.index + 1, 0, inFrom);
        this._tos.splice(info.index + 1, 0, inTo);
        this._lengths.splice(info.index + 1, 0, 1);

      } else {
        // FIXME: do not do asserts in performance critical code
        if (process.env.DEBUG)
          assert(inFrom > from && inFrom < from + length);
        const splitDistance = inFrom - from;
        // cut off old
        this._lengths[info.index] = splitDistance - 1;
        // insert splitter and remainder
        this._froms.splice(info.index + 1, 0, inFrom, from + splitDistance + 1);
        this._tos.splice(info.index + 1, 0, inTo, to + splitDistance);
        this._lengths.splice(info.index + 1, 0, 1, length - splitDistance);
      }
    }

    const prevIndex = info.index - 1;
    const touchesLeft = prevIndex >= 0 && (() => {
      const prevFrom = this._froms[prevIndex];
      const prevTo = this._tos[prevIndex];
      const prevLength = this._lengths[prevIndex];
      return inFrom === prevFrom + prevLength && inTo === prevTo + prevLength;
    })();

    const nextIndex = info.index + 1;
    const touchesRight = nextIndex < this._size && (() => {
      const nextFrom = this._froms[nextIndex];
      const nextTo = this._tos[nextIndex];
      return inFrom === nextFrom - 1 && inTo === nextTo - 1;
    })();

    if (info.place === "before") {
      if (touchesRight) {
        this._froms[info.index] -= 1;
        this._tos[info.index] -= 1;
        this._lengths[info.index] += 1;

      } else {
        this._froms.splice(info.index, 0, inFrom);
        this._tos.splice(info.index, 0, inTo);
        this._lengths.splice(info.index, 0, 1);
      }

    } else /* if (info.place === "after") */ {
      if (touchesLeft) {
        this._lengths[info.index] += 1;

      } else {
        this._froms.splice(info.index + 1, 0, inFrom);
        this._tos.splice(info.index + 1, 0, inTo);
        this._lengths.splice(info.index + 1, 0, 1);
      }
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
      return { hasTarget: false, target: 0, place: "before", index: 0 };
    }

    let left = 0;
    let right = this._size - 1;

    while (true) {
      const curr = left + Math.floor((right - left) / 2);

      const from = this._froms[curr];
      const length = this._lengths[curr];

      console.log(inFrom, from, "|", left, curr, right);

      if (right <= left + 1) {
        const leftFrom = this._froms[left];
        const leftLength = this._lengths[left];
        const rightFrom = this._froms[right];
        const rightLength = this._lengths[right];

        if (process.env.DEBUG)
          console.log(leftFrom, leftLength, inFrom, rightFrom, rightLength);

        // before
        if (inFrom < leftFrom) {
          return { hasTarget: false, target: 0, place: "before", index: left };

        // after
        } else if (inFrom >= rightFrom + rightLength) {
          return { hasTarget: false, target: 0, place: "after", index: right };

        // in left
        } else if (inFrom < leftFrom + leftLength) {
          const match = this._indexContains(left, inFrom);
          return { hasTarget: match.hasTarget, target: match.target, place: "in", index: left };

        // in right
        } else if (inFrom >= rightFrom) {
          const match = this._indexContains(right, inFrom);
          return { hasTarget: match.hasTarget, target: match.target, place: "in", index: right };

        // in middle
        } else {
          return { hasTarget: false, target: 0, place: "after", index: left };
        }
      }

      if (inFrom < from) {
        right = curr - 1;
      } else if (inFrom >= from + length) {
        left = curr + 1;
      } else {
        const currMatch = this._indexContains(curr, inFrom);
        if (currMatch.hasTarget) {
          return {
            hasTarget: currMatch.hasTarget, target: currMatch.target,
            place: "in",
            index: curr,
          };
        }
      }
    }
  }

  public get(from: number): number | undefined {
    const info = this._getInfo(from);
    return info.hasTarget ? info.target : undefined;
  }

  public clone(): CompactRemapTable {
    const cloned = new CompactRemapTable();
    /* eslint-disable-next-line @typescript-eslint/dot-notation */
    cloned["_froms"] = this._froms.slice();
    cloned["_tos"] = this._tos.slice();
    cloned["_lengths"] = this._lengths.slice();
    /* eslint-enable-next-line @typescript-eslint/dot-notation */
    return cloned;
  }
}
