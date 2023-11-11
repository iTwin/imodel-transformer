import assert = require("node:assert");

interface Run {
  from: number;
  to: number;
  length: number;
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
  /** an array of inlined @see Run objects, such that each object
   * is three ordered numbers in the array */
  private _array: number[] = [];

  private get _size() { return this._array.length / 3; }

  public remap(inFrom: number, inTo: number) {
    const info = this._getInfo(inFrom);

    if (process.env.DEBUG)
      console.log({ info });

    // FIXME: must handle splitting existing ones!
    if (info.place === "in") {
      if (info.target === inTo)
        return;

      // split the run
      const from = this._array[3 * info.index];
      const to = this._array[3 * info.index + 1];
      const length = this._array[3 * info.index + 2];

      // is first element of run
      if (inFrom === from) {
        // move up old
        this._array[3 * info.index + 0] += 1;
        this._array[3 * info.index + 1] += 1;
        this._array[3 * info.index + 2] -= 1;
        // insert new
        this._array.splice(3 * info.index, 0, inFrom, inTo, 1);

      // is last element of run
      } else if (inFrom === from + length - 1) {
        // shrink old
        this._array[3 * info.index + 2] -= 1;
        // insert new
        this._array.splice(3 * info.index + 1, 0, inFrom, inTo, 1);

      } else {
        // FIXME: do not do asserts in performance critical code
        if (process.env.DEBUG)
          assert(inFrom > from && inFrom < from + length);
        const splitDistance = inFrom - from;
        // cut off old
        this._array[3 * info.index + 2] = splitDistance - 1;
        // insert splitter and remainder
        this._array.splice(
          3 * info.index + 1, 0,
          // splitter
          inFrom, inTo, 1,
          // remainder
          from + splitDistance + 1, to + splitDistance, length - splitDistance,
        );
      }
    }

    const prevIndex = info.index - 1;
    const touchesLeft = prevIndex >= 0 && (() => {
      const prevFrom = this._array[prevIndex];
      const prevTo = this._array[prevIndex + 1];
      const prevLength = this._array[prevIndex + 2];
      return inFrom === prevFrom + prevLength && inTo === prevTo + prevLength;
    })();

    const nextIndex = info.index + 1;
    const touchesRight = nextIndex < this._size && (() => {
      const nextFrom = this._array[nextIndex];
      const nextTo = this._array[nextIndex + 1];
      return inFrom === nextFrom - 1 && inTo === nextTo - 1;
    })();

    if (info.place === "before") {
      if (touchesRight) {
        this._array[3 * info.index + 0] -= 1;
        this._array[3 * info.index + 1] -= 1;
        this._array[3 * info.index + 2] += 1;

      } else {
        // FIXME: add a private _insert function
        this._array.splice(3 * info.index, 0, inFrom, inTo, 1);
      }

    } else /* if (info.place === "after") */ {
      if (touchesLeft) {
        this._array[3 * info.index + 0] -= 1;
        this._array[3 * info.index + 1] -= 1;
        this._array[3 * info.index + 2] += 1;
      } else {
        this._array.splice(3 * info.index + 1, 0, inFrom, inTo, 1);
      }
    }
  }

  private _indexContains(index: number, inFrom: number): { target: number, hasTarget: boolean } {
    const from = this._array[3 * index];
    const to = this._array[3 * index + 1];
    const length = this._array[3 * index + 2];

    const runOffset = inFrom - from;

    if (inFrom >= from && inFrom < from + length)
      return { target: to + runOffset, hasTarget: true };

    return { target: 0, hasTarget: false };
  }

  /**
   * binary search for an id in the table
   */
  private _getInfo(inFrom: number): IndexInfo {
    let left = 0;
    let right = this._size - 1;

    while (true) {
      const curr = left + Math.floor((right - left) / 2);

      const from = this._array[3 * curr];
      const length = this._array[3 * curr + 2];

      console.log(inFrom, from, "|", left, curr, right);

      if (right === left) {
        const currMatch = this._indexContains(curr, inFrom);
        if (currMatch.hasTarget) {
          return {
            hasTarget: currMatch.hasTarget, target: currMatch.target,
            place: "in",
            index: curr,
          };
        } else if (right === 0) {
          return {
            hasTarget: false, target: 0, place: "before", index: left,
          };
        } else if (left === this._size) {
          return {
            hasTarget: false, target: 0, place: "after", index: right,
          };
        } else {
          assert(false);
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
    // eslint-disable-next-line @typescript-eslint/dot-notation
    cloned["_array"] = this._array.slice();
    return cloned;
  }
}
