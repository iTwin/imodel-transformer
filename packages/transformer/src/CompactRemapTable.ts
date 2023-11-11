interface Run {
  from: number;
  to: number;
  length: number;
}

interface IndexInfo {
  index: number;
  place: "before" | "in" | "right-after" | "right-before" | "after";
  /** not valid unless @see hasTarget is true
   * only exists when the operation could determine a target and succeeded */
  target: number;
  hasTarget: boolean;
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

  public remap(from: number, to: number) {
    const info = this._getInfo(from);

    // FIXME: must handle splitting existing ones!
    if (info.place === "in")
      return;

    else if (info.place === "before")
      this._array.splice(3 * info.index, 0, from, to, 1);

    else if (info.place === "right-after")
      this._array[3 * info.index + 2] += 1;

    else if (info.place === "after")
      this._array.splice(3 * (info.index + 1), 0, from, to, 1);
  }

  /**
   * binary search for an id in the table
   */
  private _getInfo(inFrom: number): IndexInfo {
    if (this._size === 0)
      return { index: 0, place: "before", target: undefined };

    let curr = Math.floor(this._size / 2);
    while (true) {
      const from = this._array[3 * curr];
      const to = this._array[3 * curr + 1];
      const length = this._array[3 * curr + 2];
      const runOffset = inFrom - from;

      if (inFrom >= from && inFrom < from + length)
        return { index: curr, place: "in", target: to + runOffset };
      else if (inFrom === from + length)
        return { index: curr, place: "right-after", target: undefined };
      else if (inFrom < from)
        curr = Math.floor(curr / 2);
      else
        curr = Math.floor(curr + curr / 2);

      if (curr === 0)
        return { index: 0, place: "before", target: undefined };
      if (curr === this._size - 1)
        return { index: this._size, place: "after", target: undefined };
    }
  }

  public get(from: number): number | undefined {
    return this._getInfo(from).target;
  }

  public clone(): CompactRemapTable {
    const cloned = new CompactRemapTable();
    // eslint-disable-next-line @typescript-eslint/dot-notation
    cloned["_array"] = [...this._array];
    return cloned;
  }
}
