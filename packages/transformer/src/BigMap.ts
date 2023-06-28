const MAX_MAP_SIZE = 10_000_000;

export class BigMap<K, V> extends Map<K, V> {
  private _maps: Map<K, V>[];
  private _size: number;

  public override get size(): number {
    return this._size;
  }

  public constructor() {
    super();
    this._maps = [new Map()];
    this._size = 0;
  }

  public override clear(): void {
    this._maps.forEach((m) => m.clear());
    this._size = 0;
  }

  public override delete(key: K): boolean {
    const wasDeleted = this._maps.some((m) => m.delete(key));
    if (wasDeleted) {
      this._size--;
    }

    return wasDeleted;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public override forEach(callbackfn: (value: V, key: K, map: Map<K, V>) => void, thisArg?: any): void {
    this._maps.forEach((m) => {
      m.forEach(callbackfn, thisArg);
    });
  }

  public override get(key: K): V | undefined {
    for (const map of this._maps) {
      const value = map.get(key);
      if (value !== undefined) {
        return value;
      }
    }

    return;
  }

  public override has(key: K): boolean {
    return this._maps.some((m) => m.has(key));
  }

  public override set(key: K, value: V): this {
    // duplicate key
    for (const map of this._maps) {
      if (map.has(key)) {
        map.set(key, value);
        return this;
      }
    }

    const lastMap = this._maps[this._maps.length - 1];
    if (lastMap.size < MAX_MAP_SIZE) { // last map has free space
      lastMap.set(key, value);
    } else { // item will be put into new map
      const newMap = new Map();
      newMap.set(key, value);
      this._maps.push(newMap);
    }

    this._size++;

    return this;
  }
}
