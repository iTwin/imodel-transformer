/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module Utils
 */
import { Id64String } from "@itwin/core-bentley";

/**
 * This class serves as a temporary solution to address the issue of the V8 JavaScript engine's Map implementation,
 * which is limited to approximately 16.7 million elements. Currently, only Id64 string representations are
 * supported by this Map; any other type will lead to errors. It's worth noting that our future plan is to
 * replace this stopgap with a more robust solution, utilizing a temporary SQLite database (https://github.com/iTwin/imodel-transformer/issues/83).
 * @internal
 */
export class BigMap<V> extends Map<Id64String, V> {
    private _maps: Record<string, Map<string, V>>;
    private _size: number;

    public override get size(): number {
      return this._size;
    }

    public constructor() {
      super();
      this._maps = {
        0: new Map(),
        1: new Map(),
        2: new Map(),
        3: new Map(),
        4: new Map(),
        5: new Map(),
        6: new Map(),
        7: new Map(),
        8: new Map(),
        9: new Map(),
        a: new Map(),
        b: new Map(),
        c: new Map(),
        d: new Map(),
        e: new Map(),
        f: new Map(),
      };
      this._size = 0;
    }

    public override clear(): void {
      Object.values(this._maps).forEach((m) => m.clear());
      this._size = 0;
    }

    public override delete(key: Id64String): boolean {
      const wasDeleted = this._maps[key[key.length - 1]].delete(key);
      if (wasDeleted) {
        this._size--;
      }

      return wasDeleted;
    }

    public override forEach(callbackfn: (value: V, key: Id64String, map: Map<Id64String, V>) => void, thisArg?: any): void {
      Object.values(this._maps).forEach((m) => {
        m.forEach(callbackfn, thisArg);
      });
    }

    public override get(key: Id64String): V | undefined {
      return this._maps[key[key.length - 1]].get(key);
    }

    public override has(key: Id64String): boolean {
      return this._maps[key[key.length - 1]].has(key);
    }

    public override set(key: Id64String, value: V): this {
      const mapForKey = this._maps[key[key.length - 1]];
      if (mapForKey === undefined)
        throw Error(`Tried to set ${key}, but that key has no submap`);
      const beforeSize = mapForKey.size;
      mapForKey.set(key, value);
      const afterSize = mapForKey.size;
      this._size += (afterSize - beforeSize);
      return this;
    }
}
