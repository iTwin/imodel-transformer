/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module Utils
 */

import { Id64, Id64String } from "@itwin/core-bentley";
import { CodeProps, QueryBinder } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";

/** Folds a code value the way the `NOCASE` collation of `bis_Element.CodeValue` compares it: only ASCII letters are case-insensitive. */
function foldCodeValue(value: string): string {
  return value.replace(/[A-Z]+/g, (letters) => letters.toLowerCase());
}

const unindexed = Symbol("unindexed");
type CodeBucket = Map<string, Id64String>;

/** @internal */
export interface TargetCodeIndexOptions {
  /** Lookups in a (CodeSpec, CodeScope) pair that are queried directly before the pair is loaded. Loading costs about as much as a lookup per ~100 codes, so pairs with few lookups are not worth loading. */
  lookupsBeforeLoad: number;
  /** A pair with more codes than this is not indexed. */
  maxBucketCodes: number;
  /** No more pairs are indexed once this many codes are indexed. */
  maxIndexedCodes: number;
}

/** Finds target elements by Code during a transformation. Once a (CodeSpec, CodeScope) pair has been looked up more than `lookupsBeforeLoad` times, all codes in that pair are loaded with one query and later lookups in the pair are answered from memory.
 *
 * A lookup returns the same element as `SELECT ECInstanceId FROM BisCore:Element WHERE CodeSpec.Id=? AND CodeScope.Id=? AND CodeValue=?` if these hold:
 * - Every code written to a loaded pair is passed to [[recordElementCode]]. The transformer records the elements it imports. Writes made directly through an `EditTxn` while an indexed operation runs are out-of-band, like deletes for [[EntityExistenceCache]].
 * - Deleted elements and changed codes need no recording: an indexed element is returned only if it still has the code, otherwise the lookup falls back to the query.
 *
 * The index is only used inside [[withIndex]] and is cleared when the outermost call starts and ends.
 * A pair with more than `maxBucketCodes` codes, or any pair once `maxIndexedCodes` codes are indexed, is not indexed and uses the query for every lookup.
 * @internal
 */
export class TargetCodeIndex {
  private readonly _buckets = new Map<string, CodeBucket | typeof unindexed>();
  private readonly _unloadedLookups = new Map<string, number>();
  private readonly _options: TargetCodeIndexOptions;
  private _indexedCodes = 0;
  private _depth = 0;

  public constructor(
    private readonly _db: IModelDb,
    options?: Partial<TargetCodeIndexOptions>
  ) {
    this._options = {
      lookupsBeforeLoad: 64,
      maxBucketCodes: 10_000,
      maxIndexedCodes: 1_000_000,
      ...options,
    };
  }

  /** Runs `operation` with the index enabled. The outermost call starts and ends with an empty index. */
  public async withIndex<T>(operation: () => Promise<T>): Promise<T> {
    if (this._depth++ === 0) this.clear();
    try {
      return await operation();
    } finally {
      if (--this._depth === 0) this.clear();
    }
  }

  /** Drops all indexed codes. */
  public clear(): void {
    this._buckets.clear();
    this._unloadedLookups.clear();
    this._indexedCodes = 0;
  }

  /** Returns the id of the element with exactly this code value, compared with `NOCASE` collation, or `undefined` if there is none. */
  public async findElementId(
    code: Required<CodeProps>
  ): Promise<Id64String | undefined> {
    if (this._depth === 0) return this.queryElementId(code);
    const bucket = await this.getBucket(code.spec, code.scope);
    if (bucket === unindexed) return this.queryElementId(code);

    const key = foldCodeValue(code.value);
    const indexedId = bucket.get(key);
    if (indexedId === undefined || this.hasCode(indexedId, code, key))
      return indexedId;

    bucket.delete(key);
    this._indexedCodes--;
    const currentId = await this.queryElementId(code);
    if (currentId !== undefined) {
      bucket.set(key, currentId);
      this._indexedCodes++;
    }
    return currentId;
  }

  /** Records that the element was inserted or updated with this code. */
  public recordElementCode(elementId: Id64String, code: CodeProps): void {
    // empty code values are stored as NULL, which no lookup matches
    if (
      this._depth === 0 ||
      !code.value ||
      !Id64.isValidId64(code.spec) ||
      !Id64.isValidId64(code.scope)
    )
      return;
    const bucket = this._buckets.get(this.bucketKey(code.spec, code.scope));
    if (bucket === undefined || bucket === unindexed) return;
    const key = foldCodeValue(code.value);
    if (!bucket.has(key)) this._indexedCodes++;
    bucket.set(key, elementId);
  }

  private bucketKey(spec: Id64String, scope: Id64String): string {
    return `${Id64.fromString(spec)}/${Id64.fromString(scope)}`;
  }

  private async getBucket(
    spec: Id64String,
    scope: Id64String
  ): Promise<CodeBucket | typeof unindexed> {
    const bucketKey = this.bucketKey(spec, scope);
    let bucket = this._buckets.get(bucketKey);
    if (bucket === undefined) {
      const lookups = (this._unloadedLookups.get(bucketKey) ?? 0) + 1;
      if (lookups <= this._options.lookupsBeforeLoad) {
        this._unloadedLookups.set(bucketKey, lookups);
        return unindexed;
      }
      this._unloadedLookups.delete(bucketKey);
      bucket = await this.loadBucket(spec, scope);
      this._buckets.set(bucketKey, bucket);
    }
    return bucket;
  }

  private async loadBucket(
    spec: Id64String,
    scope: Id64String
  ): Promise<CodeBucket | typeof unindexed> {
    const limit = Math.min(
      this._options.maxBucketCodes,
      this._options.maxIndexedCodes - this._indexedCodes
    );
    if (limit <= 0) return unindexed;
    const rows = await this._db
      .createQueryReader(
        `SELECT ECInstanceId, CodeValue FROM BisCore:Element WHERE CodeSpec.Id=? AND CodeScope.Id=? AND CodeValue IS NOT NULL LIMIT ${
          limit + 1
        }`,
        new QueryBinder()
          .bindId(1, Id64.fromString(spec))
          .bindId(2, Id64.fromString(scope)),
        { usePrimaryConn: true }
      )
      .toArray();
    if (rows.length > limit) return unindexed;
    const bucket: CodeBucket = new Map();
    for (const [id, value] of rows)
      bucket.set(foldCodeValue(value as string), id as Id64String);
    this._indexedCodes += bucket.size;
    return bucket;
  }

  private hasCode(
    elementId: Id64String,
    code: Required<CodeProps>,
    key: string
  ): boolean {
    const current = this._db.elements.tryGetElementProps(elementId)?.code;
    return (
      current?.value !== undefined &&
      Id64.fromString(current.spec) === Id64.fromString(code.spec) &&
      Id64.fromString(current.scope) === Id64.fromString(code.scope) &&
      foldCodeValue(current.value) === key
    );
  }

  private async queryElementId(
    code: Required<CodeProps>
  ): Promise<Id64String | undefined> {
    const queryReader = this._db.createQueryReader(
      "SELECT ECInstanceId FROM BisCore:Element WHERE CodeSpec.Id=? AND CodeScope.Id=? AND CodeValue=?",
      new QueryBinder()
        .bindId(1, code.spec)
        .bindId(2, Id64.fromString(code.scope))
        .bindString(3, code.value),
      { usePrimaryConn: true }
    );
    return (await queryReader.step()) ? queryReader.current[0] : undefined;
  }
}
