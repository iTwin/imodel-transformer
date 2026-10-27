/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module Utils
 */

import { LRUMap } from "@itwin/core-bentley";
import { EntityReference } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";
import { EntityUnifier } from "./EntityUnifier.js";

/** Default maximum number of cached entities per [IModelDb]($core-backend) in an [[EntityExistenceCache]].
 * @internal
 */
export const DEFAULT_ENTITY_EXISTENCE_CACHE_LIMIT = 1_000_000;

/** A positive-only read-through cache of entity existence checks, scoped to a single transformation run.
 *
 * Entities are created continuously as a transformation progresses, so a negative
 * ("does not exist") result may become stale at any moment — therefore only positive
 * results are ever cached. Positive results can only be invalidated by deletes, which the
 * transformer routes through [[invalidate]] and [[clearDb]].
 *
 * The cache is keyed by [IModelDb]($core-backend) instance, so same-iModel transformations
 * (where sourceDb === targetDb) share a single set and source/target roles cannot be confused.
 *
 * [IModelImporter]($transformer) delete operations invalidate registered caches. Deletes performed
 * directly through an `EditTxn` while a transformation is active are out-of-band and must not be
 * mixed with transformer operations. A successful high-level `process()` clears the cache when the
 * transformation finalizes; low-level or failed transformations clear it when the transformer is
 * disposed.
 *
 * Each db's entries are held in a least-recently-used [LRUMap]($core-bentley) bounded by `limit`, so memory does not
 * grow with iModel size (and stays below V8's ~16.7 million entry `Map`/`Set` limit). A cache hit marks the entry as
 * recently used, which keeps frequently referenced entities (models, categories, code scopes, recent parents) cached.
 * Evicting an entry only costs one more existence query. The default limit is much larger than core's element and
 * model props caches because each entry is only a key, and a transformation is a long-running batch operation.
 * @internal
 */
export class EntityExistenceCache {
  private _existsByDb = new Map<IModelDb, LRUMap<EntityReference, true>>();

  /** @param limit maximum number of cached entities per db */
  public constructor(
    private readonly _limit = DEFAULT_ENTITY_EXISTENCE_CACHE_LIMIT
  ) {}

  private getDbCache(db: IModelDb): LRUMap<EntityReference, true> {
    let cache = this._existsByDb.get(db);
    if (cache === undefined) {
      cache = new LRUMap(this._limit);
      this._existsByDb.set(db, cache);
    }
    return cache;
  }

  /** Check whether an entity exists in `db`, consulting the cache first.
   * A `true` result is cached; a `false` result is never cached and will be re-queried.
   */
  public async exists(
    db: IModelDb,
    entityReference: EntityReference
  ): Promise<boolean> {
    const cache = this.getDbCache(db);
    // `get` (unlike `has`) marks the entry as recently used.
    if (cache.get(entityReference)) return true;
    const found = await EntityUnifier.exists(db, { entityReference });
    if (found) cache.set(entityReference, true);
    return found;
  }

  /** Read through the cache to check which of the supplied entities exist in `db`, populating it on a miss. */
  public async existsAll(
    db: IModelDb,
    entityReferences: Iterable<EntityReference>
  ): Promise<ReadonlySet<EntityReference>> {
    const cache = this.getDbCache(db);
    const found = new Set<EntityReference>();
    const unverified: EntityReference[] = [];

    for (const entityReference of entityReferences) {
      if (cache.get(entityReference)) found.add(entityReference);
      else unverified.push(entityReference);
    }

    if (unverified.length === 0) return found;

    const queried = await EntityUnifier.existsAll(db, unverified);
    for (const entityReference of queried) {
      cache.set(entityReference, true);
      found.add(entityReference);
    }
    return found;
  }

  /** Record that an entity is known to exist in `db` (e.g. it was just imported). */
  public markExists(db: IModelDb, entityReference: EntityReference): void {
    this.getDbCache(db).set(entityReference, true);
  }

  /** Remove a single entity from the cache for `db`, forcing a re-query on next check. */
  public invalidate(db: IModelDb, entityReference: EntityReference): void {
    this._existsByDb.get(db)?.delete(entityReference);
  }

  /** Drop all cached results for `db`. Used when a delete may cascade to entities the
   * caller cannot enumerate (e.g. element tree deletes that remove descendants and submodels).
   */
  public clearDb(db: IModelDb): void {
    this._existsByDb.delete(db);
  }

  /** Drop all cached results. */
  public clear(): void {
    this._existsByDb.clear();
  }
}
