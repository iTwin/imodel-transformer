/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module Utils
 */

import { EntityReference } from "@itwin/core-common";
import { IModelDb } from "@itwin/core-backend";
import { EntityUnifier } from "./EntityUnifier";

/** A positive-only cache of entity existence checks, scoped to a single transformation run.
 *
 * Entities are created continuously as a transformation progresses, so a negative
 * ("does not exist") result may become stale at any moment — therefore only positive
 * results are ever cached. Positive results can only be invalidated by deletes, which the
 * transformer routes through [[invalidate]] and [[clearDb]].
 *
 * The cache is keyed by [IModelDb]($core-backend) instance, so same-iModel transformations
 * (where sourceDb === targetDb) share a single set and source/target roles cannot be confused.
 *
 * @note Deletes performed out-of-band (e.g. by an [IModelImporter]($transformer) or exporter
 * handler subclass that deletes entities without going through the transformer's onDelete*
 * overrides) are not observed by this cache. The cache is scoped to a single `process()` run
 * and cleared when the transformation finalizes or the transformer is disposed.
 * @internal
 */
export class EntityExistenceCache {
  private _existsByDb = new Map<IModelDb, Set<EntityReference>>();

  private getDbSet(db: IModelDb): Set<EntityReference> {
    let set = this._existsByDb.get(db);
    if (set === undefined) {
      set = new Set();
      this._existsByDb.set(db, set);
    }
    return set;
  }

  /** Check whether an entity exists in `db`, consulting the cache first.
   * A `true` result is cached; a `false` result is never cached and will be re-queried.
   */
  public async exists(
    db: IModelDb,
    entityReference: EntityReference
  ): Promise<boolean> {
    const set = this.getDbSet(db);
    if (set.has(entityReference)) return true;
    const found = await EntityUnifier.exists(db, { entityReference });
    if (found) set.add(entityReference);
    return found;
  }

  /** Check whether an entity is already known to exist, without querying. */
  public isKnownToExist(
    db: IModelDb,
    entityReference: EntityReference
  ): boolean {
    return this._existsByDb.get(db)?.has(entityReference) ?? false;
  }

  /** Record that an entity is known to exist in `db` (e.g. it was just imported). */
  public markExists(db: IModelDb, entityReference: EntityReference): void {
    this.getDbSet(db).add(entityReference);
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
