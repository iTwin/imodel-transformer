/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelDb } from "@itwin/core-backend";
import { CollectedChanges, DeletionBatch } from "../../ChangesetScanner";
import { ChangedInstanceIds, ChangeRecord } from "../../IModelExporter";
import { IModelTransformer } from "../../IModelTransformer";

// __PUBLISH_EXTRACT_START__ ChangeScanning.cached-changes-transformer
/** Changes read ahead of time from one range of changesets. */
export interface CachedChanges {
  /** Every change in the range, in the order it was made. */
  changes: ChangeRecord[];
  /** The instances deleted in the range. */
  deletions: DeletionBatch;
}

/** A store of changes read ahead of time, for example by a service that processes each pushed changeset. */
export interface ChangeCache {
  /** Returns the changes in an inclusive range of changeset indexes, or undefined unless it has all of them. */
  tryGetChanges(
    iModel: IModelDb,
    range: readonly [number, number]
  ): Promise<CachedChanges | undefined>;
}

/** Reads source changes from a cache, and downloads only the changesets in ranges the cache doesn't have. */
export class CachedChangesTransformer extends IModelTransformer {
  private readonly _cache: ChangeCache;

  public constructor(
    cache: ChangeCache,
    ...args: ConstructorParameters<typeof IModelTransformer>
  ) {
    super(...args);
    this._cache = cache;
  }

  protected override async collectChanges(
    ranges: readonly (readonly [number, number])[],
    changedInstanceIds = new ChangedInstanceIds(this.sourceDb)
  ): Promise<CollectedChanges> {
    const deletionBatches: DeletionBatch[] = [];
    // Handle the ranges in order, adding every change to the same changedInstanceIds.
    for (const range of ranges) {
      const cached = await this._cache.tryGetChanges(this.sourceDb, range);
      if (cached === undefined) {
        const scanned = await super.collectChanges([range], changedInstanceIds);
        deletionBatches.push(...scanned.deletionBatches);
        continue;
      }
      for (const change of cached.changes)
        await changedInstanceIds.addChangeRecord(change);
      deletionBatches.push(cached.deletions);
    }
    return { changedInstanceIds, deletionBatches };
  }
}
// __PUBLISH_EXTRACT_END__
