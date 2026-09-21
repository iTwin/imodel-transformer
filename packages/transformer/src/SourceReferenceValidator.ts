/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import { IModelDb } from "@itwin/core-backend";
import { EntityReference } from "@itwin/core-common";
import { EntityExistenceCache } from "./EntityExistenceCache";

/** Keeps reference validation aligned with the exporter's existing 1,000-owner
 * batches, bounding memory and virtual-set sizes while amortizing query overhead.
 * @internal
 */
export const defaultSourceReferenceValidationBatchSize = 1000;

/** @internal */
export interface MissingSourceReference {
  readonly entityId: Id64String;
  readonly referenceId: EntityReference;
}

/** Buffers and deduplicates source references before checking their existence in bulk.
 * @internal
 */
export class SourceReferenceValidator {
  private readonly _pendingReferences = new Map<EntityReference, Id64String>();

  public constructor(
    private readonly _sourceDb: IModelDb,
    private readonly _existenceCache: EntityExistenceCache,
    private readonly _batchSize: number
  ) {
    if (!Number.isSafeInteger(_batchSize) || _batchSize <= 0) {
      throw new Error(
        "Source reference validation batch size must be positive."
      );
    }
  }

  /** Add references to the pending batch, validating whenever it reaches the configured size. */
  public async add(
    referenceIds: Iterable<EntityReference>,
    entityId: Id64String
  ): Promise<MissingSourceReference | undefined> {
    for (const referenceId of referenceIds) {
      if (this._pendingReferences.has(referenceId)) continue;

      this._pendingReferences.set(referenceId, entityId);
      if (this._pendingReferences.size >= this._batchSize) {
        const missingReference = await this.flush();
        if (missingReference !== undefined) return missingReference;
      }
    }
    return undefined;
  }

  /** Validate all pending references. Successful batches are removed; failed queries remain retryable. */
  public async flush(): Promise<MissingSourceReference | undefined> {
    if (this._pendingReferences.size === 0) return undefined;

    const found = await this._existenceCache.existsAll(this._sourceDb, [
      ...this._pendingReferences.keys(),
    ]);
    for (const [referenceId, entityId] of this._pendingReferences) {
      if (!found.has(referenceId)) return { referenceId, entityId };
    }

    this._pendingReferences.clear();
    return undefined;
  }

  public clear(): void {
    this._pendingReferences.clear();
  }
}
