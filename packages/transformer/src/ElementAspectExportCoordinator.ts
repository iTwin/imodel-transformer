/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";

export type ElementAspectExportPreparation = (
  excludedElementAspectClassFullNames: ReadonlySet<string>,
  ownerElementIds: ReadonlySet<Id64String>
) => Promise<void>;

/** Coordinates scoped batches of accepted ElementAspect owners, including preparation before aspect export.
 * @internal
 */
export class ElementAspectExportCoordinator {
  private readonly _acceptedOwnerIds = new Set<Id64String>();
  private readonly _processedOwnerIds = new Set<Id64String>();
  private _batchSize: number;
  private _depth = 0;
  private _prepare?: ElementAspectExportPreparation;

  public constructor(
    private readonly _defaultBatchSize: number,
    private readonly _excludedClassFullNames: () => ReadonlySet<string>,
    private readonly _exportAspects: (
      ownerElementIds: ReadonlySet<Id64String>
    ) => Promise<void>,
    private readonly _resetCaches: () => void = () => {}
  ) {
    this._batchSize = _defaultBatchSize;
  }

  /** Whether an accepted-owner collection scope is active. */
  public get isActive(): boolean {
    return this._depth > 0;
  }

  /** Sets the callback that prepares each accepted-owner group before its aspects are exported. */
  public setPreparation(prepare: ElementAspectExportPreparation): void {
    this._prepare = prepare;
  }

  /** Begins an accepted-owner collection scope.
   * Nested scopes share the outer scope's state and batch size.
   */
  public begin(batchSize = this._defaultBatchSize): void {
    if (this._depth === 0) {
      this.resetScopeState();
      this._resetCaches();
      this._batchSize = batchSize;
    }
    this._depth++;
  }

  /** Ends an accepted-owner collection scope.
   * Ending the outermost scope exports pending owners and resets all scope state.
   */
  public async end(): Promise<void> {
    if (this._depth === 0) {
      throw new Error("No scoped element export is active.");
    }

    this._depth--;
    if (this._depth > 0) return;

    try {
      if (this._acceptedOwnerIds.size > 0) {
        await this.flush();
      }
    } finally {
      this.reset();
    }
  }

  /** Discards the active scope and its pending owner state without exporting it. */
  public abort(): void {
    this.reset();
  }

  /** Runs element export within an accepted-owner collection scope.
   * A scope created by this call is aborted if element export fails; an existing outer scope remains responsible for its own failure handling.
   */
  public async run(exportElements: () => Promise<void>): Promise<void> {
    if (this.isActive) {
      await exportElements();
      return;
    }

    this.begin();
    try {
      await exportElements();
    } catch (error) {
      this.abort();
      throw error;
    }
    await this.end();
  }

  /** Adds an accepted source owner to the current group and exports the group when it reaches the configured batch size. */
  public async addAcceptedOwner(elementId: Id64String): Promise<void> {
    if (!this.isActive || this._processedOwnerIds.has(elementId)) return;

    this._acceptedOwnerIds.add(elementId);
    if (this._acceptedOwnerIds.size >= this._batchSize) {
      await this.flush();
    }
  }

  /** Runs preparation and aspect export for an explicit owner set in bounded groups. */
  public async exportOwners(
    ownerElementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    let ownerIds = ownerElementIds;
    if (this.isActive && ownerElementIds.size > 0) {
      const unprocessedOwnerIds = new Set<Id64String>();
      for (const elementId of ownerElementIds) {
        if (!this._processedOwnerIds.has(elementId)) {
          unprocessedOwnerIds.add(elementId);
          this._processedOwnerIds.add(elementId);
        }
      }
      if (unprocessedOwnerIds.size === 0) return;
      ownerIds = unprocessedOwnerIds;
    }

    if (ownerIds.size <= this._defaultBatchSize) {
      await this.exportOwnerBatch(ownerIds);
      return;
    }

    let ownerBatch = new Set<Id64String>();
    for (const elementId of ownerIds) {
      ownerBatch.add(elementId);
      if (ownerBatch.size === this._defaultBatchSize) {
        await this.exportOwnerBatch(ownerBatch);
        ownerBatch = new Set<Id64String>();
      }
    }
    if (ownerBatch.size > 0) {
      await this.exportOwnerBatch(ownerBatch);
    }
  }

  private async exportOwnerBatch(
    ownerElementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    await this._prepare?.(this._excludedClassFullNames(), ownerElementIds);
    await this._exportAspects(ownerElementIds);
  }

  private async flush(): Promise<void> {
    const ownerElementIds = new Set(this._acceptedOwnerIds);
    this._acceptedOwnerIds.clear();
    await this.exportOwners(ownerElementIds);
  }

  private resetScopeState(): void {
    this._acceptedOwnerIds.clear();
    this._processedOwnerIds.clear();
  }

  private reset(): void {
    this._depth = 0;
    this.resetScopeState();
    this._batchSize = this._defaultBatchSize;
  }
}
