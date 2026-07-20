/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";

export type ElementAspectExportPreparation = (
  excludedElementAspectClassFullNames: ReadonlySet<string>,
  elementIds: ReadonlySet<Id64String>
) => Promise<void>;

/** Coordinates accepted ElementAspect owner scopes, flushing bounded owner groups and running preparation before aspect export.
 * @internal
 */
export class ElementAspectExportCoordinator {
  private readonly _acceptedOwnerIds = new Set<Id64String>();
  private readonly _ownerExportDecisions = new Map<Id64String, boolean>();
  private _batchSize: number;
  private _depth = 0;
  private _prepare?: ElementAspectExportPreparation;

  public constructor(
    private readonly _defaultBatchSize: number,
    private readonly _excludedClassFullNames: () => ReadonlySet<string>,
    private readonly _exportAspects: (
      elementIds: ReadonlySet<Id64String>
    ) => Promise<void>
  ) {
    this._batchSize = _defaultBatchSize;
  }

  /** Whether an accepted-owner collection scope is active. */
  public get isActive(): boolean {
    return this._depth > 0;
  }

  /** Number of cached owner export decisions in the current group. */
  public get ownerExportDecisionCount(): number {
    return this._ownerExportDecisions.size;
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
      this.resetBatchState();
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
      } else {
        await this.exportOwners(new Set<Id64String>());
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
    if (!this.isActive) return;

    this._acceptedOwnerIds.add(elementId);
    if (this._acceptedOwnerIds.size >= this._batchSize) {
      await this.flush();
    }
  }

  /** Returns the cached export decision for an owner in the current group. */
  public getOwnerExportDecision(elementId: Id64String): boolean | undefined {
    return this._ownerExportDecisions.get(elementId);
  }

  /** Records that an owner passed element export filtering in the active scope. */
  public recordAcceptedOwnerDecision(elementId: Id64String): void {
    if (this.isActive) {
      this._ownerExportDecisions.set(elementId, true);
    }
  }

  /** Clears cached owner export decisions. */
  public clearOwnerExportDecisions(): void {
    this._ownerExportDecisions.clear();
  }

  /** Runs preparation and aspect export for an explicit owner set. */
  public async exportOwners(
    elementIds: ReadonlySet<Id64String>
  ): Promise<void> {
    await this._prepare?.(this._excludedClassFullNames(), elementIds);
    await this._exportAspects(elementIds);
  }

  private async flush(): Promise<void> {
    const elementIds = new Set(this._acceptedOwnerIds);
    this._acceptedOwnerIds.clear();
    try {
      await this.exportOwners(elementIds);
    } finally {
      this._ownerExportDecisions.clear();
    }
  }

  private resetBatchState(): void {
    this._acceptedOwnerIds.clear();
    this._ownerExportDecisions.clear();
  }

  private reset(): void {
    this._depth = 0;
    this.resetBatchState();
    this._batchSize = this._defaultBatchSize;
  }
}

const coordinators = new WeakMap<object, ElementAspectExportCoordinator>();

export function registerElementAspectExportCoordinator(
  owner: object,
  coordinator: ElementAspectExportCoordinator
): void {
  coordinators.set(owner, coordinator);
}

export function getElementAspectExportCoordinator(
  owner: object
): ElementAspectExportCoordinator {
  const coordinator = coordinators.get(owner);
  if (coordinator === undefined) {
    throw new Error("ElementAspect export coordinator is not registered.");
  }
  return coordinator;
}
