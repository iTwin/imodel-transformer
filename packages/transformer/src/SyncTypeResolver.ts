/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { Id64String } from "@itwin/core-bentley";
import {
  ElementOwnsExternalSourceAspects,
  ExternalSourceAspect,
  IModelDb,
} from "@itwin/core-backend";
import { IModel } from "@itwin/core-common";
import type { TargetScopeProvenanceJsonProps } from "./IModelTransformer";
import { ProvenanceManager } from "./ProvenanceManager";

/** @internal */
export type SyncType = "not-sync" | "forward" | "reverse";

/** Options for constructing a SyncTypeResolver.
 * @internal
 */
export interface SyncTypeResolverOptions {
  sourceDb: IModelDb;
  targetDb: IModelDb;
  targetScopeElementId: Id64String;
  isProvenanceInitTransform?: boolean;
  allowNoScopingESA?: boolean;
  hasArgsForProcessChanges?: boolean;
}

/**
 * Encapsulates sync direction determination logic.
 * Given a source and target iModel, resolves whether the transformation is
 * a forward sync, reverse sync, or not a sync at all.
 * @internal
 */
export class SyncTypeResolver {
  public static noEsaSyncDirectionErrorMessage =
    "Couldn't find an external source aspect to determine sync direction. This often means that the master->branch relationship has not been established. Consider running the transformer with wasSourceIModelCopiedToTarget set to true.";

  private _syncType?: SyncType;
  private readonly _opts: SyncTypeResolverOptions;

  public constructor(opts: SyncTypeResolverOptions) {
    this._opts = opts;
  }

  /**
   * Determines the sync direction "forward" or "reverse" of a given sourceDb and targetDb by looking for the scoping ESA.
   * If the sourceDb's iModelId is found as the identifier of the expected scoping ESA in the targetDb, then it is a forward synchronization.
   * If the targetDb's iModelId is found as the identifier of the expected scoping ESA in the sourceDb, then it is a reverse synchronization.
   * @throws if no scoping ESA can be found in either the sourceDb or targetDb which describes a master branch relationship between the two databases.
   * @returns "forward" or "reverse"
   */
  public static async determineSyncType(
    sourceDb: IModelDb,
    targetDb: IModelDb,
    targetScopeElementId: Id64String
  ): Promise<"forward" | "reverse"> {
    const aspectProps = {
      id: undefined as string | undefined,
      version: undefined as string | undefined,
      classFullName: ExternalSourceAspect.classFullName,
      element: {
        id: targetScopeElementId,
        relClassName: ElementOwnsExternalSourceAspects.classFullName,
      },
      scope: { id: IModel.rootSubjectId },
      identifier: sourceDb.iModelId,
      kind: ExternalSourceAspect.Kind.Scope,
      jsonProperties: undefined as TargetScopeProvenanceJsonProps | undefined,
    };
    // First check if the targetDb is the branch (branch is the provenanceDb)
    const esaPropsFromTargetDb =
      await ProvenanceManager.queryScopeExternalSourceAspect(
        targetDb,
        aspectProps
      );
    if (esaPropsFromTargetDb !== undefined) {
      return "forward";
    }

    // Now check if the sourceDb is the branch
    aspectProps.identifier = targetDb.iModelId;
    const esaPropsFromSourceDb =
      await ProvenanceManager.queryScopeExternalSourceAspect(
        sourceDb,
        aspectProps
      );

    if (esaPropsFromSourceDb !== undefined) {
      return "reverse";
    }
    throw new Error(SyncTypeResolver.noEsaSyncDirectionErrorMessage);
  }

  private async resolve(): Promise<SyncType> {
    if (this._opts.isProvenanceInitTransform) {
      return "forward";
    }
    if (!this._opts.hasArgsForProcessChanges) {
      return "not-sync";
    }
    try {
      return await SyncTypeResolver.determineSyncType(
        this._opts.sourceDb,
        this._opts.targetDb,
        this._opts.targetScopeElementId
      );
    } catch (err) {
      if (
        err instanceof Error &&
        err.message === SyncTypeResolver.noEsaSyncDirectionErrorMessage &&
        this._opts.allowNoScopingESA
      ) {
        return "forward";
      }
      throw err;
    }
  }

  /** Returns true if this is a reverse synchronization. Lazily resolves the sync type. */
  public async getIsReverseSynchronization(): Promise<boolean> {
    if (this._syncType === undefined) this._syncType = await this.resolve();
    return this._syncType === "reverse";
  }

  /** Returns true if this is a forward synchronization. Lazily resolves the sync type. */
  public async getIsForwardSynchronization(): Promise<boolean> {
    if (this._syncType === undefined) this._syncType = await this.resolve();
    return this._syncType === "forward";
  }

  public get allowNoScopingESA(): boolean {
    return this._opts.allowNoScopingESA ?? false;
  }

  public set allowNoScopingESA(value: boolean) {
    this._opts.allowNoScopingESA = value;
  }

  /** Returns the resolved sync type. Asserts that it has already been resolved. */
  public getResolvedSyncType(): SyncType {
    if (this._syncType === undefined)
      throw new Error(
        "SyncType has not been resolved yet. Call getIsReverseSynchronization() or getIsForwardSynchronization() first."
      );
    return this._syncType;
  }
}
