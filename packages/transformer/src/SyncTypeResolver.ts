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
import type { IModelCloneContext } from "./IModelCloneContext";
import { ProvenanceManager } from "./ProvenanceManager";

/** @internal */
export type SyncType = "not-sync" | "forward" | "reverse";

/**
 * Encapsulates sync direction determination logic.
 * Given a source and target iModel, resolves whether the transformation is
 * a forward sync, reverse sync, or not a sync at all.
 * @internal
 */
export class SyncTypeResolver {
  public static noEsaSyncDirectionErrorMessage =
    "Couldn't find an external source aspect to determine sync direction. This often means that the master->branch relationship has not been established. Consider running the transformer with wasSourceIModelCopiedToTarget set to true.";

  public readonly context: IModelCloneContext;

  private _syncType?: SyncType;
  private readonly _targetScopeElementId: Id64String;
  private readonly _isProvenanceInitTransform: boolean;
  private readonly _isSyncTransform: boolean;

  public constructor(
    context: IModelCloneContext,
    targetScopeElementId: Id64String,
    isProvenanceInitTransform: boolean = false,
    isSyncTransform: boolean = false
  ) {
    this.context = context;
    this._targetScopeElementId = targetScopeElementId;
    this._isProvenanceInitTransform = isProvenanceInitTransform;
    this._isSyncTransform = isSyncTransform;
  }

  /**
   * Determines the sync direction "forward" or "reverse" of a given sourceDb and targetDb by looking for the scoping ESA.
   * If the sourceDb's iModelId is found as the identifier of the expected scoping ESA in the targetDb, then it is a forward synchronization.
   * If the targetDb's iModelId is found as the identifier of the expected scoping ESA in the sourceDb, then it is a reverse synchronization.
   * @note This is a standalone utility that does not depend on instance state.
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

  /** Returns the sync type, lazily resolving it on first call. */
  public async getSyncType(): Promise<SyncType> {
    if (this._syncType === undefined) {
      if (this._isProvenanceInitTransform) {
        this._syncType = "forward";
      } else if (!this._isSyncTransform) {
        this._syncType = "not-sync";
      } else {
        this._syncType = await SyncTypeResolver.determineSyncType(
          this.context.sourceDb,
          this.context.targetDb,
          this._targetScopeElementId
        );
      }
    }
    return this._syncType;
  }

  public async getIsReverseSynchronization(): Promise<boolean> {
    return (await this.getSyncType()) === "reverse";
  }

  public async getIsForwardSynchronization(): Promise<boolean> {
    return (await this.getSyncType()) === "forward";
  }
}
