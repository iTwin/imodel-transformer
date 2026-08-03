/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
/* eslint-disable @itwin/no-internal -- Core has no public seam for constructing an offline hub or supplying its checkpoint. */
import {
  BackendHubAccess,
  BriefcaseManager,
  CheckpointManager,
  DownloadRequest,
  IModelHost,
  IModelJsFs,
  LocalHub,
  SnapshotDb,
} from "@itwin/core-backend";
import { Guid, GuidString } from "@itwin/core-bentley";

/** Stateful local hub behavior shared by transformer and performance tests. */
export class LocalTestHub implements BackendHubAccess {
  private readonly _hubs = new Map<GuidString, LocalHub>();
  private _rootDir?: string;
  private _iTwinId?: GuidString;

  public constructor(private readonly _directoryName: string) {}

  public get isActive(): boolean {
    return this._rootDir !== undefined;
  }

  public get iTwinId(): GuidString {
    if (!this._iTwinId) throw new Error(`${this._directoryName} is not active`);
    return this._iTwinId;
  }

  public start(name: string, outputDir: string): void {
    if (this.isActive)
      throw new Error(`${this._directoryName} is already active`);

    this._rootDir = path.join(outputDir, this._directoryName, name);
    IModelJsFs.recursiveMkDirSync(this._rootDir);
    IModelJsFs.purgeDirSync(this._rootDir);
    this._iTwinId = Guid.createValue();
  }

  public stop(): void {
    if (!this._rootDir) return;

    const errors: unknown[] = [];
    for (const hub of this._hubs.values()) {
      try {
        hub.cleanup();
      } catch (error) {
        errors.push(error);
      }
    }
    this._hubs.clear();
    try {
      IModelJsFs.purgeDirSync(this._rootDir);
      IModelJsFs.removeSync(this._rootDir);
    } catch (error) {
      errors.push(error);
    } finally {
      this._rootDir = undefined;
      this._iTwinId = undefined;
    }

    if (errors.length > 0)
      throw new AggregateError(errors, `Failed to stop ${this._directoryName}`);
  }

  private get _rootDirName(): string {
    if (!this._rootDir) throw new Error(`${this._directoryName} is not active`);
    return this._rootDir;
  }

  private findHub(iModelId: GuidString): LocalHub {
    const hub = this._hubs.get(iModelId);
    if (!hub)
      throw new Error(`Local hub for iModel ${iModelId} was not created`);
    return hub;
  }

  private changesetIndexFromArg(
    arg: Parameters<BackendHubAccess["queryChangeset"]>[0]
  ): number {
    return (
      arg.changeset.index ??
      this.findHub(arg.iModelId).getChangesetIndex(arg.changeset.id)
    );
  }

  private async getAccessToken(accessToken?: string): Promise<string> {
    return accessToken ?? IModelHost.getAccessToken();
  }

  /** Supplies the checkpoint file needed by BriefcaseManager. */
  public async downloadCheckpoint(request: DownloadRequest): Promise<void> {
    const hub = this.findHub(request.checkpoint.iModelId);
    const requestedIndex = hub.getIndexFromChangeset(
      request.checkpoint.changeset
    );
    const checkpointIndex = hub.queryPreviousCheckpoint(requestedIndex);
    hub.downloadCheckpoint({
      changeset: hub.getChangesetByIndex(checkpointIndex),
      targetFile: request.localFile,
    });

    if (checkpointIndex !== requestedIndex) {
      const db = SnapshotDb.openForApplyChangesets(request.localFile);
      try {
        await BriefcaseManager.pullAndApplyChangesets(db, {
          accessToken: "",
          toIndex: requestedIndex,
        });
      } finally {
        db.close();
      }
    }
  }

  public readonly createNewIModel: BackendHubAccess["createNewIModel"] = async (
    arg
  ) => {
    if (arg.iTwinId !== this.iTwinId)
      throw new Error(`Unknown iTwin ${arg.iTwinId}`);

    const iModelId = Guid.createValue();
    const hub = new LocalHub(path.join(this._rootDirName, iModelId), {
      ...arg,
      iModelId,
    });
    this._hubs.set(iModelId, hub);
    return iModelId;
  };

  public readonly deleteIModel: BackendHubAccess["deleteIModel"] = async (
    arg
  ) => {
    const hub = this.findHub(arg.iModelId);
    hub.cleanup();
    this._hubs.delete(arg.iModelId);
  };

  public readonly queryIModelByName: BackendHubAccess["queryIModelByName"] =
    async (arg) => {
      for (const hub of this._hubs.values()) {
        if (hub.iTwinId === arg.iTwinId && hub.iModelName === arg.iModelName)
          return hub.iModelId;
      }
      return undefined;
    };

  public readonly getChangesetFromNamedVersion: BackendHubAccess["getChangesetFromNamedVersion"] =
    async (arg) => this.findHub(arg.iModelId).findNamedVersion(arg.versionName);

  public readonly getChangesetFromVersion: BackendHubAccess["getChangesetFromVersion"] =
    async (arg) => {
      const hub = this.findHub(arg.iModelId);
      const version = arg.version;
      if (version.isFirst) return hub.getChangesetByIndex(0);

      const asOf = version.getAsOfChangeSet();
      if (asOf) return hub.getChangesetById(asOf);

      const versionName = version.getName();
      if (versionName) return hub.findNamedVersion(versionName);

      return hub.getLatestChangeset();
    };

  public readonly getLatestChangeset: BackendHubAccess["getLatestChangeset"] =
    async (arg) => this.findHub(arg.iModelId).getLatestChangeset();

  public readonly getMyBriefcaseIds: BackendHubAccess["getMyBriefcaseIds"] =
    async (arg) =>
      this.findHub(arg.iModelId).getBriefcaseIds(
        await this.getAccessToken(arg.accessToken)
      );

  public readonly acquireNewBriefcaseId: BackendHubAccess["acquireNewBriefcaseId"] =
    async (arg) =>
      this.findHub(arg.iModelId).acquireNewBriefcaseId(
        await this.getAccessToken(arg.accessToken),
        arg.briefcaseAlias
      );

  public readonly releaseBriefcase: BackendHubAccess["releaseBriefcase"] =
    async (arg) => {
      this.findHub(arg.iModelId).releaseBriefcaseId(arg.briefcaseId);
    };

  public readonly downloadChangeset: BackendHubAccess["downloadChangeset"] =
    async (arg) =>
      this.findHub(arg.iModelId).downloadChangeset({
        index: this.changesetIndexFromArg(arg),
        targetDir: arg.targetDir,
      });

  public readonly downloadChangesets: BackendHubAccess["downloadChangesets"] =
    async (arg) =>
      this.findHub(arg.iModelId).downloadChangesets({
        range: arg.range,
        targetDir: arg.targetDir,
      });

  public readonly queryChangeset: BackendHubAccess["queryChangeset"] = async (
    arg
  ) =>
    this.findHub(arg.iModelId).getChangesetByIndex(
      this.changesetIndexFromArg(arg)
    );

  public readonly queryChangesets: BackendHubAccess["queryChangesets"] = async (
    arg
  ) => this.findHub(arg.iModelId).queryChangesets(arg.range);

  public readonly pushChangeset: BackendHubAccess["pushChangeset"] = async (
    arg
  ) => this.findHub(arg.iModelId).addChangeset(arg.changesetProps);

  public readonly queryV2Checkpoint: BackendHubAccess["queryV2Checkpoint"] =
    async () => undefined;

  public readonly acquireLocks: BackendHubAccess["acquireLocks"] = async (
    arg,
    locks
  ) => this.findHub(arg.iModelId).acquireLocks(locks, arg);

  public readonly abandonLocks: NonNullable<BackendHubAccess["abandonLocks"]> =
    async (arg, locks) => this.findHub(arg.iModelId).abandonLocks(locks, arg);

  public readonly queryAllLocks: BackendHubAccess["queryAllLocks"] = async (
    arg
  ) => this.findHub(arg.iModelId).queryAllLocks(arg.briefcaseId);

  public readonly releaseAllLocks: BackendHubAccess["releaseAllLocks"] = async (
    arg
  ) => {
    const hub = this.findHub(arg.iModelId);
    hub.releaseAllLocks({
      briefcaseId: arg.briefcaseId,
      changesetIndex: hub.getIndexFromChangeset(arg.changeset),
    });
  };

  public readonly abandonAllLocks: NonNullable<
    BackendHubAccess["abandonAllLocks"]
  > = async (arg) => this.findHub(arg.iModelId).abandonAllLocks(arg);
}

/**
 * Route offline checkpoint downloads to a local test hub until the returned function is called.
 * Restoration is idempotent and never overwrites a replacement installed by another owner.
 */
export function installCheckpointDownload(hub: LocalTestHub): () => void {
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Preserve the exact function another owner installed so restoration does not replace it with a bound wrapper.
  const previous = CheckpointManager.downloadCheckpoint;
  const replacement = async (request: DownloadRequest) =>
    hub.downloadCheckpoint(request);
  CheckpointManager.downloadCheckpoint = replacement;

  let installed = true;
  return () => {
    if (!installed) return;
    installed = false;
    if (CheckpointManager.downloadCheckpoint !== replacement)
      throw new Error(
        "CheckpointManager.downloadCheckpoint changed before the local test hub was restored"
      );
    CheckpointManager.downloadCheckpoint = previous;
  };
}
/* eslint-enable @itwin/no-internal */
