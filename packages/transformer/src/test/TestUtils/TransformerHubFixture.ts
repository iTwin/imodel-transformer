/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterAll, afterEach, assert, beforeAll, beforeEach, vi } from "vitest";
import * as path from "node:path";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  BriefcaseManager,
  IModelHost,
  IModelJsFs,
  NativeLoggerCategory,
  SnapshotDb,
} from "@itwin/core-backend";
import { AccessToken, GuidString, Logger, LogLevel } from "@itwin/core-bentley";
import { ChangesetFileProps, IModelVersion } from "@itwin/core-common";
import { TransformerLoggerCategory } from "../../imodel-transformer.js";
import { HubWrappers } from "../IModelTransformerUtils.js";
import { IModelTestUtils, TestUserType } from "./IModelTestUtils.js";
import { KnownTestLocations } from "./KnownTestLocations.js";
import { transformerTestHub } from "./TransformerTestHub.js";

/** Hub state and helpers shared by the test files that transform iModels on the local test hub. */
export interface TransformerHubFixture {
  /** Set by the fixture's `beforeAll` hook. */
  readonly iTwinId: GuidString;
  /** Set by the fixture's `beforeAll` hook. */
  readonly accessToken: AccessToken;
  /** A directory for this test file's seed files. */
  readonly outputDir: string;
  /** Creates a hub iModel from an empty snapshot that `prepareIModel` may populate. */
  readonly createPopulatedIModelHubIModel: (
    iModelName: string,
    prepareIModel?: (iModel: SnapshotDb) => void | Promise<void>
  ) => Promise<string>;
  /** Creates a hub iModel and opens a briefcase of it that holds the root and dictionary locks. */
  readonly prepareBriefcase: (name: string) => Promise<BriefcaseDb>;
  /** Closes and deletes a briefcase opened by `prepareBriefcase`, then deletes its hub iModel. */
  readonly closeAndDeleteBriefcase: (iModel: BriefcaseDb) => Promise<void>;
  readonly saveAndPushChanges: (
    db: BriefcaseDb,
    description: string
  ) => Promise<void>;
}

/**
 * Registers hooks that run the enclosing tests against a local test hub named `name`, and returns the hub's state and helpers.
 * Call it once at the top of a test file's root `describe`.
 */
export function useTransformerHub(name: string): TransformerHubFixture {
  const outputDir = path.join(KnownTestLocations.outputDir, name);
  let iTwinId: GuidString | undefined;
  let accessToken: AccessToken | undefined;
  let restoreCheckpointDownload: (() => void) | undefined;

  beforeAll(async () => {
    transformerTestHub.start(name, KnownTestLocations.outputDir);
    iTwinId = transformerTestHub.iTwinId;
    IModelJsFs.recursiveMkDirSync(outputDir);
    accessToken = await HubWrappers.getAccessToken(TestUserType.Regular);

    if (process.env.TRANSFORMER_TESTS_USE_LOG) {
      Logger.initializeToConsole();
      Logger.setLevelDefault(LogLevel.Error);
      Logger.setLevel(TransformerLoggerCategory.IModelExporter, LogLevel.Trace);
      Logger.setLevel(TransformerLoggerCategory.IModelImporter, LogLevel.Trace);
      Logger.setLevel(
        TransformerLoggerCategory.IModelTransformer,
        LogLevel.Trace
      );
      Logger.setLevel(NativeLoggerCategory.Changeset, LogLevel.Trace);
    }
  });

  beforeEach(() => {
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
  });

  afterEach(() => {
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });

  afterAll(() => transformerTestHub.stop());

  const requireITwinId = () => {
    assert(iTwinId !== undefined, "the test hub has not started");
    return iTwinId;
  };
  const requireAccessToken = () => {
    assert(accessToken !== undefined, "the test hub has not started");
    return accessToken;
  };

  return {
    get iTwinId() {
      return requireITwinId();
    },
    get accessToken() {
      return requireAccessToken();
    },
    outputDir,

    createPopulatedIModelHubIModel: async (iModelName, prepareIModel) => {
      const seedFileName = path.join(outputDir, `${iModelName}.bim`);
      if (IModelJsFs.existsSync(seedFileName))
        IModelJsFs.removeSync(seedFileName);

      const seedDb = SnapshotDb.createEmpty(seedFileName, {
        rootSubject: { name: iModelName },
      });
      assert.isTrue(IModelJsFs.existsSync(seedFileName));
      await prepareIModel?.(seedDb);
      seedDb.close();

      return transformerTestHub.createNewIModel({
        iTwinId: requireITwinId(),
        iModelName,
        description: "source",
        version0: seedFileName,
        noLocks: true,
      });
    },

    prepareBriefcase: async (iModelName) => {
      const iTwin = requireITwinId();
      const iModelId = await HubWrappers.createIModel(
        requireAccessToken(),
        iTwin,
        iModelName
      );
      const briefcase = await HubWrappers.downloadAndOpenBriefcase({
        accessToken: await IModelHost.getAccessToken(),
        iTwinId: iTwin,
        iModelId,
        asOf: IModelVersion.latest().toJSON(),
      });
      await briefcase.locks.acquireLocks({
        shared: "0x10",
        exclusive: "0x1",
      });
      return briefcase;
    },

    closeAndDeleteBriefcase: async (iModel) => {
      const iModelId = iModel.iModelId;
      await HubWrappers.closeAndDeleteBriefcaseDb(requireAccessToken(), iModel);
      await transformerTestHub.deleteIModel({
        iTwinId: requireITwinId(),
        iModelId,
      });
    },

    saveAndPushChanges: async (db, description) =>
      IModelTestUtils.saveAndPushChanges(requireAccessToken(), db, description),
  };
}

/** Records the changeset files downloaded until `restore` is called. */
export function recordChangesetDownloads() {
  const files: ChangesetFileProps[] = [];
  const download = BriefcaseManager.downloadChangesets.bind(BriefcaseManager);
  const spy = vi
    .spyOn(BriefcaseManager, "downloadChangesets")
    .mockImplementation(async (args) => {
      const downloaded = await download(args);
      files.push(...downloaded);
      return downloaded;
    });
  return { files, restore: () => spy.mockRestore() };
}
