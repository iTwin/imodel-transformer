/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterAll, afterEach, assert, beforeAll, beforeEach } from "vitest";
import * as path from "node:path";
import { AccessToken, GuidString, Logger, LogLevel } from "@itwin/core-bentley";
import {
  BriefcaseDb,
  IModelHost,
  IModelJsFs,
  NativeLoggerCategory,
  SnapshotDb,
} from "@itwin/core-backend";
import { HubWrappers, IModelTestUtils, TestUserType } from "./IModelTestUtils";
import { KnownTestLocations } from "./KnownTestLocations";
import { transformerTestHub } from "./TransformerTestHub";
import { installCheckpointDownload } from "@itwin/imodel-transformer-test-utils";
import { TransformerLoggerCategory } from "../../TransformerLoggerCategory";
import { IModelVersion } from "@itwin/core-common";

export interface HubTestContext {
  readonly iTwinId: GuidString;
  readonly accessToken: AccessToken;
  readonly saveAndPushChanges: (
    db: BriefcaseDb,
    description: string
  ) => Promise<void>;
}

export async function createPopulatedHubIModel(
  outputDir: string,
  iTwinId: GuidString,
  iModelName: string,
  prepareIModel?: (db: SnapshotDb) => void | Promise<void>
): Promise<GuidString> {
  const seedFileName = path.join(outputDir, `${iModelName}.bim`);
  if (IModelJsFs.existsSync(seedFileName)) IModelJsFs.removeSync(seedFileName);

  const seedDb = SnapshotDb.createEmpty(seedFileName, {
    rootSubject: { name: iModelName },
  });
  assert(IModelJsFs.existsSync(seedFileName));
  try {
    await prepareIModel?.(seedDb);
  } finally {
    seedDb.close();
  }

  return transformerTestHub.createNewIModel({
    iTwinId,
    iModelName,
    description: "source",
    version0: seedFileName,
    noLocks: true,
  });
}

export async function prepareHubBriefcase(
  accessToken: AccessToken,
  iTwinId: GuidString,
  iModelName: string
): Promise<BriefcaseDb> {
  const iModelId = await HubWrappers.createIModel(
    accessToken,
    iTwinId,
    iModelName
  );
  let briefcase: BriefcaseDb | undefined;
  try {
    briefcase = await HubWrappers.downloadAndOpenBriefcase({
      accessToken: await IModelHost.getAccessToken(),
      iTwinId,
      iModelId,
      asOf: IModelVersion.latest().toJSON(),
    });
    await briefcase.locks.acquireLocks({
      shared: "0x10",
      exclusive: "0x1",
    });
    return briefcase;
  } catch (error) {
    if (briefcase)
      await closeAndDeleteHubBriefcase(accessToken, iTwinId, briefcase);
    else await transformerTestHub.deleteIModel({ iTwinId, iModelId });
    throw error;
  }
}

export async function closeAndDeleteHubBriefcase(
  accessToken: AccessToken,
  iTwinId: GuidString,
  briefcase: BriefcaseDb
): Promise<void> {
  try {
    await HubWrappers.closeAndDeleteBriefcaseDb(accessToken, briefcase);
  } finally {
    // eslint-disable-next-line @itwin/no-internal
    await transformerTestHub.deleteIModel({
      iTwinId,
      iModelId: briefcase.iModelId,
    });
  }
}

/** Register the common local-hub lifecycle for a standalone hub test file. */
export function registerHubTestContext(
  suiteName: string,
  assignContext: (context: HubTestContext) => void
): string {
  const outputDir = path.join(KnownTestLocations.outputDir, suiteName);
  beforeAll(async () => {
    transformerTestHub.start(suiteName, KnownTestLocations.outputDir);
    IModelJsFs.recursiveMkDirSync(outputDir);

    const accessToken = await HubWrappers.getAccessToken(TestUserType.Regular);
    const iTwinId = transformerTestHub.iTwinId;
    const context: HubTestContext = {
      iTwinId,
      accessToken,
      saveAndPushChanges: async (db, description) =>
        IModelTestUtils.saveAndPushChanges(accessToken, db, description),
    };
    assignContext(context);

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

  let restoreCheckpointDownload: (() => void) | undefined;
  beforeEach(() => {
    restoreCheckpointDownload = installCheckpointDownload(transformerTestHub);
  });

  afterEach(() => {
    restoreCheckpointDownload?.();
    restoreCheckpointDownload = undefined;
  });

  afterAll(() => transformerTestHub.stop());

  return outputDir;
}
