/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { AccessToken } from "@itwin/core-bentley";
import {
  BriefcaseDb,
  BriefcaseManager,
  EditTxn,
  IModelHost,
  SnapshotDb,
} from "@itwin/core-backend";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
// eslint-disable-next-line @itwin/no-internal
import { _hubAccess } from "@itwin/core-backend/lib/cjs/internal/Symbols";

export interface ReconstructedHub {
  readonly accessToken: AccessToken;
  readonly iTwinId: string;
  readonly sourceDb: BriefcaseDb;
  readonly sourceIModelId: string;
  readonly targetDb: BriefcaseDb;
  readonly targetIModelId: string;
}

export async function createAndOpenIModel(
  accessToken: AccessToken,
  iTwinId: string,
  iModelName: string,
  seedFileName: string
): Promise<{ db: BriefcaseDb; iModelId: string }> {
  const iModelId = await IModelHost[_hubAccess].createNewIModel({
    accessToken,
    iTwinId,
    iModelName,
    noLocks: true,
    version0: seedFileName,
  });
  const briefcase = await BriefcaseManager.downloadBriefcase({
    accessToken,
    iTwinId,
    iModelId,
  });
  try {
    return {
      db: await BriefcaseDb.open({ fileName: briefcase.fileName }),
      iModelId,
    };
  } catch (error) {
    try {
      await BriefcaseManager.deleteBriefcaseFiles(
        briefcase.fileName,
        accessToken
      );
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Briefcase open and cleanup both failed"
      );
    }
    throw error;
  }
}

export function createEmptySeed(
  fileName: string,
  rootSubjectName: string
): void {
  fs.mkdirSync(path.dirname(fileName), { recursive: true });
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: rootSubjectName },
  });
  db.close();
}

export function createStartedEditTxn(db: BriefcaseDb): EditTxn {
  const editTxn = new EditTxn(db, "Quick performance fixture");
  editTxn.start();
  return editTxn;
}

export async function closeAndDeleteBriefcase(
  accessToken: AccessToken,
  db: BriefcaseDb
): Promise<void> {
  const fileName = db.pathName;
  db.close();
  await BriefcaseManager.deleteBriefcaseFiles(fileName, accessToken);
}

async function cleanupHub(
  accessToken: AccessToken,
  briefcases: readonly BriefcaseDb[]
): Promise<unknown[]> {
  const errors: unknown[] = [];
  for (const briefcase of briefcases) {
    try {
      await closeAndDeleteBriefcase(accessToken, briefcase);
    } catch (error) {
      errors.push(error);
    }
  }
  try {
    if (HubMock.isValid) HubMock.shutdown();
  } catch (error) {
    errors.push(error);
  }
  return errors;
}

export async function reconstructHub(
  outputDir: string,
  mockName: string,
  createSourceSeed: (fileName: string) => Promise<void> | void
): Promise<ReconstructedHub> {
  if (HubMock.isValid) throw new Error("Only one HubMock may be active");

  fs.mkdirSync(outputDir, { recursive: true });
  if (!fs.statSync(outputDir).isDirectory())
    throw new Error(`HubMock output path is not a directory: ${outputDir}`);
  try {
    HubMock.startup(mockName, outputDir);
  } catch (error) {
    try {
      if (HubMock.isValid) HubMock.shutdown();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "HubMock startup and cleanup both failed"
      );
    }
    throw error;
  }
  const accessToken = "quick-performance-tests";
  const iTwinId = HubMock.iTwinId;
  const openBriefcases: BriefcaseDb[] = [];
  try {
    const seedDir = path.join(outputDir, "seeds");
    const sourceSeed = path.join(seedDir, `${mockName}-source.bim`);
    const targetSeed = path.join(seedDir, `${mockName}-target.bim`);
    fs.mkdirSync(seedDir, { recursive: true });
    await createSourceSeed(sourceSeed);
    createEmptySeed(targetSeed, `${mockName}-target`);

    const source = await createAndOpenIModel(
      accessToken,
      iTwinId,
      `${mockName}-source`,
      sourceSeed
    );
    openBriefcases.push(source.db);
    const target = await createAndOpenIModel(
      accessToken,
      iTwinId,
      `${mockName}-target`,
      targetSeed
    );
    openBriefcases.push(target.db);
    return {
      accessToken,
      iTwinId,
      sourceDb: source.db,
      sourceIModelId: source.iModelId,
      targetDb: target.db,
      targetIModelId: target.iModelId,
    };
  } catch (error) {
    const cleanupErrors = await cleanupHub(accessToken, openBriefcases);
    if (cleanupErrors.length > 0)
      throw new AggregateError(
        [error, ...cleanupErrors],
        "Hub reconstruction and cleanup both failed"
      );
    throw error;
  }
}

export async function disposeReconstructedHub(
  hub: ReconstructedHub
): Promise<void> {
  const errors = await cleanupHub(hub.accessToken, [
    hub.sourceDb,
    hub.targetDb,
  ]);
  if (errors.length > 0)
    throw new AggregateError(errors, "Failed to dispose reconstructed HubMock");
}
