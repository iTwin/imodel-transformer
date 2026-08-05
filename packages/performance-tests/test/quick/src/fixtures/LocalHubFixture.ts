/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { AccessToken } from "@itwin/core-bentley";
import {
  installCheckpointDownload,
  LocalTestHubSnapshot,
} from "@itwin/imodel-transformer-test-utils";
import {
  BriefcaseDb,
  BriefcaseManager,
  EditTxn,
  SnapshotDb,
} from "@itwin/core-backend";
import { quickTestHub } from "./QuickTestHub.js";

export interface ReconstructedSourceHub {
  readonly accessToken: AccessToken;
  readonly iTwinId: string;
  readonly sourceDb: BriefcaseDb;
  readonly sourceIModelId: string;
}

export interface ReconstructedHub extends ReconstructedSourceHub {
  readonly targetDb: BriefcaseDb;
  readonly targetIModelId: string;
}

export async function createAndOpenIModel(
  accessToken: AccessToken,
  iTwinId: string,
  iModelName: string,
  seedFileName: string
): Promise<{ db: BriefcaseDb; iModelId: string }> {
  const iModelId = await quickTestHub.createNewIModel({
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

let restoreCheckpointDownload: (() => void) | undefined;

function startQuickTestHub(name: string, outputDir: string): void {
  quickTestHub.start(name, outputDir);
  try {
    restoreCheckpointDownload = installCheckpointDownload(quickTestHub);
  } catch (error) {
    quickTestHub.stop();
    throw error;
  }
}

export function stopQuickTestHub(): void {
  const errors: unknown[] = [];
  try {
    restoreCheckpointDownload?.();
  } catch (error) {
    errors.push(error);
  } finally {
    restoreCheckpointDownload = undefined;
  }

  try {
    if (quickTestHub.isActive) quickTestHub.stop();
  } catch (error) {
    errors.push(error);
  }

  if (errors.length > 0)
    throw new AggregateError(errors, "Failed to stop quick test hub");
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
    stopQuickTestHub();
  } catch (error) {
    errors.push(error);
  }
  return errors;
}

async function reconstruct<T extends ReconstructedSourceHub>(
  outputDir: string,
  mockName: string,
  createSourceSeed: (fileName: string) => Promise<void> | void,
  finish: (
    context: {
      accessToken: AccessToken;
      iTwinId: string;
      seedDir: string;
      track: (db: BriefcaseDb) => void;
    },
    source: { db: BriefcaseDb; iModelId: string }
  ) => Promise<T>
): Promise<T> {
  if (quickTestHub.isActive)
    throw new Error("Only one quick test hub may be active");

  fs.mkdirSync(outputDir, { recursive: true });
  if (!fs.statSync(outputDir).isDirectory())
    throw new Error(
      `Quick test hub output path is not a directory: ${outputDir}`
    );
  try {
    startQuickTestHub(mockName, outputDir);
  } catch (error) {
    try {
      stopQuickTestHub();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Quick test hub startup and cleanup both failed"
      );
    }
    throw error;
  }
  const accessToken = "quick-performance-tests";
  const iTwinId = quickTestHub.iTwinId;
  const openBriefcases: BriefcaseDb[] = [];
  try {
    const seedDir = path.join(outputDir, "seeds");
    const sourceSeed = path.join(seedDir, `${mockName}-source.bim`);
    fs.mkdirSync(seedDir, { recursive: true });
    await createSourceSeed(sourceSeed);

    const source = await createAndOpenIModel(
      accessToken,
      iTwinId,
      `${mockName}-source`,
      sourceSeed
    );
    openBriefcases.push(source.db);
    return await finish(
      {
        accessToken,
        iTwinId,
        seedDir,
        track: (db) => openBriefcases.push(db),
      },
      source
    );
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

/** Start a local test hub holding only a source iModel. Used by artifact-backed topologies. */
export async function reconstructSourceHub(
  outputDir: string,
  mockName: string,
  createSourceSeed: (fileName: string) => Promise<void> | void
): Promise<ReconstructedSourceHub> {
  return reconstruct(
    outputDir,
    mockName,
    createSourceSeed,
    async ({ accessToken, iTwinId }, source) => ({
      accessToken,
      iTwinId,
      sourceDb: source.db,
      sourceIModelId: source.iModelId,
    })
  );
}

/** Start a local test hub holding a source iModel and an empty target iModel. */
export async function reconstructHub(
  outputDir: string,
  mockName: string,
  createSourceSeed: (fileName: string) => Promise<void> | void
): Promise<ReconstructedHub> {
  return reconstruct(
    outputDir,
    mockName,
    createSourceSeed,
    async ({ accessToken, iTwinId, seedDir, track }, source) => {
      const targetSeed = path.join(seedDir, `${mockName}-target.bim`);
      createEmptySeed(targetSeed, `${mockName}-target`);
      const target = await createAndOpenIModel(
        accessToken,
        iTwinId,
        `${mockName}-target`,
        targetSeed
      );
      track(target.db);
      return {
        accessToken,
        iTwinId,
        sourceDb: source.db,
        sourceIModelId: source.iModelId,
        targetDb: target.db,
        targetIModelId: target.iModelId,
      };
    }
  );
}

export async function restoreHub(
  outputDir: string,
  mockName: string,
  snapshot: LocalTestHubSnapshot,
  sourceBriefcaseFileName: string,
  targetBriefcaseFileName: string
): Promise<ReconstructedHub> {
  if (quickTestHub.isActive)
    throw new Error("Only one quick test hub may be active");
  quickTestHub.restore(mockName, outputDir, snapshot);
  try {
    restoreCheckpointDownload = installCheckpointDownload(quickTestHub);
  } catch (error) {
    try {
      stopQuickTestHub();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Quick test hub restoration and cleanup both failed"
      );
    }
    throw error;
  }

  const sourceSnapshot = snapshot.iModels[0];
  const targetSnapshot = snapshot.iModels[1];
  if (!sourceSnapshot || !targetSnapshot) {
    const snapshotError = new Error(
      "A restored incremental fixture requires source and target iModels"
    );
    try {
      stopQuickTestHub();
    } catch (cleanupError) {
      throw new AggregateError(
        [snapshotError, cleanupError],
        "Restored hub validation and cleanup both failed"
      );
    }
    throw snapshotError;
  }

  const opened: BriefcaseDb[] = [];
  try {
    const sourceDb = await BriefcaseDb.open({
      fileName: sourceBriefcaseFileName,
    });
    opened.push(sourceDb);
    const targetDb = await BriefcaseDb.open({
      fileName: targetBriefcaseFileName,
    });
    opened.push(targetDb);
    return {
      accessToken: snapshot.accessToken,
      iTwinId: snapshot.iTwinId,
      sourceDb,
      sourceIModelId: sourceSnapshot.iModelId,
      targetDb,
      targetIModelId: targetSnapshot.iModelId,
    };
  } catch (error) {
    const cleanupErrors = await cleanupHub(snapshot.accessToken, opened);
    if (cleanupErrors.length > 0)
      throw new AggregateError(
        [error, ...cleanupErrors],
        "Restored hub opening and cleanup both failed"
      );
    throw error;
  }
}

export async function disposeReconstructedHub(
  hub: ReconstructedHub | ReconstructedSourceHub
): Promise<void> {
  const briefcases: BriefcaseDb[] = [hub.sourceDb];
  if ("targetDb" in hub) briefcases.push(hub.targetDb);
  const errors = await cleanupHub(hub.accessToken, briefcases);
  if (errors.length > 0)
    throw new AggregateError(
      errors,
      "Failed to dispose reconstructed quick test hub"
    );
}
