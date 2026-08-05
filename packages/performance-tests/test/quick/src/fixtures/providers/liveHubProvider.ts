/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { BriefcaseManager } from "@itwin/core-backend";
import { ChangesetFileProps } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
import {
  artifactBriefcaseFileName,
  artifactChangesetDirectoryName,
  artifactChangesetPropsFileName,
  artifactSourceSeedFileName,
  artifactTargetBriefcaseFileName,
  artifactTargetChangesetDirectoryName,
  artifactTargetChangesetPropsFileName,
  artifactTargetSeedFileName,
  changesetArtifactFileName,
  FixtureArtifactChangesetManifest,
  fixtureArtifactContentHash,
  FixtureArtifactManifest,
  fixtureArtifactVersion,
  readChangesetFileProps,
  readFixtureArtifact,
  toRelativeChangesetProps,
  writeFixtureArtifactManifest,
} from "../FixtureArtifact.js";
import {
  BuiltFixture,
  FixtureProvider,
  PreparedDataset,
  requireFixtureArtifact,
  requireLiveHubDataset,
} from "../FixtureProvider.js";
import { ConfiguredFixture } from "../FixtureRecipe.js";
import {
  createStartedEditTxn,
  disposeReconstructedHub,
  ReconstructedHub,
  reconstructHub,
  restoreHub,
  stopQuickTestHub,
} from "../LocalHubFixture.js";

const fixtureAccessToken = "quick-performance-tests";

interface CapturedTimeline {
  readonly changesets: readonly ChangesetFileProps[];
  readonly manifest: FixtureArtifactChangesetManifest;
}

async function captureTimeline(
  accessToken: string,
  iModelId: string,
  artifactDir: string,
  scratchDir: string,
  changesetDirectory: string,
  propsFile: string
): Promise<CapturedTimeline> {
  fs.mkdirSync(scratchDir, { recursive: true });
  const changesets = await BriefcaseManager.downloadChangesets({
    accessToken,
    iModelId,
    range: { first: 1 },
    targetDir: scratchDir,
  });
  const destination = path.join(artifactDir, changesetDirectory);
  fs.mkdirSync(destination, { recursive: true });
  for (const changeset of changesets)
    fs.copyFileSync(
      changeset.pathname,
      path.join(destination, changesetArtifactFileName(changeset))
    );
  fs.writeFileSync(
    path.join(artifactDir, propsFile),
    `${JSON.stringify(
      toRelativeChangesetProps(changesets, changesetDirectory),
      undefined,
      2
    )}\n`
  );
  const indices = changesets.map((changeset) => changeset.index);
  return {
    changesets,
    manifest: {
      directory: changesetDirectory,
      propsFile,
      count: changesets.length,
      baseChangesetIndex: 0,
      firstIndex: indices.length > 0 ? Math.min(...indices) : undefined,
      lastIndex: indices.length > 0 ? Math.max(...indices) : undefined,
    },
  };
}

async function releaseBuildHub(
  hub: ReconstructedHub,
  sourceClosed: boolean,
  targetClosed: boolean,
  sourceFileName?: string,
  targetFileName?: string
): Promise<unknown[]> {
  const errors: unknown[] = [];
  for (const [db, closed, capturedFileName] of [
    [hub.sourceDb, sourceClosed, sourceFileName],
    [hub.targetDb, targetClosed, targetFileName],
  ] as const) {
    let fileName = capturedFileName;
    if (!closed && fileName === undefined) {
      try {
        fileName = db.pathName;
      } catch (error) {
        errors.push(error);
      }
    }
    if (!closed) {
      try {
        db.close();
      } catch (error) {
        errors.push(error);
      }
    }
    try {
      if (fileName && fs.existsSync(fileName))
        await BriefcaseManager.deleteBriefcaseFiles(fileName, hub.accessToken);
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

/** Captures a prepared live source/target hub once and restores a private copy per sample. */
export const liveHubFixtureProvider: FixtureProvider = {
  async build(
    fixture: ConfiguredFixture,
    artifactDir: string
  ): Promise<BuiltFixture> {
    const { descriptor } = fixture;
    const start = process.hrtime.bigint();
    fs.rmSync(artifactDir, { recursive: true, force: true });
    fs.mkdirSync(artifactDir, { recursive: true });

    const buildHubDir = path.join(artifactDir, ".build-hub");
    const sourceScratchDir = path.join(artifactDir, ".source-changesets");
    const targetScratchDir = path.join(artifactDir, ".target-changesets");
    const mockName = `quick-artifact-${descriptor.id}`;
    let hub: ReconstructedHub | undefined;
    let sourceBriefcasePath: string | undefined;
    let sourceClosed = false;
    let targetBriefcasePath: string | undefined;
    let targetClosed = false;
    let completed = false;
    try {
      let recipeState: unknown;
      hub = await reconstructHub(buildHubDir, mockName, async (sourceSeed) => {
        recipeState = await fixture.createSeed(sourceSeed);
      });
      sourceBriefcasePath = hub.sourceDb.pathName;
      targetBriefcasePath = hub.targetDb.pathName;

      const editTxn = createStartedEditTxn(hub.targetDb);
      const transformer = new IModelTransformer({
        source: hub.sourceDb,
        target: editTxn,
      });
      try {
        await transformer.processSchemas();
        await transformer.process();
      } finally {
        transformer.dispose();
        if (editTxn.isActive) editTxn.end();
      }
      await hub.targetDb.pushChanges({
        accessToken: hub.accessToken,
        description: "establish quick fixture provenance",
      });
      await fixture.applySourceChangesets(
        hub.sourceDb,
        hub.accessToken,
        recipeState
      );
      await fixture.validate?.(hub.sourceDb);

      const sourceTimeline = await captureTimeline(
        hub.accessToken,
        hub.sourceIModelId,
        artifactDir,
        sourceScratchDir,
        artifactChangesetDirectoryName,
        artifactChangesetPropsFileName
      );
      const targetTimeline = await captureTimeline(
        hub.accessToken,
        hub.targetIModelId,
        artifactDir,
        targetScratchDir,
        artifactTargetChangesetDirectoryName,
        artifactTargetChangesetPropsFileName
      );
      const sourceBriefcase = {
        fileName: artifactBriefcaseFileName,
        briefcaseId: hub.sourceDb.briefcaseId,
        changeset: hub.sourceDb.changeset,
      };
      const targetBriefcase = {
        fileName: artifactTargetBriefcaseFileName,
        briefcaseId: hub.targetDb.briefcaseId,
        changeset: hub.targetDb.changeset,
      };
      const liveHubIdentity = {
        iTwinId: hub.iTwinId,
        sourceIModelId: hub.sourceIModelId,
        targetIModelId: hub.targetIModelId,
      };
      hub.sourceDb.close();
      sourceClosed = true;
      hub.targetDb.close();
      targetClosed = true;
      fs.copyFileSync(
        sourceBriefcasePath,
        path.join(artifactDir, sourceBriefcase.fileName)
      );
      fs.copyFileSync(
        targetBriefcasePath,
        path.join(artifactDir, targetBriefcase.fileName)
      );
      fs.copyFileSync(
        path.join(buildHubDir, "seeds", `${mockName}-source.bim`),
        path.join(artifactDir, artifactSourceSeedFileName)
      );
      fs.copyFileSync(
        path.join(buildHubDir, "seeds", `${mockName}-target.bim`),
        path.join(artifactDir, artifactTargetSeedFileName)
      );

      const releaseErrors = await releaseBuildHub(
        hub,
        sourceClosed,
        targetClosed,
        sourceBriefcasePath,
        targetBriefcasePath
      );
      hub = undefined;
      if (releaseErrors.length > 0)
        throw new AggregateError(
          releaseErrors,
          "Failed to release the live fixture build hub"
        );
      fs.rmSync(buildHubDir, { recursive: true, force: true });
      fs.rmSync(sourceScratchDir, { recursive: true, force: true });
      fs.rmSync(targetScratchDir, { recursive: true, force: true });

      const buildMilliseconds =
        Number(process.hrtime.bigint() - start) / 1_000_000;
      const manifest: FixtureArtifactManifest = {
        artifactVersion: fixtureArtifactVersion,
        contentHash: fixtureArtifactContentHash(artifactDir),
        descriptor,
        briefcase: {
          ...sourceBriefcase,
          byteLength: fs.statSync(
            path.join(artifactDir, sourceBriefcase.fileName)
          ).size,
        },
        changesets: sourceTimeline.manifest,
        liveHub: {
          iTwinId: liveHubIdentity.iTwinId,
          source: {
            iModelId: liveHubIdentity.sourceIModelId,
            iModelName: `${mockName}-source`,
            version0File: artifactSourceSeedFileName,
          },
          target: {
            briefcase: {
              ...targetBriefcase,
              byteLength: fs.statSync(
                path.join(artifactDir, targetBriefcase.fileName)
              ).size,
            },
            changesets: targetTimeline.manifest,
            iModelId: liveHubIdentity.targetIModelId,
            iModelName: `${mockName}-target`,
            version0File: artifactTargetSeedFileName,
          },
        },
        buildMilliseconds,
        builtAt: new Date().toISOString(),
      };
      writeFixtureArtifactManifest(artifactDir, manifest);
      const result = {
        fixture,
        descriptor,
        directory: artifactDir,
        buildMilliseconds,
        artifact: readFixtureArtifact(artifactDir),
      };
      completed = true;
      return result;
    } catch (error) {
      if (hub) {
        const cleanupErrors = await releaseBuildHub(
          hub,
          sourceClosed,
          targetClosed,
          sourceBriefcasePath,
          targetBriefcasePath
        );
        if (cleanupErrors.length > 0)
          throw new AggregateError(
            [error, ...cleanupErrors],
            "Live fixture artifact build and cleanup both failed"
          );
      }
      throw error;
    } finally {
      fs.rmSync(buildHubDir, { recursive: true, force: true });
      fs.rmSync(sourceScratchDir, { recursive: true, force: true });
      fs.rmSync(targetScratchDir, { recursive: true, force: true });
      if (!completed) fs.rmSync(artifactDir, { recursive: true, force: true });
    }
  },

  async materialize(
    built: BuiltFixture,
    sampleDir: string,
    sampleName: string
  ): Promise<PreparedDataset> {
    const artifact = requireFixtureArtifact(built);
    const { liveHub } = artifact.manifest;
    if (!liveHub)
      throw new Error(
        `Fixture "${built.descriptor.id}" does not contain a live hub artifact`
      );
    const start = process.hrtime.bigint();
    fs.rmSync(sampleDir, { recursive: true, force: true });
    fs.mkdirSync(path.dirname(sampleDir), { recursive: true });
    fs.cpSync(artifact.directory, sampleDir, { recursive: true });

    const sourceChangesets = readChangesetFileProps(
      sampleDir,
      artifact.manifest.changesets.propsFile
    );
    const targetChangesets = readChangesetFileProps(
      sampleDir,
      liveHub.target.changesets.propsFile
    );
    const hub = await restoreHub(
      sampleDir,
      sampleName,
      {
        accessToken: fixtureAccessToken,
        iTwinId: liveHub.iTwinId,
        iModels: [
          {
            briefcaseIds: [
              artifact.manifest.briefcase.briefcaseId,
              ...sourceChangesets.map((changeset) => changeset.briefcaseId),
            ],
            changesets: sourceChangesets,
            iModelId: liveHub.source.iModelId,
            iModelName: liveHub.source.iModelName,
            version0: path.join(sampleDir, liveHub.source.version0File),
          },
          {
            briefcaseIds: [
              liveHub.target.briefcase.briefcaseId,
              ...targetChangesets.map((changeset) => changeset.briefcaseId),
            ],
            changesets: targetChangesets,
            iModelId: liveHub.target.iModelId,
            iModelName: liveHub.target.iModelName,
            version0: path.join(sampleDir, liveHub.target.version0File),
          },
        ],
      },
      path.join(sampleDir, artifact.manifest.briefcase.fileName),
      path.join(sampleDir, liveHub.target.briefcase.fileName)
    );
    return {
      topology: "source-and-empty-target",
      descriptor: built.descriptor,
      hub,
      reconstructionMilliseconds:
        Number(process.hrtime.bigint() - start) / 1_000_000,
    };
  },

  async disposeSample(dataset: PreparedDataset): Promise<void> {
    await disposeReconstructedHub(requireLiveHubDataset(dataset).hub);
  },

  async disposeBuild(built: BuiltFixture): Promise<void> {
    fs.rmSync(built.directory, { recursive: true, force: true });
  },
};

export async function materializeLiveHubFixture(
  fixture: ConfiguredFixture,
  sampleDir: string,
  sampleName: string
): Promise<PreparedDataset> {
  const artifactDir = path.join(sampleDir, ".fixture-artifact");
  const built = await liveHubFixtureProvider.build(fixture, artifactDir);
  return liveHubFixtureProvider.materialize(
    built,
    path.join(sampleDir, "working"),
    sampleName
  );
}
