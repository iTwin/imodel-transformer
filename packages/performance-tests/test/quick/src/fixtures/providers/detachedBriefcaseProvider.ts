/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { BriefcaseDb, BriefcaseManager } from "@itwin/core-backend";
import {
  artifactBriefcaseFileName,
  artifactBriefcasePath,
  artifactChangesetDirectoryName,
  artifactChangesetPropsFileName,
  changesetArtifactFileName,
  FixtureArtifactManifest,
  fixtureArtifactVersion,
  readChangesetFileProps,
  readFixtureArtifact,
  readFixtureRecipeData,
  toRelativeChangesetProps,
  writeFixtureArtifactManifest,
  writeFixtureRecipeData,
} from "../FixtureArtifact.js";
import {
  BuiltFixture,
  FixtureProvider,
  PreparedDataset,
  requireDetachedDataset,
  requireFixtureArtifact,
} from "../FixtureProvider.js";
import { ConfiguredFixture } from "../FixtureRecipe.js";
import {
  ReconstructedSourceHub,
  reconstructSourceHub,
  shutdownHubMock,
} from "../LocalHubFixture.js";

/**
 * Tolerant teardown for a build that may have failed at any point. Collects errors rather than
 * throwing so an originating error is never masked.
 *
 * `briefcaseClosed` distinguishes the success path — which closes the briefcase early so its bytes
 * can be copied — from a failure that left it open.
 */
async function releaseBuildHub(
  hub: ReconstructedSourceHub,
  briefcaseFileName: string | undefined,
  briefcaseClosed: boolean
): Promise<unknown[]> {
  const errors: unknown[] = [];
  if (!briefcaseClosed) {
    try {
      hub.sourceDb.close();
    } catch (error) {
      errors.push(error);
    }
  }
  try {
    if (briefcaseFileName && fs.existsSync(briefcaseFileName))
      await BriefcaseManager.deleteBriefcaseFiles(
        briefcaseFileName,
        hub.accessToken
      );
  } catch (error) {
    errors.push(error);
  }
  try {
    shutdownHubMock();
  } catch (error) {
    errors.push(error);
  }
  return errors;
}

/**
 * The `source-only` topology: a source briefcase plus its pushed changeset files, with no target
 * and no hub at measure time. Stage 1 does all the expensive changeset generation exactly once;
 * stage 2 is a filesystem copy.
 *
 * The captured bytes are relocatable: `BriefcaseDb.open` makes no hub calls, and
 * `ChangedInstanceIds.initialize` accepts `csFileProps` without one.
 */
export const detachedBriefcaseFixtureProvider: FixtureProvider = {
  async build(
    fixture: ConfiguredFixture,
    artifactDir: string
  ): Promise<BuiltFixture> {
    const { descriptor } = fixture;
    const start = process.hrtime.bigint();
    fs.rmSync(artifactDir, { recursive: true, force: true });
    fs.mkdirSync(artifactDir, { recursive: true });

    const hubDir = path.join(artifactDir, ".build-hub");
    const scratchDir = path.join(artifactDir, ".build-changesets");
    const changesetDir = path.join(artifactDir, artifactChangesetDirectoryName);

    let hub: ReconstructedSourceHub | undefined;
    let briefcaseFileName: string | undefined;
    let briefcaseClosed = false;
    try {
      let recipeState: unknown;
      hub = await reconstructSourceHub(
        hubDir,
        `quick-artifact-${descriptor.id}`,
        async (sourceSeed) => {
          recipeState = await fixture.createSeed(sourceSeed);
        }
      );
      briefcaseFileName = hub.sourceDb.pathName;
      // The seed is uploaded as version 0, so the briefcase starts before any changeset.
      const baseChangesetIndex = hub.sourceDb.changeset.index ?? 0;

      const recipeData = await fixture.applySourceChangesets(
        hub.sourceDb,
        hub.accessToken,
        recipeState
      );
      await fixture.validate?.(hub.sourceDb);

      fs.mkdirSync(scratchDir, { recursive: true });
      const downloaded = await BriefcaseManager.downloadChangesets({
        accessToken: hub.accessToken,
        iModelId: hub.sourceIModelId,
        range: { first: baseChangesetIndex + 1 },
        targetDir: scratchDir,
      });

      const briefcaseId = hub.sourceDb.briefcaseId;
      const briefcaseChangeset = hub.sourceDb.changeset;
      // Close before copying so every page is flushed into the file we capture.
      hub.sourceDb.close();
      briefcaseClosed = true;
      const destination = artifactBriefcasePath(artifactDir);
      fs.copyFileSync(briefcaseFileName, destination);

      fs.mkdirSync(changesetDir, { recursive: true });
      for (const changeset of downloaded)
        fs.copyFileSync(
          changeset.pathname,
          path.join(changesetDir, changesetArtifactFileName(changeset))
        );
      fs.writeFileSync(
        path.join(artifactDir, artifactChangesetPropsFileName),
        `${JSON.stringify(toRelativeChangesetProps(downloaded), undefined, 2)}\n`
      );

      const releaseErrors = await releaseBuildHub(
        hub,
        briefcaseFileName,
        briefcaseClosed
      );
      if (releaseErrors.length > 0)
        throw new AggregateError(
          releaseErrors,
          "Failed to release the fixture build hub"
        );
      hub = undefined;

      const recipeDataFile =
        recipeData === undefined || recipeData === null
          ? undefined
          : writeFixtureRecipeData(artifactDir, recipeData);

      const buildMilliseconds =
        Number(process.hrtime.bigint() - start) / 1_000_000;
      const indices = downloaded.map((changeset) => changeset.index);
      const manifest: FixtureArtifactManifest = {
        artifactVersion: fixtureArtifactVersion,
        descriptor,
        briefcase: {
          fileName: artifactBriefcaseFileName,
          briefcaseId,
          changeset: {
            id: briefcaseChangeset.id,
            index: briefcaseChangeset.index,
          },
          byteLength: fs.statSync(destination).size,
        },
        changesets: {
          directory: artifactChangesetDirectoryName,
          propsFile: artifactChangesetPropsFileName,
          count: downloaded.length,
          baseChangesetIndex,
          firstIndex: indices.length > 0 ? Math.min(...indices) : undefined,
          lastIndex: indices.length > 0 ? Math.max(...indices) : undefined,
        },
        recipeDataFile,
        buildMilliseconds,
        builtAt: new Date().toISOString(),
      };
      writeFixtureArtifactManifest(artifactDir, manifest);
      return {
        fixture,
        descriptor,
        directory: artifactDir,
        buildMilliseconds,
        artifact: readFixtureArtifact(artifactDir),
      };
    } catch (error) {
      if (hub) {
        const cleanupErrors = await releaseBuildHub(
          hub,
          briefcaseFileName,
          briefcaseClosed
        );
        if (cleanupErrors.length > 0)
          throw new AggregateError(
            [error, ...cleanupErrors],
            "Fixture artifact build and cleanup both failed"
          );
      }
      throw error;
    } finally {
      // Build-time scaffolding must never reach a working copy.
      fs.rmSync(hubDir, { recursive: true, force: true });
      fs.rmSync(scratchDir, { recursive: true, force: true });
    }
  },

  async materialize(
    built: BuiltFixture,
    sampleDir: string
  ): Promise<PreparedDataset> {
    const artifact = requireFixtureArtifact(built);
    const start = process.hrtime.bigint();
    fs.rmSync(sampleDir, { recursive: true, force: true });
    fs.mkdirSync(path.dirname(sampleDir), { recursive: true });
    fs.cpSync(artifact.directory, sampleDir, { recursive: true });

    const csFileProps = readChangesetFileProps(sampleDir);
    const recipe = readFixtureRecipeData(sampleDir, artifact.manifest);
    const sourceDb = await BriefcaseDb.open({
      fileName: artifactBriefcasePath(sampleDir),
      readonly: true,
    });
    return {
      topology: "source-only",
      descriptor: built.descriptor,
      directory: sampleDir,
      sourceDb,
      csFileProps,
      manifest: artifact.manifest,
      recipe,
      reconstructionMilliseconds:
        Number(process.hrtime.bigint() - start) / 1_000_000,
    };
  },

  async disposeSample(dataset: PreparedDataset): Promise<void> {
    // No hub, no briefcase registration to release: the working copy is just files.
    requireDetachedDataset(dataset).sourceDb.close();
  },

  async disposeBuild(built: BuiltFixture): Promise<void> {
    fs.rmSync(built.directory, { recursive: true, force: true });
  },
};
