/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { SnapshotDb } from "@itwin/core-backend";
import { BriefcaseIdValue } from "@itwin/core-common";
import {
  artifactBriefcaseFileName,
  artifactBriefcasePath,
  artifactChangesetDirectoryName,
  artifactChangesetPropsFileName,
  artifactStandaloneTargetFileName,
  fixtureArtifactContentHash,
  FixtureArtifactManifest,
  fixtureArtifactVersion,
  readFixtureArtifact,
  sha256File,
  writeFixtureArtifactManifest,
} from "../FixtureArtifact.js";
import {
  BuiltFixture,
  FixtureProvider,
  PreparedDataset,
  requireFixtureArtifact,
  requireStandaloneDataset,
} from "../FixtureProvider.js";
import {
  assertExternalFixtureSourceOutsideDirectory,
  ConfiguredFixture,
} from "../FixtureRecipe.js";

function openStandaloneSource(
  fileName: string,
  environmentName?: string
): SnapshotDb {
  let db: SnapshotDb | undefined;
  try {
    db = SnapshotDb.openFile(fileName);
    if (db.getBriefcaseId() !== Number(BriefcaseIdValue.Unassigned)) {
      db.close();
      db = undefined;
      throw new Error("the database has an assigned iModelHub briefcase ID");
    }
    db.elements.getRootSubject();
    return db;
  } catch (error) {
    db?.close();
    const detail =
      error instanceof Error ? `: ${error.message}` : `: ${String(error)}`;
    throw new Error(
      `${
        environmentName ?? "Standalone fixture source"
      } must be an openable standalone SnapshotDb .bim file${detail}`,
      { cause: error }
    );
  }
}

export const standaloneFixtureProvider: FixtureProvider = {
  async build(
    fixture: ConfiguredFixture,
    artifactDir: string
  ): Promise<BuiltFixture> {
    const { descriptor } = fixture;
    const start = process.hrtime.bigint();
    assertExternalFixtureSourceOutsideDirectory(fixture, artifactDir);
    fs.rmSync(artifactDir, { recursive: true, force: true });
    fs.mkdirSync(artifactDir, { recursive: true });
    const sourceFile = artifactBriefcasePath(artifactDir);
    let completed = false;
    try {
      if (fixture.externalSourceFileName === undefined)
        await fixture.createSeed(sourceFile);
      else fs.copyFileSync(fixture.externalSourceFileName, sourceFile);

      const sourceDb = openStandaloneSource(
        sourceFile,
        fixture.externalSourceFileName === undefined
          ? undefined
          : "QUICK_PERF_STANDALONE_BIM"
      );
      try {
        if (fixture.externalSourceFileName === undefined)
          await fixture.validate?.(sourceDb);
      } finally {
        sourceDb.close();
      }
      for (const suffix of ["-shm", "-wal"])
        fs.rmSync(`${sourceFile}${suffix}`, { force: true });

      fs.mkdirSync(path.join(artifactDir, artifactChangesetDirectoryName));
      fs.writeFileSync(
        path.join(artifactDir, artifactChangesetPropsFileName),
        "[]\n"
      );
      const sourceSha256 = sha256File(sourceFile);
      if (
        descriptor.source !== undefined &&
        descriptor.source.sha256 !== sourceSha256
      )
        throw new Error(
          `QUICK_PERF_STANDALONE_BIM changed while its fixture artifact was being built: expected ${descriptor.source.sha256}, copied ${sourceSha256}`
        );

      const buildMilliseconds =
        Number(process.hrtime.bigint() - start) / 1_000_000;
      const manifest: FixtureArtifactManifest = {
        artifactVersion: fixtureArtifactVersion,
        contentHash: fixtureArtifactContentHash(artifactDir),
        descriptor,
        briefcase: {
          fileName: artifactBriefcaseFileName,
          briefcaseId: 0,
          changeset: { id: "", index: 0 },
          byteLength: fs.statSync(sourceFile).size,
        },
        changesets: {
          directory: artifactChangesetDirectoryName,
          propsFile: artifactChangesetPropsFileName,
          count: 0,
          baseChangesetIndex: 0,
        },
        standalone: {
          sourceFile: artifactBriefcaseFileName,
          sourceSha256,
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
    } finally {
      if (!completed) fs.rmSync(artifactDir, { recursive: true, force: true });
    }
  },

  async materialize(
    built: BuiltFixture,
    sampleDir: string
  ): Promise<PreparedDataset> {
    const artifact = requireFixtureArtifact(built);
    if (artifact.manifest.standalone === undefined)
      throw new Error(
        `Fixture "${built.descriptor.id}" does not contain a standalone artifact`
      );
    const start = process.hrtime.bigint();
    fs.rmSync(sampleDir, { recursive: true, force: true });
    fs.mkdirSync(path.dirname(sampleDir), { recursive: true });
    fs.cpSync(artifact.directory, sampleDir, { recursive: true });
    const sourceDb = openStandaloneSource(artifactBriefcasePath(sampleDir));
    try {
      const targetDb = SnapshotDb.createEmpty(
        path.join(sampleDir, artifactStandaloneTargetFileName),
        { rootSubject: { name: `Target for ${built.descriptor.id}` } }
      );
      return {
        topology: "standalone-source-and-empty-target",
        descriptor: built.descriptor,
        directory: sampleDir,
        sourceDb,
        targetDb,
        manifest: artifact.manifest,
        reconstructionMilliseconds:
          Number(process.hrtime.bigint() - start) / 1_000_000,
      };
    } catch (error) {
      sourceDb.close();
      throw error;
    }
  },

  async disposeSample(dataset: PreparedDataset): Promise<void> {
    const standalone = requireStandaloneDataset(dataset);
    const errors: unknown[] = [];
    for (const db of [standalone.sourceDb, standalone.targetDb]) {
      try {
        db.close();
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0)
      throw new AggregateError(
        errors,
        "Failed to close standalone quick performance sample databases"
      );
  },

  async disposeBuild(built: BuiltFixture): Promise<void> {
    fs.rmSync(built.directory, { recursive: true, force: true });
  },
};
