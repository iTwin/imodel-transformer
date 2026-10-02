/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { EditTxn, SnapshotDb, StandaloneDb } from "@itwin/core-backend";
import { OpenMode } from "@itwin/core-bentley";
import { BriefcaseIdValue } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
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

function removeSqliteSidecars(fileName: string): void {
  for (const suffix of ["-shm", "-wal"])
    fs.rmSync(`${fileName}${suffix}`, { force: true });
}

/** Populate a standalone target with one full transformation that records provenance. */
async function populateStandaloneTarget(
  sourceFile: string,
  targetFile: string,
  fixtureId: string
): Promise<void> {
  const sourceDb = openStandaloneSource(sourceFile);
  try {
    const targetDb = StandaloneDb.createEmpty(targetFile, {
      rootSubject: { name: `Target for ${fixtureId}` },
    });
    try {
      const editTxn = new EditTxn(targetDb, "Quick standalone target");
      editTxn.start();
      const transformer = new IModelTransformer(
        { source: sourceDb, target: editTxn },
        { loadSourceGeometry: true }
      );
      try {
        await transformer.processSchemas();
        await transformer.process();
        editTxn.saveChanges("populate quick standalone target");
      } finally {
        transformer.dispose();
        if (editTxn.isActive) editTxn.end();
      }
    } finally {
      targetDb.close();
    }
  } finally {
    sourceDb.close();
  }
  removeSqliteSidecars(sourceFile);
  removeSqliteSidecars(targetFile);
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
      removeSqliteSidecars(sourceFile);

      const populatedTarget =
        descriptor.layout.topology === "standalone-source-and-populated-target";
      if (populatedTarget)
        await populateStandaloneTarget(
          sourceFile,
          path.join(artifactDir, artifactStandaloneTargetFileName),
          descriptor.id
        );

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
          ...(populatedTarget
            ? { targetFile: artifactStandaloneTargetFileName }
            : {}),
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
      const { targetFile } = artifact.manifest.standalone;
      if (targetFile !== undefined) {
        const populatedTargetDb = StandaloneDb.openFile(
          path.join(sampleDir, targetFile),
          OpenMode.ReadWrite
        );
        return {
          topology: "standalone-source-and-populated-target",
          descriptor: built.descriptor,
          directory: sampleDir,
          sourceDb,
          targetDb: populatedTargetDb,
          manifest: artifact.manifest,
          reconstructionMilliseconds:
            Number(process.hrtime.bigint() - start) / 1_000_000,
        };
      }
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
    if (
      dataset.topology !== "standalone-source-and-empty-target" &&
      dataset.topology !== "standalone-source-and-populated-target"
    )
      throw new Error(
        `Standalone fixture provider cannot dispose a "${dataset.topology}" sample`
      );
    const errors: unknown[] = [];
    for (const db of [dataset.sourceDb, dataset.targetDb]) {
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
