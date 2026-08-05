/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { SnapshotDb } from "@itwin/core-backend";
import {
  artifactBriefcasePath,
  artifactManifestFileName,
  readFixtureArtifact,
} from "../../src/fixtures/FixtureArtifact.js";
import {
  BuiltFixture,
  getFixtureProvider,
  requireStandaloneDataset,
} from "../../src/fixtures/FixtureProvider.js";
import { standaloneFullTransformFixture } from "../../src/fixtures/recipes/standaloneFullTransform.js";
import { withExternalFixtureSource } from "../../src/fixtures/FixtureRecipe.js";
import { standaloneFixtureProvider } from "../../src/fixtures/providers/standaloneProvider.js";
import { standaloneFullTransformation } from "../../src/scenarios/standaloneFullTransformation.js";
import {
  shutdownIsolatedHost,
  startIsolatedHost,
} from "../support/isolatedHost.js";

function sha256(fileName: string): string {
  return createHash("sha256").update(fs.readFileSync(fileName)).digest("hex");
}

async function countPhysicalObjects(db: SnapshotDb): Promise<number> {
  const reader = db.createQueryReader(
    "SELECT count(*) cnt FROM Generic.PhysicalObject",
    undefined,
    { usePrimaryConn: true }
  );
  if (!(await reader.step())) throw new Error("Count query returned no row");
  return reader.current.cnt as number;
}

describe("standalone fixture artifact", () => {
  let built: BuiltFixture;
  let root: string;

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-standalone-artifact-"));
    await startIsolatedHost();
    built = await standaloneFixtureProvider.build(
      standaloneFullTransformFixture,
      path.join(root, "generated-artifact")
    );
  });

  afterAll(async () => {
    if (built) await standaloneFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("selects the standalone provider and emits an immutable source artifact", () => {
    expect(getFixtureProvider(built.descriptor)).to.equal(
      standaloneFixtureProvider
    );
    const artifact = readFixtureArtifact(built.directory);
    expect(artifact.manifest.standalone).to.deep.include({
      sourceFile: "briefcase.bim",
      sourceSha256: sha256(artifactBriefcasePath(built.directory)),
    });
    expect(fs.readdirSync(built.directory).sort()).to.deep.equal([
      "briefcase.bim",
      "changesets",
      "csFileProps.json",
      artifactManifestFileName,
    ]);
  });

  it("materializes pristine sources and fresh targets for deterministic full transforms", async () => {
    const sourceHashes: string[] = [];
    const digests: string[] = [];
    for (let sample = 0; sample < 2; sample++) {
      const sampleDir = path.join(root, `sample-${sample}`);
      const dataset = requireStandaloneDataset(
        await standaloneFixtureProvider.materialize(
          built,
          sampleDir,
          `standalone-${sample}`
        )
      );
      const sourceFile = dataset.sourceDb.pathName;
      const sourceHash = sha256(sourceFile);
      sourceHashes.push(sourceHash);
      expect(dataset.sourceDb.isReadonly).to.be.true;
      expect(await countPhysicalObjects(dataset.sourceDb)).to.equal(10000);
      expect(await countPhysicalObjects(dataset.targetDb)).to.equal(0);
      const scenario = standaloneFullTransformation(dataset);
      try {
        await scenario.prepare?.();
        await scenario.measure();
        digests.push(await scenario.finish());
      } finally {
        scenario.abort();
        await standaloneFixtureProvider.disposeSample(dataset);
      }
      expect(sha256(sourceFile)).to.equal(sourceHash);
      fs.rmSync(sampleDir, { recursive: true, force: true });
      expect(fs.existsSync(sampleDir)).to.be.false;
    }
    expect(new Set(sourceHashes).size).to.equal(1);
    expect(new Set(digests).size).to.equal(1);
  });

  it("copies an external BIM into the artifact and records its byte identity", async () => {
    const external = path.join(root, "user-source.bim");
    const externalDb = SnapshotDb.createEmpty(external, {
      rootSubject: { name: "External source" },
    });
    externalDb.close();
    const fixture = withExternalFixtureSource(
      standaloneFullTransformFixture,
      external
    );
    const externalBuilt = await standaloneFixtureProvider.build(
      fixture,
      path.join(root, "external-artifact")
    );
    try {
      const artifact = readFixtureArtifact(externalBuilt.directory);
      const expectedIdentity = {
        kind: "external-bim",
        fileName: "user-source.bim",
        byteLength: fs.statSync(external).size,
        sha256: sha256(external),
      };
      expect(artifact.manifest.descriptor.source).to.deep.equal({
        ...expectedIdentity,
      });
      expect(artifactBriefcasePath(externalBuilt.directory)).to.not.equal(
        external
      );
      expect(sha256(artifactBriefcasePath(externalBuilt.directory))).to.equal(
        sha256(external)
      );
      fs.rmSync(external);
      const dataset = requireStandaloneDataset(
        await standaloneFixtureProvider.materialize(
          externalBuilt,
          path.join(root, "external-sample"),
          "external-sample"
        )
      );
      const scenario = standaloneFullTransformation(dataset);
      try {
        await scenario.prepare?.();
        await scenario.measure();
        await scenario.finish();
      } finally {
        scenario.abort();
        await standaloneFixtureProvider.disposeSample(dataset);
        fs.rmSync(dataset.directory, { recursive: true, force: true });
      }
    } finally {
      await standaloneFixtureProvider.disposeBuild(externalBuilt);
    }
  });

  it("attempts both database closes when standalone sample cleanup fails", async () => {
    const sampleDir = path.join(root, "cleanup-sample");
    const dataset = requireStandaloneDataset(
      await standaloneFixtureProvider.materialize(
        built,
        sampleDir,
        "cleanup-sample"
      )
    );
    const closeSource = dataset.sourceDb.close.bind(dataset.sourceDb);
    const sourceClose = vi
      .spyOn(dataset.sourceDb, "close")
      .mockImplementation(() => {
        throw new Error("source close failed");
      });
    const targetClose = vi.spyOn(dataset.targetDb, "close");
    try {
      await expect(
        standaloneFixtureProvider.disposeSample(dataset)
      ).rejects.toThrow(/Failed to close standalone/);
      expect(targetClose).toHaveBeenCalledOnce();
    } finally {
      sourceClose.mockRestore();
      targetClose.mockRestore();
      closeSource();
      fs.rmSync(sampleDir, { recursive: true, force: true });
    }
  });

  it("rejects unopenable external BIM bytes and removes the failed artifact", async () => {
    const invalid = path.join(root, "invalid.bim");
    const output = path.join(root, "invalid-artifact");
    fs.writeFileSync(invalid, "not an iModel");
    const fixture = withExternalFixtureSource(
      standaloneFullTransformFixture,
      invalid
    );
    await expect(
      standaloneFixtureProvider.build(fixture, output)
    ).rejects.toThrow(/openable standalone SnapshotDb/);
    expect(fs.existsSync(output)).to.be.false;
  });

  it("rejects source corruption and manifest hash mismatches", () => {
    const corrupt = path.join(root, "corrupt-artifact");
    fs.cpSync(built.directory, corrupt, { recursive: true });
    const source = artifactBriefcasePath(corrupt);
    const bytes = fs.readFileSync(source);
    bytes[0] ^= 0xff;
    fs.writeFileSync(source, bytes);
    expect(() => readFixtureArtifact(corrupt)).to.throw(
      /standalone source hash|content hash/
    );
    fs.rmSync(corrupt, { recursive: true, force: true });
  });

  it("rejects a standalone source hash mismatch even when the content hash is recomputed", () => {
    const corrupt = path.join(root, "standalone-hash-mismatch");
    fs.cpSync(built.directory, corrupt, { recursive: true });
    const manifestFile = path.join(corrupt, artifactManifestFileName);
    const manifest = JSON.parse(fs.readFileSync(manifestFile, "utf8")) as {
      contentHash: string;
      standalone: { sourceSha256: string };
    };
    manifest.standalone.sourceSha256 = "0".repeat(64);
    fs.writeFileSync(
      manifestFile,
      `${JSON.stringify(manifest, undefined, 2)}\n`
    );
    expect(() => readFixtureArtifact(corrupt)).to.throw(
      /standalone source hash/
    );
    fs.rmSync(corrupt, { recursive: true, force: true });
  });

  it.each([
    ["POSIX traversal", "../outside.bim"],
    ["Windows traversal", "..\\outside.bim"],
    ["absolute path", "<absolute>"],
  ])("rejects a standalone source with a %s", (label, sourceFileInput) => {
    const sourceFile =
      sourceFileInput === "<absolute>"
        ? path.resolve(root, "outside.bim")
        : sourceFileInput;
    const corrupt = path.join(
      root,
      `standalone-source-path-${label.replaceAll(" ", "-")}`
    );
    fs.cpSync(built.directory, corrupt, { recursive: true });
    const manifestFile = path.join(corrupt, artifactManifestFileName);
    const manifest = JSON.parse(fs.readFileSync(manifestFile, "utf8")) as {
      standalone: { sourceFile: string };
    };
    manifest.standalone.sourceFile = sourceFile;
    fs.writeFileSync(
      manifestFile,
      `${JSON.stringify(manifest, undefined, 2)}\n`
    );
    expect(() => readFixtureArtifact(corrupt)).to.throw(
      /Standalone fixture artifact manifest has incompatible source/
    );
    fs.rmSync(corrupt, { recursive: true, force: true });
  });

  it("rejects an external source inside the artifact directory before deleting it", async () => {
    const artifactDir = path.join(root, "overlap-artifact");
    fs.mkdirSync(artifactDir, { recursive: true });
    const external = path.join(artifactDir, "original.bim");
    fs.copyFileSync(artifactBriefcasePath(built.directory), external);
    const fixture = withExternalFixtureSource(
      standaloneFullTransformFixture,
      external
    );
    await expect(
      standaloneFixtureProvider.build(fixture, artifactDir)
    ).rejects.toThrow(/outside benchmark-managed directories/);
    expect(fs.existsSync(external)).to.be.true;
    fs.rmSync(artifactDir, { recursive: true, force: true });
  });
});
