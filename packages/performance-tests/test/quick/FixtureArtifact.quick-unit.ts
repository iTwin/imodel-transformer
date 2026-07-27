/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BriefcaseDb, IModelHost } from "@itwin/core-backend";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
import { ChangedInstanceIds } from "@itwin/imodel-transformer";
import {
  artifactBriefcasePath,
  artifactManifestFileName,
  readChangesetFileProps,
  readFixtureArtifact,
} from "./FixtureArtifact";
import { balancedIncrementalSourceOnlyDescriptor } from "./FixtureCatalog";
import { requireDetachedDataset } from "./FixtureMaterializer";
import {
  BuiltFixture,
  getFixtureProvider,
  requireFixtureArtifact,
} from "./FixtureProvider";
import { detachedBriefcaseFixtureProvider } from "./providers/detachedBriefcaseProvider";

describe("detached fixture artifact", () => {
  const descriptor = balancedIncrementalSourceOnlyDescriptor;
  let root: string;
  let built: BuiltFixture;

  before(async function () {
    this.timeout(600_000);
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-artifact-"));
    await IModelHost.startup();
    built = await detachedBriefcaseFixtureProvider.build(
      descriptor,
      path.join(root, "fixture-artifact")
    );
  });

  after(async () => {
    await detachedBriefcaseFixtureProvider.disposeBuild(built);
    if (IModelHost.isValid) await IModelHost.shutdown();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("selects the detached provider from the descriptor topology", () => {
    expect(getFixtureProvider(descriptor)).to.equal(
      detachedBriefcaseFixtureProvider
    );
  });

  /**
   * The load-bearing invariant of the whole two-stage design: a briefcase's bytes are portable and
   * `BriefcaseDb.open` consults no hub. Stage 2 is only a filesystem copy because this holds.
   *
   * `openDgnDb` makes no hub calls today, but nothing in core-backend's API contract promises it
   * never will. If a future version validates a briefcase id or parent changeset against a hub,
   * this fails here with a clear cause instead of surfacing as an inscrutable benchmark failure.
   */
  it("opens a relocated briefcase readonly with no hub of any kind", async () => {
    expect(HubMock.isValid).to.equal(
      false,
      "stage 1 must release its build hub before any working copy is opened"
    );
    const manifest = requireFixtureArtifact(built).manifest;
    const relocated = path.join(root, "relocated", "renamed.bim");
    fs.mkdirSync(path.dirname(relocated), { recursive: true });
    fs.copyFileSync(artifactBriefcasePath(built.directory), relocated);

    const sourceDb = await BriefcaseDb.open({
      fileName: relocated,
      readonly: true,
    });
    try {
      // The copy still advertises a briefcase id and a parent changeset belonging to an iModel
      // that no longer exists anywhere. Retaining them is harmless precisely because open is
      // hub-free; if either were ever resolved remotely, the open above would have thrown.
      expect(sourceDb.briefcaseId).to.equal(manifest.briefcase.briefcaseId);
      expect(sourceDb.changeset.id).to.equal(manifest.briefcase.changeset.id);
      // realpath because macOS resolves /var to /private/var on open.
      expect(fs.realpathSync(sourceDb.pathName)).to.equal(
        fs.realpathSync(relocated)
      );
      // Readable, not merely openable.
      expect(sourceDb.elements.getRootSubject()).to.not.be.undefined;
    } finally {
      sourceDb.close();
    }
    fs.rmSync(path.dirname(relocated), { recursive: true, force: true });
  });

  it("emits a self-describing artifact and leaves no build scaffolding", () => {
    const entries = fs.readdirSync(built.directory).sort();
    expect(entries).to.deep.equal([
      "briefcase.bim",
      "changesets",
      "csFileProps.json",
      artifactManifestFileName,
    ]);
    const artifact = readFixtureArtifact(built.directory);
    expect(artifact.manifest.descriptor.recipeHash).to.equal(
      descriptor.recipeHash
    );
    expect(artifact.manifest.changesets.count).to.equal(
      descriptor.distribution.operations.sourceChangesets
    );
    expect(
      fs.readdirSync(path.join(built.directory, "changesets"))
    ).to.have.length(artifact.manifest.changesets.count);
  });

  it("stores relocatable changeset pathnames", () => {
    const raw = JSON.parse(
      fs.readFileSync(path.join(built.directory, "csFileProps.json"), "utf8")
    ) as { pathname: string }[];
    for (const props of raw) {
      expect(path.isAbsolute(props.pathname)).to.equal(
        false,
        "artifact pathnames must be relative so the artifact can be copied"
      );
      expect(props.pathname.startsWith("changesets/")).to.be.true;
    }
    // The reader is the only supported consumer: it rebases to the copy's location.
    for (const props of readChangesetFileProps(built.directory))
      expect(fs.existsSync(props.pathname)).to.be.true;
  });

  it("materializes independent working copies without a hub", async function () {
    this.timeout(300_000);
    expect(HubMock.isValid).to.equal(
      false,
      "stage 2 must not require a live hub"
    );
    const copyCosts: number[] = [];
    const digests: string[] = [];
    for (let sample = 0; sample < 3; sample++) {
      const dataset = requireDetachedDataset(
        await detachedBriefcaseFixtureProvider.materialize(
          built,
          path.join(root, `sample-${sample}`),
          `sample-${sample}`
        )
      );
      try {
        copyCosts.push(dataset.reconstructionMilliseconds);
        const changes = await ChangedInstanceIds.initialize({
          iModel: dataset.sourceDb,
          csFileProps: dataset.csFileProps,
        });
        if (!changes)
          throw new Error("ChangedInstanceIds.initialize returned nothing");
        digests.push(
          JSON.stringify({
            inserts: changes.element.insertIds.size,
            updates: changes.element.updateIds.size,
            deletes: changes.element.deleteIds.size,
          })
        );
      } finally {
        await detachedBriefcaseFixtureProvider.disposeSample(dataset);
      }
    }
    expect(new Set(digests).size).to.equal(
      1,
      "every working copy must present the same fixture"
    );
    // Not an assertion of speed — a record of the budget stage 2 consumes per sample.
    const median = [...copyCosts].sort((a, b) => a - b)[1];
    const artifactBytes = fs
      .readdirSync(built.directory, { recursive: true, withFileTypes: true })
      .filter((entry) => entry.isFile())
      .reduce(
        (sum, entry) =>
          sum + fs.statSync(path.join(entry.parentPath, entry.name)).size,
        0
      );
    process.stdout.write(
      `\n      artifact: ${(artifactBytes / 1024 / 1024).toFixed(
        2
      )}MiB; stage-1 build: ${built.buildMilliseconds.toFixed(
        0
      )}ms; stage-2 copy per sample: ${copyCosts
        .map((cost) => cost.toFixed(1))
        .join(", ")}ms (median ${median.toFixed(1)}ms)\n`
    );
  });

  it("rejects an artifact whose manifest is inconsistent with its contents", () => {
    const corrupt = path.join(root, "corrupt-artifact");
    fs.cpSync(built.directory, corrupt, { recursive: true });
    fs.rmSync(artifactBriefcasePath(corrupt));
    expect(() => readFixtureArtifact(corrupt)).to.throw();
    fs.rmSync(corrupt, { recursive: true, force: true });
  });

  it("rejects changeset props that escape the artifact directory", () => {
    const corrupt = path.join(root, "escaping-artifact");
    fs.cpSync(built.directory, corrupt, { recursive: true });
    const propsFile = path.join(corrupt, "csFileProps.json");
    const props = JSON.parse(fs.readFileSync(propsFile, "utf8")) as {
      pathname: string;
    }[];
    props[0].pathname = "/tmp/elsewhere.cs";
    fs.writeFileSync(propsFile, JSON.stringify(props));
    expect(() => readChangesetFileProps(corrupt)).to.throw();
    fs.rmSync(corrupt, { recursive: true, force: true });
  });
});
