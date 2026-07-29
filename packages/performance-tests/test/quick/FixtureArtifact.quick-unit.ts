/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BriefcaseDb } from "@itwin/core-backend";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
import { ChangedInstanceIds } from "@itwin/imodel-transformer";
import { DatasetDescriptor } from "./DatasetDescriptor";
import {
  artifactBriefcasePath,
  artifactManifestFileName,
  readChangesetFileProps,
  readFixtureArtifact,
  readFixtureRecipeData,
  writeFixtureRecipeData,
} from "./FixtureArtifact";
import { balancedIncrementalSourceOnlyDescriptor } from "./FixtureCatalog";
import {
  balancedIncrementalRecipe,
  registerFixtureRecipe,
} from "./FixtureRecipe";
import { requireDetachedDataset } from "./FixtureMaterializer";
import {
  BuiltFixture,
  getFixtureProvider,
  requireFixtureArtifact,
} from "./FixtureProvider";
import { detachedBriefcaseFixtureProvider } from "./providers/detachedBriefcaseProvider";
import { shutdownIsolatedHost, startIsolatedHost } from "./isolatedHost";

describe("detached fixture artifact", () => {
  const descriptor = balancedIncrementalSourceOnlyDescriptor;
  let root: string;
  let built: BuiltFixture;

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-artifact-"));
    await startIsolatedHost();
    built = await detachedBriefcaseFixtureProvider.build(
      descriptor,
      path.join(root, "fixture-artifact")
    );
  });

  afterAll(async () => {
    await detachedBriefcaseFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
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

  it("omits recipe data when the recipe returns nothing", () => {
    // `balanced-incremental` opts out, so the key must be absent rather than null.
    const raw = JSON.parse(
      fs.readFileSync(
        path.join(built.directory, artifactManifestFileName),
        "utf8"
      )
    ) as Record<string, unknown>;
    expect(raw).to.not.have.property("recipeDataFile");
    expect(fs.existsSync(path.join(built.directory, "recipe.json"))).to.be
      .false;
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

  it("materializes independent working copies without a hub", async () => {
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

  it("carries recipe data across the stage boundary unchanged", () => {
    const dir = path.join(root, "recipe-data");
    fs.mkdirSync(dir, { recursive: true });
    // The shape a scanning oracle needs: deleted ids cannot be recovered from a tip-pinned
    // briefcase, so they only exist if the recipe hands them over here.
    const data = {
      elements: { insertIds: ["0x20", "0x21"], deleteIds: ["0x22"] },
      counts: { changesets: 8 },
      nested: [{ ok: true }, { ok: false }],
    };
    expect(writeFixtureRecipeData(dir, data)).to.equal("recipe.json");
    const manifest = {
      ...readFixtureArtifact(built.directory).manifest,
      recipeDataFile: "recipe.json",
    };
    expect(readFixtureRecipeData(dir, manifest)).to.deep.equal(data);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  /**
   * Values that `JSON.stringify` mangles rather than rejecting are the dangerous ones: a `Set`
   * serializes to `{}`, so an oracle would read an empty id list and report a false pass. These
   * must fail at build time, where the message points at the recipe.
   */
  it("rejects recipe data that JSON cannot represent", () => {
    const dir = path.join(root, "bad-recipe-data");
    fs.mkdirSync(dir, { recursive: true });
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const cases: [string, unknown][] = [
      ["Set", { ids: new Set(["0x20"]) }],
      ["Map", { ids: new Map([["a", 1]]) }],
      ["Date", { builtAt: new Date() }],
      ["BigInt", { count: BigInt(1) }],
      ["NaN", { ratio: NaN }],
      ["Infinity", { ratio: Infinity }],
      ["undefined", { ids: undefined }],
      ["function", { make: () => 1 }],
      ["class instance", { at: new (class Point {})() }],
      ["circular", circular],
    ];
    for (const [label, value] of cases)
      expect(() => writeFixtureRecipeData(dir, value), label).to.throw(
        /Recipe data at/
      );
    expect(
      fs.existsSync(path.join(dir, "recipe.json")),
      "a rejected value must not leave a partial artifact"
    ).to.be.false;
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it("fails loudly when declared recipe data is missing", () => {
    const artifact = readFixtureArtifact(built.directory);
    expect(() =>
      readFixtureRecipeData(built.directory, {
        ...artifact.manifest,
        recipeDataFile: "recipe.json",
      })
    ).to.throw(/missing/);
  });
});

/**
 * The end-to-end contract W3's oracle depends on: whatever a recipe returns must survive stage 1,
 * the filesystem copy, and stage 2 — identically for every sample, and therefore identically for
 * both arms of an A/B comparison.
 */
describe("recipe data across the stage boundary", () => {
  const emitted = {
    elements: { insertIds: ["0x20", "0x21"], deleteIds: ["0x22"] },
    changesets: 8,
  };
  const recipeId = "balanced-incremental-with-data";
  const descriptor: DatasetDescriptor = {
    ...balancedIncrementalSourceOnlyDescriptor,
    id: "balanced-incremental-emitting-data",
    layout: {
      ...balancedIncrementalSourceOnlyDescriptor.layout,
      recipe: recipeId,
    },
  };
  let root: string;
  let built: BuiltFixture;

  beforeAll(async () => {
    registerFixtureRecipe({
      id: recipeId,
      createSeed: async (fileName, forDescriptor) =>
        balancedIncrementalRecipe.createSeed(fileName, forDescriptor),
      applySourceChangesets: async (db, token, forDescriptor, state) => {
        await balancedIncrementalRecipe.applySourceChangesets(
          db,
          token,
          forDescriptor,
          state
        );
        return emitted;
      },
      validate: async (db, forDescriptor) =>
        balancedIncrementalRecipe.validate(db, forDescriptor),
    });
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-recipe-data-"));
    await startIsolatedHost();
    built = await detachedBriefcaseFixtureProvider.build(
      descriptor,
      path.join(root, "fixture-artifact")
    );
  });

  afterAll(async () => {
    await detachedBriefcaseFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("records the data file in the manifest rather than requiring a probe", () => {
    const artifact = readFixtureArtifact(built.directory);
    expect(artifact.manifest.recipeDataFile).to.equal("recipe.json");
    expect(fs.existsSync(path.join(built.directory, "recipe.json"))).to.be.true;
  });

  it("surfaces identical data on every working copy", async () => {
    const datasets = [];
    for (const name of ["sample-0", "sample-1"])
      datasets.push(
        requireDetachedDataset(
          await detachedBriefcaseFixtureProvider.materialize(
            built,
            path.join(root, "samples", name),
            name
          )
        )
      );
    try {
      for (const dataset of datasets)
        expect(dataset.recipe).to.deep.equal(emitted);
      // Byte-identical, not merely deep-equal: this is what makes an A/B verdict trustworthy.
      const [first, second] = datasets.map((dataset) =>
        fs.readFileSync(path.join(dataset.directory, "recipe.json"))
      );
      expect(first.equals(second)).to.be.true;
    } finally {
      for (const dataset of datasets)
        await detachedBriefcaseFixtureProvider.disposeSample(dataset);
    }
  });
});

/**
 * The vacuous-pass guard, exercised through the real build rather than the writer alone.
 *
 * A recipe returning a `Set` of ids is the realistic mistake: `JSON.stringify(new Set([...]))` is
 * `"{}"`, so without validation stage 1 would happily emit an artifact whose ledger is an empty
 * object. A scanning oracle would then compare its results against nothing and report green. The
 * build must fail instead, and must not leave behind a directory that later reads as a valid
 * artifact.
 */
describe("recipe data that cannot survive JSON", () => {
  const recipeId = "balanced-incremental-emitting-a-set";
  const descriptor: DatasetDescriptor = {
    ...balancedIncrementalSourceOnlyDescriptor,
    id: "balanced-incremental-emitting-a-set",
    layout: {
      ...balancedIncrementalSourceOnlyDescriptor.layout,
      recipe: recipeId,
    },
  };
  let root: string;

  beforeAll(async () => {
    registerFixtureRecipe({
      id: recipeId,
      createSeed: async (fileName, forDescriptor) =>
        balancedIncrementalRecipe.createSeed(fileName, forDescriptor),
      applySourceChangesets: async (db, token, forDescriptor, state) => {
        await balancedIncrementalRecipe.applySourceChangesets(
          db,
          token,
          forDescriptor,
          state
        );
        return { elements: { deleteIds: new Set(["0x20", "0x21"]) } };
      },
      validate: async (db, forDescriptor) =>
        balancedIncrementalRecipe.validate(db, forDescriptor),
    });
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-bad-recipe-"));
    await startIsolatedHost();
  });

  afterAll(async () => {
    await shutdownIsolatedHost();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("fails the build instead of emitting an empty ledger", async () => {
    const artifactDir = path.join(root, "fixture-artifact");
    let built: BuiltFixture | undefined;
    try {
      built = await detachedBriefcaseFixtureProvider.build(
        descriptor,
        artifactDir
      );
    } catch (error) {
      expect((error as Error).message).to.match(/Recipe data at .* is a Set/);
    }
    expect(built, "the build must not succeed").to.equal(undefined);
    // A half-written artifact must never read back as usable.
    expect(() => readFixtureArtifact(artifactDir)).to.throw();
    expect(fs.existsSync(path.join(artifactDir, "recipe.json"))).to.be.false;
  });
});
