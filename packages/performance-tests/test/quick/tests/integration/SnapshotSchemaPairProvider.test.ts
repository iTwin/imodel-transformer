/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { dynamicSchemaUnionMediumDescriptor } from "../../src/catalogs/FixtureCatalog.js";
import { FixtureDescriptor } from "../../src/fixtures/FixtureDescriptor.js";
import {
  BuiltFixture,
  getFixtureProvider,
  PreparedDataset,
  requireSnapshotSchemaPairDataset,
} from "../../src/fixtures/FixtureProvider.js";
import { registerFixtureRecipe } from "../../src/fixtures/FixtureRecipe.js";
import { snapshotSchemaPairFixtureProvider } from "../../src/fixtures/providers/snapshotSchemaPairProvider.js";
import { expectDynamicSchemaUnionExpectation } from "../../src/fixtures/recipes/dynamicSchemaUnion.js";
import {
  shutdownIsolatedHost,
  startIsolatedHost,
} from "../support/isolatedHost.js";

describe("snapshotSchemaPairFixtureProvider", () => {
  let root: string;
  let built: BuiltFixture;

  beforeAll(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-schema-pair-"));
    await startIsolatedHost();
    built = await snapshotSchemaPairFixtureProvider.build(
      dynamicSchemaUnionMediumDescriptor,
      path.join(root, "fixture-artifact")
    );
  });

  afterAll(async () => {
    await snapshotSchemaPairFixtureProvider.disposeBuild(built);
    await shutdownIsolatedHost();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("selects the schema-pair provider from the descriptor topology", () => {
    expect(getFixtureProvider(dynamicSchemaUnionMediumDescriptor)).to.equal(
      snapshotSchemaPairFixtureProvider
    );
  });

  it("materializes a pristine, deterministic source/target pair per sample", async () => {
    const samples: PreparedDataset[] = [];
    try {
      for (const name of ["sample-0", "sample-1"]) {
        const dataset = await snapshotSchemaPairFixtureProvider.materialize(
          built,
          path.join(root, name),
          name
        );
        samples.push(dataset);
      }
      const prepared = samples.map(requireSnapshotSchemaPairDataset);

      for (const dataset of prepared) {
        expect(dataset.reconstructionMilliseconds).to.be.a("number");
        expect(dataset.reconstructionMilliseconds).to.be.greaterThanOrEqual(0);
        const expectation = expectDynamicSchemaUnionExpectation(
          dataset.expectation
        );
        // The provider must own two independently created databases, not one shared file.
        expect(dataset.sourceDb.pathName).to.not.equal(
          dataset.targetDb.pathName
        );
        // Spot-check one class from every partition on both sides of the pair.
        expect(
          dataset.sourceDb.containsClass(
            `${expectation.schemaName}:${expectation.sharedClassNames[0]}`
          )
        ).to.be.true;
        expect(
          dataset.sourceDb.containsClass(
            `${expectation.schemaName}:${expectation.sourceOnlyClassNames[0]}`
          )
        ).to.be.true;
        expect(
          dataset.targetDb.containsClass(
            `${expectation.schemaName}:${expectation.sharedClassNames[0]}`
          )
        ).to.be.true;
        expect(
          dataset.targetDb.containsClass(
            `${expectation.schemaName}:${expectation.targetOnlyClassNames[0]}`
          )
        ).to.be.true;
        // Every sample must observe an identical, deterministic recipe.
        expect(expectation).to.deep.equal(
          expectDynamicSchemaUnionExpectation(prepared[0].expectation)
        );
      }
    } finally {
      for (const dataset of samples)
        await snapshotSchemaPairFixtureProvider.disposeSample(dataset);
    }
  });

  it("closes both databases and removes the sample directory on dispose", async () => {
    const sampleDir = path.join(root, "dispose-sample");
    const dataset = requireSnapshotSchemaPairDataset(
      await snapshotSchemaPairFixtureProvider.materialize(
        built,
        sampleDir,
        "dispose-sample"
      )
    );
    expect(fs.existsSync(sampleDir)).to.be.true;
    await snapshotSchemaPairFixtureProvider.disposeSample(dataset);
    expect(fs.existsSync(sampleDir)).to.be.false;
    // A closed SnapshotDb throws when used; this is evidence the handle was actually released
    // and not merely forgotten, since a lingering native handle would keep the file locked.
    expect(() =>
      dataset.sourceDb.containsClass("BisCore:PhysicalElement")
    ).to.throw();
  });
});

describe("snapshotSchemaPairFixtureProvider partial-failure cleanup", () => {
  let root: string;
  const brokenImportRecipeId = "dynamic-schema-union-broken-target";
  const brokenImportDescriptor: FixtureDescriptor = {
    ...dynamicSchemaUnionMediumDescriptor,
    id: "dynamic-schema-union-broken-target-fixture",
    layout: {
      ...dynamicSchemaUnionMediumDescriptor.layout,
      recipe: brokenImportRecipeId,
    },
  };
  const throwingRecipeId = "dynamic-schema-union-throwing-recipe";
  const throwingRecipeDescriptor: FixtureDescriptor = {
    ...dynamicSchemaUnionMediumDescriptor,
    id: "dynamic-schema-union-throwing-recipe-fixture",
    layout: {
      ...dynamicSchemaUnionMediumDescriptor.layout,
      recipe: throwingRecipeId,
    },
  };

  beforeAll(async () => {
    registerFixtureRecipe({
      kind: "schema-pair",
      id: brokenImportRecipeId,
      async createSchemaPair() {
        return {
          sourceSchemaXml: `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="BrokenPair" alias="bp" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
</ECSchema>`,
          // References a class that does not exist: the native importer must reject this.
          targetSchemaXml: `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="BrokenPair" alias="bp" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
  <ECEntityClass typeName="Broken">
    <BaseClass>bis:DoesNotExist</BaseClass>
  </ECEntityClass>
</ECSchema>`,
          expectation: undefined,
        };
      },
    });
    registerFixtureRecipe({
      kind: "schema-pair",
      id: throwingRecipeId,
      async createSchemaPair() {
        throw new Error("schema-pair recipe failed");
      },
    });
    root = fs.mkdtempSync(path.join(os.tmpdir(), "quick-schema-pair-broken-"));
    await startIsolatedHost();
  });

  afterAll(async () => {
    await shutdownIsolatedHost();
    fs.rmSync(root, { recursive: true, force: true });
  });

  it("removes the sample directory and closes both databases when the target import fails", async () => {
    const built = await snapshotSchemaPairFixtureProvider.build(
      brokenImportDescriptor,
      path.join(root, "fixture-artifact")
    );
    const sampleDir = path.join(root, "broken-sample");
    await expect(
      snapshotSchemaPairFixtureProvider.materialize(
        built,
        sampleDir,
        "broken-sample"
      )
    ).rejects.toThrow();
    expect(
      fs.existsSync(sampleDir),
      "a failed materialization must not leave a partial working copy"
    ).to.be.false;
  });

  it("removes the sample directory when schema generation fails", async () => {
    const built = await snapshotSchemaPairFixtureProvider.build(
      throwingRecipeDescriptor,
      path.join(root, "fixture-artifact")
    );
    const sampleDir = path.join(root, "throwing-recipe-sample");
    await expect(
      snapshotSchemaPairFixtureProvider.materialize(
        built,
        sampleDir,
        "throwing-recipe-sample"
      )
    ).rejects.toThrow("schema-pair recipe failed");
    expect(
      fs.existsSync(sampleDir),
      "a failed recipe must not leave a partial working copy"
    ).to.be.false;
  });
});
