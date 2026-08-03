/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { BriefcaseDb } from "@itwin/core-backend";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  configureFixture,
  defineFixtureRecipe,
  FixtureConfiguration,
} from "../../src/fixtures/FixtureRecipe.js";

interface TestParameters {
  readonly scale: number;
}

describe("configured fixture derivation", () => {
  let directory: string;
  let implementationFile: string;
  let schemaFile: string;

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), "quick-recipe-"));
    implementationFile = path.join(directory, "recipe.ts");
    schemaFile = path.join(directory, "schema.xml");
    fs.writeFileSync(implementationFile, "export const recipeVersion = 1;\n");
    fs.writeFileSync(schemaFile, "<ECSchema version='1'/>\n");
  });

  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true });
  });

  function recipe(validate?: () => Promise<void>) {
    return defineFixtureRecipe<TestParameters, undefined>({
      id: "test-recipe",
      identity: {
        implementationFiles: [implementationFile],
        schemaFiles: [schemaFile],
        values: { identityVersion: 1 },
      },
      distribution: ({ scale }) => ({
        base: {
          aspects: scale,
          elements: scale,
          geometricElements: 0,
          relationships: 0,
        },
        operations: {
          aspects: { deletes: 0, inserts: 0, updates: 0 },
          elements: { deletes: 0, inserts: 0, updates: 0 },
          relationships: { deletes: 0, inserts: 0, updates: 0 },
          geometryUpdates: 0,
          sourceChangesets: 0,
        },
      }),
      async createSeed() {},
      async applySourceChangesets() {},
      validate:
        validate === undefined
          ? undefined
          : async () => {
              await validate();
            },
    });
  }

  function configuration(
    overrides: Partial<FixtureConfiguration<TestParameters>> = {}
  ): FixtureConfiguration<TestParameters> {
    return {
      id: "test-fixture",
      version: 1,
      label: "test fixture",
      scenarioClaims: ["test"],
      topology: "source-only",
      seed: 42,
      parameters: { scale: 2 },
      ...overrides,
    };
  }

  it("derives stable descriptors, distributions, and generator versions", () => {
    const first = configureFixture(recipe(), configuration());
    const second = configureFixture(recipe(), configuration());

    expect(first.descriptor).to.deep.equal(second.descriptor);
    expect(first.descriptor.distribution.base.elements).to.equal(2);
    expect(first.descriptor.generator.node).to.equal(process.version);
    expect(first.descriptor.layout.recipe).to.equal(first.recipeId);
    expect("validate" in first).to.equal(false);
  });

  it("invalidates identity for parameters, topology, implementation, and schema", () => {
    const baseHash = configureFixture(recipe(), configuration()).descriptor
      .recipeHash;
    const parameterHash = configureFixture(
      recipe(),
      configuration({ parameters: { scale: 3 } })
    ).descriptor.recipeHash;
    const topologyHash = configureFixture(
      recipe(),
      configuration({ topology: "source-and-empty-target" })
    ).descriptor.recipeHash;

    fs.writeFileSync(implementationFile, "export const recipeVersion = 2;\n");
    const implementationHash = configureFixture(recipe(), configuration())
      .descriptor.recipeHash;
    fs.writeFileSync(schemaFile, "<ECSchema version='2'/>\n");
    const schemaHash = configureFixture(recipe(), configuration()).descriptor
      .recipeHash;

    expect(
      new Set([
        baseHash,
        parameterHash,
        topologyHash,
        implementationHash,
        schemaHash,
      ]).size
    ).to.equal(5);
  });

  it("invalidates identity for every fixture field exposed to recipes", () => {
    const hashes = [
      configuration(),
      configuration({ id: "other-fixture" }),
      configuration({ version: 2 }),
      configuration({ label: "other label" }),
      configuration({ scenarioClaims: ["other claim"] }),
      configuration({ topology: "source-and-empty-target" }),
      configuration({ seed: 43 }),
    ].map(
      (fixtureConfiguration) =>
        configureFixture(recipe(), fixtureConfiguration).descriptor.recipeHash
    );

    expect(new Set(hashes).size).to.equal(hashes.length);
  });

  it("normalizes identity file line endings", () => {
    fs.writeFileSync(implementationFile, "first\r\nsecond\r\n");
    const windowsHash = configureFixture(recipe(), configuration()).descriptor
      .recipeHash;
    fs.writeFileSync(implementationFile, "first\nsecond\n");
    const posixHash = configureFixture(recipe(), configuration()).descriptor
      .recipeHash;

    expect(windowsHash).to.equal(posixHash);
  });

  it.each([Number.NaN, Infinity, -Infinity, 1.5, 2 ** 53])(
    "rejects invalid fixture seed %s",
    (seed) => {
      expect(() =>
        configureFixture(recipe(), configuration({ seed }))
      ).to.throw("Fixture seed must be a safe integer");
    }
  );

  it.each([Number.NaN, Infinity, -Infinity, -1, 0, 1.5, 2 ** 53])(
    "rejects invalid fixture version %s",
    (version) => {
      expect(() =>
        configureFixture(recipe(), configuration({ version }))
      ).to.throw("Fixture version must be a positive safe integer");
    }
  );

  it("deeply freezes distributions even when their root is already frozen", () => {
    const base = {
      aspects: 0,
      elements: 2,
      geometricElements: 0,
      relationships: 0,
    };
    const distribution = Object.freeze({
      base,
      operations: {
        aspects: { deletes: 0, inserts: 0, updates: 0 },
        elements: { deletes: 0, inserts: 0, updates: 0 },
        relationships: { deletes: 0, inserts: 0, updates: 0 },
        geometryUpdates: 0,
        sourceChangesets: 0,
      },
    });
    const fixture = configureFixture(
      defineFixtureRecipe({
        ...recipe(),
        distribution: () => distribution,
      }),
      configuration()
    );

    expect(Object.isFrozen(fixture.descriptor.distribution.base)).to.be.true;
    expect(() => {
      base.elements = 3;
    }).to.throw();
    expect(fixture.descriptor.distribution.base.elements).to.equal(2);
  });

  it("copies and freezes recipe identity inputs", () => {
    const values = { revision: 1 };
    const fixtureRecipe = defineFixtureRecipe({
      ...recipe(),
      identity: {
        implementationFiles: [implementationFile],
        values,
      },
    });
    values.revision = 2;

    expect(fixtureRecipe.identity.values).to.deep.equal({ revision: 1 });
    expect(Object.isFrozen(fixtureRecipe.identity.values)).to.be.true;
  });

  it("exposes validation only when the recipe declares it", async () => {
    let calls = 0;
    const fixture = configureFixture(
      recipe(async () => {
        calls++;
      }),
      configuration()
    );

    await fixture.validate?.({} as BriefcaseDb);
    expect(calls).to.equal(1);
  });

  it("rejects structured values that cannot be hashed without collisions", () => {
    expect(() =>
      configureFixture(
        recipe(),
        configuration({
          parameters: new Map([["scale", 2]]) as unknown as TestParameters,
        })
      )
    ).to.throw("only plain objects and arrays");

    expect(() =>
      configureFixture(
        defineFixtureRecipe({
          ...recipe(),
          identity: {
            implementationFiles: [implementationFile],
            values: new Date(0),
          },
        }),
        configuration()
      )
    ).to.throw("only plain objects and arrays");
  });
});
