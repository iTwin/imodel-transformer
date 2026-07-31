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
