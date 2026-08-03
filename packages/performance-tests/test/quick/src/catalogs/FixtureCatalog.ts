/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as path from "node:path";
import {
  canonicalSha256,
  FixtureDescriptor,
  FixtureDistribution,
} from "../fixtures/FixtureDescriptor.js";
import {
  dynamicSchemaUnionRecipe,
  dynamicSchemaUnionScale,
} from "../fixtures/recipes/dynamicSchemaUnion.js";
import { quickPath, quickRootDirectory } from "../support/paths.js";

const localRequire = createRequire(import.meta.url);
const scale = 25;
const distribution = {
  base: {
    aspects: 480 * scale,
    elements: 240 * scale,
    geometricElements: 120 * scale,
    relationships: 120 * scale,
  },
  operations: {
    elements: {
      inserts: 24 * scale,
      updates: 24 * scale,
      deletes: 24 * scale,
    },
    aspects: {
      inserts: 24 * scale,
      updates: 24 * scale,
      deletes: 48 * scale,
    },
    relationships: {
      inserts: 12 * scale,
      updates: 12 * scale,
      deletes: 33 * scale,
    },
    geometryUpdates: 6 * scale,
    sourceChangesets: 8,
  },
} as const;

function packageVersion(packageName: string): string {
  const packageJson = JSON.parse(
    fs.readFileSync(localRequire.resolve(`${packageName}/package.json`), "utf8")
  ) as { version: string };
  return packageJson.version;
}

/**
 * `@itwin/ecschema-editing` and `@itwin/ecschema-locaters` are optional peers of
 * `@itwin/imodel-transformer`. Resolve their versions through the transformer package's own
 * installed copies rather than this package's `node_modules`: that is the engine that actually
 * executes schema differencing and merging at runtime, and it does not depend on whatever this
 * package happens to declare.
 */
const transformerRequire = createRequire(
  path.join(quickRootDirectory, "..", "..", "..", "transformer", "package.json")
);

function transformerDependencyVersion(packageName: string): string {
  const packageJson = JSON.parse(
    fs.readFileSync(
      transformerRequire.resolve(`${packageName}/package.json`),
      "utf8"
    )
  ) as { version: string };
  return packageJson.version;
}

const generator = {
  coreBackend: packageVersion("@itwin/core-backend"),
  node: process.version,
  transformer: packageVersion("@itwin/imodel-transformer"),
};

const recipeIdentity = (topology: string) => ({
  schema: "QuickPerf.01.00.00",
  seed: 328,
  topology,
  distribution,
  inputs: {
    recipe: fs.readFileSync(
      quickPath("src", "fixtures", "recipes", "balancedIncremental.ts"),
      "utf8"
    ),
    schema: fs.readFileSync(
      quickPath("assets", "schemas", "QuickPerf.ecschema.xml"),
      "utf8"
    ),
    lockfile: fs.readFileSync(
      path.resolve(
        quickRootDirectory,
        "..",
        "..",
        "..",
        "..",
        "pnpm-lock.yaml"
      ),
      "utf8"
    ),
  },
  versions: generator,
});

const scenarioClaims = [
  "incremental synchronization",
  "aspect lifecycle",
  "relationship lifecycle",
  "mixed scalar and geometry element changes",
];

export const balancedIncrementalDescriptor: FixtureDescriptor = {
  id: "balanced-incremental",
  version: 1,
  label: "balanced incremental",
  scenarioClaims,
  layout: {
    kind: "reconstructed",
    topology: "source-and-empty-target",
    recipe: "balanced-incremental",
    seed: 328,
  },
  distribution,
  generator,
  recipeHash: canonicalSha256(recipeIdentity("source-and-empty-target")),
};

/**
 * The same change mix captured as a relocatable artifact instead of rebuilt per sample.
 *
 * Topology, not recipe, is what differs: there is no target iModel, so nothing re-enters the hub
 * and the source briefcase plus its changesets can be copied per sample.
 */
export const balancedIncrementalSourceOnlyDescriptor: FixtureDescriptor = {
  id: "balanced-incremental-source-only",
  version: 1,
  label: "balanced incremental (source only)",
  scenarioClaims: [...scenarioClaims, "changeset scanning"],
  layout: {
    kind: "reconstructed",
    topology: "source-only",
    recipe: "balanced-incremental",
    seed: 328,
  },
  distribution,
  generator,
  recipeHash: canonicalSha256(recipeIdentity("source-only")),
};

const dynamicSchemaUnionDistribution: FixtureDistribution = {
  base: { aspects: 0, elements: 0, geometricElements: 0, relationships: 0 },
  operations: {
    aspects: { deletes: 0, inserts: 0, updates: 0 },
    elements: { deletes: 0, inserts: 0, updates: 0 },
    relationships: { deletes: 0, inserts: 0, updates: 0 },
    geometryUpdates: 0,
    sourceChangesets: 0,
  },
};

const dynamicSchemaUnionRecipeIdentity = {
  schemaPair: "QuickPerfDynamic.01.00.12+01.00.09",
  topology: "snapshot-schema-pair",
  scale: dynamicSchemaUnionScale,
  distribution: dynamicSchemaUnionDistribution,
  inputs: {
    recipe: fs.readFileSync(
      quickPath("src", "fixtures", "recipes", "dynamicSchemaUnion.ts"),
      "utf8"
    ),
    lockfile: fs.readFileSync(
      path.resolve(
        quickRootDirectory,
        "..",
        "..",
        "..",
        "..",
        "pnpm-lock.yaml"
      ),
      "utf8"
    ),
  },
  versions: {
    ...generator,
    ecschemaEditing: transformerDependencyVersion("@itwin/ecschema-editing"),
    ecschemaLocaters: transformerDependencyVersion("@itwin/ecschema-locaters"),
  },
};

/**
 * A local, already-divergent dynamic schema pair sized to make differencing and merging
 * measurable. No elements, aspects, or relationships: this fixture exists only to benchmark
 * `IModelTransformer.processSchemas({ strategy: new DynamicSchemaUnionStrategy() })`.
 */
export const dynamicSchemaUnionMediumDescriptor: FixtureDescriptor = {
  id: "dynamic-schema-union-medium",
  version: 1,
  label: "dynamic schema union (medium)",
  scenarioClaims: ["dynamic schema union"],
  layout: {
    kind: "reconstructed",
    topology: "snapshot-schema-pair",
    recipe: dynamicSchemaUnionRecipe.id,
    // No randomness is involved; the recipe is fully deterministic.
    seed: 0,
  },
  distribution: dynamicSchemaUnionDistribution,
  generator,
  recipeHash: canonicalSha256(dynamicSchemaUnionRecipeIdentity),
};

const fixtures = new Map<string, FixtureDescriptor>([
  [balancedIncrementalDescriptor.id, balancedIncrementalDescriptor],
  [
    balancedIncrementalSourceOnlyDescriptor.id,
    balancedIncrementalSourceOnlyDescriptor,
  ],
  [dynamicSchemaUnionMediumDescriptor.id, dynamicSchemaUnionMediumDescriptor],
]);

export function registerFixtureDescriptor(descriptor: FixtureDescriptor): void {
  if (fixtures.has(descriptor.id))
    throw new Error(`Duplicate quick performance fixture: ${descriptor.id}`);
  fixtures.set(descriptor.id, descriptor);
}

export function listFixtureIds(): string[] {
  return [...fixtures.keys()];
}

export function getFixtureDescriptor(id: string): FixtureDescriptor {
  const descriptor = fixtures.get(id);
  if (!descriptor)
    throw new Error(
      `Unknown quick performance fixture "${id}". Available fixtures: ${listFixtureIds().join(
        ", "
      )}`
    );
  return descriptor;
}
