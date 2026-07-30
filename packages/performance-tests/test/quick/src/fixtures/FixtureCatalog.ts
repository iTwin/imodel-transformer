/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as path from "node:path";
import { FixtureDescriptor } from "./FixtureDescriptor.js";
import { canonicalSha256 } from "./FixtureManifest.js";
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

const fixtures = new Map<string, FixtureDescriptor>([
  [balancedIncrementalDescriptor.id, balancedIncrementalDescriptor],
  [
    balancedIncrementalSourceOnlyDescriptor.id,
    balancedIncrementalSourceOnlyDescriptor,
  ],
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
