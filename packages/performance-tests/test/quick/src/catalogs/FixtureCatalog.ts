/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import {
  canonicalSha256,
  FixtureDescriptor,
} from "../fixtures/FixtureDescriptor.js";
import { quickPath, quickRootDirectory } from "../support/paths.js";
import { resolvedVersions } from "../support/versions.js";

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

const generator = resolvedVersions();

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

/**
 * Update-heavy multi-changeset source, sized so that scanning it dominates timer noise.
 *
 * Region sizes are derived from `base.elements` by `scanRegionSizes`; see
 * `fixtures/recipes/updateHeavyScan` for what each region proves.
 *
 * Calibrated at 3,520 elements x 20 changesets: the scan measures ~3.46 s with a coefficient of
 * variation of 1.0% over 8 samples, against ~51 ms of per-sample copy, verification and teardown.
 * Scan cost is linear in changed rows at roughly 49 ms per (1,000 elements x changeset), so the
 * shape is a single `scanScale` knob.
 */
const scanScale = 16;
const scanDistribution = {
  base: {
    // Region A (updated throughout) plus region B (updated, then deleted last).
    aspects: 220 * scanScale,
    elements: 220 * scanScale,
    geometricElements: 0,
    relationships: 40 * scanScale,
  },
  operations: {
    elements: {
      // Regions C and D, both inserted in the first changeset.
      inserts: 30 * scanScale,
      // Every seeded element is updated, plus region C after its insert.
      updates: 240 * scanScale,
      // Region B at the end, region D at the end.
      deletes: 30 * scanScale,
    },
    aspects: {
      inserts: 20 * scanScale,
      updates: 220 * scanScale,
      // Region B's owned aspects, cascade-deleted with their elements.
      deletes: 20 * scanScale,
    },
    relationships: {
      inserts: 10 * scanScale,
      updates: 10 * scanScale,
      deletes: 10 * scanScale,
    },
    geometryUpdates: 0,
    sourceChangesets: 20,
  },
} as const;

const scanRecipeIdentity = {
  schema: "QuickPerfScan.01.00.00",
  seed: 328,
  topology: "source-only",
  distribution: scanDistribution,
  inputs: {
    recipe: fs.readFileSync(
      quickPath("src", "fixtures", "recipes", "updateHeavyScan.ts"),
      "utf8"
    ),
    schema: fs.readFileSync(
      quickPath("assets", "schemas", "QuickPerfScan.ecschema.xml"),
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
};

export const updateHeavyScanDescriptor: FixtureDescriptor = {
  id: "update-heavy-scan",
  version: 1,
  label: "update-heavy scan",
  scenarioClaims: [
    "changeset scanning",
    "changed-instance squashing",
    "aspect lifecycle",
    "relationship lifecycle",
  ],
  layout: {
    kind: "reconstructed",
    topology: "source-only",
    recipe: "update-heavy-scan",
    seed: 328,
  },
  distribution: scanDistribution,
  generator,
  recipeHash: canonicalSha256(scanRecipeIdentity),
};

const fixtures = new Map<string, FixtureDescriptor>([
  [balancedIncrementalDescriptor.id, balancedIncrementalDescriptor],
  [
    balancedIncrementalSourceOnlyDescriptor.id,
    balancedIncrementalSourceOnlyDescriptor,
  ],
  [updateHeavyScanDescriptor.id, updateHeavyScanDescriptor],
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
