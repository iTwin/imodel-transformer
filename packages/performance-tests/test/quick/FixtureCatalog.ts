/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "fs";
import * as path from "path";
import { DatasetDescriptor } from "./DatasetDescriptor";
import { canonicalSha256 } from "./FixtureManifest";
import { quickSourceDirectory, quickSourcePath } from "./quickPaths";
import { resolvedVersions } from "./versions";

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
      quickSourcePath("recipes", "balancedIncremental.ts"),
      "utf8"
    ),
    schema: fs.readFileSync(
      quickSourcePath("schemas", "QuickPerf.ecschema.xml"),
      "utf8"
    ),
    lockfile: fs.readFileSync(
      path.resolve(
        quickSourceDirectory,
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

export const balancedIncrementalDescriptor: DatasetDescriptor = {
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
export const balancedIncrementalSourceOnlyDescriptor: DatasetDescriptor = {
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
 * Region sizes are derived from `base.elements` by `scanRegionSizes`; see `recipes/updateHeavyScan`
 * for what each region proves.
 *
 * Calibrated at 3,520 elements x 20 changesets: the scan measures ~3.46 s with a coefficient of
 * variation of 1.0% over 8 samples, against ~51 ms of per-sample copy, verification and teardown.
 * Scan cost is linear in changed rows at roughly 49 ms per (1,000 elements x changeset), so the
 * shape is a single `scanScale` knob.
 *
 * The size is bounded by comparison mode rather than by a single run. One run is 9 executions and
 * finishes in well under a minute (measured: 52 s including the 19.3 s build), but an A/B
 * comparison at its escalated width is 128 executions, where the build cost amortizes away and the
 * per-execution cost is all that matters. This shape keeps that case inside a 15 minute budget with
 * roughly 2x headroom for slower CI hardware; a larger shape would measure the same thing no better
 * and fit the escalated case worse.
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
      quickSourcePath("recipes", "updateHeavyScan.ts"),
      "utf8"
    ),
    schema: fs.readFileSync(
      quickSourcePath("schemas", "QuickPerfScan.ecschema.xml"),
      "utf8"
    ),
    lockfile: fs.readFileSync(
      path.resolve(
        quickSourceDirectory,
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

export const updateHeavyScanDescriptor: DatasetDescriptor = {
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

const fixtures = new Map<string, DatasetDescriptor>([
  [balancedIncrementalDescriptor.id, balancedIncrementalDescriptor],
  [
    balancedIncrementalSourceOnlyDescriptor.id,
    balancedIncrementalSourceOnlyDescriptor,
  ],
  [updateHeavyScanDescriptor.id, updateHeavyScanDescriptor],
]);

export function registerFixtureDescriptor(descriptor: DatasetDescriptor): void {
  if (fixtures.has(descriptor.id))
    throw new Error(`Duplicate quick performance fixture: ${descriptor.id}`);
  fixtures.set(descriptor.id, descriptor);
}

export function listFixtureIds(): string[] {
  return [...fixtures.keys()];
}

export function getFixtureDescriptor(id: string): DatasetDescriptor {
  const descriptor = fixtures.get(id);
  if (!descriptor)
    throw new Error(
      `Unknown quick performance fixture "${id}". Available fixtures: ${listFixtureIds().join(
        ", "
      )}`
    );
  return descriptor;
}
