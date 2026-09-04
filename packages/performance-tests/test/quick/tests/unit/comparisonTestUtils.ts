/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import type { ComparisonSample } from "../../src/comparison/ComparisonReport.js";
import {
  FixtureArtifactManifest,
  fixtureArtifactVersion,
} from "../../src/fixtures/FixtureArtifact.js";

export function benchmarkSample(
  overrides: Partial<ComparisonSample> = {}
): ComparisonSample {
  return {
    cpuSystemMilliseconds: 1,
    cpuUserMilliseconds: 2,
    fixtureBuildMilliseconds: 10,
    fixtureGenerator: {
      coreBackend: "5.10.3",
      node: "24.18.0",
      transformer: "0.6.0",
    },
    fixtureContentHash: "fixture-content-hash",
    fixtureId: "update-heavy-scan",
    fixtureInventory: {
      byteLength: 1_048_576,
      schemaCount: 10,
      classCount: 100,
      propertyCount: 500,
      modelCount: 5,
      elementCount: 1_000,
    },
    fixtureRecipeHash: "recipe-hash",
    fixtureVersion: 1,
    measured: true,
    operations: {
      aspects: { deletes: 1, inserts: 1, updates: 1 },
      elements: { deletes: 1, inserts: 1, updates: 1 },
      geometryUpdates: 1,
      relationships: { deletes: 1, inserts: 1, updates: 1 },
      sourceChangesets: 1,
    },
    reconstructionMilliseconds: 3,
    reportSchemaVersion: 1,
    rssDeltaBytes: 4,
    sample: 1,
    scenarioId: "changeset-scanning",
    scenarioConfiguration: { mode: "scan" },
    semanticDigest: "semantic-digest",
    teardownMilliseconds: 5,
    topology: "source-only",
    transformerProvenance: {
      coreBackendVersion: "5.10.3",
      contentHash: "transformer-content-hash",
      entryPoint: "transformer-entry-point",
      version: "0.6.0",
    },
    verificationMilliseconds: 6,
    wallMilliseconds: 100,
    workerPeakRssBytes: 50 * 1048576,
    ...overrides,
  };
}

export function armSamples(
  measuredMilliseconds: readonly number[]
): ComparisonSample[] {
  return [
    benchmarkSample({ measured: false, sample: 0, wallMilliseconds: 90 }),
    ...measuredMilliseconds.map((wallMilliseconds, index) =>
      benchmarkSample({ sample: index + 1, wallMilliseconds })
    ),
  ];
}

export function fixtureArtifactManifest(): FixtureArtifactManifest {
  return {
    artifactVersion: fixtureArtifactVersion,
    contentHash: "fixture-content-hash",
    descriptor: {
      id: "update-heavy-scan",
      version: 1,
      label: "update-heavy-scan",
      scenarioClaims: ["changeset scanning"],
      layout: {
        kind: "reconstructed",
        topology: "source-only",
        recipe: "update-heavy-scan",
        seed: 1,
      },
      distribution: {
        base: {
          aspects: 1,
          elements: 1,
          geometricElements: 1,
          relationships: 1,
        },
        operations: benchmarkSample().operations,
      },
      generator: benchmarkSample().fixtureGenerator,
      recipeHash: "recipe-hash",
    },
    iModelInventory: {
      byteLength: 1_048_576,
      schemaCount: 10,
      classCount: 100,
      propertyCount: 500,
      modelCount: 5,
      elementCount: 1_000,
    },
    briefcase: {
      fileName: "briefcase.bim",
      briefcaseId: 1,
      changeset: { id: "changeset", index: 1 },
      byteLength: 1,
    },
    changesets: {
      directory: "changesets",
      propsFile: "csFileProps.json",
      count: 1,
      baseChangesetIndex: 0,
      firstIndex: 1,
      lastIndex: 1,
    },
    buildMilliseconds: 10,
    builtAt: "2026-08-04T00:00:00.000Z",
  };
}
