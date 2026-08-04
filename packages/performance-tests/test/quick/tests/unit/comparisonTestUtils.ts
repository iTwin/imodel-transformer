/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BenchmarkSample } from "../../src/framework/BenchmarkRunner.js";

export function benchmarkSample(
  overrides: Partial<BenchmarkSample> = {}
): BenchmarkSample {
  return {
    cpuSystemMilliseconds: 1,
    cpuUserMilliseconds: 2,
    fixtureContentHash:
      "1111111111111111111111111111111111111111111111111111111111111111",
    fixtureBuildMilliseconds: 10,
    fixtureGenerator: {
      coreBackend: "5.10.3",
      node: "24.18.0",
      transformer: "0.6.0",
    },
    fixtureId: "update-heavy-scan",
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
    semanticDigest: "semantic-digest",
    teardownMilliseconds: 5,
    topology: "source-only",
    transformerVersion: "2.0.0-dev.39",
    verificationMilliseconds: 6,
    wallMilliseconds: 100,
    ...overrides,
  };
}

export function armSamples(
  measuredMilliseconds: readonly number[]
): BenchmarkSample[] {
  return [
    benchmarkSample({ measured: false, sample: 0, wallMilliseconds: 90 }),
    ...measuredMilliseconds.map((wallMilliseconds, index) =>
      benchmarkSample({ sample: index + 1, wallMilliseconds })
    ),
  ];
}
