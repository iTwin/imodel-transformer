/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { PreparedDataset } from "../fixtures/FixtureProvider.js";
import { standaloneFullTransform50kFixture } from "../fixtures/recipes/standaloneFullTransform.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import { createStandaloneFullTransformation } from "./standaloneFullTransformation.js";

/**
 * The `standalone-full-transformation` pipeline with the `@alpha`
 * `experimentalSourceElementPrefetch` option enabled, so source-element reads are offloaded to a
 * child process and overlap target writes. The prefetcher's child boot happens inside the timed
 * `measure()` (it starts within `IModelTransformer.process()`), so samples include the full cost
 * consumers would pay. The transformer stops the prefetch child when `process()` settles, and the
 * scenario's `abort()` disposes the transformer, so the harness never leaks the child process.
 *
 * Compare against the baseline on the same fixture:
 *
 * ```sh
 * QUICK_PERF_SCENARIO=standalone-full-transformation \
 * QUICK_PERF_FIXTURE=standalone-full-transform-50k pnpm test:quick
 * QUICK_PERF_SCENARIO=prefetch-full-transformation pnpm test:quick
 * ```
 */
export function prefetchFullTransformation(
  dataset: PreparedDataset
): BenchmarkScenario {
  return createStandaloneFullTransformation(dataset, {
    experimentalSourceElementPrefetch: true,
  });
}

export const prefetchFullTransformationScenario: BenchmarkScenarioDefinition = {
  id: "prefetch-full-transformation",
  defaultFixtureId: "standalone-full-transform-50k",
  capabilities: {
    topology: "standalone-source-and-empty-target",
    requiredClaims: ["full transformation"],
  },
  configuration: { experimentalSourceElementPrefetch: "true" },
  factory: prefetchFullTransformation,
};

export const prefetchFullTransformationBenchmark = defineBenchmark({
  scenario: prefetchFullTransformationScenario,
  fixtures: [standaloneFullTransform50kFixture],
});
