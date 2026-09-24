/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import { legacyGuidlessDeletionFallbackFixture } from "../fixtures/recipes/deletionHeavyIncremental.js";
import { incrementalSynchronization } from "./incrementalSynchronization.js";

/**
 * Measures the same incremental transformation as `incremental-synchronization` while deleting
 * legacy or special elements that have no FederationGuid. Typical modern iModels are expected to
 * use FederationGuid values; this scenario isolates the provenance fallback for exceptions.
 */
export const legacyGuidlessDeletionFallbackScenario: BenchmarkScenarioDefinition =
  {
    id: "legacy-guidless-deletion-fallback",
    defaultFixtureId: "legacy-guidless-deletion-fallback",
    capabilities: {
      topology: "source-and-empty-target",
      requiredClaims: ["legacy guidless deletion fallback"],
    },
    factory: incrementalSynchronization,
  };

export const legacyGuidlessDeletionFallbackBenchmark = defineBenchmark({
  scenario: legacyGuidlessDeletionFallbackScenario,
  fixtures: [legacyGuidlessDeletionFallbackFixture],
});
