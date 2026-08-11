/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import { largeBaseIncrementalFixture } from "../fixtures/recipes/largeBaseIncremental.js";
import { incrementalSynchronization } from "./incrementalSynchronization.js";

/**
 * The same measured operation as `incremental-synchronization` — a changes-mode
 * `IModelTransformer.process()` into a prepared target — but against a fixture whose
 * base is three orders of magnitude larger than its delta (50,000 base elements,
 * 50 changed). Incremental synchronization should scale with changeset size, so this
 * scenario exposes any cost proportional to the unchanged base (issue #98) that the
 * proportionally-churned `balanced-incremental` fixture dilutes.
 */
export const largeBaseIncrementalSynchronizationScenario: BenchmarkScenarioDefinition =
  {
    id: "large-base-incremental-synchronization",
    defaultFixtureId: "large-base-incremental",
    capabilities: {
      topology: "source-and-empty-target",
      requiredClaims: ["large-base incremental synchronization"],
    },
    factory: incrementalSynchronization,
  };

export const largeBaseIncrementalSynchronizationBenchmark = defineBenchmark({
  scenario: largeBaseIncrementalSynchronizationScenario,
  fixtures: [largeBaseIncrementalFixture],
});
