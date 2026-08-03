/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ConfiguredFixture } from "../fixtures/FixtureRecipe.js";
import { BenchmarkScenarioDefinition } from "./BenchmarkScenario.js";

/** One author-owned contribution to the quick performance registry. */
export interface BenchmarkRegistration {
  readonly scenario: BenchmarkScenarioDefinition;
  readonly fixtures?: readonly ConfiguredFixture[];
}

/**
 * Freeze a registration and its fixture list so callers cannot desynchronize the registry's
 * immutable lookup maps from the definitions returned for diagnostics and contract tests.
 */
export function defineBenchmark(
  registration: BenchmarkRegistration
): BenchmarkRegistration {
  const scenario = Object.freeze({
    ...registration.scenario,
    capabilities: Object.freeze({
      ...registration.scenario.capabilities,
      requiredClaims:
        registration.scenario.capabilities.requiredClaims === undefined
          ? undefined
          : Object.freeze([
              ...registration.scenario.capabilities.requiredClaims,
            ]),
    }),
  });
  return Object.freeze({
    ...registration,
    scenario,
    fixtures:
      registration.fixtures === undefined
        ? undefined
        : Object.freeze([...registration.fixtures]),
  });
}
