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

export function defineBenchmark(
  registration: BenchmarkRegistration
): BenchmarkRegistration {
  return Object.freeze({
    ...registration,
    fixtures:
      registration.fixtures === undefined
        ? undefined
        : Object.freeze([...registration.fixtures]),
  });
}
