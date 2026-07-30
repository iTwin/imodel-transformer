/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BenchmarkScenarioDefinition } from "./BenchmarkScenario.js";
import { FixtureDescriptor } from "../fixtures/FixtureDescriptor.js";
import { getFixtureDescriptor } from "../fixtures/FixtureCatalog.js";
import { getFixtureRecipe } from "../fixtures/FixtureRecipe.js";
import { getScenarioDefinition } from "../scenarios/ScenarioCatalog.js";

export interface ResolvedBenchmarkRun {
  readonly scenario: BenchmarkScenarioDefinition;
  readonly descriptor: FixtureDescriptor;
}

export const defaultQuickPerformanceMeasuredSamples = 1;

function orUndefined(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === "" ? undefined : value.trim();
}

export function resolveMeasuredSamples(value: string | undefined): number {
  const configured = orUndefined(value);
  if (configured === undefined) return defaultQuickPerformanceMeasuredSamples;
  if (!/^[1-9]\d*$/.test(configured)) {
    throw new Error(
      `QUICK_PERF_SAMPLES must be a positive integer; received "${configured}"`
    );
  }
  const measuredSamples = Number(configured);
  if (!Number.isSafeInteger(measuredSamples)) {
    throw new Error(
      `QUICK_PERF_SAMPLES must be a safe integer; received "${configured}"`
    );
  }
  return measuredSamples;
}

export function resolveMeasuredSamplesFromEnvironment(
  env: NodeJS.ProcessEnv = process.env
): number {
  return resolveMeasuredSamples(env.QUICK_PERF_SAMPLES);
}

/**
 * Validate a resolved scenario/fixture pair.
 *
 * Capabilities describe what a scenario *needs*; they do not choose a fixture. Selection has
 * already happened by the time this runs, so a mismatch is a configuration error, not a signal to
 * pick something else.
 */
export function assertScenarioSupportsFixture(
  scenario: BenchmarkScenarioDefinition,
  descriptor: FixtureDescriptor
): void {
  const { capabilities } = scenario;
  if (descriptor.layout.topology !== capabilities.topology)
    throw new Error(
      `Scenario "${scenario.id}" requires a "${capabilities.topology}" fixture but "${descriptor.id}" is "${descriptor.layout.topology}"`
    );
  const missing = (capabilities.requiredClaims ?? []).filter(
    (claim) => !descriptor.scenarioClaims.includes(claim)
  );
  if (missing.length > 0)
    throw new Error(
      `Fixture "${descriptor.id}" does not claim [${missing.join(
        ", "
      )}] required by scenario "${scenario.id}"`
    );
  // A fixture is useless if nothing knows how to generate it.
  getFixtureRecipe(descriptor.layout.recipe);
}

/**
 * Resolve the scenario and the fixture it will run against. `fixtureId` overrides the scenario's
 * declared default; omit it for the normal path.
 */
export function resolveBenchmarkRun(
  scenarioId?: string,
  fixtureId?: string
): ResolvedBenchmarkRun {
  const scenario = getScenarioDefinition(scenarioId);
  const descriptor = getFixtureDescriptor(
    fixtureId ?? scenario.defaultFixtureId
  );
  assertScenarioSupportsFixture(scenario, descriptor);
  return { scenario, descriptor };
}

/** Resolve from the environment, as both entry points do. */
export function resolveBenchmarkRunFromEnvironment(
  env: NodeJS.ProcessEnv = process.env
): ResolvedBenchmarkRun {
  // CI passes unset inputs through as empty strings; treat those as "not specified".
  return resolveBenchmarkRun(
    orUndefined(env.QUICK_PERF_SCENARIO),
    orUndefined(env.QUICK_PERF_FIXTURE)
  );
}
