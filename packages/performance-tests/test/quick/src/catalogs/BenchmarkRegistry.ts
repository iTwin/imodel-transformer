/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ConfiguredFixture } from "../fixtures/FixtureRecipe.js";
import { BenchmarkRegistration } from "../framework/BenchmarkRegistration.js";
import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";
import { changesetScanningBenchmark } from "../scenarios/changesetScanning.js";
import {
  exportOnlyHierarchyTraversalBenchmark,
  exportOnlyLinearTraversalBenchmark,
} from "../scenarios/exportOnlyTraversal.js";
import { incrementalSynchronizationBenchmark } from "../scenarios/incrementalSynchronization.js";
import { schemaProcessingBenchmark } from "../scenarios/schemaProcessing.js";
import { standaloneFullTransformationBenchmark } from "../scenarios/standaloneFullTransformation.js";

// Every benchmark is added in exactly one explicit place so the compiled CLI remains predictable.
const registrations: readonly BenchmarkRegistration[] = Object.freeze([
  incrementalSynchronizationBenchmark,
  changesetScanningBenchmark,
  schemaProcessingBenchmark,
  standaloneFullTransformationBenchmark,
  exportOnlyHierarchyTraversalBenchmark,
  exportOnlyLinearTraversalBenchmark,
]);

const scenarios = new Map<string, BenchmarkScenarioDefinition>();
const fixtures = new Map<string, ConfiguredFixture>();
for (const registration of registrations) {
  if (scenarios.has(registration.scenario.id))
    throw new Error(
      `Duplicate quick performance scenario: ${registration.scenario.id}`
    );
  scenarios.set(registration.scenario.id, registration.scenario);
  for (const fixture of registration.fixtures ?? []) {
    if (fixtures.has(fixture.descriptor.id))
      throw new Error(
        `Duplicate quick performance fixture: ${fixture.descriptor.id}`
      );
    fixtures.set(fixture.descriptor.id, fixture);
  }
}

export function listBenchmarkRegistrations(): readonly BenchmarkRegistration[] {
  return registrations;
}

export function listRegisteredScenarios(): readonly BenchmarkScenarioDefinition[] {
  return [...scenarios.values()];
}

export function listRegisteredFixtures(): readonly ConfiguredFixture[] {
  return [...fixtures.values()];
}

export function getRegisteredScenario(
  id: string
): BenchmarkScenarioDefinition | undefined {
  return scenarios.get(id);
}

export function getRegisteredFixture(
  id: string
): ConfiguredFixture | undefined {
  return fixtures.get(id);
}
