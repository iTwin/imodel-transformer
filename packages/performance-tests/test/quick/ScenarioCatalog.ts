/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BenchmarkScenarioDefinition } from "./BenchmarkScenario";
import { changesetScanningScenario } from "./scenarios/changesetScanning";
import { incrementalSynchronizationScenario } from "./scenarios/incrementalSynchronization";

export const defaultQuickPerformanceScenarioId =
  incrementalSynchronizationScenario.id;

const scenarios = new Map<string, BenchmarkScenarioDefinition>([
  [incrementalSynchronizationScenario.id, incrementalSynchronizationScenario],
  [changesetScanningScenario.id, changesetScanningScenario],
]);

export function registerScenarioDefinition(
  scenario: BenchmarkScenarioDefinition
): void {
  if (scenarios.has(scenario.id))
    throw new Error(`Duplicate quick performance scenario: ${scenario.id}`);
  scenarios.set(scenario.id, scenario);
}

export function listScenarioIds(): string[] {
  return [...scenarios.keys()];
}

export function getScenarioDefinition(
  requestedId?: string
): BenchmarkScenarioDefinition {
  const scenarioId = requestedId ?? defaultQuickPerformanceScenarioId;
  const scenario = scenarios.get(scenarioId);
  if (!scenario)
    throw new Error(
      `Unknown quick performance scenario "${scenarioId}". Available scenarios: ${listScenarioIds().join(
        ", "
      )}`
    );
  return scenario;
}
