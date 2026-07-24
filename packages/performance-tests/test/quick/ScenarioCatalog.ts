/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BenchmarkScenarioDefinition } from "./BenchmarkScenario";
import { incrementalSynchronizationScenario } from "./scenarios/incrementalSynchronization";

export const defaultQuickPerformanceScenarioId =
  incrementalSynchronizationScenario.id;

const scenarios = new Map<string, BenchmarkScenarioDefinition>([
  [incrementalSynchronizationScenario.id, incrementalSynchronizationScenario],
]);

export function getScenarioDefinition(
  requestedId?: string
): BenchmarkScenarioDefinition {
  const scenarioId = requestedId ?? defaultQuickPerformanceScenarioId;
  const scenario = scenarios.get(scenarioId);
  if (!scenario)
    throw new Error(
      `Unknown quick performance scenario "${scenarioId}". Available scenarios: ${[
        ...scenarios.keys(),
      ].join(", ")}`
    );
  return scenario;
}
