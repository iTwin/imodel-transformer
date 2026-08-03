/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  getRegisteredScenario,
  listRegisteredScenarios,
} from "./BenchmarkRegistry.js";
import { BenchmarkScenarioDefinition } from "../framework/BenchmarkScenario.js";
import { incrementalSynchronizationScenario } from "../scenarios/incrementalSynchronization.js";

export const defaultQuickPerformanceScenarioId =
  incrementalSynchronizationScenario.id;

export function listScenarioIds(): string[] {
  return listRegisteredScenarios().map((scenario) => scenario.id);
}

export function getScenarioDefinition(
  requestedId?: string
): BenchmarkScenarioDefinition {
  const scenarioId = requestedId ?? defaultQuickPerformanceScenarioId;
  const scenario = getRegisteredScenario(scenarioId);
  if (!scenario)
    throw new Error(
      `Unknown quick performance scenario "${scenarioId}". Available scenarios: ${listScenarioIds().join(
        ", "
      )}`
    );
  return scenario;
}
