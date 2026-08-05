/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { FixtureTopology } from "../fixtures/FixtureDescriptor.js";
import { PreparedDataset } from "../fixtures/FixtureProvider.js";

export interface BenchmarkScenario {
  abort(): void;
  /** Unmeasured setup performed after materialization and before timing starts. */
  prepare?(): Promise<void>;
  finish(): Promise<string>;
  measure(): Promise<void>;
}

export type ScenarioConfiguration = Readonly<Record<string, string>>;

export type BenchmarkScenarioFactory = (
  dataset: PreparedDataset
) => BenchmarkScenario;

/**
 * What a scenario needs from a fixture. Capabilities *validate* a resolved scenario/fixture pair;
 * they never select one. Selection is {@link BenchmarkScenarioDefinition.defaultFixtureId} or an
 * explicit override.
 */
export interface BenchmarkScenarioCapabilities {
  readonly topology: FixtureTopology;
  /** Claims the fixture must advertise in `scenarioClaims`. */
  readonly requiredClaims?: readonly string[];
}

export interface BenchmarkScenarioDefinition {
  readonly id: string;
  readonly defaultFixtureId: string;
  readonly capabilities: BenchmarkScenarioCapabilities;
  /** Opaque, scenario-owned configuration recorded with every sample. */
  readonly configuration?: ScenarioConfiguration;
  readonly factory: BenchmarkScenarioFactory;
  /** Wall-clock budget for the whole run, in milliseconds. */
  readonly budgetMilliseconds?: number;
}

export const defaultScenarioBudgetMilliseconds = 15 * 60 * 1000;

export function scenarioBudgetMilliseconds(
  scenario: BenchmarkScenarioDefinition
): number {
  return scenario.budgetMilliseconds ?? defaultScenarioBudgetMilliseconds;
}
