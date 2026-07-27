/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { PreparedDataset } from "./FixtureMaterializer";

export interface BenchmarkScenario {
  abort(): void;
  finish(): Promise<string>;
  measure(): Promise<void>;
}

export type BenchmarkScenarioFactory = (
  dataset: PreparedDataset
) => BenchmarkScenario;

export const defaultScenarioBudgetMilliseconds = 15 * 60 * 1000;

export interface BenchmarkScenarioDefinition {
  readonly id: string;
  readonly factory: BenchmarkScenarioFactory;
  /**
   * Wall time allowed for the measured run, excluding checkout, install and build.
   * Defaults to {@link defaultScenarioBudgetMilliseconds}.
   */
  readonly budgetMilliseconds?: number;
}

export function scenarioBudgetMilliseconds(
  definition: BenchmarkScenarioDefinition
): number {
  const budget =
    definition.budgetMilliseconds ?? defaultScenarioBudgetMilliseconds;
  if (!Number.isFinite(budget) || budget <= 0)
    throw new Error(
      `Scenario "${definition.id}" declares an invalid budget: ${budget}`
    );
  return budget;
}
