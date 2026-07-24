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

export interface BenchmarkScenarioDefinition {
  readonly id: string;
  readonly factory: BenchmarkScenarioFactory;
}
