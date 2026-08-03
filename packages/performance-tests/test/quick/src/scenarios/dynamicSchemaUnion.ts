/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { DynamicSchemaUnionStrategy } from "@itwin/imodel-transformer/schema-processing";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import {
  PreparedDataset,
  requireSnapshotSchemaPairDataset,
} from "../fixtures/FixtureProvider.js";
import { expectDynamicSchemaUnionExpectation } from "../fixtures/recipes/dynamicSchemaUnion.js";
import { assertDynamicSchemaUnion } from "../fixtures/validation/validateFixture.js";

/**
 * Measures only `IModelTransformer.processSchemas({ strategy: new DynamicSchemaUnionStrategy() })`
 * against a local, already-divergent dynamic schema pair. No elements, aspects, or relationships
 * are involved; the fixture and this scenario exist purely to measure schema differencing and
 * merge cost.
 */
export function dynamicSchemaUnion(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { sourceDb, targetDb, expectation } =
    requireSnapshotSchemaPairDataset(dataset);
  const dynamicSchemaUnionExpectation =
    expectDynamicSchemaUnionExpectation(expectation);

  const editTxn = new EditTxn(
    targetDb,
    "Quick performance dynamic schema union"
  );
  editTxn.start();
  const transformer = new IModelTransformer({
    source: sourceDb,
    target: editTxn,
  });
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    transformer.dispose();
    if (editTxn.isActive) editTxn.end();
    disposed = true;
  };
  return {
    abort: dispose,
    async measure() {
      await transformer.processSchemas({
        strategy: new DynamicSchemaUnionStrategy(),
      });
    },
    async finish() {
      dispose();
      return assertDynamicSchemaUnion(
        sourceDb,
        targetDb,
        dynamicSchemaUnionExpectation
      );
    },
  };
}

export const dynamicSchemaUnionScenario: BenchmarkScenarioDefinition = {
  id: "dynamic-schema-union",
  defaultFixtureId: "dynamic-schema-union-medium",
  capabilities: {
    topology: "snapshot-schema-pair",
    requiredClaims: ["dynamic schema union"],
  },
  factory: dynamicSchemaUnion,
};
