/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelTransformer } from "@itwin/imodel-transformer";
import { HubMock } from "@itwin/core-backend/lib/cjs/internal/HubMock";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../BenchmarkScenario";
import { PreparedDataset } from "../FixtureMaterializer";
import { createStartedEditTxn } from "../LocalHubFixture";
import {
  assertSemanticallyEqual,
  assertSynchronizationProvenance,
} from "../validation/validateFixture";

export function incrementalSynchronization(
  dataset: PreparedDataset
): BenchmarkScenario {
  if (!HubMock.isValid)
    throw new Error("Quick performance scenarios require an active HubMock");
  const editTxn = createStartedEditTxn(dataset.hub.targetDb);
  const transformer = new IModelTransformer(
    { source: dataset.hub.sourceDb, target: editTxn },
    { argsForProcessChanges: {} }
  );
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
      await transformer.process();
    },
    async finish() {
      dispose();
      await assertSynchronizationProvenance(
        dataset.hub.sourceDb,
        dataset.hub.targetDb
      );
      return assertSemanticallyEqual(
        dataset.hub.sourceDb,
        dataset.hub.targetDb
      );
    },
  };
}

export const incrementalSynchronizationScenario: BenchmarkScenarioDefinition = {
  id: "incremental-synchronization",
  factory: incrementalSynchronization,
};
