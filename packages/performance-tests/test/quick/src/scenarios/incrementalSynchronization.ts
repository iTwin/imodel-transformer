/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelExporter, IModelTransformer } from "@itwin/imodel-transformer";
import { Id64String } from "@itwin/core-bentley";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import {
  PreparedDataset,
  requireLiveHubDataset,
} from "../fixtures/FixtureProvider.js";
import { createStartedEditTxn } from "../fixtures/LocalHubFixture.js";
import {
  assertSemanticallyEqual,
  assertSynchronizationProvenance,
} from "../fixtures/validation/validateFixture.js";
import { balancedIncrementalFixture } from "../fixtures/recipes/balancedIncremental.js";
import { deletionHeavyIncrementalFixture } from "../fixtures/recipes/deletionHeavyIncremental.js";
import { hierarchicalIncrementalFixture } from "../fixtures/recipes/hierarchicalIncremental.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";

/** EXPERIMENT: overriding exportElement forces the legacy per-element traversal. */
class LegacyTraversalExporter extends IModelExporter {
  public override async exportElement(elementId: Id64String) {
    return super.exportElement(elementId);
  }
}

export function incrementalSynchronization(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { hub } = requireLiveHubDataset(dataset);
  const editTxn = createStartedEditTxn(hub.targetDb);
  const transformer = new IModelTransformer(
    { source: new LegacyTraversalExporter(hub.sourceDb), target: editTxn },
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
      await assertSynchronizationProvenance(hub.sourceDb, hub.targetDb);
      return assertSemanticallyEqual(hub.sourceDb, hub.targetDb);
    },
  };
}

export const incrementalSynchronizationScenario: BenchmarkScenarioDefinition = {
  id: "incremental-synchronization",
  defaultFixtureId: "balanced-incremental",
  capabilities: {
    topology: "source-and-empty-target",
    requiredClaims: ["incremental synchronization"],
  },
  factory: incrementalSynchronization,
};

export const incrementalSynchronizationBenchmark = defineBenchmark({
  scenario: incrementalSynchronizationScenario,
  fixtures: [
    balancedIncrementalFixture,
    deletionHeavyIncrementalFixture,
    hierarchicalIncrementalFixture,
  ],
});
