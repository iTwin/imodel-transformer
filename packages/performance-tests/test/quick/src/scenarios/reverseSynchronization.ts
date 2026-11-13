/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { BriefcaseDb, ExternalSourceAspect } from "@itwin/core-backend";
import { IModel, QueryBinder } from "@itwin/core-common";
import { IModelTransformer } from "@itwin/imodel-transformer";
import {
  BenchmarkScenario,
  BenchmarkScenarioDefinition,
} from "../framework/BenchmarkScenario.js";
import { defineBenchmark } from "../framework/BenchmarkRegistration.js";
import {
  PreparedDataset,
  requireLiveHubDataset,
} from "../fixtures/FixtureProvider.js";
import { createStartedEditTxn } from "../fixtures/LocalHubFixture.js";
import { reverseDeletionHeavyFixture } from "../fixtures/recipes/reverseDeletionHeavy.js";
import { assertSemanticallyEqual } from "../fixtures/validation/validateFixture.js";

/** Asserts that the branch scope provenance records the branch changeset just synchronized back. */
export async function assertReverseSynchronizationProvenance(
  masterDb: BriefcaseDb,
  branchDb: BriefcaseDb
): Promise<void> {
  const binder = new QueryBinder()
    .bindId("elementId", IModel.rootSubjectId)
    .bindId("scopeId", IModel.rootSubjectId)
    .bindString("kind", ExternalSourceAspect.Kind.Scope)
    .bindString("identifier", masterDb.iModelId);
  const reader = branchDb.createQueryReader(
    `SELECT JsonProperties jsonProperties
     FROM bis.ExternalSourceAspect
     WHERE Element.Id=:elementId
       AND Scope.Id=:scopeId
       AND Kind=:kind
       AND Identifier=:identifier`,
    binder,
    { usePrimaryConn: true }
  );
  if (!(await reader.step()))
    throw new Error("Branch synchronization scope provenance was not found");
  const jsonProperties = JSON.parse(
    (reader.current.jsonProperties as string | undefined) ?? "{}"
  ) as { reverseSyncVersion?: string };
  if (await reader.step())
    throw new Error(
      "Branch contains duplicate synchronization scope provenance"
    );
  const expectedVersion = `${branchDb.changeset.id};${branchDb.changeset.index}`;
  if (jsonProperties.reverseSyncVersion !== expectedVersion)
    throw new Error(
      `Reverse synchronization version mismatch: expected=${expectedVersion}, actual=${String(
        jsonProperties.reverseSyncVersion
      )}`
    );
}

/** Synchronizes branch (fixture target) changes back into the master (fixture source). */
export function reverseSynchronization(
  dataset: PreparedDataset
): BenchmarkScenario {
  const { hub } = requireLiveHubDataset(dataset);
  const masterDb = hub.sourceDb;
  const branchDb = hub.targetDb;
  const masterEditTxn = createStartedEditTxn(masterDb);
  const branchEditTxn = createStartedEditTxn(branchDb);
  const transformer = new IModelTransformer(
    { source: branchDb, target: masterEditTxn },
    { argsForProcessChanges: {}, sourceEditTxn: branchEditTxn }
  );
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    transformer.dispose();
    if (masterEditTxn.isActive) masterEditTxn.end();
    if (branchEditTxn.isActive) branchEditTxn.end();
    disposed = true;
  };
  return {
    abort: dispose,
    async measure() {
      await transformer.process();
    },
    async finish() {
      dispose();
      await assertReverseSynchronizationProvenance(masterDb, branchDb);
      return assertSemanticallyEqual(branchDb, masterDb);
    },
  };
}

export const reverseSynchronizationScenario: BenchmarkScenarioDefinition = {
  id: "reverse-synchronization",
  defaultFixtureId: "deletion-heavy-reverse",
  capabilities: {
    topology: "source-and-empty-target",
    requiredClaims: ["reverse synchronization"],
  },
  factory: reverseSynchronization,
};

export const reverseSynchronizationBenchmark = defineBenchmark({
  scenario: reverseSynchronizationScenario,
  fixtures: [reverseDeletionHeavyFixture],
});
