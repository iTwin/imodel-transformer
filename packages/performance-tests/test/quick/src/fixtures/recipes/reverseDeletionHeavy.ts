/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import { BriefcaseDb, IModelDb, withEditTxn } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import {
  configureFixture,
  defineFixtureRecipe,
  FixtureRecipeContext,
} from "../FixtureRecipe.js";
import { createStartedEditTxn } from "../LocalHubFixture.js";
import { quickPath } from "../../support/paths.js";
import { queryCount } from "../validation/validateFixture.js";
import {
  createDeletionHeavySeed,
  deletionHeavyFederationGuid,
  DeletionHeavyIncrementalParameters,
} from "./deletionHeavyIncremental.js";

const defaultSurvivorElements = 25;
/** One changeset initializes the reverse synchronization version; one deletes elements. */
const reverseTargetChangesets = 2;

function reverseDeletionHeavyDistribution(
  parameters: Readonly<DeletionHeavyIncrementalParameters>
): FixtureDistribution {
  if (!Number.isSafeInteger(parameters.scale) || parameters.scale < 1)
    throw new Error(
      "Reverse deletion-heavy fixture scale must be a positive safe integer"
    );
  const baseElements = parameters.scale + defaultSurvivorElements;
  return {
    base: {
      aspects: 2 * baseElements,
      elements: baseElements,
      geometricElements: 0,
      relationships: 0,
    },
    operations: {
      elements: { inserts: 0, updates: 0, deletes: parameters.scale },
      aspects: { inserts: 0, updates: 0, deletes: 2 * parameters.scale },
      relationships: { inserts: 0, updates: 0, deletes: 0 },
      geometryUpdates: 0,
      sourceChangesets: 0,
      targetChangesets: reverseTargetChangesets,
    },
  };
}

async function assertElementAndAspectCounts(
  db: IModelDb,
  expected: number,
  role: string
): Promise<void> {
  const actual = {
    elements: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    ),
    multiAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM QuickPerf.BalancedMultiAspect"
    ),
    uniqueAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM QuickPerf.BalancedUniqueAspect"
    ),
  };
  const expectedCounts = {
    elements: expected,
    multiAspects: expected,
    uniqueAspects: expected,
  };
  if (JSON.stringify(actual) !== JSON.stringify(expectedCounts))
    throw new Error(
      `Reverse deletion-heavy ${role} mismatch: expected=${JSON.stringify(
        expectedCounts
      )}, actual=${JSON.stringify(actual)}`
    );
}

/**
 * Mirrors a branch's first synchronization: record the branch's current changeset as the
 * reverse synchronization version so later reverse synchronizations skip the provenance changeset.
 */
async function initializeReverseSynchronizationVersion(
  targetDb: BriefcaseDb,
  sourceDb: BriefcaseDb,
  accessToken: string
): Promise<void> {
  const editTxn = createStartedEditTxn(targetDb);
  const transformer = new IModelTransformer({
    source: sourceDb,
    target: editTxn,
  });
  try {
    await transformer.process();
    await transformer.updateSynchronizationVersion({
      initializeReverseSyncVersion: true,
    });
  } finally {
    transformer.dispose();
    if (editTxn.isActive) editTxn.end();
  }
  await targetDb.pushChanges({
    accessToken,
    description: "initialize reverse synchronization version",
  });
}

async function applyReverseDeletionHeavyChangesets(
  targetDb: BriefcaseDb,
  sourceDb: BriefcaseDb,
  accessToken: string,
  context: FixtureRecipeContext<DeletionHeavyIncrementalParameters>
): Promise<void> {
  const provenanceChangesetIndex = targetDb.changeset.index;
  if (provenanceChangesetIndex === undefined)
    throw new Error("Reverse deletion-heavy target has no changeset index");
  await initializeReverseSynchronizationVersion(
    targetDb,
    sourceDb,
    accessToken
  );

  const elementIdsToDelete: Id64String[] = [];
  for (let index = 0; index < context.parameters.scale; ++index) {
    const elementId = targetDb.elements.getIdFromFederationGuid(
      deletionHeavyFederationGuid(index)
    );
    if (elementId === undefined)
      throw new Error(
        `Reverse deletion-heavy target is missing element ${index}`
      );
    elementIdsToDelete.push(elementId);
  }
  withEditTxn(targetDb, "delete branch fixture elements", (txn) => {
    for (const elementId of elementIdsToDelete) txn.deleteElement(elementId);
  });
  await targetDb.pushChanges({
    accessToken,
    description: `delete ${elementIdsToDelete.length} branch fixture elements`,
  });

  const { base, operations } = context.descriptor.distribution;
  await assertElementAndAspectCounts(
    targetDb,
    base.elements - operations.elements.deletes,
    "target"
  );
  const expectedIndex =
    provenanceChangesetIndex + (operations.targetChangesets ?? 0);
  if (targetDb.changeset.index !== expectedIndex)
    throw new Error(
      `Expected target changeset ${expectedIndex}, got ${targetDb.changeset.index}`
    );
}

/** Recipe for a reverse synchronization workload whose branch deletes many elements in one changeset. */
export const reverseDeletionHeavyRecipe = defineFixtureRecipe({
  id: "deletion-heavy-reverse",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "reverseDeletionHeavy.ts"),
      quickPath("src", "fixtures", "recipes", "deletionHeavyIncremental.ts"),
      quickPath("src", "fixtures", "validation", "validateFixture.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerf.ecschema.xml")],
    values: { schema: "QuickPerf.01.00.00" },
  },
  distribution: reverseDeletionHeavyDistribution,
  createSeed: createDeletionHeavySeed,
  async applySourceChangesets() {},
  applyTargetChangesets: async (targetDb, sourceDb, accessToken, context) =>
    applyReverseDeletionHeavyChangesets(
      targetDb,
      sourceDb,
      accessToken,
      context
    ),
  validate: async (db, context) => {
    await assertElementAndAspectCounts(
      db,
      context.descriptor.distribution.base.elements,
      "source"
    );
    if (!db.isBriefcaseDb() || db.changeset.index !== 0)
      throw new Error("Reverse deletion-heavy source must have no changesets");
  },
});

/** Reverse synchronization fixture whose branch deletes 10,000 elements in one changeset. */
export const reverseDeletionHeavyFixture = configureFixture(
  reverseDeletionHeavyRecipe,
  {
    id: "deletion-heavy-reverse",
    version: 1,
    label: "deletion-heavy reverse",
    scenarioClaims: ["reverse synchronization", "element deletion"],
    topology: "source-and-empty-target",
    seed: 663,
    parameters: { scale: 10_000 },
  }
);
