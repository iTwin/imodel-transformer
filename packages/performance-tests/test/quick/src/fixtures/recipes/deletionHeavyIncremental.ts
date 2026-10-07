/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  ElementAspectProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  BriefcaseDb,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import {
  configureFixture,
  defineFixtureRecipe,
  FixtureRecipeContext,
} from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";
import { assertFixtureDistribution } from "../validation/validateFixture.js";

const uniqueAspectClass = "QuickPerf:BalancedUniqueAspect";
const multiAspectClass = "QuickPerf:BalancedMultiAspect";
const defaultSurvivorElements = 25;

export interface DeletionHeavyIncrementalParameters {
  readonly scale: number;
}

function deletionHeavyDistribution(
  parameters: Readonly<DeletionHeavyIncrementalParameters>
): FixtureDistribution {
  if (!Number.isSafeInteger(parameters.scale) || parameters.scale < 1)
    throw new Error(
      "Deletion-heavy fixture scale must be a positive safe integer"
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
      elements: {
        inserts: 0,
        updates: 0,
        deletes: parameters.scale,
      },
      aspects: {
        inserts: 0,
        updates: 0,
        deletes: 2 * parameters.scale,
      },
      relationships: { inserts: 0, updates: 0, deletes: 0 },
      geometryUpdates: 0,
      sourceChangesets: 1,
    },
  };
}

interface DeletionHeavyRecipeState {
  readonly elementIdsToDelete: readonly Id64String[];
}

async function createDeletionHeavySeed(
  fileName: string,
  context: FixtureRecipeContext<DeletionHeavyIncrementalParameters>
): Promise<DeletionHeavyRecipeState> {
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: context.descriptor.id },
  });
  try {
    await db.importSchemas([...context.schemaFiles]);
    const elementIdsToDelete = withEditTxn(
      db,
      "create deletion-heavy seed",
      (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "DeletionHeavyModel"
        );
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "DeletionHeavyCategory",
          {}
        );
        const ids: Id64String[] = [];
        const elementCount = context.descriptor.distribution.base.elements;
        for (let index = 0; index < elementCount; ++index) {
          const elementId = txn.insertElement({
            category: categoryId,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
            federationGuid: `00000000-0000-4000-8000-${index
              .toString()
              .padStart(12, "0")}`,
            model: modelId,
            userLabel: `deletion-heavy-${index}`,
          } as PhysicalElementProps);
          txn.insertAspect({
            classFullName: uniqueAspectClass,
            element: new ElementOwnsUniqueAspect(elementId),
            payload: `unique-${index}`,
            sequence: index,
          } as ElementAspectProps);
          txn.insertAspect({
            classFullName: multiAspectClass,
            element: new ElementOwnsMultiAspects(elementId),
            payload: `multi-${index}`,
            sequence: index,
          } as ElementAspectProps);
          if (index < context.parameters.scale) ids.push(elementId);
        }
        return ids;
      }
    );
    return { elementIdsToDelete };
  } finally {
    db.close();
  }
}

async function applyDeletionHeavyChangeset(
  db: BriefcaseDb,
  accessToken: string,
  _context: FixtureRecipeContext<DeletionHeavyIncrementalParameters>,
  state: DeletionHeavyRecipeState
): Promise<void> {
  withEditTxn(db, "delete fixture elements", (txn) => {
    for (const elementId of state.elementIdsToDelete)
      txn.deleteElement(elementId);
  });
  await db.pushChanges({
    accessToken,
    description: `delete ${state.elementIdsToDelete.length} fixture elements`,
  });
}

/** Recipe for an incremental synchronization workload that deletes many elements in one changeset. */
export const deletionHeavyIncrementalRecipe = defineFixtureRecipe({
  id: "deletion-heavy-incremental",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "deletionHeavyIncremental.ts"),
      quickPath("src", "fixtures", "validation", "validateFixture.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerf.ecschema.xml")],
    values: { schema: "QuickPerf.01.00.00" },
  },
  distribution: deletionHeavyDistribution,
  createSeed: createDeletionHeavySeed,
  applySourceChangesets: applyDeletionHeavyChangeset,
  validate: async (db, context) =>
    assertFixtureDistribution(db, context.descriptor),
});

/** Incremental synchronization fixture that deletes 10,000 elements in one changeset. */
export const deletionHeavyIncrementalFixture = configureFixture(
  deletionHeavyIncrementalRecipe,
  {
    id: "deletion-heavy-incremental",
    version: 1,
    label: "deletion-heavy incremental",
    scenarioClaims: ["incremental synchronization", "element deletion"],
    topology: "source-and-empty-target",
    seed: 662,
    parameters: { scale: 10_000 },
  }
);
