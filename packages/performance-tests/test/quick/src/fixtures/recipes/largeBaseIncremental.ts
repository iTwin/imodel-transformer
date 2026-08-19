/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import { Code, IModel, PhysicalElementProps } from "@itwin/core-common";
import {
  BriefcaseDb,
  IModelDb,
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

/**
 * Big-base/tiny-delta shape of the large-base incremental recipe.
 *
 * The base is one flat `PhysicalModel` holding `baseElements` label-free non-geometric
 * `PhysicalObject`s; the changesets touch `elementInserts + elementUpdates` of them. The
 * point of the recipe is the ratio: incremental synchronization cost should scale with
 * the changeset, so a large unchanged base makes any per-base-element work stand out.
 *
 * Base elements deliberately carry no `UserLabel`, no geometry, no aspects, and no
 * relationships. Semantic validation compares labeled elements, so labeling only the
 * changed elements keeps per-sample validation proportional to the delta, and the extra
 * content classes would dilute the traversal signal this recipe isolates. Only changed
 * elements are labeled: `inserted-large-<n>` and `updated-large-<n>`.
 */
export interface LargeBaseIncrementalParameters {
  readonly scale: number;
}

interface LargeBaseWorkload {
  readonly baseElements: number;
  readonly elementInserts: number;
  readonly elementUpdates: number;
}

const baseElementsPerScale = 2_000;
const elementInsertsPerScale = 1;
const elementUpdatesPerScale = 1;
/** Inserts land in one changeset and updates in a second one. */
const largeBaseSourceChangesets = 2;

function largeBaseWorkload(
  parameters: Readonly<LargeBaseIncrementalParameters>
): LargeBaseWorkload {
  const { scale } = parameters;
  if (!Number.isInteger(scale) || scale < 1)
    throw new Error("Large-base fixture scale must be a positive integer");
  return {
    baseElements: baseElementsPerScale * scale,
    elementInserts: elementInsertsPerScale * scale,
    elementUpdates: elementUpdatesPerScale * scale,
  };
}

function largeBaseDistribution(
  parameters: Readonly<LargeBaseIncrementalParameters>
): FixtureDistribution {
  const { baseElements, elementInserts, elementUpdates } =
    largeBaseWorkload(parameters);
  return {
    base: {
      aspects: 0,
      elements: baseElements,
      geometricElements: 0,
      relationships: 0,
    },
    operations: {
      elements: {
        inserts: elementInserts,
        updates: elementUpdates,
        deletes: 0,
      },
      aspects: { inserts: 0, updates: 0, deletes: 0 },
      relationships: { inserts: 0, updates: 0, deletes: 0 },
      geometryUpdates: 0,
      sourceChangesets: largeBaseSourceChangesets,
    },
  };
}

export interface LargeBaseRecipeState {
  readonly categoryId: Id64String;
  readonly elementIds: readonly Id64String[];
  readonly modelId: Id64String;
}

function elementProps(
  modelId: Id64String,
  categoryId: Id64String,
  index: number,
  userLabel?: string
): PhysicalElementProps {
  return {
    category: categoryId,
    classFullName: PhysicalObject.classFullName,
    code: new Code({
      scope: IModel.rootSubjectId,
      spec: IModel.rootSubjectId,
      value: `large-base-${index}`,
    }),
    model: modelId,
    userLabel,
  };
}

export async function createLargeBaseSeed(
  fileName: string,
  context: FixtureRecipeContext<LargeBaseIncrementalParameters>
): Promise<LargeBaseRecipeState> {
  const { descriptor, parameters, schemaFiles } = context;
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: descriptor.id },
  });
  try {
    // The base carries no aspects, but the scenario's semantic comparison prepares
    // queries against the QuickPerf aspect classes, so the schema must exist.
    await db.importSchemas([...schemaFiles]);
    const { categoryId, modelId } = withEditTxn(
      db,
      "create large-base model and category",
      (txn) => ({
        modelId: PhysicalModel.insert(txn, IModel.rootSubjectId, "LargeModel"),
        categoryId: SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "LargeCategory",
          {}
        ),
      })
    );
    const { baseElements } = largeBaseWorkload(parameters);
    const elementIds = withEditTxn(db, "insert large base elements", (txn) =>
      Array.from({ length: baseElements }, (_, index) =>
        txn.insertElement(elementProps(modelId, categoryId, index))
      )
    );
    return { categoryId, elementIds, modelId };
  } finally {
    db.close();
  }
}

export async function applyLargeBaseChangesets(
  db: BriefcaseDb,
  accessToken: string,
  context: FixtureRecipeContext<LargeBaseIncrementalParameters>,
  state: LargeBaseRecipeState
): Promise<void> {
  const { baseElements, elementInserts, elementUpdates } = largeBaseWorkload(
    context.parameters
  );

  withEditTxn(db, "insert large-base delta elements", (txn) => {
    for (let index = 0; index < elementInserts; index++)
      txn.insertElement(
        elementProps(
          state.modelId,
          state.categoryId,
          baseElements + index,
          `inserted-large-${index}`
        )
      );
  });
  await db.pushChanges({
    accessToken,
    description: "large-base delta 1: element inserts",
  });

  const stride = Math.floor(baseElements / elementUpdates);
  withEditTxn(db, "update large-base delta elements", (txn) => {
    for (let index = 0; index < elementUpdates; index++) {
      const ownerId = state.elementIds[index * stride];
      const props = db.elements.getElementProps<PhysicalElementProps>(ownerId);
      txn.updateElement({
        ...props,
        userLabel: `updated-large-${index}`,
      });
    }
  });
  await db.pushChanges({
    accessToken,
    description: "large-base delta 2: element updates",
  });
}

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(`Count query returned no rows: ${ecsql}`);
  return reader.current.cnt as number;
}

export async function assertLargeBaseDistribution(
  db: IModelDb,
  context: FixtureRecipeContext<LargeBaseIncrementalParameters>
): Promise<void> {
  const { distribution } = context.descriptor;
  const expected = {
    elements:
      distribution.base.elements + distribution.operations.elements.inserts,
    inserted: distribution.operations.elements.inserts,
    updated: distribution.operations.elements.updates,
  };
  const actual = {
    elements: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    ),
    inserted: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE UserLabel LIKE 'inserted-large-%'"
    ),
    updated: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE UserLabel LIKE 'updated-large-%'"
    ),
  };
  if (JSON.stringify(actual) !== JSON.stringify(expected))
    throw new Error(
      `Large-base fixture distribution mismatch: expected=${JSON.stringify(
        expected
      )}, actual=${JSON.stringify(actual)}`
    );
  if (!db.isBriefcaseDb())
    throw new Error("Large-base fixture validation requires a briefcase");
  if (db.changeset.index !== distribution.operations.sourceChangesets)
    throw new Error(
      `Expected ${distribution.operations.sourceChangesets} source changesets, got ${db.changeset.index}`
    );
}

export const largeBaseIncrementalRecipe = defineFixtureRecipe({
  id: "large-base-incremental",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "largeBaseIncremental.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerf.ecschema.xml")],
    values: { schema: "QuickPerf.01.00.00" },
  },
  distribution: largeBaseDistribution,
  createSeed: createLargeBaseSeed,
  applySourceChangesets: applyLargeBaseChangesets,
  validate: assertLargeBaseDistribution,
});

export const largeBaseIncrementalFixture = configureFixture(
  largeBaseIncrementalRecipe,
  {
    id: "large-base-incremental",
    version: 1,
    label: "large base, tiny delta incremental",
    scenarioClaims: [
      "incremental synchronization",
      "large-base incremental synchronization",
    ],
    topology: "source-and-empty-target",
    seed: 98,
    parameters: { scale: 25 },
  }
);

export const largeBaseIncrementalDescriptor =
  largeBaseIncrementalFixture.descriptor;
