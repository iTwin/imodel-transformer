/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModel, QueryBinder } from "@itwin/core-common";
import {
  IModelDb,
  SnapshotDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";

export interface MixedHierarchyFullTransformParameters {
  readonly rootCount: number;
  readonly sideChildrenPerSpine: number;
  readonly spineDepth: number;
}

function elementCount(
  parameters: Readonly<MixedHierarchyFullTransformParameters>
): number {
  for (const [name, value] of Object.entries(parameters)) {
    if (!Number.isSafeInteger(value) || value < 1)
      throw new Error(`${name} must be a positive safe integer`);
  }
  return (
    parameters.rootCount *
    parameters.spineDepth *
    (parameters.sideChildrenPerSpine + 1)
  );
}

function distribution(
  parameters: Readonly<MixedHierarchyFullTransformParameters>
): FixtureDistribution {
  return {
    base: {
      aspects: 0,
      elements: elementCount(parameters),
      geometricElements: 0,
      relationships: 0,
    },
    operations: {
      aspects: { deletes: 0, inserts: 0, updates: 0 },
      elements: { deletes: 0, inserts: 0, updates: 0 },
      relationships: { deletes: 0, inserts: 0, updates: 0 },
      geometryUpdates: 0,
      sourceChangesets: 0,
    },
  };
}

async function countFixtureSubjects(db: IModelDb): Promise<number> {
  const reader = db.createQueryReader(
    "SELECT count(*) cnt FROM bis.Subject s WHERE s.ECInstanceId <> :rootSubjectId",
    new QueryBinder().bindId("rootSubjectId", IModel.rootSubjectId),
    { usePrimaryConn: true }
  );
  if (!(await reader.step()))
    throw new Error("Mixed hierarchy subject count query returned no row");
  return reader.current.cnt as number;
}

/** Builds several deep spines with leaf fan-out at every level. */
export const mixedHierarchyFullTransformRecipe = defineFixtureRecipe({
  id: "mixed-hierarchy-full-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "mixedHierarchyFullTransform.ts"),
    ],
    values: { hierarchy: "spines-with-side-children-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      withEditTxn(db, "create mixed hierarchy workload", (txn) => {
        for (
          let rootIndex = 0;
          rootIndex < context.parameters.rootCount;
          rootIndex++
        ) {
          let parentId = IModel.rootSubjectId;
          for (let depth = 0; depth < context.parameters.spineDepth; depth++) {
            const spineId = Subject.insert(
              txn,
              parentId,
              `Spine-${rootIndex}-${depth}`
            );
            for (
              let sideIndex = 0;
              sideIndex < context.parameters.sideChildrenPerSpine;
              sideIndex++
            ) {
              Subject.insert(
                txn,
                spineId,
                `Side-${rootIndex}-${depth}-${sideIndex}`
              );
            }
            parentId = spineId;
          }
        }
      });
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const actual = await countFixtureSubjects(db);
    const expected = elementCount(context.parameters);
    if (actual !== expected)
      throw new Error(
        `Mixed hierarchy fixture expected ${expected} subjects, found ${actual}`
      );
  },
});

export const mixedHierarchyFullTransformFixture = configureFixture(
  mixedHierarchyFullTransformRecipe,
  {
    id: "mixed-hierarchy-full-transform",
    version: 1,
    label: "mixed deep-and-wide hierarchy full transformation",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 130363,
    parameters: {
      rootCount: 20,
      sideChildrenPerSpine: 19,
      spineDepth: 25,
    },
  }
);

export const largeMixedHierarchyFullTransformFixture = configureFixture(
  mixedHierarchyFullTransformRecipe,
  {
    id: "mixed-hierarchy-full-transform-large",
    version: 1,
    label: "large mixed deep-and-wide hierarchy full transformation",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 982451653,
    parameters: {
      rootCount: 20,
      sideChildrenPerSpine: 99,
      spineDepth: 50,
    },
  }
);
