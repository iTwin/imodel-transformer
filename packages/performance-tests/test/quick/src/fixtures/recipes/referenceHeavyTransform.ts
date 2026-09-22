/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Code, IModel, PhysicalElementProps } from "@itwin/core-common";
import {
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";

export interface ReferenceHeavyTransformParameters {
  readonly parentElementCount: number;
  readonly childElementCount: number;
}

function distribution(
  parameters: Readonly<ReferenceHeavyTransformParameters>
): FixtureDistribution {
  if (
    !Number.isSafeInteger(parameters.parentElementCount) ||
    parameters.parentElementCount < 1
  )
    throw new Error(
      "Reference-heavy parentElementCount must be a positive safe integer"
    );
  if (
    !Number.isSafeInteger(parameters.childElementCount) ||
    parameters.childElementCount < parameters.parentElementCount
  )
    throw new Error(
      "Reference-heavy childElementCount must be a safe integer at least as large as parentElementCount"
    );
  return {
    base: {
      aspects: 0,
      elements: parameters.parentElementCount + parameters.childElementCount,
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

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Reference-heavy fixture count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

/**
 * Creates a standalone source with one physical model, one spatial category,
 * `parentElementCount` root PhysicalObjects, and `childElementCount` child
 * PhysicalObjects. Children reference parents round-robin through
 * ElementOwnsChildElements, producing many navigation-reference occurrences
 * over a bounded set of unique references.
 */
export const referenceHeavyTransformRecipe = defineFixtureRecipe({
  id: "reference-heavy-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "referenceHeavyTransform.ts"),
    ],
    values: { content: "physical-object-parent-round-robin-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const { parentElementCount, childElementCount } = context.parameters;
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      withEditTxn(db, "create reference-heavy workload", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "QuickReferenceHeavyCategory",
          {}
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "QuickReferenceHeavyModel"
        );
        const parentIds = Array.from(
          { length: parentElementCount },
          (_, index) => {
            const props: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: Code.createEmpty(),
              userLabel: `ReferenceParent-${index}`,
            };
            return txn.insertElement(props);
          }
        );
        for (let index = 0; index < childElementCount; index++) {
          const props: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: `ReferenceChild-${index}`,
            parent: {
              id: parentIds[index % parentIds.length],
              relClassName: "BisCore:ElementOwnsChildElements",
            },
          };
          txn.insertElement(props);
        }
      });
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const { parentElementCount, childElementCount } = context.parameters;
    const expected = {
      elements: parentElementCount + childElementCount,
      parentElements: parentElementCount,
      childElements: childElementCount,
      referencedParents: parentElementCount,
    };
    const actual = {
      elements: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject"
      ),
      parentElements: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE Parent.Id IS NULL"
      ),
      childElements: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE Parent.Id IS NOT NULL"
      ),
      referencedParents: await queryCount(
        db,
        "SELECT count(DISTINCT Parent.Id) cnt FROM Generic.PhysicalObject WHERE Parent.Id IS NOT NULL"
      ),
    };
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new Error(
        `Reference-heavy fixture distribution mismatch: expected=${JSON.stringify(
          expected
        )}, actual=${JSON.stringify(actual)}`
      );
  },
});

export const referenceHeavyTransformFixture = configureFixture(
  referenceHeavyTransformRecipe,
  {
    id: "reference-heavy-transform",
    version: 1,
    label: "reference-heavy standalone transformation",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 32452843,
    parameters: { parentElementCount: 8000, childElementCount: 37000 },
  }
);

export const referenceHeavyTransformDescriptor =
  referenceHeavyTransformFixture.descriptor;
