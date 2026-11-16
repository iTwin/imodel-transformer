/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ElementAspectProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { FixtureDistribution, FixtureTopology } from "../FixtureDescriptor.js";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { quickPath } from "../../support/paths.js";

const uniqueAspectClass = "QuickPerf:BalancedUniqueAspect";
const multiAspectClass = "QuickPerf:BalancedMultiAspect";

export interface AspectHeavyNoopParameters {
  readonly elementCount: number;
  /** Includes one unique aspect and `aspectsPerElement - 1` multi-aspects. */
  readonly aspectsPerElement: number;
}

function distribution(
  parameters: Readonly<AspectHeavyNoopParameters>
): FixtureDistribution {
  if (
    !Number.isSafeInteger(parameters.elementCount) ||
    parameters.elementCount < 1
  )
    throw new Error(
      "Aspect-heavy elementCount must be a positive safe integer"
    );
  if (
    !Number.isSafeInteger(parameters.aspectsPerElement) ||
    parameters.aspectsPerElement < 2
  )
    throw new Error(
      "Aspect-heavy aspectsPerElement must be a safe integer of at least 2"
    );
  const aspectCount = parameters.elementCount * parameters.aspectsPerElement;
  if (!Number.isSafeInteger(aspectCount))
    throw new Error("Aspect-heavy total aspect count must be a safe integer");
  return {
    base: {
      aspects: aspectCount,
      elements: parameters.elementCount,
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
      `Aspect-heavy fixture count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

export const aspectHeavyNoopRecipe = defineFixtureRecipe({
  id: "aspect-heavy-noop",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "aspectHeavyNoop.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerf.ecschema.xml")],
    values: { aspectLayout: "one-unique-plus-multi-aspects-per-owner-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      await db.importSchemas([...context.schemaFiles]);
      withEditTxn(db, "create aspect-heavy no-op workload", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "QuickAspectHeavyCategory",
          {}
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "QuickAspectHeavyModel"
        );
        for (
          let elementIndex = 0;
          elementIndex < context.parameters.elementCount;
          elementIndex++
        ) {
          const elementId = txn.insertElement({
            category: categoryId,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
            federationGuid: `00000000-0000-4000-8000-${(elementIndex + 1)
              .toString(16)
              .padStart(12, "0")}`,
            model: modelId,
            userLabel: `AspectHeavy-${elementIndex}`,
          } as PhysicalElementProps);
          txn.insertAspect({
            classFullName: uniqueAspectClass,
            element: new ElementOwnsUniqueAspect(elementId),
            payload: `unique-${elementIndex}`,
            sequence: elementIndex,
          } as ElementAspectProps);
          for (
            let aspectIndex = 1;
            aspectIndex < context.parameters.aspectsPerElement;
            aspectIndex++
          ) {
            txn.insertAspect({
              classFullName: multiAspectClass,
              element: new ElementOwnsMultiAspects(elementId),
              payload: `multi-${elementIndex}-${aspectIndex}`,
              sequence: aspectIndex,
            } as ElementAspectProps);
          }
        }
      });
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const expected = {
      elements: context.parameters.elementCount,
      uniqueAspects: context.parameters.elementCount,
      multiAspects:
        context.parameters.elementCount *
        (context.parameters.aspectsPerElement - 1),
    };
    const actual = {
      elements: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject"
      ),
      uniqueAspects: await queryCount(
        db,
        `SELECT count(*) cnt FROM ${uniqueAspectClass}`
      ),
      multiAspects: await queryCount(
        db,
        `SELECT count(*) cnt FROM ${multiAspectClass}`
      ),
    };
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new Error(
        `Aspect-heavy fixture distribution mismatch: expected=${JSON.stringify(
          expected
        )}, actual=${JSON.stringify(actual)}`
      );
  },
});

function configureAspectHeavyNoopFixture(
  id: string,
  elementCount: number,
  aspectsPerElement: number,
  topology: FixtureTopology = "standalone-source-and-empty-target"
) {
  return configureFixture(aspectHeavyNoopRecipe, {
    id,
    version: 1,
    label: `${elementCount} elements, ${aspectsPerElement} aspects each`,
    scenarioClaims: ["aspect-heavy repeated transformation"],
    topology,
    seed: 49979687,
    parameters: { elementCount, aspectsPerElement },
  });
}

export const aspectHeavyNoopFixture = configureAspectHeavyNoopFixture(
  "aspect-heavy-noop-1k-3",
  1_000,
  3
);

export const aspectHeavyNoopLargeFixture = configureAspectHeavyNoopFixture(
  "aspect-heavy-noop-10k-3",
  10_000,
  3
);

export const aspectHeavyProcessChangesFixture = configureAspectHeavyNoopFixture(
  "aspect-heavy-process-changes-1k-3",
  1_000,
  3,
  "source-and-empty-target"
);
