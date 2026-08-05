/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ColorDef,
  GeometryStreamBuilder,
  IModel,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import {
  Box,
  Point3d,
  Range3d,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
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

export interface StandaloneFullTransformParameters {
  readonly elementCount: number;
}

function distribution(
  parameters: Readonly<StandaloneFullTransformParameters>
): FixtureDistribution {
  if (
    !Number.isSafeInteger(parameters.elementCount) ||
    parameters.elementCount < 1
  )
    throw new Error(
      "Standalone full-transform elementCount must be a positive safe integer"
    );
  return {
    base: {
      aspects: 0,
      elements: parameters.elementCount,
      geometricElements: parameters.elementCount,
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

// Keep this local: the transformer helper is not exported, while the legacy
// performance helper pulls test-only dependencies into the compiled CLI.
function createBoxGeometry() {
  const builder = new GeometryStreamBuilder();
  const box = Box.createRange(
    Range3d.create(Point3d.createZero(), Point3d.create(1, 1, 1)),
    true
  );
  if (!box) throw new Error("Failed to create standalone fixture box geometry");
  builder.appendGeometry(box);
  return builder.geometryStream;
}

async function countPhysicalObjects(db: IModelDb): Promise<number> {
  const reader = db.createQueryReader(
    "SELECT count(*) cnt FROM Generic.PhysicalObject",
    undefined,
    { usePrimaryConn: true }
  );
  if (!(await reader.step()))
    throw new Error("Standalone fixture element count query returned no row");
  return reader.current.cnt as number;
}

/**
 * Creates a standalone source with one physical model, one spatial category, and
 * `elementCount` Generic.PhysicalObjects. Each object has unit-box geometry and a
 * deterministic placement on a 100-column grid. The default configured fixture
 * contains 10,000 objects and no aspects, relationships, or changesets.
 */
export const standaloneFullTransformRecipe = defineFixtureRecipe({
  id: "standalone-full-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "standaloneFullTransform.ts"),
    ],
    values: { geometry: "unit-box-grid-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      const geometry = createBoxGeometry();
      withEditTxn(db, "create standalone full-transform workload", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "QuickFullTransformCategory",
          { color: ColorDef.green.toJSON() }
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "QuickFullTransformModel"
        );
        for (let index = 0; index < context.parameters.elementCount; index++) {
          const props: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: `Element-${index}`,
            geom: geometry,
            placement: Placement3d.fromJSON({
              origin: {
                x: index % 100,
                y: Math.floor(index / 100),
                z: 0,
              },
              angles: YawPitchRollAngles.createDegrees(0, 0, 0).toJSON(),
            }),
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
    const actual = await countPhysicalObjects(db);
    if (actual !== context.parameters.elementCount)
      throw new Error(
        `Standalone fixture expected ${context.parameters.elementCount} physical objects, found ${actual}`
      );
  },
});

export const standaloneFullTransformFixture = configureFixture(
  standaloneFullTransformRecipe,
  {
    id: "standalone-full-transform",
    version: 1,
    label: "standalone full transformation",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 104729,
    parameters: { elementCount: 10000 },
  }
);

export const standaloneFullTransformDescriptor =
  standaloneFullTransformFixture.descriptor;
