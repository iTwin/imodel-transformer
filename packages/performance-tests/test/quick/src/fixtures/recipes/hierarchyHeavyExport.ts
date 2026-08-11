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

export interface HierarchyHeavyExportParameters {
  readonly modelCount: number;
  readonly assembliesPerModel: number;
  readonly childrenPerAssembly: number;
}

function elementCount(
  parameters: Readonly<HierarchyHeavyExportParameters>
): number {
  return (
    parameters.modelCount *
    parameters.assembliesPerModel *
    (1 + parameters.childrenPerAssembly)
  );
}

function distribution(
  parameters: Readonly<HierarchyHeavyExportParameters>
): FixtureDistribution {
  for (const [name, value] of Object.entries(parameters)) {
    if (!Number.isSafeInteger(value) || value < 1)
      throw new Error(
        `Hierarchy-heavy export ${name} must be a positive safe integer`
      );
  }
  const geometricElements = elementCount(parameters);
  return {
    base: {
      aspects: 0,
      // partitions are counted separately from the geometric assembly contents
      elements: geometricElements + parameters.modelCount,
      geometricElements,
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
  if (!box)
    throw new Error("Failed to create hierarchy-heavy fixture box geometry");
  builder.appendGeometry(box);
  return builder.geometryStream;
}

async function countRows(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error("Hierarchy-heavy fixture count query returned no row");
  return reader.current.cnt as number;
}

/**
 * Creates a standalone source whose element tree is deliberately hierarchy-rich: `modelCount`
 * physical models each containing `assembliesPerModel` parent objects with
 * `childrenPerAssembly` child objects. Traversing it the default way costs one contents query
 * per model plus one child query per element, which is what the export-only traversal
 * scenarios measure against the single-scan linear traversal.
 */
export const hierarchyHeavyExportRecipe = defineFixtureRecipe({
  id: "hierarchy-heavy-export",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "hierarchyHeavyExport.ts"),
    ],
    values: { geometry: "unit-box-grid-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const { modelCount, assembliesPerModel, childrenPerAssembly } =
      context.parameters;
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      const geometry = createBoxGeometry();
      const categoryId = withEditTxn(
        db,
        "create hierarchy-heavy export category",
        (txn) =>
          SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "HierarchyHeavyExportCategory",
            { color: ColorDef.green.toJSON() }
          )
      );
      for (let model = 0; model < modelCount; model++) {
        withEditTxn(db, `create hierarchy-heavy model ${model}`, (txn) => {
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            `HierarchyHeavyModel-${model}`
          );
          for (let assembly = 0; assembly < assembliesPerModel; assembly++) {
            const commonProps: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: modelId,
              category: categoryId,
              code: Code.createEmpty(),
              geom: geometry,
              placement: Placement3d.fromJSON({
                origin: {
                  x: assembly % 100,
                  y: Math.floor(assembly / 100),
                  z: model,
                },
                angles: YawPitchRollAngles.createDegrees(0, 0, 0).toJSON(),
              }),
            };
            const parentId = txn.insertElement({
              ...commonProps,
              userLabel: `Assembly-${model}-${assembly}`,
            });
            for (let child = 0; child < childrenPerAssembly; child++) {
              txn.insertElement({
                ...commonProps,
                userLabel: `Child-${model}-${assembly}-${child}`,
                parent: {
                  id: parentId,
                  relClassName: "BisCore:PhysicalElementAssemblesElements",
                },
              });
            }
          }
        });
      }
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const expectedObjects = elementCount(context.parameters);
    const actualObjects = await countRows(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    );
    if (actualObjects !== expectedObjects)
      throw new Error(
        `Hierarchy-heavy fixture expected ${expectedObjects} physical objects, found ${actualObjects}`
      );
    const expectedChildren =
      context.parameters.modelCount *
      context.parameters.assembliesPerModel *
      context.parameters.childrenPerAssembly;
    const actualChildren = await countRows(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject WHERE Parent.Id IS NOT NULL"
    );
    if (actualChildren !== expectedChildren)
      throw new Error(
        `Hierarchy-heavy fixture expected ${expectedChildren} child objects, found ${actualChildren}`
      );
  },
});

export const hierarchyHeavyExportFixture = configureFixture(
  hierarchyHeavyExportRecipe,
  {
    id: "hierarchy-heavy-export",
    version: 1,
    label: "hierarchy-heavy export-only traversal source",
    scenarioClaims: ["export-only traversal"],
    topology: "standalone-source-and-empty-target",
    seed: 104729,
    // 40 models x 125 assemblies x (1 parent + 1 child) = 10,000 physical objects
    parameters: {
      modelCount: 40,
      assembliesPerModel: 125,
      childrenPerAssembly: 1,
    },
  }
);

export const hierarchyHeavyExportDescriptor =
  hierarchyHeavyExportFixture.descriptor;
