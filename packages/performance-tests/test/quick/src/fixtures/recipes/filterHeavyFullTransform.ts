/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ColorDef,
  GeometricElement3dProps,
  GeometryStreamBuilder,
  IModel,
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
  SpatialLocation,
  withEditTxn,
} from "@itwin/core-backend";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";

export interface FilterHeavyFullTransformParameters {
  readonly elementCount: number;
}

/** Category whose elements the filter-heavy scenario excludes. */
export const filterHeavyExcludedCategoryName = "FilterHeavyExcludedCategory";
/** Category whose elements the filter-heavy scenario keeps. */
export const filterHeavyIncludedCategoryName = "FilterHeavyIncludedCategory";
/** Element class the filter-heavy scenario excludes polymorphically.
 * Hardcoded because `SpatialLocation.classFullName` requires schema registration,
 * which is unavailable at module load.
 */
export const filterHeavyExcludedClassFullName = "Generic:SpatialLocation";

function distribution(
  parameters: Readonly<FilterHeavyFullTransformParameters>
): FixtureDistribution {
  if (
    !Number.isSafeInteger(parameters.elementCount) ||
    parameters.elementCount < 4
  )
    throw new Error(
      "Filter-heavy full-transform elementCount must be a safe integer of at least 4"
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
  if (!box)
    throw new Error("Failed to create filter-heavy fixture box geometry");
  builder.appendGeometry(box);
  return builder.geometryStream;
}

/**
 * Deterministic per-index workload split by `index % 4`:
 * - 0, 1: `Generic.PhysicalObject` in the included category (exported),
 * - 2: `Generic.PhysicalObject` in the excluded category (rejected by category),
 * - 3: `Generic.SpatialLocation` in the included category (rejected by class).
 */
function quadrant(
  index: number
): "pass" | "excluded-category" | "excluded-class" {
  const bucket = index % 4;
  if (bucket === 2) return "excluded-category";
  if (bucket === 3) return "excluded-class";
  return "pass";
}

async function countElements(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error("Filter-heavy fixture count query returned no row");
  return reader.current.cnt as number;
}

/**
 * Creates a standalone source designed for filter-heavy transforms: one physical model,
 * two spatial categories, and `elementCount` geometric elements with unit-box geometry on a
 * deterministic 100-column grid. Half the elements are `Generic.PhysicalObject`s in the
 * included category; a quarter are `Generic.PhysicalObject`s in the excluded category; a
 * quarter are `Generic.SpatialLocation`s in the included category. A scenario that excludes
 * `Generic.SpatialLocation` and the excluded category therefore rejects ~50% of elements.
 */
export const filterHeavyFullTransformRecipe = defineFixtureRecipe({
  id: "filter-heavy-full-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "filterHeavyFullTransform.ts"),
    ],
    values: { geometry: "unit-box-grid-v1", split: "pass2-cat1-class1-v1" },
  },
  distribution,
  async createSeed(fileName, context) {
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      const geometry = createBoxGeometry();
      withEditTxn(db, "create filter-heavy full-transform workload", (txn) => {
        const includedCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          filterHeavyIncludedCategoryName,
          { color: ColorDef.green.toJSON() }
        );
        const excludedCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          filterHeavyExcludedCategoryName,
          { color: ColorDef.red.toJSON() }
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "FilterHeavyFullTransformModel"
        );
        for (let index = 0; index < context.parameters.elementCount; index++) {
          const bucket = quadrant(index);
          const props: GeometricElement3dProps = {
            classFullName:
              bucket === "excluded-class"
                ? SpatialLocation.classFullName
                : PhysicalObject.classFullName,
            model: modelId,
            category:
              bucket === "excluded-category"
                ? excludedCategoryId
                : includedCategoryId,
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
    const total = context.parameters.elementCount;
    const expectedClassExcluded = Math.floor(total / 4);
    const expectedCategoryExcluded = Math.floor((total + 1) / 4);
    const expectedPass =
      total - expectedClassExcluded - expectedCategoryExcluded;
    const spatialLocations = await countElements(
      db,
      "SELECT count(*) cnt FROM Generic.SpatialLocation"
    );
    if (spatialLocations !== expectedClassExcluded)
      throw new Error(
        `Filter-heavy fixture expected ${expectedClassExcluded} spatial locations, found ${spatialLocations}`
      );
    const excludedCategoryElements = await countElements(
      db,
      `SELECT count(*) cnt FROM bis.GeometricElement3d g
       JOIN bis.SpatialCategory c ON c.ECInstanceId = g.Category.Id
       WHERE c.CodeValue = '${filterHeavyExcludedCategoryName}'`
    );
    if (excludedCategoryElements !== expectedCategoryExcluded)
      throw new Error(
        `Filter-heavy fixture expected ${expectedCategoryExcluded} excluded-category elements, found ${excludedCategoryElements}`
      );
    const physicalObjects = await countElements(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    );
    if (physicalObjects !== expectedPass + expectedCategoryExcluded)
      throw new Error(
        `Filter-heavy fixture expected ${
          expectedPass + expectedCategoryExcluded
        } physical objects, found ${physicalObjects}`
      );
  },
});

export const filterHeavyFullTransformFixture = configureFixture(
  filterHeavyFullTransformRecipe,
  {
    id: "filter-heavy-full-transform",
    version: 1,
    label: "filter-heavy full transformation",
    scenarioClaims: ["filter-heavy transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 104729,
    parameters: { elementCount: 10000 },
  }
);

export const filterHeavyFullTransformDescriptor =
  filterHeavyFullTransformFixture.descriptor;
