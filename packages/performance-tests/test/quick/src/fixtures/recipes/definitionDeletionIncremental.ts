/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  GeometryPartProps,
  GeometryStreamBuilder,
  GeometryStreamProps,
  IModel,
  PhysicalElementProps,
} from "@itwin/core-common";
import {
  BriefcaseDb,
  BulkDeleteElementsStatus,
  GeometryPart,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Box, Point3d, Range3d } from "@itwin/core-geometry";
import {
  configureFixture,
  defineFixtureRecipe,
  FixtureRecipeContext,
} from "../FixtureRecipe.js";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { quickPath } from "../../support/paths.js";
import { queryCount } from "../validation/validateFixture.js";

const survivorElements = 25;

export interface DefinitionDeletionIncrementalParameters {
  /** Elements deleted together with the definitions they use. */
  readonly scale: number;
  /** Spatial categories deleted together with their elements. */
  readonly categoryCount: number;
  /** Geometry parts deleted together with the elements whose geometry places them; zero for none. */
  readonly geometryPartCount: number;
}

function validateParameters(
  parameters: Readonly<DefinitionDeletionIncrementalParameters>
): void {
  for (const [name, value, minimum] of [
    ["scale", parameters.scale, 1],
    ["categoryCount", parameters.categoryCount, 1],
    ["geometryPartCount", parameters.geometryPartCount, 0],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < minimum)
      throw new Error(
        `Definition-deletion fixture ${name} must be a safe integer >= ${minimum}`
      );
  }
}

function definitionDeletionDistribution(
  parameters: Readonly<DefinitionDeletionIncrementalParameters>
): FixtureDistribution {
  validateParameters(parameters);
  return {
    base: {
      aspects: 0,
      elements: parameters.scale + survivorElements,
      geometricElements:
        parameters.geometryPartCount > 0 ? parameters.scale : 0,
      relationships: 0,
    },
    operations: {
      elements: { inserts: 0, updates: 0, deletes: parameters.scale },
      aspects: { inserts: 0, updates: 0, deletes: 0 },
      relationships: { inserts: 0, updates: 0, deletes: 0 },
      geometryUpdates: 0,
      sourceChangesets: 1,
    },
  };
}

interface DefinitionDeletionRecipeState {
  readonly idsToDelete: readonly Id64String[];
}

function createPartGeometry(): GeometryStreamProps {
  const builder = new GeometryStreamBuilder();
  const box = Box.createRange(
    Range3d.create(Point3d.createZero(), Point3d.create(1, 1, 1)),
    true
  );
  if (!box) throw new Error("Failed to create geometry part box");
  builder.appendGeometry(box);
  return builder.geometryStream;
}

function createElementGeometry(partId: Id64String): GeometryStreamProps {
  const builder = new GeometryStreamBuilder();
  if (!builder.appendGeometryPart3d(partId))
    throw new Error(`Failed to place geometry part ${partId}`);
  return builder.geometryStream;
}

async function createDefinitionDeletionSeed(
  fileName: string,
  context: FixtureRecipeContext<DefinitionDeletionIncrementalParameters>
): Promise<DefinitionDeletionRecipeState> {
  const { scale, categoryCount, geometryPartCount } = context.parameters;
  const db = SnapshotDb.createEmpty(fileName, {
    rootSubject: { name: context.descriptor.id },
  });
  try {
    await db.importSchemas([...context.schemaFiles]);
    const idsToDelete = withEditTxn(
      db,
      "create definition-deletion seed",
      (txn) => {
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "DefinitionDeletionModel"
        );
        const survivorCategoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "SurvivorCategory",
          {}
        );
        const categoryIds: Id64String[] = [];
        for (let index = 0; index < categoryCount; ++index)
          categoryIds.push(
            SpatialCategory.insert(
              txn,
              IModel.dictionaryId,
              `DeletedCategory${index}`,
              {}
            )
          );
        const partIds: Id64String[] = [];
        const partGeometry = createPartGeometry();
        for (let index = 0; index < geometryPartCount; ++index)
          partIds.push(
            txn.insertElement({
              classFullName: GeometryPart.classFullName,
              model: IModel.dictionaryId,
              code: GeometryPart.createCode(
                db,
                IModel.dictionaryId,
                `DeletedPart${index}`
              ),
              geom: partGeometry,
            } as GeometryPartProps)
          );
        const elementGeometry = partIds.map(createElementGeometry);

        const ids: Id64String[] = [];
        const elementCount = context.descriptor.distribution.base.elements;
        for (let index = 0; index < elementCount; ++index) {
          const deleted = index < scale;
          const elementId = txn.insertElement({
            category: deleted
              ? categoryIds[index % categoryIds.length]
              : survivorCategoryId,
            classFullName: PhysicalObject.classFullName,
            code: Code.createEmpty(),
            federationGuid: `00000000-0000-4000-8000-${index
              .toString()
              .padStart(12, "0")}`,
            model: modelId,
            userLabel: `definition-deletion-${index}`,
            placement: {
              origin: { x: (index % 100) * 2, y: Math.floor(index / 100) * 2 },
              angles: {},
            },
            geom:
              deleted && elementGeometry.length > 0
                ? elementGeometry[index % elementGeometry.length]
                : undefined,
          } as PhysicalElementProps);
          if (deleted) ids.push(elementId);
        }
        return [...ids, ...categoryIds, ...partIds];
      }
    );
    return { idsToDelete };
  } finally {
    db.close();
  }
}

async function applyDefinitionDeletionChangeset(
  db: BriefcaseDb,
  accessToken: string,
  _context: FixtureRecipeContext<DefinitionDeletionIncrementalParameters>,
  state: DefinitionDeletionRecipeState
): Promise<void> {
  withEditTxn(db, "delete fixture elements and definitions", (txn) => {
    const result = txn.deleteElements([...state.idsToDelete]);
    if (result.status !== BulkDeleteElementsStatus.Success)
      throw new Error(
        `Failed to delete definition-deletion fixture elements: ${JSON.stringify(
          result
        )}`
      );
  });
  await db.pushChanges({
    accessToken,
    description: `delete ${state.idsToDelete.length} elements and definitions`,
  });
}

async function assertDefinitionDeletionDistribution(
  db: IModelDb,
  context: FixtureRecipeContext<DefinitionDeletionIncrementalParameters>
): Promise<void> {
  const { distribution } = context.descriptor;
  const expected = {
    elements:
      distribution.base.elements - distribution.operations.elements.deletes,
    categories: 1,
    geometryParts: 0,
  };
  const actual = {
    elements: await queryCount(
      db,
      "SELECT count(*) cnt FROM Generic.PhysicalObject"
    ),
    categories: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.SpatialCategory"
    ),
    geometryParts: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.GeometryPart"
    ),
  };
  if (JSON.stringify(actual) !== JSON.stringify(expected))
    throw new Error(
      `Definition-deletion fixture distribution mismatch: expected=${JSON.stringify(
        expected
      )}, actual=${JSON.stringify(actual)}`
    );
  if (!db.isBriefcaseDb())
    throw new Error(
      "Definition-deletion fixture validation requires a briefcase"
    );
  if (db.changeset.index !== distribution.operations.sourceChangesets)
    throw new Error(
      `Expected ${distribution.operations.sourceChangesets} source changesets, got ${db.changeset.index}`
    );
}

/** Recipe for an incremental synchronization workload that deletes definitions together with the elements that use them. */
export const definitionDeletionIncrementalRecipe = defineFixtureRecipe({
  id: "definition-deletion-incremental",
  identity: {
    implementationFiles: [
      quickPath(
        "src",
        "fixtures",
        "recipes",
        "definitionDeletionIncremental.ts"
      ),
      quickPath("src", "fixtures", "validation", "validateFixture.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "QuickPerf.ecschema.xml")],
    values: { schema: "QuickPerf.01.00.00" },
  },
  distribution: definitionDeletionDistribution,
  createSeed: createDefinitionDeletionSeed,
  applySourceChangesets: applyDefinitionDeletionChangeset,
  validate: assertDefinitionDeletionDistribution,
});

/** Incremental synchronization fixture that deletes 100 categories and the 10,000 elements using them in one changeset. */
export const categoryDeletionIncrementalFixture = configureFixture(
  definitionDeletionIncrementalRecipe,
  {
    id: "category-deletion-incremental",
    version: 1,
    label: "categories deleted with their elements",
    scenarioClaims: ["incremental synchronization", "element deletion"],
    topology: "source-and-empty-target",
    seed: 663,
    parameters: { scale: 10_000, categoryCount: 100, geometryPartCount: 0 },
  }
);

/** Incremental synchronization fixture that deletes 100 categories, 100 geometry parts, and the 10,000 elements using them in one changeset. */
export const geometryPartDeletionIncrementalFixture = configureFixture(
  definitionDeletionIncrementalRecipe,
  {
    id: "geometry-part-deletion-incremental",
    version: 1,
    label: "categories and geometry parts deleted with their elements",
    scenarioClaims: ["incremental synchronization", "element deletion"],
    topology: "source-and-empty-target",
    seed: 664,
    parameters: { scale: 10_000, categoryCount: 100, geometryPartCount: 100 },
  }
);
