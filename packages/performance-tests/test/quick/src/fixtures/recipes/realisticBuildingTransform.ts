/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ColorDef,
  ElementAspectProps,
  GeometryPartProps,
  GeometryStreamBuilder,
  GeometryStreamProps,
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
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  GeometryPart,
  IModelDb,
  PhysicalObject,
  RenderMaterialElement,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { quickPath } from "../../support/paths.js";
import {
  createRealisticBuildingModelStructure,
  deterministicRealisticBuildingFederationGuid,
  insertRealisticBuildingInformationRecords,
  insertRealisticBuildingRelationships,
  insertRealisticBuildingSyntheticDefinitions,
  queryRealisticBuildingCount,
  remainingRealisticBuildingEntityCount,
} from "./realisticBuildingShared.js";

const schemaName = "QuickSyntheticBuilding";
const schemaAlias = "qsb";
const definitionClassNames = [
  "AssemblyDefinition",
  "MaterialSpecification",
  "TypeDefinition",
] as const;
const uniqueAspectClassNames = [
  "AssetIdentity",
  "Classification",
  "ConstructionPhase",
  "QualityRecord",
  "SourceRecord",
  "SystemAssignment",
  "ValidationRecord",
] as const;
const multiAspectClassName = "PropertyRecord";
const referenceClassName = "ElementReference";
const driveClassName = "ElementDependency";
const informationRecordClassName = "ProjectRecord";
const geometryVariantCount = 8;

export interface RealisticBuildingTransformParameters {
  readonly elementCount: number;
  readonly geometricElementCount: number;
  readonly geometryBearingElementCount: number;
  readonly definitionElementCount: number;
  readonly modelCount: number;
  readonly multiAspectCount: number;
  readonly uniqueAspectCount: number;
  readonly refersToRelationshipCount: number;
  readonly drivesRelationshipCount: number;
  readonly spatialCategoryCount: number;
  readonly renderMaterialCount: number;
  readonly geometryPartCount: number;
}

export interface RealisticBuildingCounts {
  readonly elements: number;
  readonly geometricElements: number;
  readonly geometryBearingElements: number;
  readonly definitionElements: number;
  readonly models: number;
  readonly multiAspects: number;
  readonly multiAspectOwners: number;
  readonly uniqueAspects: number;
  readonly uniqueAspectClasses: readonly number[];
  readonly refersToRelationships: number;
  readonly drivesRelationships: number;
  readonly spatialCategories: number;
  readonly renderMaterials: number;
  readonly geometryParts: number;
  readonly definitionClasses: readonly number[];
}

export const realisticBuildingTransformParameters: RealisticBuildingTransformParameters =
  Object.freeze({
    elementCount: 3440,
    geometricElementCount: 1608,
    geometryBearingElementCount: 1064,
    definitionElementCount: 1812,
    modelCount: 14,
    multiAspectCount: 2424,
    uniqueAspectCount: 2538,
    refersToRelationshipCount: 2672,
    drivesRelationshipCount: 276,
    spatialCategoryCount: 24,
    renderMaterialCount: 64,
    geometryPartCount: 64,
  });

function assertPositiveCount(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(
      `Realistic-building ${name} must be a positive safe integer`
    );
}

function assertNonnegativeCount(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error(
      `Realistic-building ${name} must be a nonnegative safe integer`
    );
}

function validateParameters(
  parameters: Readonly<RealisticBuildingTransformParameters>
): void {
  for (const name of [
    "elementCount",
    "geometricElementCount",
    "definitionElementCount",
    "modelCount",
    "spatialCategoryCount",
  ] as const)
    assertPositiveCount(parameters[name], name);
  for (const name of [
    "geometryBearingElementCount",
    "multiAspectCount",
    "uniqueAspectCount",
    "refersToRelationshipCount",
    "drivesRelationshipCount",
    "renderMaterialCount",
    "geometryPartCount",
  ] as const)
    assertNonnegativeCount(parameters[name], name);
  if (parameters.geometryBearingElementCount > parameters.geometricElementCount)
    throw new Error(
      "Realistic-building geometry-bearing elements cannot exceed geometric elements"
    );
  if (
    parameters.geometricElementCount + parameters.definitionElementCount >
    parameters.elementCount
  )
    throw new Error(
      "Realistic-building geometric and definition elements cannot exceed total elements"
    );
  if (parameters.modelCount < 5)
    throw new Error("Realistic-building modelCount must be at least five");
  if (
    parameters.definitionElementCount <
    parameters.spatialCategoryCount * 2 +
      parameters.renderMaterialCount +
      parameters.geometryPartCount
  )
    throw new Error(
      "Realistic-building definitionElementCount cannot be smaller than its category, material, and geometry-part definitions"
    );
}

function distribution(
  parameters: Readonly<RealisticBuildingTransformParameters>
): FixtureDistribution {
  validateParameters(parameters);
  return {
    base: {
      aspects: parameters.multiAspectCount + parameters.uniqueAspectCount,
      elements: parameters.elementCount,
      geometricElements: parameters.geometricElementCount,
      relationships:
        parameters.refersToRelationshipCount +
        parameters.drivesRelationshipCount,
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

function buildSyntheticSchema(): string {
  const definitionClasses = definitionClassNames
    .map(
      (
        className
      ) => `  <ECEntityClass typeName="${className}" modifier="Sealed">
    <BaseClass>bis:DefinitionElement</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <ECProperty propertyName="token" typeName="string"/>
  </ECEntityClass>`
    )
    .join("\n");
  const uniqueAspects = uniqueAspectClassNames
    .map(
      (
        className
      ) => `  <ECEntityClass typeName="${className}" modifier="Sealed">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <ECProperty propertyName="token" typeName="string"/>
  </ECEntityClass>`
    )
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaName}" alias="${schemaAlias}" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
  <ECCustomAttributes>
    <DynamicSchema xmlns="CoreCustomAttributes.01.00.03"/>
  </ECCustomAttributes>
${definitionClasses}
  <ECEntityClass typeName="${informationRecordClassName}" modifier="Sealed">
    <BaseClass>bis:InformationRecordElement</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <ECProperty propertyName="token" typeName="string"/>
  </ECEntityClass>
${uniqueAspects}
  <ECEntityClass typeName="${multiAspectClassName}" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <ECProperty propertyName="token" typeName="string"/>
  </ECEntityClass>
  <ECRelationshipClass typeName="${referenceClassName}" strength="referencing" modifier="Sealed">
    <BaseClass>bis:ElementRefersToElements</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <Source multiplicity="(0..*)" roleLabel="references" polymorphic="true">
      <Class class="bis:Element"/>
    </Source>
    <Target multiplicity="(0..*)" roleLabel="is referenced by" polymorphic="true">
      <Class class="bis:Element"/>
    </Target>
  </ECRelationshipClass>
  <ECRelationshipClass typeName="${driveClassName}" strength="referencing" modifier="Sealed">
    <BaseClass>bis:ElementDrivesElement</BaseClass>
    <Source multiplicity="(0..*)" roleLabel="drives" polymorphic="true">
      <Class class="bis:Element"/>
    </Source>
    <Target multiplicity="(0..*)" roleLabel="is driven by" polymorphic="true">
      <Class class="bis:Element"/>
    </Target>
  </ECRelationshipClass>
</ECSchema>`;
}

function createGeometryVariants(): readonly GeometryStreamProps[] {
  return Array.from({ length: geometryVariantCount }, (_, index) => {
    const builder = new GeometryStreamBuilder();
    const x = 0.6 + (index % 4) * 0.35;
    const y = 0.8 + (index % 3) * 0.4;
    const z = 0.5 + (index % 5) * 0.45;
    const box = Box.createRange(
      Range3d.create(Point3d.createZero(), Point3d.create(x, y, z)),
      index % 2 === 0
    );
    if (!box)
      throw new Error(
        `Failed to create realistic-building geometry variant ${index}`
      );
    builder.appendGeometry(box);
    return builder.geometryStream;
  });
}

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  return queryRealisticBuildingCount(db, ecsql, "Realistic-building fixture");
}

function expectedUniqueAspectClassCounts(total: number): number[] {
  return uniqueAspectClassNames.map(
    (_, classIndex) =>
      Math.floor(total / uniqueAspectClassNames.length) +
      (classIndex < total % uniqueAspectClassNames.length ? 1 : 0)
  );
}

function expectedMultiAspectOwnerCount(total: number): number {
  let remaining = total;
  let ownerCount = 0;
  while (remaining > 0) {
    remaining -= Math.min((ownerCount % 3) + 1, remaining);
    ownerCount++;
  }
  return ownerCount;
}

export async function queryRealisticBuildingCounts(
  db: IModelDb
): Promise<RealisticBuildingCounts> {
  return {
    elements: await queryCount(db, "SELECT count(*) cnt FROM bis.Element"),
    geometricElements: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.GeometricElement3d"
    ),
    geometryBearingElements: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.GeometricElement3d WHERE GeometryStream IS NOT NULL"
    ),
    definitionElements: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.DefinitionElement"
    ),
    models: await queryCount(db, "SELECT count(*) cnt FROM bis.Model"),
    multiAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementMultiAspect"
    ),
    multiAspectOwners: await queryCount(
      db,
      `SELECT count(DISTINCT Element.Id) cnt FROM ${schemaName}.${multiAspectClassName}`
    ),
    uniqueAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementUniqueAspect"
    ),
    uniqueAspectClasses: await Promise.all(
      uniqueAspectClassNames.map(async (className) => {
        const count = await queryCount(
          db,
          `SELECT count(*) cnt FROM ${schemaName}.${className}`
        );
        return count;
      })
    ),
    refersToRelationships: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementRefersToElements"
    ),
    drivesRelationships: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementDrivesElement"
    ),
    spatialCategories: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.SpatialCategory"
    ),
    renderMaterials: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.RenderMaterial"
    ),
    geometryParts: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.GeometryPart"
    ),
    definitionClasses: await Promise.all(
      definitionClassNames.map(async (className) => {
        const count = await queryCount(
          db,
          `SELECT count(*) cnt FROM ${schemaName}.${className}`
        );
        return count;
      })
    ),
  };
}

function expectedCounts(
  parameters: Readonly<RealisticBuildingTransformParameters>,
  includeDriveRelationships = true
): RealisticBuildingCounts {
  const predefinedDefinitions =
    parameters.spatialCategoryCount * 2 +
    parameters.renderMaterialCount +
    parameters.geometryPartCount;
  const syntheticDefinitions =
    parameters.definitionElementCount - predefinedDefinitions;
  return {
    elements: parameters.elementCount,
    geometricElements: parameters.geometricElementCount,
    geometryBearingElements: parameters.geometryBearingElementCount,
    definitionElements: parameters.definitionElementCount,
    models: parameters.modelCount,
    multiAspects: parameters.multiAspectCount,
    multiAspectOwners: expectedMultiAspectOwnerCount(
      parameters.multiAspectCount
    ),
    uniqueAspects: parameters.uniqueAspectCount,
    uniqueAspectClasses: expectedUniqueAspectClassCounts(
      parameters.uniqueAspectCount
    ),
    refersToRelationships: parameters.refersToRelationshipCount,
    drivesRelationships: includeDriveRelationships
      ? parameters.drivesRelationshipCount
      : 0,
    spatialCategories: parameters.spatialCategoryCount,
    renderMaterials: parameters.renderMaterialCount,
    geometryParts: parameters.geometryPartCount,
    definitionClasses: definitionClassNames.map(
      (_, index) =>
        Math.floor(syntheticDefinitions / definitionClassNames.length) +
        (index < syntheticDefinitions % definitionClassNames.length ? 1 : 0)
    ),
  };
}

export const realisticBuildingExpectedCounts = Object.freeze(
  expectedCounts(realisticBuildingTransformParameters)
);
export const realisticBuildingFullTransformExpectedCounts = Object.freeze(
  expectedCounts(realisticBuildingTransformParameters, false)
);

/**
 * Builds a standalone source whose aggregate transformer-relevant structure is
 * approximately twice that of a representative building iModel. All names,
 * schema classes, identifiers, placements, and lightweight geometry are synthetic.
 */
export const realisticBuildingTransformRecipe = defineFixtureRecipe<
  RealisticBuildingTransformParameters,
  void
>({
  id: "realistic-building-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "realisticBuildingTransform.ts"),
      quickPath("src", "fixtures", "recipes", "realisticBuildingShared.ts"),
    ],
    values: {
      content: "aggregate-building-structure-v1",
      geometryVariants: geometryVariantCount,
      schema: schemaName,
    },
  },
  distribution,
  async createSeed(fileName, context) {
    const parameters = context.parameters;
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      await db.importSchemaStrings([buildSyntheticSchema()]);

      const { definitionModelId, informationModelId, physicalModelIds } =
        await createRealisticBuildingModelStructure(db, {
          definitionModelName: "Synthetic Definitions",
          fixtureLabel: "Realistic-building fixture",
          informationModelName: "Synthetic Records",
          modelCount: parameters.modelCount,
          physicalModelName: (index) => `Synthetic Physical Model ${index}`,
          transactionDescription: "create realistic-building models",
        });

      const geometryVariants = createGeometryVariants();
      const categoryIds: string[] = [];
      const definitionIds: string[] = [];
      withEditTxn(
        db,
        "create realistic-building built-in definitions",
        (txn) => {
          for (
            let index = 0;
            index < parameters.spatialCategoryCount;
            index++
          ) {
            const categoryId = SpatialCategory.insert(
              txn,
              definitionModelId,
              `Synthetic Category ${index}`,
              {
                color: ColorDef.from(
                  40 + ((index * 47) % 180),
                  50 + ((index * 31) % 170),
                  60 + ((index * 23) % 160)
                ).toJSON(),
              }
            );
            categoryIds.push(categoryId);
            definitionIds.push(categoryId);
          }
          for (let index = 0; index < parameters.renderMaterialCount; index++) {
            const materialId = RenderMaterialElement.insert(
              txn,
              definitionModelId,
              `Synthetic Material ${index}`,
              {
                paletteName: `Synthetic Palette ${index % 4}`,
                diffuse: 0.45 + (index % 4) * 0.1,
                specular: (index % 3) * 0.08,
              }
            );
            definitionIds.push(materialId);
          }
          for (let index = 0; index < parameters.geometryPartCount; index++) {
            const props: GeometryPartProps = {
              classFullName: GeometryPart.classFullName,
              model: definitionModelId,
              code: Code.createEmpty(),
              federationGuid: deterministicRealisticBuildingFederationGuid(
                "10000000",
                context.descriptor.layout.seed,
                index
              ),
              userLabel: `Synthetic Geometry Part ${index}`,
              geom: geometryVariants[index % geometryVariants.length],
            };
            definitionIds.push(txn.insertElement(props));
          }
        }
      );

      const physicalIds: string[] = [];
      withEditTxn(db, "create realistic-building physical elements", (txn) => {
        for (let index = 0; index < parameters.geometricElementCount; index++) {
          const props: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: physicalModelIds[index % physicalModelIds.length],
            category: categoryIds[index % categoryIds.length],
            code: Code.createEmpty(),
            federationGuid: deterministicRealisticBuildingFederationGuid(
              "10000000",
              context.descriptor.layout.seed,
              parameters.geometryPartCount + index
            ),
            userLabel: `Synthetic Physical Element ${index}`,
            placement: Placement3d.fromJSON({
              origin: {
                x: (index % 48) * 2.5,
                y: (Math.floor(index / 48) % 36) * 2.25,
                z: Math.floor(index / (48 * 36)) * 3.2,
              },
              angles: YawPitchRollAngles.createDegrees(
                (index % 12) * 7.5,
                0,
                0
              ).toJSON(),
            }),
            geom:
              index < parameters.geometryBearingElementCount
                ? geometryVariants[
                    (index + context.descriptor.layout.seed) %
                      geometryVariants.length
                  ]
                : undefined,
          };
          physicalIds.push(txn.insertElement(props));
        }
      });

      const syntheticDefinitionCount =
        await remainingRealisticBuildingEntityCount(
          db,
          "bis.DefinitionElement",
          parameters.definitionElementCount,
          "Realistic-building",
          "built-in definitions"
        );
      insertRealisticBuildingSyntheticDefinitions(db, {
        classFullNames: definitionClassNames.map(
          (className) => `${schemaName}:${className}`
        ),
        count: syntheticDefinitionCount,
        definitionIds,
        federationGuid: (index) =>
          deterministicRealisticBuildingFederationGuid(
            "10000000",
            context.descriptor.layout.seed,
            parameters.geometryPartCount +
              parameters.geometricElementCount +
              index
          ),
        modelId: definitionModelId,
        token: (index) => `definition-${index % 97}`,
        transactionDescription:
          "create realistic-building synthetic definitions",
        userLabel: (index) => `Synthetic Definition ${index}`,
      });

      const informationRecordCount =
        await remainingRealisticBuildingEntityCount(
          db,
          "bis.Element",
          parameters.elementCount,
          "Realistic-building",
          "modeled elements"
        );
      const informationIds = insertRealisticBuildingInformationRecords(db, {
        classFullName: `${schemaName}:${informationRecordClassName}`,
        count: informationRecordCount,
        federationGuid: (index) =>
          deterministicRealisticBuildingFederationGuid(
            "10000000",
            context.descriptor.layout.seed,
            parameters.geometryPartCount +
              parameters.geometricElementCount +
              syntheticDefinitionCount +
              index
          ),
        modelId: informationModelId,
        token: (index) => `record-${index % 17}`,
        transactionDescription: "create realistic-building information records",
        userLabel: (index) => `Synthetic Project Record ${index}`,
      });

      const elementIds = [...physicalIds, ...definitionIds, ...informationIds];
      const requiredMultiAspectOwnerCount = expectedMultiAspectOwnerCount(
        parameters.multiAspectCount
      );
      if (requiredMultiAspectOwnerCount > elementIds.length)
        throw new Error(
          `Realistic-building multi-aspects require ${requiredMultiAspectOwnerCount} owners, but only ${elementIds.length} are available`
        );
      withEditTxn(db, "create realistic-building aspects", (txn) => {
        for (let index = 0; index < parameters.uniqueAspectCount; index++) {
          const classIndex = index % uniqueAspectClassNames.length;
          const props: ElementAspectProps & {
            ordinal: number;
            token: string;
          } = {
            classFullName: `${schemaName}:${uniqueAspectClassNames[classIndex]}`,
            element: new ElementOwnsUniqueAspect(
              elementIds[
                Math.floor(index / uniqueAspectClassNames.length) %
                  elementIds.length
              ]
            ),
            ordinal: index,
            token: `unique-${classIndex}-${index % 113}`,
          };
          txn.insertAspect(props);
        }

        let inserted = 0;
        let ownerIndex = 0;
        while (inserted < parameters.multiAspectCount) {
          const ownerCardinality = Math.min(
            (ownerIndex % 3) + 1,
            parameters.multiAspectCount - inserted
          );
          for (let ordinal = 0; ordinal < ownerCardinality; ordinal++) {
            const props: ElementAspectProps & {
              ordinal: number;
              token: string;
            } = {
              classFullName: `${schemaName}:${multiAspectClassName}`,
              element: new ElementOwnsMultiAspects(elementIds[ownerIndex]),
              ordinal,
              token: `multi-${ownerIndex % 127}-${ordinal}`,
            };
            txn.insertAspect(props);
            inserted++;
          }
          ownerIndex++;
        }
      });

      withEditTxn(db, "create realistic-building relationships", (txn) => {
        insertRealisticBuildingRelationships(txn, {
          definitionIds,
          driveClassFullName: `${schemaName}:${driveClassName}`,
          driveCount: parameters.drivesRelationshipCount,
          driveTargetOffset: 7,
          driveTargetStride: 13,
          elementIds,
          physicalIds,
          referenceClassFullName: `${schemaName}:${referenceClassName}`,
          referenceCount: parameters.refersToRelationshipCount,
          referenceOffsetStride: 17,
          seed: context.descriptor.layout.seed,
        });
      });
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const expected = expectedCounts(context.parameters);
    const actual = await queryRealisticBuildingCounts(db);
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new Error(
        `Realistic-building fixture distribution mismatch: expected=${JSON.stringify(
          expected
        )}, actual=${JSON.stringify(actual)}`
      );
  },
});

export const realisticBuildingTransformFixture = configureFixture(
  realisticBuildingTransformRecipe,
  {
    id: "realistic-building-transform",
    version: 1,
    label: "realistic synthetic building full transformation",
    scenarioClaims: ["full transformation", "drive relationship processing"],
    topology: "standalone-source-and-empty-target",
    seed: 32452843,
    parameters: realisticBuildingTransformParameters,
  }
);

export const realisticBuildingTransformDescriptor =
  realisticBuildingTransformFixture.descriptor;
