/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ColorDef,
  ElementAspectProps,
  ElementProps,
  ExternalSourceAspectProps,
  GeometryPartProps,
  GeometryStreamBuilder,
  GeometryStreamProps,
  IModel,
  PhysicalElementProps,
  Placement3d,
  RelationshipProps,
} from "@itwin/core-common";
import {
  Box,
  Point3d,
  Range3d,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
import {
  DefinitionModel,
  ElementOwnsExternalSourceAspects,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  ExternalSourceAspect,
  GeometryPart,
  IModelDb,
  InformationRecordModel,
  PhysicalModel,
  PhysicalObject,
  RenderMaterialElement,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { quickPath } from "../../support/paths.js";

const schemaName = "QuickSyntheticFacilityLarge";
const schemaAlias = "qsfl";
const definitionClassNames = [
  "ComponentDefinition",
  "EquipmentDefinition",
  "MaterialDefinition",
  "SpecificationDefinition",
] as const;
const uniqueAspectBaseClassName = "SyntheticUniqueAttribute";
const multiAspectBaseClassName = "SyntheticMultiAttribute";
const referenceClassName = "SyntheticReference";
const driveClassName = "SyntheticDependency";
const informationRecordClassName = "SyntheticRecord";
const geometryVariantCount = 8;
export const realisticBuildingTransformLargeSourceGeometryBytes = 2_872_124;

export interface RealisticBuildingTransformLargeParameters {
  readonly elementCount: number;
  readonly geometricElementCount: number;
  readonly geometryBearingElementCount: number;
  readonly definitionElementCount: number;
  readonly modelCount: number;
  readonly includedUniqueAspectCount: number;
  readonly includedUniqueAspectClassCount: number;
  readonly includedMultiAspectCount: number;
  readonly includedMultiAspectClassCount: number;
  readonly externalSourceAspectCount: number;
  readonly externalSourceAspectOwnerCount: number;
  readonly refersToRelationshipCount: number;
  readonly drivesRelationshipCount: number;
  readonly spatialCategoryCount: number;
  readonly renderMaterialCount: number;
  readonly geometryPartCount: number;
}

export interface RealisticBuildingTransformLargeCounts {
  readonly elements: number;
  readonly geometricElements: number;
  readonly geometryBearingElements: number;
  readonly definitionElements: number;
  readonly models: number;
  readonly elementAspects: number;
  readonly includedUniqueAspects: number;
  readonly includedMultiAspects: number;
  readonly includedAspectClasses: readonly number[];
  readonly externalSourceAspects: number;
  readonly externalSourceAspectOwners: number;
  readonly refersToRelationships: number;
  readonly drivesRelationships: number;
  readonly spatialCategories: number;
  readonly renderMaterials: number;
  readonly geometryParts: number;
  readonly geometryPartsWithGeometry: number;
  readonly definitionClasses: readonly number[];
}

export const realisticBuildingTransformLargeParameters: RealisticBuildingTransformLargeParameters =
  Object.freeze({
    elementCount: 46_715,
    geometricElementCount: 25_467,
    geometryBearingElementCount: 15_000,
    definitionElementCount: 18_626,
    modelCount: 32,
    includedUniqueAspectCount: 1_320,
    includedUniqueAspectClassCount: 17,
    includedMultiAspectCount: 1_320,
    includedMultiAspectClassCount: 17,
    externalSourceAspectCount: 41_117,
    externalSourceAspectOwnerCount: 40_661,
    refersToRelationshipCount: 3_000,
    drivesRelationshipCount: 140,
    spatialCategoryCount: 64,
    renderMaterialCount: 128,
    geometryPartCount: 17_547,
  });

function assertPositiveCount(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(
      `Large realistic-building ${name} must be a positive safe integer`
    );
}

function assertNonnegativeCount(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error(
      `Large realistic-building ${name} must be a nonnegative safe integer`
    );
}

function validateParameters(
  parameters: Readonly<RealisticBuildingTransformLargeParameters>
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
    "includedUniqueAspectCount",
    "includedUniqueAspectClassCount",
    "includedMultiAspectCount",
    "includedMultiAspectClassCount",
    "externalSourceAspectCount",
    "externalSourceAspectOwnerCount",
    "refersToRelationshipCount",
    "drivesRelationshipCount",
    "renderMaterialCount",
    "geometryPartCount",
  ] as const)
    assertNonnegativeCount(parameters[name], name);
  if (parameters.geometryBearingElementCount > parameters.geometricElementCount)
    throw new Error(
      "Large realistic-building geometry-bearing elements cannot exceed geometric elements"
    );
  if (
    parameters.geometricElementCount + parameters.definitionElementCount >
    parameters.elementCount
  )
    throw new Error(
      "Large realistic-building geometric and definition elements cannot exceed total elements"
    );
  if (parameters.modelCount < 5)
    throw new Error(
      "Large realistic-building modelCount must be at least five"
    );
  if (
    parameters.definitionElementCount <
    parameters.spatialCategoryCount * 2 +
      parameters.renderMaterialCount +
      parameters.geometryPartCount
  )
    throw new Error(
      "Large realistic-building definitionElementCount cannot be smaller than its category, material, and geometry-part definitions"
    );
  if (
    parameters.includedUniqueAspectCount > 0 &&
    parameters.includedUniqueAspectClassCount === 0
  )
    throw new Error(
      "Large realistic-building unique aspects require at least one unique aspect class"
    );
  if (
    parameters.includedMultiAspectCount > 0 &&
    parameters.includedMultiAspectClassCount === 0
  )
    throw new Error(
      "Large realistic-building multi-aspects require at least one multi-aspect class"
    );
  if (
    parameters.externalSourceAspectOwnerCount >
    parameters.externalSourceAspectCount
  )
    throw new Error(
      "Large realistic-building external-source-aspect owners cannot exceed aspects"
    );
  if (
    parameters.externalSourceAspectCount > 0 &&
    parameters.externalSourceAspectOwnerCount === 0
  )
    throw new Error(
      "Large realistic-building external-source aspects require at least one owner"
    );
  if (
    parameters.geometryBearingElementCount > 0 &&
    parameters.geometryPartCount === 0
  )
    throw new Error(
      "Large realistic-building geometry-bearing elements require geometry parts"
    );
}

function distribution(
  parameters: Readonly<RealisticBuildingTransformLargeParameters>
): FixtureDistribution {
  validateParameters(parameters);
  return {
    base: {
      aspects:
        parameters.includedUniqueAspectCount +
        parameters.includedMultiAspectCount +
        parameters.externalSourceAspectCount,
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

function numberedClassNames(prefix: string, count: number): string[] {
  return Array.from(
    { length: count },
    (_, index) => `${prefix}${index.toString().padStart(2, "0")}`
  );
}

function buildSyntheticSchema(
  parameters: Readonly<RealisticBuildingTransformLargeParameters>
): string {
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
  const aspectClasses = [
    ...numberedClassNames(
      "UniqueAttribute",
      parameters.includedUniqueAspectClassCount
    ).map(
      (
        className
      ) => `  <ECEntityClass typeName="${className}" modifier="Sealed">
    <BaseClass>${uniqueAspectBaseClassName}</BaseClass>
  </ECEntityClass>`
    ),
    ...numberedClassNames(
      "MultiAttribute",
      parameters.includedMultiAspectClassCount
    ).map(
      (
        className
      ) => `  <ECEntityClass typeName="${className}" modifier="Sealed">
    <BaseClass>${multiAspectBaseClassName}</BaseClass>
  </ECEntityClass>`
    ),
  ].join("\n");
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
  <ECEntityClass typeName="${uniqueAspectBaseClassName}" modifier="Abstract">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <ECProperty propertyName="token" typeName="string"/>
  </ECEntityClass>
  <ECEntityClass typeName="${multiAspectBaseClassName}" modifier="Abstract">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
    <ECProperty propertyName="ordinal" typeName="int"/>
    <ECProperty propertyName="token" typeName="string"/>
  </ECEntityClass>
${aspectClasses}
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

function deterministicFederationGuid(seed: number, index: number): string {
  const suffix = (BigInt(seed) * 1_000_003n + BigInt(index + 1))
    .toString(16)
    .padStart(12, "0")
    .slice(-12);
  return `20000000-0000-4000-8000-${suffix}`;
}

function createGeometryVariants(): readonly GeometryStreamProps[] {
  return Array.from({ length: geometryVariantCount }, (_, index) => {
    const builder = new GeometryStreamBuilder();
    const x = 0.35 + (index % 4) * 0.2;
    const y = 0.45 + (index % 3) * 0.25;
    const z = 0.3 + (index % 5) * 0.2;
    const box = Box.createRange(
      Range3d.create(Point3d.createZero(), Point3d.create(x, y, z)),
      index % 2 === 0
    );
    if (!box)
      throw new Error(
        `Failed to create large realistic-building geometry variant ${index}`
      );
    builder.appendGeometry(box);
    return builder.geometryStream;
  });
}

function createGeometryPartReference(partId: string): GeometryStreamProps {
  const builder = new GeometryStreamBuilder();
  builder.appendGeometryPart3d(partId);
  return builder.geometryStream;
}

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Large realistic-building fixture count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

export async function queryRealisticBuildingTransformLargeGeometryBytes(
  db: IModelDb
): Promise<number> {
  const queryGeometryBytes = async (className: string) => {
    const reader = db.createQueryReader(
      `SELECT SUM(LENGTH(GeometryStream)) cnt FROM ${className} WHERE GeometryStream IS NOT NULL`,
      undefined,
      { usePrimaryConn: true }
    );
    if (!(await reader.step()))
      throw new Error(
        `Large realistic-building geometry byte query returned no row for ${className}`
      );
    return Number(reader.current.cnt ?? 0);
  };
  return (
    (await queryGeometryBytes("bis.GeometricElement3d")) +
    (await queryGeometryBytes("bis.GeometryPart"))
  );
}

function expectedClassCounts(total: number, classCount: number): number[] {
  return Array.from(
    { length: classCount },
    (_, classIndex) =>
      Math.floor(total / classCount) + (classIndex < total % classCount ? 1 : 0)
  );
}

function includedAspectClassNames(
  parameters: Readonly<RealisticBuildingTransformLargeParameters>
): string[] {
  return [
    ...numberedClassNames(
      "UniqueAttribute",
      parameters.includedUniqueAspectClassCount
    ),
    ...numberedClassNames(
      "MultiAttribute",
      parameters.includedMultiAspectClassCount
    ),
  ];
}

export async function queryRealisticBuildingTransformLargeCounts(
  db: IModelDb,
  parameters: Readonly<RealisticBuildingTransformLargeParameters> = realisticBuildingTransformLargeParameters
): Promise<RealisticBuildingTransformLargeCounts> {
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
    elementAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ElementAspect"
    ),
    includedUniqueAspects: await queryCount(
      db,
      `SELECT count(*) cnt FROM ${schemaName}.${uniqueAspectBaseClassName}`
    ),
    includedMultiAspects: await queryCount(
      db,
      `SELECT count(*) cnt FROM ${schemaName}.${multiAspectBaseClassName}`
    ),
    includedAspectClasses: await Promise.all(
      includedAspectClassNames(parameters).map(async (className) =>
        queryCount(db, `SELECT count(*) cnt FROM ${schemaName}.${className}`)
      )
    ),
    externalSourceAspects: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.ExternalSourceAspect"
    ),
    externalSourceAspectOwners: await queryCount(
      db,
      "SELECT count(DISTINCT Element.Id) cnt FROM bis.ExternalSourceAspect"
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
    geometryPartsWithGeometry: await queryCount(
      db,
      "SELECT count(*) cnt FROM bis.GeometryPart WHERE GeometryStream IS NOT NULL"
    ),
    definitionClasses: await Promise.all(
      definitionClassNames.map(async (className) =>
        queryCount(db, `SELECT count(*) cnt FROM ${schemaName}.${className}`)
      )
    ),
  };
}

function expectedCounts(
  parameters: Readonly<RealisticBuildingTransformLargeParameters>,
  includeExternalSourceAspects: boolean
): RealisticBuildingTransformLargeCounts {
  const predefinedDefinitions =
    parameters.spatialCategoryCount * 2 +
    parameters.renderMaterialCount +
    parameters.geometryPartCount;
  const syntheticDefinitions =
    parameters.definitionElementCount - predefinedDefinitions;
  const externalSourceAspects = includeExternalSourceAspects
    ? parameters.externalSourceAspectCount
    : 0;
  return {
    elements: parameters.elementCount,
    geometricElements: parameters.geometricElementCount,
    geometryBearingElements: parameters.geometryBearingElementCount,
    definitionElements: parameters.definitionElementCount,
    models: parameters.modelCount,
    elementAspects:
      parameters.includedUniqueAspectCount +
      parameters.includedMultiAspectCount +
      externalSourceAspects,
    includedUniqueAspects: parameters.includedUniqueAspectCount,
    includedMultiAspects: parameters.includedMultiAspectCount,
    includedAspectClasses: [
      ...expectedClassCounts(
        parameters.includedUniqueAspectCount,
        parameters.includedUniqueAspectClassCount
      ),
      ...expectedClassCounts(
        parameters.includedMultiAspectCount,
        parameters.includedMultiAspectClassCount
      ),
    ],
    externalSourceAspects,
    externalSourceAspectOwners: includeExternalSourceAspects
      ? parameters.externalSourceAspectOwnerCount
      : 0,
    refersToRelationships: parameters.refersToRelationshipCount,
    drivesRelationships: parameters.drivesRelationshipCount,
    spatialCategories: parameters.spatialCategoryCount,
    renderMaterials: parameters.renderMaterialCount,
    geometryParts: parameters.geometryPartCount,
    geometryPartsWithGeometry: parameters.geometryPartCount,
    definitionClasses: definitionClassNames.map(
      (_, index) =>
        Math.floor(syntheticDefinitions / definitionClassNames.length) +
        (index < syntheticDefinitions % definitionClassNames.length ? 1 : 0)
    ),
  };
}

export const realisticBuildingTransformLargeSourceExpectedCounts =
  Object.freeze(
    expectedCounts(realisticBuildingTransformLargeParameters, true)
  );
export const realisticBuildingTransformLargeTargetExpectedCounts =
  Object.freeze(
    expectedCounts(realisticBuildingTransformLargeParameters, false)
  );

export const realisticBuildingTransformLargeRecipe = defineFixtureRecipe<
  RealisticBuildingTransformLargeParameters,
  void
>({
  id: "realistic-building-transform-large",
  identity: {
    implementationFiles: [
      quickPath(
        "src",
        "fixtures",
        "recipes",
        "realisticBuildingTransformLarge.ts"
      ),
    ],
    values: {
      content: "aggregate-large-facility-structure-v1",
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
      await db.importSchemaStrings([buildSyntheticSchema(parameters)]);

      const initialModelCount = await queryCount(
        db,
        "SELECT count(*) cnt FROM bis.Model"
      );
      const additionalModelCount = parameters.modelCount - initialModelCount;
      if (additionalModelCount < 3)
        throw new Error(
          `Large realistic-building fixture needs at least three additional models, found ${additionalModelCount}`
        );

      let definitionModelId = IModel.dictionaryId;
      let informationModelId = IModel.repositoryModelId;
      const physicalModelIds: string[] = [];
      withEditTxn(db, "create large realistic-building models", (txn) => {
        definitionModelId = DefinitionModel.insert(
          txn,
          IModel.rootSubjectId,
          "Synthetic Large Definitions"
        );
        informationModelId = InformationRecordModel.insert(
          txn,
          IModel.rootSubjectId,
          "Synthetic Large Records"
        );
        for (let index = 0; index < additionalModelCount - 2; index++)
          physicalModelIds.push(
            PhysicalModel.insert(
              txn,
              IModel.rootSubjectId,
              `Synthetic Large Physical Model ${index}`
            )
          );
      });

      const geometryVariants = createGeometryVariants();
      const categoryIds: string[] = [];
      const geometryPartIds: string[] = [];
      const definitionIds: string[] = [];
      let federationGuidIndex = 0;
      const nextFederationGuid = () =>
        deterministicFederationGuid(
          context.descriptor.layout.seed,
          federationGuidIndex++
        );

      withEditTxn(
        db,
        "create large realistic-building built-in definitions",
        (txn) => {
          for (
            let index = 0;
            index < parameters.spatialCategoryCount;
            index++
          ) {
            const categoryId = SpatialCategory.insert(
              txn,
              definitionModelId,
              `Synthetic Large Category ${index}`,
              {
                color: ColorDef.from(
                  35 + ((index * 43) % 190),
                  45 + ((index * 29) % 180),
                  55 + ((index * 19) % 170)
                ).toJSON(),
              }
            );
            categoryIds.push(categoryId);
            definitionIds.push(categoryId);
          }
          for (let index = 0; index < parameters.renderMaterialCount; index++) {
            definitionIds.push(
              RenderMaterialElement.insert(
                txn,
                definitionModelId,
                `Synthetic Large Material ${index}`,
                {
                  paletteName: `Synthetic Large Palette ${index % 8}`,
                  diffuse: 0.4 + (index % 5) * 0.1,
                  specular: (index % 4) * 0.06,
                }
              )
            );
          }
          for (let index = 0; index < parameters.geometryPartCount; index++) {
            const props: GeometryPartProps = {
              classFullName: GeometryPart.classFullName,
              model: definitionModelId,
              code: Code.createEmpty(),
              federationGuid: nextFederationGuid(),
              userLabel: `Synthetic Large Geometry Part ${index}`,
              geom: geometryVariants[index % geometryVariants.length],
            };
            const partId = txn.insertElement(props);
            geometryPartIds.push(partId);
            definitionIds.push(partId);
          }
        }
      );

      const physicalIds: string[] = [];
      withEditTxn(
        db,
        "create large realistic-building physical elements",
        (txn) => {
          for (
            let index = 0;
            index < parameters.geometricElementCount;
            index++
          ) {
            const props: PhysicalElementProps = {
              classFullName: PhysicalObject.classFullName,
              model: physicalModelIds[index % physicalModelIds.length],
              category: categoryIds[index % categoryIds.length],
              code: Code.createEmpty(),
              federationGuid: nextFederationGuid(),
              userLabel: `Synthetic Large Physical Element ${index}`,
              placement: Placement3d.fromJSON({
                origin: {
                  x: (index % 120) * 1.75,
                  y: (Math.floor(index / 120) % 90) * 1.5,
                  z: Math.floor(index / (120 * 90)) * 2.8,
                },
                angles: YawPitchRollAngles.createDegrees(
                  (index % 18) * 5,
                  0,
                  0
                ).toJSON(),
              }),
              geom:
                index < parameters.geometryBearingElementCount
                  ? createGeometryPartReference(
                      geometryPartIds[
                        (index * 37 + context.descriptor.layout.seed) %
                          geometryPartIds.length
                      ]
                    )
                  : undefined,
            };
            physicalIds.push(txn.insertElement(props));
          }
        }
      );

      const currentDefinitionCount = await queryCount(
        db,
        "SELECT count(*) cnt FROM bis.DefinitionElement"
      );
      const syntheticDefinitionCount =
        parameters.definitionElementCount - currentDefinitionCount;
      if (syntheticDefinitionCount < 0)
        throw new Error(
          `Large realistic-building built-in definitions exceed target by ${-syntheticDefinitionCount}`
        );
      withEditTxn(
        db,
        "create large realistic-building synthetic definitions",
        (txn) => {
          for (let index = 0; index < syntheticDefinitionCount; index++) {
            const props: ElementProps & { ordinal: number; token: string } = {
              classFullName: `${schemaName}:${definitionClassNames[index % definitionClassNames.length]}`,
              model: definitionModelId,
              code: Code.createEmpty(),
              federationGuid: nextFederationGuid(),
              userLabel: `Synthetic Large Definition ${index}`,
              ordinal: index,
              token: `large-definition-${index % 193}`,
            };
            definitionIds.push(txn.insertElement(props));
          }
        }
      );

      const currentElementCount = await queryCount(
        db,
        "SELECT count(*) cnt FROM bis.Element"
      );
      const informationRecordCount =
        parameters.elementCount - currentElementCount;
      if (informationRecordCount < 0)
        throw new Error(
          `Large realistic-building modeled elements exceed target by ${-informationRecordCount}`
        );
      const informationIds: string[] = [];
      withEditTxn(
        db,
        "create large realistic-building information records",
        (txn) => {
          for (let index = 0; index < informationRecordCount; index++) {
            const props: ElementProps & { ordinal: number; token: string } = {
              classFullName: `${schemaName}:${informationRecordClassName}`,
              model: informationModelId,
              code: Code.createEmpty(),
              federationGuid: nextFederationGuid(),
              userLabel: `Synthetic Large Record ${index}`,
              ordinal: index,
              token: `large-record-${index % 251}`,
            };
            informationIds.push(txn.insertElement(props));
          }
        }
      );

      const aspectOwnerIds = [
        ...physicalIds,
        ...definitionIds,
        ...informationIds,
      ];
      if (parameters.externalSourceAspectOwnerCount > aspectOwnerIds.length)
        throw new Error(
          `Large realistic-building external-source-aspect owners exceed available owners: ${parameters.externalSourceAspectOwnerCount} > ${aspectOwnerIds.length}`
        );

      const uniqueAspectClassNames = numberedClassNames(
        "UniqueAttribute",
        parameters.includedUniqueAspectClassCount
      );
      const multiAspectClassNames = numberedClassNames(
        "MultiAttribute",
        parameters.includedMultiAspectClassCount
      );
      withEditTxn(db, "create large realistic-building aspects", (txn) => {
        for (
          let index = 0;
          index < parameters.includedUniqueAspectCount;
          index++
        ) {
          const classIndex = index % uniqueAspectClassNames.length;
          const props: ElementAspectProps & {
            ordinal: number;
            token: string;
          } = {
            classFullName: `${schemaName}:${uniqueAspectClassNames[classIndex]}`,
            element: new ElementOwnsUniqueAspect(
              aspectOwnerIds[
                Math.floor(index / uniqueAspectClassNames.length) %
                  aspectOwnerIds.length
              ]
            ),
            ordinal: index,
            token: `large-unique-${classIndex}-${index % 211}`,
          };
          txn.insertAspect(props);
        }
        for (
          let index = 0;
          index < parameters.includedMultiAspectCount;
          index++
        ) {
          const classIndex = index % multiAspectClassNames.length;
          const props: ElementAspectProps & {
            ordinal: number;
            token: string;
          } = {
            classFullName: `${schemaName}:${multiAspectClassNames[classIndex]}`,
            element: new ElementOwnsMultiAspects(
              aspectOwnerIds[(index * 19 + 11) % aspectOwnerIds.length]
            ),
            ordinal: index,
            token: `large-multi-${classIndex}-${index % 223}`,
          };
          txn.insertAspect(props);
        }
        for (
          let index = 0;
          index < parameters.externalSourceAspectCount;
          index++
        ) {
          const props: ExternalSourceAspectProps = {
            classFullName: ExternalSourceAspect.classFullName,
            element: new ElementOwnsExternalSourceAspects(
              aspectOwnerIds[
                index < parameters.externalSourceAspectOwnerCount
                  ? index
                  : index % parameters.externalSourceAspectOwnerCount
              ]
            ),
            scope: { id: IModel.rootSubjectId },
            identifier: `synthetic-large-external-${index}`,
            kind: ExternalSourceAspect.Kind.Element,
            version: `v${index % 29}`,
          };
          txn.insertAspect(props);
        }
      });

      const relationshipElementIds = [
        ...physicalIds,
        ...definitionIds,
        ...informationIds,
      ];
      withEditTxn(
        db,
        "create large realistic-building relationships",
        (txn) => {
          for (
            let index = 0;
            index < parameters.refersToRelationshipCount;
            index++
          ) {
            const sourceIndex = index % relationshipElementIds.length;
            const offset =
              ((index * 29 + context.descriptor.layout.seed) %
                (relationshipElementIds.length - 1)) +
              1;
            const props: RelationshipProps & { ordinal: number } = {
              classFullName: `${schemaName}:${referenceClassName}`,
              sourceId: relationshipElementIds[sourceIndex],
              targetId:
                relationshipElementIds[
                  (sourceIndex + offset) % relationshipElementIds.length
                ],
              ordinal: index,
            };
            txn.insertRelationship(props);
          }
          for (
            let index = 0;
            index < parameters.drivesRelationshipCount;
            index++
          ) {
            const props: RelationshipProps & {
              priority: number;
              status: number;
            } = {
              classFullName: `${schemaName}:${driveClassName}`,
              sourceId: definitionIds[index % definitionIds.length],
              targetId: physicalIds[(index * 31 + 13) % physicalIds.length],
              priority: index % 8,
              status: 0,
            };
            txn.insertRelationship(props);
          }
        }
      );
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const expected = expectedCounts(context.parameters, true);
    const actual = await queryRealisticBuildingTransformLargeCounts(
      db,
      context.parameters
    );
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new Error(
        `Large realistic-building fixture distribution mismatch: expected=${JSON.stringify(
          expected
        )}, actual=${JSON.stringify(actual)}`
      );
  },
});

export const realisticBuildingTransformLargeFixture = configureFixture(
  realisticBuildingTransformLargeRecipe,
  {
    id: "realistic-building-transform-large",
    version: 1,
    label: "large realistic synthetic building full transformation",
    scenarioClaims: ["full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 49979687,
    parameters: realisticBuildingTransformLargeParameters,
  }
);

export const realisticBuildingTransformLargeDescriptor =
  realisticBuildingTransformLargeFixture.descriptor;
