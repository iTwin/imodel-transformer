/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelDb, SnapshotDb } from "@itwin/core-backend";
import { quickPath } from "../../support/paths.js";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";

export const schemaProcessingSchemaName = "QuickPerfSchemaProcessing";
export const schemaProcessingClassCount = 1200;
export const schemaProcessingPropertiesPerClass = 4;
export const schemaProcessingSourceVersion = "01.00.02";
export const schemaProcessingSourceVersionSemver = "1.0.2";

export interface SchemaProcessingParameters {
  readonly classCount: number;
  readonly propertiesPerClass: number;
}

function assertPositiveSafeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(
      `Schema-processing ${name} must be a positive safe integer`
    );
}

export function schemaProcessingClassNames(classCount: number): string[] {
  assertPositiveSafeInteger(classCount, "classCount");
  return Array.from({ length: classCount }, (_, index) => `Entity${index}`);
}

export function buildSchemaProcessingXml(
  configuration: SchemaProcessingParameters,
  version: string
): string {
  assertPositiveSafeInteger(
    configuration.propertiesPerClass,
    "propertiesPerClass"
  );
  const classes = schemaProcessingClassNames(configuration.classCount)
    .map((className) => {
      const properties = Array.from(
        { length: configuration.propertiesPerClass },
        (_, propertyIndex) =>
          `    <ECProperty propertyName="Value${propertyIndex}" typeName="string"/>`
      ).join("\n");
      return `  <ECEntityClass typeName="${className}">
    <BaseClass>bis:PhysicalElement</BaseClass>
${properties}
  </ECEntityClass>`;
    })
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaProcessingSchemaName}" alias="qpsp" version="${version}" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
  <ECCustomAttributes>
    <DynamicSchema xmlns="CoreCustomAttributes.01.00.03"/>
  </ECCustomAttributes>
${classes}
</ECSchema>`;
}

export function assertSchemaProcessingSchema(
  db: IModelDb,
  expectedVersion: string,
  classCount: number
): string[] {
  const actualVersion = db.querySchemaVersion(schemaProcessingSchemaName);
  if (actualVersion !== expectedVersion)
    throw new Error(
      `Schema-processing version mismatch: expected=${expectedVersion}, actual=${actualVersion}`
    );

  const classNames = schemaProcessingClassNames(classCount);
  for (const className of classNames) {
    if (!db.containsClass(`${schemaProcessingSchemaName}:${className}`))
      throw new Error(`Schema-processing iModel is missing class ${className}`);
  }
  return classNames;
}

const parameters: SchemaProcessingParameters = {
  classCount: schemaProcessingClassCount,
  propertiesPerClass: schemaProcessingPropertiesPerClass,
};

export const schemaProcessingRecipe = defineFixtureRecipe<
  SchemaProcessingParameters,
  void
>({
  id: "schema-processing",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "schemaProcessing.ts"),
    ],
    values: { schemaName: schemaProcessingSchemaName },
  },
  distribution(configuration) {
    schemaProcessingClassNames(configuration.classCount);
    assertPositiveSafeInteger(
      configuration.propertiesPerClass,
      "propertiesPerClass"
    );
    return {
      base: {
        aspects: 0,
        elements: 0,
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
  },
  async createSeed(fileName, context) {
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      await db.importSchemaStrings([
        buildSchemaProcessingXml(
          context.parameters,
          schemaProcessingSourceVersion
        ),
      ]);
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    assertSchemaProcessingSchema(
      db,
      schemaProcessingSourceVersionSemver,
      context.parameters.classCount
    );
  },
});

export const schemaProcessingFixture = configureFixture(
  schemaProcessingRecipe,
  {
    id: "schema-processing-large",
    version: 1,
    label: "schema processing (large)",
    scenarioClaims: ["schema processing"],
    topology: "source-only",
    seed: 662,
    parameters,
  }
);
