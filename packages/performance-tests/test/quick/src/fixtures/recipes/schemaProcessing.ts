/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { SnapshotDb } from "@itwin/core-backend";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { quickPath } from "../../support/paths.js";

export const schemaProcessingSchemaName = "QuickPerfSchemaProcessing";
export const schemaProcessingClassCount = 1200;
const propertiesPerClass = 4;

interface SchemaProcessingParameters {
  readonly classCount: number;
  readonly propertiesPerClass: number;
}

function buildSchemaXml(configuration: SchemaProcessingParameters): string {
  const classes = Array.from(
    { length: configuration.classCount },
    (_, index) => {
      const properties = Array.from(
        { length: configuration.propertiesPerClass },
        (_value, propertyIndex) =>
          `    <ECProperty propertyName="Value${propertyIndex}" typeName="string"/>`
      ).join("\n");
      return `  <ECEntityClass typeName="Entity${index}">
    <BaseClass>bis:PhysicalElement</BaseClass>
${properties}
  </ECEntityClass>`;
    }
  ).join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaProcessingSchemaName}" alias="qpsp" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
${classes}
</ECSchema>`;
}

const parameters: SchemaProcessingParameters = {
  classCount: schemaProcessingClassCount,
  propertiesPerClass,
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
  distribution: () => ({
    base: { aspects: 0, elements: 0, geometricElements: 0, relationships: 0 },
    operations: {
      aspects: { deletes: 0, inserts: 0, updates: 0 },
      elements: { deletes: 0, inserts: 0, updates: 0 },
      relationships: { deletes: 0, inserts: 0, updates: 0 },
      geometryUpdates: 0,
      sourceChangesets: 0,
    },
  }),
  async createSeed(fileName, context) {
    const db = SnapshotDb.createEmpty(fileName, {
      rootSubject: { name: context.descriptor.id },
    });
    try {
      await db.importSchemaStrings([buildSchemaXml(context.parameters)]);
    } finally {
      db.close();
    }
  },
  async applySourceChangesets() {},
  async validate(db) {
    const version = db.querySchemaVersion(schemaProcessingSchemaName);
    if (version !== "1.0.0")
      throw new Error(
        `Schema-processing fixture version mismatch: expected=1.0.0, actual=${version}`
      );
  },
});

export const schemaProcessingFixture = configureFixture(
  schemaProcessingRecipe,
  {
    id: "schema-processing-large",
    version: 1,
    label: "default schema processing (large)",
    scenarioClaims: ["default schema processing"],
    topology: "source-only",
    seed: 662,
    parameters,
  }
);
