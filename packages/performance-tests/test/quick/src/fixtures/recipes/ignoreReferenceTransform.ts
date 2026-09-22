/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import {
  Code,
  IModel,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import {
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { FixtureDistribution } from "../FixtureDescriptor.js";
import { configureFixture, defineFixtureRecipe } from "../FixtureRecipe.js";
import { quickPath } from "../../support/paths.js";

export interface IgnoreReferenceTransformParameters {
  readonly holderCount: number;
  readonly referenceCount: number;
  readonly targetCount: number;
}

export const ignoreReferenceSchemaName = "QuickIgnoreRefs";
export const ignoreReferenceNavigationPropertyCount = 24;

function distribution(
  parameters: Readonly<IgnoreReferenceTransformParameters>
): FixtureDistribution {
  for (const [name, value] of Object.entries(parameters)) {
    if (!Number.isSafeInteger(value) || value < 1)
      throw new Error(
        `Ignore-reference ${name} must be a positive safe integer`
      );
  }
  return {
    base: {
      aspects: 0,
      elements: parameters.targetCount + parameters.holderCount,
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

function schemaXml(referenceCount: number): string {
  const relationships = Array.from(
    { length: referenceCount },
    (_, index) => `
    <ECRelationshipClass typeName="RefRel${index}" strength="referencing" modifier="sealed">
      <Source multiplicity="(0..*)" roleLabel="refers to" polymorphic="true">
        <Class class="RefHolder"/>
      </Source>
      <Target multiplicity="(0..1)" roleLabel="is referenced by" polymorphic="true">
        <Class class="bis:PhysicalElement"/>
      </Target>
    </ECRelationshipClass>`
  ).join("");
  const properties = Array.from(
    { length: referenceCount },
    (_, index) =>
      `<ECNavigationProperty propertyName="Ref${index}" relationshipName="RefRel${index}" direction="Forward"/>`
  ).join("\n      ");
  return `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${ignoreReferenceSchemaName}" alias="qir" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
  ${relationships}
  <ECEntityClass typeName="RefHolder">
    <BaseClass>bis:PhysicalElement</BaseClass>
    ${properties}
  </ECEntityClass>
</ECSchema>`;
}

function physicalElementProps(
  modelId: string,
  categoryId: string,
  userLabel: string
): PhysicalElementProps {
  return {
    classFullName: PhysicalObject.classFullName,
    model: modelId,
    category: categoryId,
    code: Code.createEmpty(),
    userLabel,
    placement: Placement3d.fromJSON({ origin: {}, angles: {} }),
  };
}

async function queryCount(db: IModelDb, ecsql: string): Promise<number> {
  const reader = db.createQueryReader(ecsql, undefined, {
    usePrimaryConn: true,
  });
  if (!(await reader.step()))
    throw new Error(
      `Ignore-reference fixture count query returned no row: ${ecsql}`
    );
  return reader.current.cnt as number;
}

export function populatedReferenceCountQuery(referenceCount: number): string {
  const populated = Array.from(
    { length: referenceCount },
    (_, index) => `Ref${index}.Id IS NOT NULL`
  ).join(" AND ");
  return `SELECT count(*) cnt FROM ${ignoreReferenceSchemaName}.RefHolder WHERE ${populated}`;
}

/**
 * Creates the navigation-reference workload used by the ignore-mode full-transform scenario.
 * The default fixture contains 3,000 referenced elements and 1,000 holders with 24 populated
 * navigation properties each, for 24,000 application-level reference visits.
 */
export const ignoreReferenceTransformRecipe = defineFixtureRecipe({
  id: "ignore-reference-transform",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "ignoreReferenceTransform.ts"),
    ],
    values: {
      content: "navigation-reference-holders-v1",
      schema: `${ignoreReferenceSchemaName}.01.00.00`,
    },
  },
  distribution,
  async createSeed(fileName, context) {
    const { holderCount, referenceCount, targetCount } = context.parameters;
    const schemaFileName = path.join(
      path.dirname(fileName),
      `${ignoreReferenceSchemaName}.ecschema.xml`
    );
    fs.writeFileSync(schemaFileName, schemaXml(referenceCount), "utf8");
    try {
      const db = SnapshotDb.createEmpty(fileName, {
        rootSubject: { name: context.descriptor.id },
      });
      try {
        await db.importSchemas([schemaFileName]);
        withEditTxn(db, "create ignore-reference workload", (txn) => {
          const categoryId = SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "QuickIgnoreReferenceCategory",
            {}
          );
          const modelId = PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "QuickIgnoreReferenceModel"
          );
          const targetIds = Array.from({ length: targetCount }, (_, index) =>
            txn.insertElement(
              physicalElementProps(modelId, categoryId, `Target-${index}`)
            )
          );
          for (let holder = 0; holder < holderCount; holder++) {
            const props: PhysicalElementProps & Record<string, unknown> = {
              ...physicalElementProps(modelId, categoryId, `Holder-${holder}`),
              classFullName: `${ignoreReferenceSchemaName}:RefHolder`,
            };
            for (let reference = 0; reference < referenceCount; reference++) {
              props[`ref${reference}`] = {
                id: targetIds[
                  (holder * referenceCount + reference) % targetIds.length
                ],
                relClassName: `${ignoreReferenceSchemaName}:RefRel${reference}`,
              };
            }
            txn.insertElement(props);
          }
        });
      } finally {
        db.close();
      }
    } finally {
      fs.rmSync(schemaFileName, { force: true });
    }
  },
  async applySourceChangesets() {},
  async validate(db, context) {
    const { holderCount, referenceCount, targetCount } = context.parameters;
    const actual = {
      fullyPopulatedHolders: await queryCount(
        db,
        populatedReferenceCountQuery(referenceCount)
      ),
      holders: await queryCount(
        db,
        `SELECT count(*) cnt FROM ${ignoreReferenceSchemaName}.RefHolder`
      ),
      targets: await queryCount(
        db,
        "SELECT count(*) cnt FROM Generic.PhysicalObject"
      ),
    };
    const expected = {
      fullyPopulatedHolders: holderCount,
      holders: holderCount,
      targets: targetCount,
    };
    if (JSON.stringify(actual) !== JSON.stringify(expected))
      throw new Error(
        `Ignore-reference fixture distribution mismatch: expected=${JSON.stringify(
          expected
        )}, actual=${JSON.stringify(actual)}`
      );
  },
});

export const ignoreReferenceTransformFixture = configureFixture(
  ignoreReferenceTransformRecipe,
  {
    id: "ignore-reference-transform",
    version: 1,
    label: "ignore-mode navigation-reference full transformation",
    scenarioClaims: ["ignore-mode navigation-reference full transformation"],
    topology: "standalone-source-and-empty-target",
    seed: 32452843,
    parameters: {
      holderCount: 1000,
      referenceCount: ignoreReferenceNavigationPropertyCount,
      targetCount: 3000,
    },
  }
);

export const ignoreReferenceTransformDescriptor =
  ignoreReferenceTransformFixture.descriptor;
