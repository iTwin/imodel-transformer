/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { FixtureDescriptor } from "../FixtureDescriptor.js";
import { SchemaPairFixtureRecipe } from "../FixtureRecipe.js";

/**
 * Scale constants for the generated dynamic schema pair. Large enough to make differencing and
 * merging measurable, small enough to stay well inside the quick-suite job budget.
 */
export const dynamicSchemaUnionScale = {
  sharedClassCount: 150,
  sourceOnlyClassCount: 60,
  targetOnlyClassCount: 60,
  propertiesPerClass: 4,
} as const;

const schemaName = "QuickPerfDynamic";
const referenceSchemaName = "BisCore";
/**
 * Version constraint written into the generated schema XML's `<ECSchemaReference>`. The actual
 * BisCore version present in a `SnapshotDb.createEmpty()` iModel is a core-backend bootstrap
 * detail this recipe does not control or predict; EC reference resolution only requires this
 * constraint to be compatible with whatever is actually present.
 */
const referenceSchemaVersionXml = "01.00.00";
/** Root read/write version: `DynamicSchemaUnionStrategy` requires these to match exactly. */
const rootReadVersion = 1;
const rootWriteVersion = 0;
const sourceMinorVersion = 12;
const targetMinorVersion = 9;
const expectedMergedMinorVersion =
  Math.max(sourceMinorVersion, targetMinorVersion) + 1;

/**
 * Deterministic facts about the generated dynamic schema pair, computed once by the recipe and
 * carried to the scenario so `finish()` can validate the complete union without recomputing it
 * from the schema text.
 */
export interface DynamicSchemaUnionExpectation {
  readonly schemaName: string;
  /** `Schema.schemaKey.version.toString(false)` form, e.g. `"1.0.13"`. */
  readonly expectedVersion: string;
  readonly propertyNames: readonly string[];
  readonly sharedClassNames: readonly string[];
  readonly sourceOnlyClassNames: readonly string[];
  readonly targetOnlyClassNames: readonly string[];
  readonly referenceSchemaName: string;
}

export function expectDynamicSchemaUnionExpectation(
  value: unknown
): DynamicSchemaUnionExpectation {
  const candidate = value as Partial<DynamicSchemaUnionExpectation> | null;
  if (
    candidate === null ||
    typeof candidate !== "object" ||
    typeof candidate.schemaName !== "string" ||
    typeof candidate.expectedVersion !== "string" ||
    !Array.isArray(candidate.propertyNames) ||
    !Array.isArray(candidate.sharedClassNames) ||
    !Array.isArray(candidate.sourceOnlyClassNames) ||
    !Array.isArray(candidate.targetOnlyClassNames) ||
    typeof candidate.referenceSchemaName !== "string"
  )
    throw new Error(
      "Fixture did not produce a dynamic schema union expectation"
    );
  return candidate as DynamicSchemaUnionExpectation;
}

function classNames(prefix: string, count: number): string[] {
  return Array.from({ length: count }, (_, index) => `${prefix}${index}`);
}

function propertyNames(): string[] {
  return Array.from(
    { length: dynamicSchemaUnionScale.propertiesPerClass },
    (_, index) => `Value${index}`
  );
}

function buildClassXml(name: string, properties: readonly string[]): string {
  const propertyXml = properties
    .map(
      (propertyName) =>
        `      <ECProperty propertyName="${propertyName}" typeName="string"/>`
    )
    .join("\n");
  return `  <ECEntityClass typeName="${name}">
    <BaseClass>bis:PhysicalElement</BaseClass>
${propertyXml}
  </ECEntityClass>`;
}

function buildSchemaXml(
  minor: number,
  includedClassNames: readonly string[]
): string {
  const version = `${String(rootReadVersion).padStart(2, "0")}.${String(
    rootWriteVersion
  ).padStart(2, "0")}.${minor}`;
  const classesXml = includedClassNames
    .map((name) => buildClassXml(name, propertyNames()))
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="${schemaName}" alias="${schemaName.toLowerCase()}" version="${version}" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
  <ECSchemaReference name="${referenceSchemaName}" version="${referenceSchemaVersionXml}" alias="bis"/>
  <ECCustomAttributes>
    <DynamicSchema xmlns="CoreCustomAttributes.01.00.03"/>
  </ECCustomAttributes>
${classesXml}
</ECSchema>`;
}

/**
 * Generates a deterministic, already-divergent dynamic schema pair for the successful union path:
 * shared classes identical on both sides, source-only classes the union must add, and target-only
 * classes the union must preserve. No conflicting property or reference changes.
 */
export const dynamicSchemaUnionRecipe: SchemaPairFixtureRecipe<DynamicSchemaUnionExpectation> =
  {
    kind: "schema-pair",
    id: "dynamic-schema-union",
    async createSchemaPair(_descriptor: FixtureDescriptor) {
      const shared = classNames(
        "Shared",
        dynamicSchemaUnionScale.sharedClassCount
      );
      const sourceOnly = classNames(
        "SourceOnly",
        dynamicSchemaUnionScale.sourceOnlyClassCount
      );
      const targetOnly = classNames(
        "TargetOnly",
        dynamicSchemaUnionScale.targetOnlyClassCount
      );
      const sourceSchemaXml = buildSchemaXml(sourceMinorVersion, [
        ...shared,
        ...sourceOnly,
      ]);
      const targetSchemaXml = buildSchemaXml(targetMinorVersion, [
        ...shared,
        ...targetOnly,
      ]);
      const expectation: DynamicSchemaUnionExpectation = {
        schemaName,
        expectedVersion: `${rootReadVersion}.${rootWriteVersion}.${expectedMergedMinorVersion}`,
        propertyNames: propertyNames(),
        sharedClassNames: shared,
        sourceOnlyClassNames: sourceOnly,
        targetOnlyClassNames: targetOnly,
        referenceSchemaName,
      };
      return { sourceSchemaXml, targetSchemaXml, expectation };
    },
  };
