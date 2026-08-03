/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import {
  balancedIncrementalDescriptor,
  dynamicSchemaUnionMediumDescriptor,
  getFixtureDescriptor,
} from "../../src/catalogs/FixtureCatalog.js";
import { validateFixtureDescriptor } from "../../src/fixtures/FixtureDescriptor.js";
import { dynamicSchemaUnionScenario } from "../../src/scenarios/dynamicSchemaUnion.js";
import { assertScenarioSupportsFixture } from "../../src/framework/BenchmarkResolution.js";

describe("dynamic-schema-union-medium fixture", () => {
  it("resolves by id and validates as a well-shaped descriptor", () => {
    const descriptor = getFixtureDescriptor("dynamic-schema-union-medium");
    expect(descriptor).to.equal(dynamicSchemaUnionMediumDescriptor);
    expect(validateFixtureDescriptor(descriptor)).to.equal(descriptor);
  });

  it("uses the snapshot-schema-pair topology and advertises only the needed claim", () => {
    expect(dynamicSchemaUnionMediumDescriptor.layout.topology).to.equal(
      "snapshot-schema-pair"
    );
    expect(dynamicSchemaUnionMediumDescriptor.scenarioClaims).to.deep.equal([
      "dynamic schema union",
    ]);
  });

  it("declares zero element, aspect, and relationship operation counts", () => {
    const { base, operations } =
      dynamicSchemaUnionMediumDescriptor.distribution;
    expect(base).to.deep.equal({
      aspects: 0,
      elements: 0,
      geometricElements: 0,
      relationships: 0,
    });
    expect(operations).to.deep.equal({
      aspects: { deletes: 0, inserts: 0, updates: 0 },
      elements: { deletes: 0, inserts: 0, updates: 0 },
      relationships: { deletes: 0, inserts: 0, updates: 0 },
      geometryUpdates: 0,
      sourceChangesets: 0,
    });
  });

  it("satisfies the dynamic-schema-union scenario's declared capabilities", () => {
    expect(() =>
      assertScenarioSupportsFixture(
        dynamicSchemaUnionScenario,
        dynamicSchemaUnionMediumDescriptor
      )
    ).to.not.throw();
  });

  it("has a recipe hash independent of the unrelated balanced-incremental fixture", () => {
    expect(dynamicSchemaUnionMediumDescriptor.recipeHash).to.not.equal(
      balancedIncrementalDescriptor.recipeHash
    );
  });
});
