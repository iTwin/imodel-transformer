/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import {
  balancedIncrementalRecipe,
  getFixtureRecipe,
  requireChangesetRecipe,
  requireSchemaPairRecipe,
} from "../../src/fixtures/FixtureRecipe.js";
import { dynamicSchemaUnionRecipe } from "../../src/fixtures/recipes/dynamicSchemaUnion.js";

describe("FixtureRecipe kind narrowing", () => {
  it("resolves both registered recipe kinds by id", () => {
    expect(getFixtureRecipe("balanced-incremental")).to.equal(
      balancedIncrementalRecipe
    );
    expect(getFixtureRecipe("dynamic-schema-union")).to.equal(
      dynamicSchemaUnionRecipe
    );
  });

  it("narrows a changeset recipe and rejects a schema-pair recipe", () => {
    expect(
      requireChangesetRecipe(getFixtureRecipe("balanced-incremental"))
    ).to.equal(balancedIncrementalRecipe);
    expect(() =>
      requireChangesetRecipe(getFixtureRecipe("dynamic-schema-union"))
    ).to.throw(
      'Fixture recipe "dynamic-schema-union" is not a changeset recipe (kind "schema-pair")'
    );
  });

  it("narrows a schema-pair recipe and rejects a changeset recipe", () => {
    expect(
      requireSchemaPairRecipe(getFixtureRecipe("dynamic-schema-union"))
    ).to.equal(dynamicSchemaUnionRecipe);
    expect(() =>
      requireSchemaPairRecipe(getFixtureRecipe("balanced-incremental"))
    ).to.throw(
      'Fixture recipe "balanced-incremental" is not a schema-pair recipe (kind "changeset")'
    );
  });

  it("rejects unknown recipe ids with the available set", () => {
    expect(() => getFixtureRecipe("not-a-recipe")).to.throw(
      /Available recipes: balanced-incremental, dynamic-schema-union/
    );
  });
});
