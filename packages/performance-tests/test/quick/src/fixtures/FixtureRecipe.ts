/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { AccessToken } from "@itwin/core-bentley";
import { BriefcaseDb } from "@itwin/core-backend";
import { FixtureDescriptor } from "./FixtureDescriptor.js";
import {
  applyBalancedChangesets,
  BalancedRecipeState,
  createBalancedSeed,
} from "./recipes/balancedIncremental.js";
import { dynamicSchemaUnionRecipe } from "./recipes/dynamicSchemaUnion.js";

/**
 * A changeset recipe produces the *change mix* for a `source-and-empty-target` or `source-only`
 * fixture: it seeds the source iModel and then applies a deterministic series of pushed
 * changesets. It never touches HubMock; the fixture provider owns the hub lifecycle.
 *
 * `TArtifactData` is anything the recipe must tell the scenario that cannot be recovered from the
 * artifact afterwards — most importantly the exact ids it operated on. Deleted ids are gone from
 * the tip-pinned briefcase, and deriving them from the changeset files would be circular for a
 * scenario whose job is to verify those same files. Stage 1 captures the returned value once, so
 * every sample and every A/B arm reads byte-identical expectations.
 */
export interface ChangesetFixtureRecipe<
  TState = unknown,
  TArtifactData = unknown,
> {
  readonly kind: "changeset";
  readonly id: string;
  /** Create the source seed file. Returns state carried into {@link applySourceChangesets}. */
  createSeed(fileName: string, descriptor: FixtureDescriptor): Promise<TState>;
  /**
   * Apply and push the recipe's changesets to an open source briefcase.
   *
   * Any returned value is serialized into the artifact as `recipe.json` and surfaced to the
   * scenario as `PreparedDetachedDataset.recipe`. It must round-trip through JSON; returning
   * nothing is the normal case.
   */
  applySourceChangesets(
    db: BriefcaseDb,
    accessToken: AccessToken,
    descriptor: FixtureDescriptor,
    state: TState
  ): Promise<TArtifactData | void>;
}

/** What a schema-pair recipe hands the provider: the two schemas plus deterministic expectations. */
export interface SchemaPairSeed<TExpectation = unknown> {
  readonly sourceSchemaXml: string;
  readonly targetSchemaXml: string;
  /**
   * Deterministic facts about the schemas that the scenario needs for correctness validation
   * (class names, property names, versions, references) but cannot cheaply recompute from the
   * imported schema alone.
   */
  readonly expectation: TExpectation;
}

/**
 * A schema-pair recipe produces two already-divergent, deterministic EC schemas for a
 * `snapshot-schema-pair` fixture: no hub, no briefcases, no changesets. The fixture provider owns
 * creating and populating the local `SnapshotDb`s from the returned schema XML.
 */
export interface SchemaPairFixtureRecipe<TExpectation = unknown> {
  readonly kind: "schema-pair";
  readonly id: string;
  createSchemaPair(
    descriptor: FixtureDescriptor
  ): Promise<SchemaPairSeed<TExpectation>>;
}

export type FixtureRecipe<TState = unknown, TArtifactData = unknown> =
  | ChangesetFixtureRecipe<TState, TArtifactData>
  | SchemaPairFixtureRecipe<TArtifactData>;

export function requireChangesetRecipe(
  recipe: FixtureRecipe<any, any>
): ChangesetFixtureRecipe<any, any> {
  if (recipe.kind !== "changeset")
    throw new Error(
      `Fixture recipe "${recipe.id}" is not a changeset recipe (kind "${recipe.kind}")`
    );
  return recipe;
}

export function requireSchemaPairRecipe(
  recipe: FixtureRecipe<any, any>
): SchemaPairFixtureRecipe<any> {
  if (recipe.kind !== "schema-pair")
    throw new Error(
      `Fixture recipe "${recipe.id}" is not a schema-pair recipe (kind "${recipe.kind}")`
    );
  return recipe;
}

export const balancedIncrementalRecipe: ChangesetFixtureRecipe<BalancedRecipeState> =
  {
    kind: "changeset",
    id: "balanced-incremental",
    createSeed: async (fileName, descriptor) =>
      createBalancedSeed(fileName, descriptor),
    applySourceChangesets: async (db, accessToken, descriptor, state) =>
      applyBalancedChangesets(db, accessToken, descriptor, state),
  };

const recipes = new Map<string, FixtureRecipe<any, any>>([
  [balancedIncrementalRecipe.id, balancedIncrementalRecipe],
  [dynamicSchemaUnionRecipe.id, dynamicSchemaUnionRecipe],
]);

export function registerFixtureRecipe(recipe: FixtureRecipe<any, any>): void {
  if (recipes.has(recipe.id))
    throw new Error(`Duplicate quick performance recipe: ${recipe.id}`);
  recipes.set(recipe.id, recipe);
}

export function getFixtureRecipe(id: string): FixtureRecipe<any, any> {
  const recipe = recipes.get(id);
  if (!recipe)
    throw new Error(
      `Unknown quick performance recipe "${id}". Available recipes: ${[
        ...recipes.keys(),
      ].join(", ")}`
    );
  return recipe;
}
