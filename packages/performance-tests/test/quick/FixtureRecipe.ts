/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { AccessToken } from "@itwin/core-bentley";
import { BriefcaseDb } from "@itwin/core-backend";
import { DatasetDescriptor } from "./DatasetDescriptor";
import {
  applyBalancedChangesets,
  BalancedRecipeState,
  createBalancedSeed,
} from "./recipes/balancedIncremental";

/**
 * A recipe produces the *change mix* for a fixture: it seeds the source iModel and then applies a
 * deterministic series of pushed changesets. It never touches HubMock; the fixture provider owns
 * the hub lifecycle.
 */
export interface FixtureRecipe<TState = unknown> {
  readonly id: string;
  /** Create the source seed file. Returns state carried into {@link applySourceChangesets}. */
  createSeed(fileName: string, descriptor: DatasetDescriptor): Promise<TState>;
  /** Apply and push the recipe's changesets to an open source briefcase. */
  applySourceChangesets(
    db: BriefcaseDb,
    accessToken: AccessToken,
    descriptor: DatasetDescriptor,
    state: TState
  ): Promise<void>;
}

export const balancedIncrementalRecipe: FixtureRecipe<BalancedRecipeState> = {
  id: "balanced-incremental",
  createSeed: async (fileName, descriptor) =>
    createBalancedSeed(fileName, descriptor),
  applySourceChangesets: async (db, accessToken, descriptor, state) =>
    applyBalancedChangesets(db, accessToken, descriptor, state),
};

const recipes = new Map<string, FixtureRecipe<any>>([
  [balancedIncrementalRecipe.id, balancedIncrementalRecipe],
]);

export function registerFixtureRecipe(recipe: FixtureRecipe<any>): void {
  if (recipes.has(recipe.id))
    throw new Error(`Duplicate quick performance recipe: ${recipe.id}`);
  recipes.set(recipe.id, recipe);
}

export function getFixtureRecipe(id: string): FixtureRecipe<any> {
  const recipe = recipes.get(id);
  if (!recipe)
    throw new Error(
      `Unknown quick performance recipe "${id}". Available recipes: ${[
        ...recipes.keys(),
      ].join(", ")}`
    );
  return recipe;
}
