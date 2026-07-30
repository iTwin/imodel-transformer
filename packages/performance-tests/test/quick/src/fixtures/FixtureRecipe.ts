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
import {
  applyScanChangesets,
  createScanSeed,
  ScanRecipeState,
  validateScanFixture,
} from "./recipes/updateHeavyScan.js";
import { assertFixtureDistribution } from "./validation/validateFixture.js";

/**
 * A recipe produces the *change mix* for a fixture: it seeds the source iModel and then applies a
 * deterministic series of pushed changesets. It never touches HubMock; the fixture provider owns
 * the hub lifecycle.
 *
 * `TArtifactData` is anything the recipe must tell the scenario that cannot be recovered from the
 * artifact afterwards — most importantly the exact ids it operated on. Deleted ids are gone from
 * the tip-pinned briefcase, and deriving them from the changeset files would be circular for a
 * scenario whose job is to verify those same files. Stage 1 captures the returned value once, so
 * every sample and every A/B arm reads byte-identical expectations.
 */
export interface FixtureRecipe<TState = unknown, TArtifactData = unknown> {
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
  /**
   * Assert the built source iModel matches what the descriptor promises.
   *
   * Validation is recipe-owned because it queries the classes that recipe created.
   */
  validate(db: BriefcaseDb, descriptor: FixtureDescriptor): Promise<void>;
}

export const balancedIncrementalRecipe: FixtureRecipe<BalancedRecipeState> = {
  id: "balanced-incremental",
  createSeed: async (fileName, descriptor) =>
    createBalancedSeed(fileName, descriptor),
  applySourceChangesets: async (db, accessToken, descriptor, state) =>
    applyBalancedChangesets(db, accessToken, descriptor, state),
  validate: async (db, descriptor) => assertFixtureDistribution(db, descriptor),
};

export const updateHeavyScanRecipe: FixtureRecipe<ScanRecipeState> = {
  id: "update-heavy-scan",
  createSeed: async (fileName, descriptor) =>
    createScanSeed(fileName, descriptor),
  applySourceChangesets: async (db, accessToken, descriptor, state) =>
    applyScanChangesets(db, accessToken, descriptor, state),
  validate: async (db, descriptor) => validateScanFixture(db, descriptor),
};

const recipes = new Map<string, FixtureRecipe<any, any>>([
  [balancedIncrementalRecipe.id, balancedIncrementalRecipe],
  [updateHeavyScanRecipe.id, updateHeavyScanRecipe],
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
