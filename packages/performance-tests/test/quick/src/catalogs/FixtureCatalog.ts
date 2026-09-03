/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  getRegisteredFixture,
  listRegisteredFixtures,
} from "./BenchmarkRegistry.js";
import { FixtureDescriptor } from "../fixtures/FixtureDescriptor.js";
import { ConfiguredFixture } from "../fixtures/FixtureRecipe.js";

export function listFixtureIds(): string[] {
  return listRegisteredFixtures().map((fixture) => fixture.descriptor.id);
}

export function getConfiguredFixture(id: string): ConfiguredFixture {
  const fixture = getRegisteredFixture(id);
  if (!fixture)
    throw new Error(
      `Unknown quick performance fixture "${id}". Available fixtures: ${listFixtureIds().join(
        ", "
      )}`
    );
  return fixture;
}

export function getFixtureDescriptor(id: string): FixtureDescriptor {
  return getConfiguredFixture(id).descriptor;
}
