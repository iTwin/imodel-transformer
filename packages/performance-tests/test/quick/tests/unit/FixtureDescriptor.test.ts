/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import { balancedIncrementalDescriptor } from "../../src/fixtures/recipes/balancedIncremental.js";
import {
  canonicalSha256,
  validateFixtureDescriptor,
} from "../../src/fixtures/FixtureDescriptor.js";

describe("FixtureDescriptor", () => {
  it("hashes objects independently of key insertion order", () => {
    expect(canonicalSha256({ first: 1, second: 2 })).to.equal(
      canonicalSha256({ second: 2, first: 1 })
    );
  });

  it("validates the catalog descriptor and rejects invalid input", () => {
    expect(validateFixtureDescriptor(balancedIncrementalDescriptor)).to.equal(
      balancedIncrementalDescriptor
    );
    expect(() => validateFixtureDescriptor({ id: "invalid" })).to.throw(
      "invalid shape"
    );
  });
});
