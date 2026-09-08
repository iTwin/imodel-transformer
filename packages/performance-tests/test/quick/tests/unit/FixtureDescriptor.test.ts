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
import { reusableFixtureIdentity } from "../../src/framework/BenchmarkRunner.js";

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

  it("allows runtime dependencies to differ when reusing a configured fixture", () => {
    const baseline = balancedIncrementalDescriptor;
    const candidate = {
      ...baseline,
      generator: {
        ...baseline.generator,
        coreBackend: "different-core",
        node: "different-node",
        transformer: "different-transformer",
      },
      recipeHash: "different-authoring-identity",
    };
    expect(reusableFixtureIdentity(candidate)).to.equal(
      reusableFixtureIdentity(baseline)
    );
    expect(
      reusableFixtureIdentity({
        ...candidate,
        distribution: {
          ...candidate.distribution,
          base: { ...candidate.distribution.base, elements: 1 },
        },
      })
    ).not.to.equal(reusableFixtureIdentity(baseline));
  });
});
