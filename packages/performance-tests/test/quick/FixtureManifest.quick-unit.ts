/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import { balancedIncrementalDescriptor } from "./FixtureCatalog";
import { canonicalSha256, validateDescriptor } from "./FixtureManifest";

describe("FixtureManifest", () => {
  it("hashes objects independently of key insertion order", () => {
    expect(canonicalSha256({ first: 1, second: 2 })).to.equal(
      canonicalSha256({ second: 2, first: 1 })
    );
  });

  it("validates the catalog descriptor and rejects invalid input", () => {
    expect(validateDescriptor(balancedIncrementalDescriptor)).to.equal(
      balancedIncrementalDescriptor
    );
    expect(() => validateDescriptor({ id: "invalid" })).to.throw(
      "invalid shape"
    );
  });
});
