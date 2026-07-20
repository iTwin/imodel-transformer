/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { ITwinError } from "@itwin/core-bentley";
import { version as iTwinCoreBackendVersion } from "@itwin/core-backend/package.json";
import { assert, expect } from "chai";
import * as semver from "semver";
import * as sinon from "sinon";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../../IModelTransformerError";

// The entry point performs the peer-dependency check as a module-load side effect.
describe("package entry point", () => {
  it("identifies an incompatible iTwin.js dependency", () => {
    const noStrictCheck = "TRANSFORMER_NO_STRICT_DEP_CHECK";
    const suggestVersions = "SUGGEST_TRANSFORMER_VERSIONS";
    const previousNoStrictCheck = process.env[noStrictCheck];
    const previousSuggestVersions = process.env[suggestVersions];
    const entryPointPath = require.resolve("../../imodel-transformer");
    const cachedEntryPoint = require.cache[entryPointPath];
    const satisfiesStub = sinon.stub(semver, "satisfies").returns(false);
    // eslint-disable-next-line @typescript-eslint/no-var-requires, @typescript-eslint/no-require-imports
    const transformerPackage = require("../../../../package.json") as {
      name: string;
      version: string;
      peerDependencies: Record<string, string>;
    };

    delete process.env[noStrictCheck];
    delete process.env[suggestVersions];
    delete require.cache[entryPointPath];

    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires, @typescript-eslint/no-require-imports
      require(entryPointPath);
      assert.fail("Expected the package entry point to throw");
    } catch (error) {
      expect(
        ITwinError.isError(
          error,
          IModelTransformerErrorScope,
          IModelTransformerError.DependencyVersionMismatch
        )
      ).to.be.true;
      expect(error).to.have.property(
        "message",
        `${transformerPackage.name}@${transformerPackage.version} only supports @itwin/core-backend@${transformerPackage.peerDependencies["@itwin/core-backend"]}, but @itwin/core-backend${iTwinCoreBackendVersion} was resolved when looking for the peer dependency.\nIf you know exactly what you are doing, you can disable this check by setting ${noStrictCheck}=1 in the environment\nYou can rerun with the environment variable ${suggestVersions}=1 to have this error suggest a version`
      );
    } finally {
      satisfiesStub.restore();
      delete require.cache[entryPointPath];
      if (cachedEntryPoint !== undefined)
        require.cache[entryPointPath] = cachedEntryPoint;
      if (previousNoStrictCheck === undefined)
        delete process.env[noStrictCheck];
      else process.env[noStrictCheck] = previousNoStrictCheck;
      if (previousSuggestVersions === undefined)
        delete process.env[suggestVersions];
      else process.env[suggestVersions] = previousSuggestVersions;
    }
  });
});
