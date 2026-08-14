/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it } from "vitest";
import { getRegressionTestDefinitions } from "../RegressionTestRegistration";
import { TestTransformerModule } from "../TestTransformerModule";

describe("getRegressionTestDefinitions", () => {
  it("registers each supported case and skips unsupported combinations", () => {
    const identityCase = () => "identity";
    const forkCase = () => "fork";
    const allOperations: TestTransformerModule = {
      createIdentityTransform: async () => ({ run: async () => {} }),
      createForkInitTransform: async () => ({ run: async () => {} }),
    };
    const identityOnly: TestTransformerModule = {
      createIdentityTransform: async () => ({ run: async () => {} }),
    };

    const definitions = getRegressionTestDefinitions(
      new Map([
        [
          "identity",
          {
            testCase: identityCase,
            functionNameToValidate: "createIdentityTransform",
          },
        ],
        [
          "fork",
          {
            testCase: forkCase,
            functionNameToValidate: "createForkInitTransform",
          },
        ],
      ]),
      new Map([
        ["all", allOperations],
        ["identity-only", identityOnly],
      ])
    );

    expect(
      definitions.map(({ testCaseName, moduleName }) => [
        testCaseName,
        moduleName,
      ])
    ).toEqual([
      ["identity", "all"],
      ["identity", "identity-only"],
      ["fork", "all"],
    ]);
  });
});
