/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { TestTransformerModule, TransformRunner } from "../TestTransformerModule";
import { rawEmulatedPolymorphicInsertTransform } from "@itwin/imodel-transformer/lib/cjs/JsPolymorphicInserter";

const noPlatformTestModule: TestTransformerModule = {
  async createIdentityTransform(sourceDb, targetDb): Promise<TransformRunner> {
    return {
      async run() {
        rawEmulatedPolymorphicInsertTransform(sourceDb, targetDb);
      },
    };
  },
};

export default noPlatformTestModule;
