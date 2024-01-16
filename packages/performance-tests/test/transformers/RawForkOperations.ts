/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { initializeBranchProvenance } from "@itwin/imodel-transformer";
import {
  TestTransformerModule,
  TransformRunner,
} from "../TestTransformerModule";

const rawForkOperationsTestModule: TestTransformerModule = {
  async createForkInitTransform(sourceDb, targetDb): Promise<TransformRunner> {
    return {
      async run() {
        await initializeBranchProvenance({
          master: sourceDb,
          branch: targetDb,
        });
      },
    };
  },
};

export default rawForkOperationsTestModule;
