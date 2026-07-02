/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { initializeBranchProvenance } from "@itwin/imodel-transformer";
import {
  TestTransformerModule,
  TransformRunner,
} from "../TestTransformerModule";
import { EditTxn } from "@itwin/core-backend";

const rawForkOperationsTestModule: TestTransformerModule = {
  async createForkInitTransform(sourceDb, targetDb): Promise<TransformRunner> {
    return {
      async run() {
        const editTxn = new EditTxn(targetDb, "initializeBranchProvenance");
        editTxn.start();
        await initializeBranchProvenance({
          master: sourceDb,
          branch: targetDb,
          editTxn,
        });
        editTxn.saveChanges();
        editTxn.end();
      },
    };
  },
};

export default rawForkOperationsTestModule;
