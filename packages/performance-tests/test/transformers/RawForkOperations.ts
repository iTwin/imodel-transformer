import { IModelDb } from "@itwin/core-backend";
import { TestTransformerModule, TransformRunner } from "../TestTransformerModule";
import { initializeBranchProvenance } from "@itwin/imodel-transformer";

const rawForkOperationsTestModule: TestTransformerModule = {
  async createForkInitTransform(sourceDb: IModelDb, targetDb: IModelDb): Promise<TransformRunner> {
    return {
      async run() {
        await initializeBranchProvenance({ master: sourceDb, branch: targetDb });
      },
    };
  },
};

export default rawForkOperationsTestModule;
