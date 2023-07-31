import { initializeBranchProvenance } from "@itwin/imodel-transformer";
import { TestTransformerModule, TransformRunner } from "../TestTransformerNodule";

const rawForkOperationsTestModule: TestTransformerModule = {
  async createForkInitTransform(sourceDb, targetDb): Promise<TransformRunner> {
    return {
      async run() {
        await initializeBranchProvenance({ master: sourceDb, branch: targetDb });
      },
    };
  },
};

export default rawForkOperationsTestModule;
