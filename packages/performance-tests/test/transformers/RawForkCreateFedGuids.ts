import { initializeBranchProvenance } from "@itwin/imodel-transformer";
import { TestTransformerModule, TransformRunner } from "../TestTransformerNodule";

const rawForkCreateFedGuidsTestModule: TestTransformerModule = {
  async createForkInitTransform(sourceDb, targetDb): Promise<TransformRunner> {
    return {
      async run() {
        await initializeBranchProvenance({
          master: sourceDb,
          branch: targetDb,
          createFedGuidsForMaster: true,
        });
      },
    };
  },
};

export default rawForkCreateFedGuidsTestModule;
