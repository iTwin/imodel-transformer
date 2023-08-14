import * as fs from "fs";
import { IModelDb, StandaloneDb } from "@itwin/core-backend";
import { TestTransformerModule, TransformRunner } from "../TestTransformerModule";
import { initOutputFile } from "../TestUtils";
import { initializeBranchProvenance } from "@itwin/imodel-transformer";
import { setToStandalone } from "../iModelUtils";
import path from "path";

const outputDir = path.join(__dirname, ".output");

const rawForkCreateFedGuidsTestModule: TestTransformerModule = {
  async createForkInitTransform(sourceDb: IModelDb, targetDb: IModelDb): Promise<TransformRunner> {
    return {
      async run() {
        const sourceCopy = initOutputFile(`RawForkCreateFedGuids-${sourceDb.name}-target.bim`, outputDir);
        fs.copyFileSync(sourceDb.pathName, sourceCopy);
        setToStandalone(sourceCopy);
        const sourceCopyDb = StandaloneDb.openFile(sourceCopy);
        await initializeBranchProvenance({
          master: sourceCopyDb,
          branch: targetDb,
          createFedGuidsForMaster: true,
        });
      },
    };
  },
};

export default rawForkCreateFedGuidsTestModule;
