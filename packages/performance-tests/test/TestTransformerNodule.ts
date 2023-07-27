import { IModelDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";

/**
 * the type of a module imported by the tests holding a custom transformer
 * implementation to test
 */
export interface TestTransformerModule {
  createIdentityTransform?(sourceDb: IModelDb, targetDb: IModelDb): Promise<IModelTransformer>;
  createForkInitTransform?(sourceDb: IModelDb, targetDb: IModelDb): Promise<IModelTransformer>;
}