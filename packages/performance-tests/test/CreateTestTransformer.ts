import { IModelDb } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";

/**
 * the type of a module imported by the tests holding a custom transformer
 * implementation to test
 */
export interface CreateTestTransformerModule {
  createIdentityTransformer(sourceDb: IModelDb, targetDb: IModelDb): IModelTransformer;
  createForkInitTransformer(sourceDb: IModelDb, targetDb: IModelDb): IModelTransformer;
}
