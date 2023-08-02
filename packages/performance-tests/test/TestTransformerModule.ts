import { IModelDb } from "@itwin/core-backend";

export interface TransformRunner { run: () => Promise<void> }
/**
 * the type of a module imported by the tests holding a custom transformer
 * implementation to test
 */
export interface TestTransformerModule {
  createIdentityTransform?(sourceDb: IModelDb, targetDb: IModelDb): Promise<TransformRunner>;
  createForkInitTransform?(sourceDb: IModelDb, targetDb: IModelDb): Promise<TransformRunner>;
}
