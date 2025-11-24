/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  Code,
  ExternalSourceProps,
  RepositoryLinkProps,
} from "@itwin/core-common";
import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ExternalSource,
  ExternalSourceIsInRepository,
  IModelDb,
  Relationship,
  RepositoryLink,
} from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";
import { Logger } from "@itwin/core-bentley";
import {
  TestTransformerModule,
  TransformRunner,
} from "../TestTransformerModule";

const loggerCategory = "Transformer Performance Tests Identity";

class ProgressTransformer extends IModelTransformer {
  private _count = 0;
  private _increment() {
    this._count++;
    if (this._count % 1000 === 0)
      Logger.logInfo(loggerCategory, `exported ${this._count} entities`);
  }
  public override onExportElement(sourceElement: Element): void {
    this._increment();
    return super.onExportElement(sourceElement);
  }
  public override onExportRelationship(sourceRelationship: Relationship): void {
    this._increment();
    return super.onExportRelationship(sourceRelationship);
  }
}

const nativeTransformerTestModule: TestTransformerModule = {
  async createIdentityTransform(
    sourceDb: IModelDb,
    targetDb: IModelDb
  ): Promise<TransformRunner> {
    const transformer = new ProgressTransformer(sourceDb, targetDb);
    return {
      async run() {
        await transformer.processSchemas();
        await transformer.process();
        transformer.dispose();
      },
    };
  },
  async createForkInitTransform(
    sourceDb: IModelDb,
    targetDb: IModelDb
  ): Promise<TransformRunner> {
    // create an external source and owning repository link to use as our *Target Scope Element* for future synchronizations
    const masterLinkRepoId = targetDb.elements.insertElement({
      classFullName: RepositoryLink.classFullName,
      code: RepositoryLink.createCode(
        targetDb,
        IModelDb.repositoryModelId,
        "test-imodel"
      ),
      model: IModelDb.repositoryModelId,
      // url: "https://wherever-you-got-your-imodel.net",
      format: "iModel",
      repositoryGuid: sourceDb.iModelId,
      description: "master iModel repository",
    } as RepositoryLinkProps);

    const masterExternalSourceId = targetDb.elements.insertElement({
      classFullName: ExternalSource.classFullName,
      model: IModelDb.rootSubjectId,
      code: Code.createEmpty(),
      repository: new ExternalSourceIsInRepository(masterLinkRepoId),
      connectorName: "iModel Transformer",
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      connectorVersion: require("@itwin/imodel-transformer/package.json")
        .version,
    } as ExternalSourceProps);

    const transformer = new ProgressTransformer(sourceDb, targetDb, {
      // tells the transformer that we have a raw copy of a source and the target should receive
      // provenance from the source that is necessary for performing synchronizations in the future
      wasSourceIModelCopiedToTarget: true,
      // store the synchronization provenance in the scope of our representation of the external source, master
      targetScopeElementId: masterExternalSourceId,
    });
    return {
      async run() {
        await transformer.process();
        transformer.dispose();
      },
    };
  },
};

export default nativeTransformerTestModule;
