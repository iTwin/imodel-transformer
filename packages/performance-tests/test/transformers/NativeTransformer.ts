/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  Relationship,
} from "@itwin/core-backend";
import {
  IModelTransformer,
  RelationshipEndpointFederationGuids,
} from "@itwin/imodel-transformer";
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
  public override async onExportElement(sourceElement: Element): Promise<void> {
    this._increment();
    return super.onExportElement(sourceElement);
  }
  public override async onExportRelationship(
    sourceRelationship: Relationship,
    isUpdate?: boolean,
    endpointFedGuids?: RelationshipEndpointFederationGuids
  ): Promise<void> {
    this._increment();
    return super.onExportRelationship(
      sourceRelationship,
      isUpdate,
      endpointFedGuids
    );
  }
}

const nativeTransformerTestModule: TestTransformerModule = {
  async createIdentityTransform(
    sourceDb: IModelDb,
    targetDb: IModelDb
  ): Promise<TransformRunner> {
    const editTxn = new EditTxn(targetDb, "IModelTransformer");
    editTxn.start();
    const transformer = new ProgressTransformer({
      source: sourceDb,
      target: editTxn,
    });
    return {
      async run() {
        await transformer.processSchemas();
        await transformer.process();
        transformer.dispose();
        editTxn.end();
      },
    };
  },
};

export default nativeTransformerTestModule;
