/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { IModelTransformer } from "@itwin/imodel-transformer";
import { Logger } from "@itwin/core-bentley";
import { Relationship, Element, IModelDb, SnapshotDb } from "@itwin/core-backend";
import { TestTransformerModule } from "./TestTransformerNodule"
import path from "path";
const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

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


const progressTransformerTestModule: TestTransformerModule = {
    async createIdentityTransform(sourceDb: IModelDb, targetDb: IModelDb): Promise<IModelTransformer>{
      const transformer = new ProgressTransformer(sourceDb, targetDb);
      return transformer;
    },
    async createForkInitTransform(sourceDb: IModelDb, targetDb: IModelDb): Promise<IModelTransformer>{
        const transformer = new ProgressTransformer(sourceDb, targetDb);
        return transformer;
    }
}

export default progressTransformerTestModule;