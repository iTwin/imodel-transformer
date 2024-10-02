# Example Transformation for sheets

## Overview

This example shows how to transform a sheet and all its content into a target iModel using the `SheetTransformer` classes. The goal of this transformation was to copy a 2d sheet and its contents from a bim file to another iModel using the `@itwin/imodel-transformer` client.

## Example Code

```typescript

import {
  Element,
  IModelDb,
  Sheet,
  SheetModel,
  SnapshotDb,
} from "@itwin/core-backend";
import { DbResult, Guid, Id64String } from "@itwin/core-bentley";
import {
  Code,
  ElementProps,
  GeometricModel2dProps,
  IModel,
  RelatedElement,
  SheetProps,
} from "@itwin/core-common";
import {
  IModelImporter,
  IModelTransformer,
} from "@itwin/imodel-transformer";

import { CreateSheetProps, SHEET_CONSTANTS } from "../../common/SheetCommandIpc";
import { logError } from "../util/ErrorUtility";
import { DPChannelApi } from "./DPChannelApi";

export namespace SheetApi {

  export const insertSheet = async (sheetName: string, createSheetProps: CreateSheetProps): Promise<Id64String> => {
    const iModel: IModelDb | undefined = StudioHost.getActiveBriefcase();
    let seedDb: SnapshotDb | undefined;
    let transformer: IModelTransformer | undefined;
    try {
      if (!iModel) {
        throw new Error("iModelDb undefined");
      }
      if (!sheetName) {
        throw new Error("A sheet must be named.");
      }

      // create a blank sheetModelId(where we will insert the sheet data), create documentListModel (where we will insert list of document elements)
      const [sheetModelId, documentListModelId] = await createSheetInternal(createSheetProps, iModel, sheetName);
      const seedFileName =
        "D:\\testmodels\\transformingSheetsIssue\\source.bim";
      seedDb = SnapshotDb.openFile(seedFileName);
      if (!seedDb) {
        throw new Error("Failed to open snapshot iModel.");
      }

      // Get the sheet data from the snapshot (this will contain the sheet data)
      const arr: any = [];
      const query = "SELECT * FROM BisCore.Sheet";
      seedDb.withPreparedStatement(query, (statement) => {
        while (statement.step() === DbResult.BE_SQLITE_ROW) {
          const row = statement.getRow();
          arr.push(row);
        }
      });

      const importer = new IModelImporter(iModel);
      importer.doNotUpdateElementIds.add(documentListModelId); // Do not update the documentListModelId, this is the one we've created for this iModel to receive the sheet template.
      transformer = new SheetTransformer(seedDb, importer, arr[0].id, sheetName);

      // bring all data from this source model into document list model
      transformer.context.remapElement("0x20000000009", documentListModelId);

      // bring all data(drawing graphics, line styles, etc) in arr[0] to the blank sheetModel
      transformer.context.remapElement(arr[0].id, sheetModelId);

      // export contents and sub-models to the target iModel
      await transformer.processModel("0x20000000009");

      // Save changes to DB
      iModel.saveChanges();

      return sheetModelId;
    } catch (error) {
      iModel?.abandonChanges();
      const updatedErrMsg = logError(error, "Inserting sheet failed.");
      throw new Error(updatedErrMsg);
    } finally {
      if (seedDb) {
        seedDb.close();
      }
      if (transformer) {
        transformer.dispose();
      }
    }
  };

  class SheetTransformer extends IModelTransformer {
    private _sheetIdInSource: Id64String;
    private _sheetName: string;
    public constructor(sourceDb: IModelDb, target: IModelImporter, sheetIdInSource: Id64String, sheetName: string) {
      super(sourceDb, target, { noProvenance: true });
      this._sheetIdInSource = sheetIdInSource;
      this._sheetName = sheetName;
    }

    // Override to add sheet userLabel, code, federationGuid to the target element
    public override onTransformElement(sourceElement: Element): ElementProps {
      const targetElementProps = super.onTransformElement(sourceElement);
      
      // Add userLabel, code, and federationGuid information for target props
      if (sourceElement instanceof Sheet && sourceElement.id === this._sheetIdInSource) {
        targetElementProps.userLabel = this._sheetName;
        targetElementProps.code = Sheet.createCode(this.targetDb, targetElementProps.model, this._sheetName);
      }
      targetElementProps.federationGuid = Guid.createValue(); // We want each element to have a new federation guid, so that they are not considered the same as the source elements.

      return targetElementProps;
    }
  }

   async function createSheetInternal(createSheetProps: CreateSheetProps, iModel: IModelDb, sheetName: string) {
    if (createSheetProps.scale <= 0) {
      throw new Error("A sheet's scale must be greater than 0.");
    }
    if (createSheetProps.height <= 0 || createSheetProps.width <= 0) {
      throw new Error("A sheet's height and width must be greater than 0.");
    }

    // Get or create documentListModel
    const documentListModelId = await DPChannelApi.getOrCreateDocumentList(SHEET_CONSTANTS.documentListName);

    // Acquire locks and create sheet
    await iModel.locks.acquireLocks({ shared: documentListModelId });

    // insert sheet element into iModel
    const sheetElementProps: SheetProps = {
      ...createSheetProps,
      classFullName: Sheet.classFullName,
      code: Sheet.createCode(iModel, documentListModelId, sheetName),
      model: documentListModelId
    };
    const sheetElementId = iModel.elements.insertElement(sheetElementProps);

    // insert sheet model into iModel
    const sheetModelProps: GeometricModel2dProps = {
      classFullName: SheetModel.classFullName,
      modeledElement: { id: sheetElementId, relClassName: "BisCore:ModelModelsElement" } as RelatedElement
    };
    const sheetModelId = iModel.models.insertModel(sheetModelProps);
    
    return [sheetModelId, documentListModelId];
  }
}
