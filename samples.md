# Example Transformation for sheets

## Overview

This example demonstrates how to transform a sheet and all its content into a target iModel using the `SheetTransformer` and `IModelTarget` classes.

## Example Code

```typescript
import fs from "fs";
import path from "path";
import stream from "stream";
import { promisify } from "util";

import { StudioHost } from "@bentley/studio-apps-backend-api";
import {
  CategorySelector,
  DefinitionContainer,
  DefinitionModel,
  DisplayStyle3d,
  DrawingCategory,
  DrawingModel,
  ECSqlStatement,
  IModelDb,
  IModelHost,
  ModelSelector,
  OrthographicViewDefinition,
  PhysicalModel,
  Sheet,
  SheetModel,
  SnapshotDb,
  SpatialCategory,
  TemplateRecipe2d,
  TemplateRecipe3d
} from "@itwin/core-backend";
import { DbResult, Id64Array, Id64String } from "@itwin/core-bentley";
import {
  BisCodeSpec,
  Code,
  CodeScopeSpec,
  DefinitionElementProps,
  ElementProps,
  GeometricModel2dProps,
  IModel,
  Placement2d,
  Placement3d,
  RelatedElement,
  SheetProps,
  SubCategoryAppearance
} from "@itwin/core-common";
import { Angle, Point2d, Point3d, Range2d, Range3d, StandardViewIndex, YawPitchRollAngles } from "@itwin/core-geometry";
import {
  IModelExporter,
  IModelImporter,
  IModelTransformer,
  IModelTransformOptions,
  TemplateModelCloner
} from "@itwin/imodel-transformer";
import fetch from "node-fetch";

import { CreateSheetProps, SHEET_CONSTANTS } from "../../common/SheetCommandIpc";
import { logError } from "../util/ErrorUtility";
import { DPChannelApi } from "./DPChannelApi";
import { ElementManipApi } from "./ElementManipApi";
import { ModelManipApi } from "./ModelManipApi";
export namespace SheetApi {
  const finished = promisify(stream.finished);
  async function downloadFile(fileUrl: string, outputPath: string): Promise<void> {
    const response = await fetch(fileUrl);
    if (!response.ok) throw new Error(`Failed to fetch ${fileUrl}: ${response.statusText}`);
    const fileStream = fs.createWriteStream(outputPath);
    if (response.body) {
      response.body.pipe(fileStream);
    }
    await finished(fileStream);
  }

  async function downloadDocuments(documents: any, downloadFolder: string): Promise<void> {
    for (const doc of documents.documents) {
      console.log(doc, downloadFolder);
      const fileUrl = doc._links.fileUrl.href;
      const fileName = `${doc.displayName}.${doc.extension}`;
      const outputPath = path.join(downloadFolder, fileName);
      try {
        await downloadFile(fileUrl, outputPath);
        console.log(`Downloaded ${fileName} to ${downloadFolder}`);
      } catch (error) {
        console.error(`Failed to download file: ${fileName}`, error);
      }
    }
  }

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
      // let sheetModelId: Id64String;
      // if (createSheetProps.sheetTemplate === "No Template" || createSheetProps.sheetTemplate === "") {
      const [sheetModelId, documentListModelId] = await createSheetInternal(createSheetProps, iModel, sheetName);
      const seedFileName =
        "D:\\testmodels\\transformingSheetsIssue\\source.bim";
      seedDb = SnapshotDb.openFile(seedFileName);
      if (!seedDb) {
        throw new Error("Failed to open snapshot iModel.");
      }
      // Get the sheet data from the snapshot
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
      transformer = new IModelTransformer(seedDb, importer);

      transformer.context.remapElement("0x20000000009", documentListModelId);
      await transformer.processModel("0x20000000009");

      // Save changes to DB
      iModel.saveChanges();

      const sheetIdInTarget = transformer.context.findTargetElementId(arr[0].id);
      const sheetProps = iModel.elements.getElementProps<SheetProps>(sheetIdInTarget);

      await iModel.locks.acquireLocks({ shared: documentListModelId });
      iModel.elements.updateElement({...sheetProps, userLabel: sheetName, code: Sheet.createCode(iModel, documentListModelId, sheetName)});
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

   async function createSheetInternal(createSheetProps: CreateSheetProps, _iModel: IModelDb, _sheetName: string) {
    if (createSheetProps.scale <= 0) {
      throw new Error("A sheet's scale must be greater than 0.");
    }
    if (createSheetProps.height <= 0 || createSheetProps.width <= 0) {
      throw new Error("A sheet's height and width must be greater than 0.");
    }

    // Get or make documentListModelId
    const documentListModelId = await DPChannelApi.getOrCreateDocumentList(SHEET_CONSTANTS.documentListName);

    return ["", documentListModelId];
  }