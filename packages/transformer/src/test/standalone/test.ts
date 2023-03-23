/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { expect } from "chai";
import { IModelHost, SnapshotDb } from "@itwin/core-backend";
import { Logger, LogLevel } from "@itwin/core-bentley";
import { IModelTransformer, TransformerLoggerCategory } from "../../transformer";
import { assertIdentityTransformation, IModelTransformerTestUtils } from "../IModelTransformerUtils";

async function main() {
  await IModelHost.startup();
  if (process.env.LOG) {
    Logger.initializeToConsole();
    Logger.setLevelDefault(LogLevel.Error);
    Logger.setLevel(TransformerLoggerCategory.IModelExporter, LogLevel.Trace);
    Logger.setLevel(TransformerLoggerCategory.IModelImporter, LogLevel.Trace);
    Logger.setLevel(TransformerLoggerCategory.IModelTransformer, LogLevel.Trace);
  }

  const seedDb = SnapshotDb.openFile("/home/mike/work/Juergen.Hofer.Bad.Normals.bim");
  const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "ExhaustiveIdentityTransformSource.bim");
  const sourceDb = SnapshotDb.createFrom(seedDb, sourceDbPath);

  // previously there was a bug where json display properties of models would not be transformed. This should expose that
  const [physicalModelId] = sourceDb.queryEntityIds({ from: "BisCore.PhysicalModel", limit: 1 });
  const physicalModel = sourceDb.models.getModel(physicalModelId);
  physicalModel.jsonProperties.formatter.fmtFlags.linPrec = 100;
  physicalModel.update();

  sourceDb.saveChanges();

  const targetDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "ExhaustiveIdentityTransformTarget.bim");
  const targetDb = SnapshotDb.createEmpty(targetDbPath, { rootSubject: sourceDb.rootSubject });

  const transformer = new IModelTransformer(sourceDb, targetDb);

  await transformer.processSchemas();

  await transformer.processAll();

  targetDb.saveChanges();

  await assertIdentityTransformation(sourceDb, targetDb, transformer, { compareElemGeom: true });

  const physicalModelInTargetId = transformer.context.findTargetElementId(physicalModelId);
  const physicalModelInTarget = targetDb.models.getModel(physicalModelInTargetId);
  expect(physicalModelInTarget.jsonProperties.formatter.fmtFlags.linPrec).to.equal(100);

  seedDb.close();
  sourceDb.close();
  targetDb.close();
}

main().catch(console.error);
