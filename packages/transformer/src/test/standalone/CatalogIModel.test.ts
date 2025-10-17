/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/* eslint-disable no-console */
import {
  BriefcaseDb,
  BriefcaseManager,
  IModelDb,
  IModelHost,
  IModelJsFs,
  SnapshotDb,
  StandaloneDb,
} from "@itwin/core-backend";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import * as path from "path";
import { IModelTestUtils } from "../TestUtils";
import { OpenMode } from "@itwin/core-bentley";

function initOutputFile(fileBaseName: string) {
  const outputDirName = path.join(__dirname, "output");
  if (!IModelJsFs.existsSync(outputDirName)) {
    IModelJsFs.mkdirSync(outputDirName);
  }
  const outputFileName = path.join(outputDirName, fileBaseName);
  if (IModelJsFs.existsSync(outputFileName)) {
    IModelJsFs.removeSync(outputFileName);
  }
  return outputFileName;
}

async function cloneDb(src: IModelDb, targetFileName: string) {
  const targetDbFileName = initOutputFile(`${targetFileName}.bim`);
  const targetDb = SnapshotDb.createEmpty(targetDbFileName, {
    rootSubject: { name: `${src.rootSubject.name}-clone` },
  });

  const transformer = new IModelTransformer(src, targetDb, {
    loadSourceGeometry: true,
    noProvenance: true,
  });

  await transformer.processSchemas();
  await transformer.process();
  transformer.dispose();

  targetDb.saveChanges("cloneDb");
  return targetDb;
}

describe("Substation Catalog Perf tests", () => {
  it.only("test perf time of process catalog db into empty imodel", async function () {
    const sourceDbPath = IModelTestUtils.resolveAssetFile(
      "SubstationTestBim/OpenAIStargateCat.bim"
    );
    const sourceDb = SnapshotDb.openFile(sourceDbPath);

    const dbFileName = initOutputFile("catalog-copy-v5.bim");
    const targetDb = SnapshotDb.createEmpty(dbFileName, {
      rootSubject: { name: dbFileName },
    });

    const transform = new IModelTransformer(sourceDb, targetDb);

    const elementFedGuid = sourceDb.elements.getElement("0x592").federationGuid;

    await transform.processSchemas();
    console.time("Transformation Process");
    // console.profile();
    await transform.processElement("1426");
    // console.profileEnd();
    // await transform.process();
    console.timeEnd("Transformation Process");
    targetDb.saveChanges("clone contents from source");

    const element = targetDb.elements.getElement(elementFedGuid!);
    console.log(element.classFullName);

    targetDb.close();
    sourceDb.close();
    transform.dispose();
  });

  it.only("perf test process element into asset db", async function () {
    const sourceDbPath = IModelTestUtils.resolveAssetFile(
      "SubstationTestBim/OpenAIStargateCat.bim"
    );
    const sourceDb = SnapshotDb.openFile(sourceDbPath);

    const localDbPath = IModelTestUtils.resolveAssetFile(
      "SubstationTestBim/localIModel.bim"
    );
    const localTarget = SnapshotDb.openFile(localDbPath);

    const targetDb = await cloneDb(localTarget, "localIModel-copy"); // clone db so I don't need to discard changes. maybe I can just discard?
    localTarget.close();
    console.log("target db cloned");

    const elementFedGuid = sourceDb.elements.getElement("0x592").federationGuid;
    const targetScopeElementId = sourceDb.elements.getElement("0x592").id;
    const options: IModelTransformOptions = {
      targetScopeElementId,
      noProvenance: targetScopeElementId ? undefined : true, // can't store provenance if targetScopeElementId is not defined
      // loadSourceGeometry: true,
    };

    const transform = new IModelTransformer(sourceDb, targetDb, options);

    // await transform.processSchemas();
    console.time("Transformation Process");
    // console.profile();
    await transform.processElement(
      sourceDb.elements.getElement(elementFedGuid!).id
    );
    // console.profileEnd();
    // await transform.process();
    console.timeEnd("Transformation Process");
    targetDb.saveChanges("clone contents from source");

    const element = targetDb.elements.getElement(elementFedGuid!);
    console.log(element.classFullName);
    console.log(element.id);

    targetDb.close();
    sourceDb.close();
    transform.dispose();
  });

  it.only("perf test process element into asset briefcase db", async function () {
    const sourceDbPath = IModelTestUtils.resolveAssetFile(
      "SubstationTestBim/OpenAIStargateCat.bim"
    );
    const sourceDb = SnapshotDb.openFile(sourceDbPath);

    const localDbPath = IModelTestUtils.resolveAssetFile(
      "SubstationTestBim/Substation_DataCenter_V6_B.bim"
    );
    const targetDb = StandaloneDb.openFile(localDbPath);
    // const targetDb = await BriefcaseDb.open({
    //   fileName: localDbPath,
    //   readonly: false,
    // });

    // console.time("Clone DB");
    // const targetDb = await cloneDb(localTarget, "localIModel-copy"); // clone db so I don't need to discard changes. maybe I can just discard?
    // console.timeEnd("Clone DB");
    // localTarget.close();
    // console.log("target db cloned");

    const elementFedGuid = sourceDb.elements.getElement("0x592").federationGuid;
    const targetScopeElementId = sourceDb.elements.getElement("0x592").id;
    // await targetDb.locks.acquireLocks({ exclusive: targetScopeElementId })
    const options: IModelTransformOptions = {
      targetScopeElementId,
      noProvenance: targetScopeElementId ? undefined : true, // can't store provenance if targetScopeElementId is not defined
    };

    const transform = new IModelTransformer(sourceDb, targetDb, options);

    // await transform.processSchemas();
    console.time("Transformation Process");
    // console.profile();
    await transform.processElement("0x592");
    // console.profileEnd();
    // await transform.process();
    console.timeEnd("Transformation Process");
    // targetDb.saveChanges("clone contents from source");
    // await targetDb.locks.releaseAllLocks();

    const element = targetDb.elements.getElement(elementFedGuid!);
    console.log(element.classFullName);
    console.log(element.id);

    targetDb.abandonChanges();

    targetDb.close();
    sourceDb.close();
    transform.dispose();
  });
});
