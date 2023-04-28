/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

/*
 * Tests where we perform "identity" transforms, that is just rebuilding an entire identical iModel (minus IDs)
 * through the transformation process.
 */

import "./setup";
import { assert } from "chai";
import * as path from "path";
import * as fs from "fs";
import { Element, IModelHost, IModelHostConfiguration, Relationship, SnapshotDb } from "@itwin/core-backend";
import { Logger, LogLevel, PromiseReturnType, StopWatch } from "@itwin/core-bentley";
import { IModelTransformer, TransformerLoggerCategory } from "@itwin/imodel-transformer";
//import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import { getTestIModels } from "./TestContext";
import { NodeCliAuthorizationClient } from "@itwin/node-cli-authorization";
import { BackendIModelsAccess } from "@itwin/imodels-access-backend";
import { IModelsClient } from "@itwin/imodels-client-authoring";
import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import dotenv from "dotenv";

const loggerCategory = "Transformer Performance Tests Identity";
const assetsDir = path.join(__dirname, "assets");
const outputDir = path.join(__dirname, ".output");

describe("imodel-transformer", () => {

  before(async () => {
    const logLevel = +process.env.LOG_LEVEL!;
    if (LogLevel[logLevel] !== undefined) {
      Logger.initializeToConsole();
      Logger.setLevelDefault(LogLevel.Error);
      Logger.setLevel(loggerCategory, LogLevel.Info);
      Logger.setLevel(TransformerLoggerCategory.IModelExporter, logLevel);
      Logger.setLevel(TransformerLoggerCategory.IModelImporter, logLevel);
      Logger.setLevel(TransformerLoggerCategory.IModelTransformer, logLevel);
    }
    var usrEmail;
    var usrPass;
    if(process.env.V2_CHECKPOINT_USER_NAME !== "undefined"){
      console.log('v2 check');
      usrEmail = process.env.V2_CHECKPOINT_USER_NAME;
      usrPass = process.env.V2_CHECKPOINT_USER_PASSWORD;
    }
    else if(process.env.V1_CHECKPOINT_USER_NAME !== "undefined"){
      console.log('v1 check');
      usrEmail = process.env.V1_CHECKPOINT_USER_NAME;
      usrPass = process.env.V1_CHECKPOINT_USER_PASSWORD;
    }
    else {
      console.log('imodel user');
      usrEmail = process.env.IMODEL_USER_NAME;
      usrPass = process.env.IMODEL_USER_PASSWORD;
    }
    assert(usrEmail, "user name was not configured");
    assert(usrPass, "user password was not configured");
    const user = {
      email: usrEmail,
      password: usrPass,
    };

    assert(process.env.OIDC_CLIENT_ID, "");
    assert(process.env.OIDC_REDIRECT, "");

    const authClient = process.env.CI === '1'
      ? new TestBrowserAuthorizationClient({
          clientId: process.env.OIDC_CLIENT_ID,
          redirectUri: process.env.OIDC_REDIRECT,
          scope: "imodelaccess:read storage:modify realitydata:read imodels:read library:read imodels:modify realitydata:modify savedviews:read storage:read library:modify itwinjs savedviews:modify",
          authority: "https://qa-imsoidc.bentley.com"
        }, user)
      : new NodeCliAuthorizationClient({
          clientId: process.env.OIDC_CLIENT_ID,
          redirectUri: process.env.OIDC_REDIRECT,
          scope: "imodelaccess:read storage:modify realitydata:read imodels:read library:read imodels:modify realitydata:modify savedviews:read storage:read library:modify itwinjs savedviews:modify",
        });

      await authClient.signIn();
  
    

    const hostConfig  = new IModelHostConfiguration();
    hostConfig.authorizationClient = authClient;
    const hubClient = new IModelsClient({ api: { baseUrl: `https://${process.env.IMJS_URL_PREFIX}api.bentley.com/imodels` } });
    hostConfig.hubAccess = new BackendIModelsAccess(hubClient);
    await IModelHost.startup(hostConfig);
  });

  after(async () => {
    //await fs.promises.rm(outputDir);
    await IModelHost.shutdown();
  });
  it("identity transform all", async () => {
    const report = [] as Record<string, string | number>[];
    for await (const iModel of getTestIModels()) {
      //if (iModel.tShirtSize !== "m") continue;
      Logger.logInfo(loggerCategory, `processing iModel '${iModel.name}' of size '${iModel.tShirtSize.toUpperCase()}'`);
      const sourceDb = await iModel.load();
      const toGb = (bytes: number) => `${(bytes / 1024 **3).toFixed(2)}Gb`;
      const sizeInGb = toGb(fs.statSync(sourceDb.pathName).size);
      Logger.logInfo(loggerCategory, `loaded (${sizeInGb})'`);
      const targetPath = initOutputFile(`${iModel.iTwinId}-${iModel.name}-target.bim`);
      const targetDb = SnapshotDb.createEmpty(targetPath, {rootSubject: {name: iModel.name}});
      class ProgressTransformer extends IModelTransformer {
        private _count = 0;
        private _increment() {
          this._count++;
          if (this._count % 1000 === 0) Logger.logInfo(loggerCategory, `exported ${this._count} entities`);
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
      const transformer = new ProgressTransformer(sourceDb, targetDb);
      let schemaProcessingTimer: StopWatch | undefined;
      let entityProcessingTimer: StopWatch | undefined;
      try {
        [schemaProcessingTimer] = await timed(async () => {
          await transformer.processSchemas();
        });
        Logger.logInfo(loggerCategory, `schema processing time: ${schemaProcessingTimer.elapsedSeconds}`);
        //[entityProcessingTimer] = await timed(async () => {
          //await transformer.processAll();
        //});
        //Logger.logInfo(loggerCategory, `entity processing time: ${entityProcessingTimer.elapsedSeconds}`);
      } catch (err: any) {
        Logger.logInfo(loggerCategory, `An error was encountered: ${err.message}`);
        const schemaDumpDir = fs.mkdtempSync("/tmp/identity-test-schemas-dump");
        sourceDb.nativeDb.exportSchemas(schemaDumpDir);
        Logger.logInfo(loggerCategory, `dumped schemas to: ${schemaDumpDir}`);
      } finally {
        const os = require('os');
        const record = {
          /* eslint-disable @typescript-eslint/naming-convention */
          "Name": iModel.name,
          "Id": iModel.iModelId,
          "T-shirt size": iModel.tShirtSize,
          "Size (Gb)": sizeInGb,
          "Schema processing time": schemaProcessingTimer?.elapsedSeconds ?? "N/A",
          "Entity processing time": entityProcessingTimer?.elapsedSeconds ?? "N/A",
          /* eslint-enable @typescript-eslint/naming-convention */
        };
        report.push(record);
        targetDb.close();
        sourceDb.close();
        transformer.dispose();
        fs.appendFileSync(path.join(os.tmpdir(), "report.jsonl"), `${JSON.stringify(record)}\n`);
      }
    }
  });
});

function initOutputFile(fileBaseName: string) {
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }
  const outputFileName = path.join(outputDir, fileBaseName);
  if (fs.existsSync(outputFileName)) {
    fs.unlinkSync(outputFileName);
  }
  return outputFileName;
}

function timed<F extends (() => any) | (() => Promise<any>)>(
  f: F
): [StopWatch, ReturnType<F>] | Promise<[StopWatch, PromiseReturnType<F>]> {
  const stopwatch = new StopWatch();
  stopwatch.start();
  const result = f();
  if (result instanceof Promise) {
    return result.then<[StopWatch, PromiseReturnType<F>]>((innerResult) => {
      stopwatch.stop();
      return [stopwatch, innerResult];
    });
  } else {
    stopwatch.stop();
    return [stopwatch, result];
  }
}
