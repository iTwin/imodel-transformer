/* eslint-disable @typescript-eslint/no-var-requires */
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
// import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import { getTestIModels } from "./TestContext";
import { initOutputFile } from "./TestUtils";
import { NodeCliAuthorizationClient } from "@itwin/node-cli-authorization";
import { BackendIModelsAccess } from "@itwin/imodels-access-backend";
import { IModelsClient } from "@itwin/imodels-client-authoring";
import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import { Reporter } from "@itwin/perf-tools";
import * as os from "os";

async function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
const testCasesMap = new Map<string, any>();
testCasesMap.set("identity transform", require("./identity-transformer"));
// testCasesMap.set("prepare fork transform", require("./prepare-fork"));

const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

const setupTestData = async () => {
  const logLevel = +process.env.LOG_LEVEL!;
  if (LogLevel[logLevel] !== undefined) {
    Logger.initializeToConsole();
    Logger.setLevelDefault(LogLevel.Error);
    Logger.setLevel(loggerCategory, LogLevel.Info);
    Logger.setLevel(TransformerLoggerCategory.IModelExporter, logLevel);
    Logger.setLevel(TransformerLoggerCategory.IModelImporter, logLevel);
    Logger.setLevel(TransformerLoggerCategory.IModelTransformer, logLevel);
  }
  let usrEmail;
  let usrPass;
  if(process.env.V2_CHECKPOINT_USER_NAME){
    usrEmail = process.env.V2_CHECKPOINT_USER_NAME;
    usrPass = process.env.V2_CHECKPOINT_USER_PASSWORD;
  } else if(process.env.V1_CHECKPOINT_USER_NAME){
    usrEmail = process.env.V1_CHECKPOINT_USER_NAME;
    usrPass = process.env.V1_CHECKPOINT_USER_PASSWORD;
  } else {
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
  const authClient = process.env.CI === "1"
    ? new TestBrowserAuthorizationClient({
      clientId: process.env.OIDC_CLIENT_ID,
      redirectUri: process.env.OIDC_REDIRECT,
      scope: "itwins:read imodels:read imodels:modify",
      authority: "https://qa-ims.bentley.com",
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

  const arrImodels = [];
  for await (const iModel of getTestIModels()) {
    arrImodels.push(iModel);
  }
  return arrImodels;
};

void (async function () {
  const testIModels = await setupTestData();
  var reporter = new Reporter();
  const reportPath = initOutputFile("report.csv", outputDir);

  // could probably add an outer describe here (surounding both foreaches) for any before after logic that isn't related to getting the test data. but idk for sure 
  describe('Transformer Regression Tests', function () {
    testIModels.forEach(async (value, index, _array) => {
      const iModel = value;
      if (index === 2 || index === 5) {
        describe(`Transforms of ${iModel.name}`, async () => {
          testCasesMap.forEach(async (testCase, key, _map) => {
            it(key, async () => {
              reporter = await testCase.default(iModel, os, reporter);  // add timeout(0)
            }).timeout(0);
          });
        });
      }
    });
  });
  after(async () => {
    reporter.exportCSV(reportPath);
  });
  // See 'DELAYED ROOT SUITE' on https://mochajs.org/
  // This function is a special bcallback function provided by mocha when passing it the --delay flag. This gives us an opportunity to load in the iModels that we'll be testing so we can dynamically generate testcases.
  run();
})();