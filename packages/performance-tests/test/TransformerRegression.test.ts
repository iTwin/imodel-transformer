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
import { IModelHost, IModelHostConfiguration } from "@itwin/core-backend";
import { Logger, LogLevel } from "@itwin/core-bentley";
import { TransformerLoggerCategory } from "@itwin/imodel-transformer";
import { getTestIModels } from "./TestContext";
import { filterIModels, initOutputFile, preFetchAsyncIterator } from "./TestUtils";
import { NodeCliAuthorizationClient } from "@itwin/node-cli-authorization";
import { BackendIModelsAccess } from "@itwin/imodels-access-backend";
import { IModelsClient } from "@itwin/imodels-client-authoring";
import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import { Reporter } from "@itwin/perf-tools";
import rawInserts from "./rawInserts";

/* eslint-disable @typescript-eslint/no-var-requires */
const testCasesMap = new Map([
  ["identity transform", require("./identity-transformer")],
]);
/* eslint-enable @typescript-eslint/no-var-requires */

const loggerCategory = "Transformer Performance Tests Identity";
const outputDir = path.join(__dirname, ".output");

const setupTestData = async () => {
  const logLevel = process.env.LOG_LEVEL ? Number(process.env.LOG_LEVEL) : LogLevel.Warning;
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
  assert(process.env.IMJS_URL_PREFIX, "");
  assert(process.env.OIDC_SCOPES, "List scopes");
  const authClient = process.env.CI === "1"
    ? new TestBrowserAuthorizationClient({
      clientId: process.env.OIDC_CLIENT_ID,
      redirectUri: process.env.OIDC_REDIRECT,
      scope: process.env.OIDC_SCOPES,
      authority: `https://${process.env.IMJS_URL_PREFIX}ims.bentley.com`,
    }, user)
    : new NodeCliAuthorizationClient({
      clientId: process.env.OIDC_CLIENT_ID,
      redirectUri: process.env.OIDC_REDIRECT,
      scope: process.env.OIDC_SCOPES,
    });

  await authClient.signIn();

  const hostConfig  = new IModelHostConfiguration();
  hostConfig.authorizationClient = authClient;
  const hubClient = new IModelsClient({ api: { baseUrl: `https://${process.env.IMJS_URL_PREFIX}api.bentley.com/imodels` } });
  hostConfig.hubAccess = new BackendIModelsAccess(hubClient);
  await IModelHost.startup(hostConfig);

  return preFetchAsyncIterator(getTestIModels(filterIModels));
};

async function runRegressionTests() {
  const testIModels = await setupTestData();
  let reporter = new Reporter();
  const reportPath = initOutputFile("report.csv", outputDir);

  describe("Transformer Regression Tests", function () {
    testIModels.forEach(async (iModel) => {
      describe(`Transforms of ${iModel.name}`, async () => {
        testCasesMap.forEach(async (testCase, key) => {
          it(key, async () => {
            reporter = await testCase.default(iModel, reporter);
          }).timeout(0);
        });
      });
    });
  });

  after(async () => {
    reporter.exportCSV(reportPath);
  });
  run();
}

void runRegressionTests();
