/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

/*
 * Tests where we perform "identity" transforms, that is just rebuilding an entire identical iModel (minus IDs)
 * through the transformation process.
 */

import "./setup";
import assert from "assert";
import * as path from "path";
import * as fs from "fs";
import { BriefcaseDb, IModelHost, IModelHostConfiguration } from "@itwin/core-backend";
import { DbResult, Guid, Logger, LogLevel } from "@itwin/core-bentley";
import { TransformerLoggerCategory } from "@itwin/imodel-transformer";
import { downloadBriefcase, getTestIModels } from "./TestContext";
import { filterIModels, initOutputFile, preFetchAsyncIterator } from "./TestUtils";
import { NodeCliAuthorizationClient } from "@itwin/node-cli-authorization";
import { BackendIModelsAccess } from "@itwin/imodels-access-backend";
import { IModelsClient } from "@itwin/imodels-client-authoring";
import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import { Reporter } from "@itwin/perf-tools";
import rawInserts from "./rawInserts";
import { getBranchName } from "./GitUtils";
import { ReporterInfo } from "./ReporterUtils";

// cases
import identityTransformer from "./cases/identity-transformer";
import prepareFork from "./cases/prepare-fork";

const testCasesMap = new Map([
  ["identity transform", identityTransformer],
  ["prepare-fork", prepareFork],
]);

const loggerCategory = "Transformer Performance Regression Tests";
const outputDir = path.join(__dirname, ".output");

const setupTestData = async () => {
  const logLevel = process.env.LOG_LEVEL ? Number(process.env.LOG_LEVEL) : LogLevel.Error;

  assert(LogLevel[logLevel] !== undefined, "unknown log level");

  Logger.initializeToConsole();
  Logger.setLevelDefault(logLevel);
  Logger.setLevel(TransformerLoggerCategory.IModelExporter, logLevel);
  Logger.setLevel(TransformerLoggerCategory.IModelImporter, logLevel);
  Logger.setLevel(TransformerLoggerCategory.IModelTransformer, logLevel);

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

  assert(process.env.OIDC_CLIENT_ID, "OIDC_CLIENT_ID not set");
  assert(process.env.OIDC_REDIRECT, "OIDC_REDIRECT not set");
  assert(process.env.IMJS_URL_PREFIX, "IMJS_URL_PREFIX not set");
  assert(process.env.OIDC_SCOPES, "OIDC_SCOPES not set");

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
  const branchName =  await getBranchName();

  describe("Transformer Regression Tests", function () {
    testIModels.forEach(async (iModel) => {
      let sourceDb: BriefcaseDb;
      let reportInfo: ReporterInfo;

      describe(`Transforms of ${iModel.name}`, async () => {
        before(async () => {
          Logger.logInfo(loggerCategory, `processing iModel '${iModel.name}' of size '${iModel.tShirtSize.toUpperCase()}'`);
          await iModel.getFileName();
          assert(iModel._cachedFileName, "Imodel file name not Cached to TestIModel Object");
          sourceDb = await BriefcaseDb.open({
            fileName: iModel._cachedFileName,
            readonly: true,
          });
          const fedGuidSaturation = sourceDb.withStatement(
            `
            SELECT
            CAST(SUM(hasGuid) AS DOUBLE)/COUNT(*) ratio 
            FROM (
              SELECT IIF(FederationGuid IS NOT NULL, 1, 0) AS hasGuid,
              1 AS total FROM bis.Element
            )`,
            (stmt) => {
              assert(stmt.step() === DbResult.BE_SQLITE_ROW);
              return stmt.getValue(0).getDouble();
            }
          );
          Logger.logInfo(loggerCategory, `Federation Guid Saturation '${fedGuidSaturation}'`);
          const toGb = (bytes: number) => `${(bytes / 1024 **3).toFixed(2)}Gb`;
          const sizeInGb = toGb(fs.statSync(sourceDb.pathName).size);
          Logger.logInfo(loggerCategory, `loaded (${sizeInGb})'`);
          reportInfo = {
            "Id": iModel.iModelId,
            "T-shirt size": iModel.tShirtSize,
            "Gb size": sizeInGb,
            "Branch Name": branchName,
            "Federation Guid Saturation 0-1": fedGuidSaturation,
          };
          sourceDb.close();
        });

        testCasesMap.forEach(async (testCase, key) => {
          before(async () => {
            assert(iModel._cachedFileName, "Imodel file name not Cached to TestIModel Object");
            sourceDb = await BriefcaseDb.open({
              fileName: iModel._cachedFileName,
              readonly: true,
            });
          });

          it(key, async () => {
            reporter = await testCase(sourceDb, reporter, reportInfo);
          }).timeout(0);
          
          sourceDb.close(); // closing to ensure connection cache reusage doesn't affect results
        });
      });
    });

    const _15minutes = 15 * 60 * 1000;

    it("Transform vs raw inserts", async () => {
      return rawInserts(reporter, branchName);
    }).timeout(0);

  });

  after(async () => {
    reporter.exportCSV(reportPath);
  });

  run();
}

void runRegressionTests();
