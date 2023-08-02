/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

/*
 * Tests where we perform "identity" transforms, that is just rebuilding an entire identical iModel (minus IDs)
 * through the transformation process.
 */

import "./setup";
import * as fs from "fs";
import * as path from "path";
import { BackendIModelsAccess } from "@itwin/imodels-access-backend";
import { BriefcaseDb, IModelHost, IModelHostConfiguration } from "@itwin/core-backend";
import { DbResult, Logger, LogLevel } from "@itwin/core-bentley";
import { IModelsClient } from "@itwin/imodels-client-authoring";
import { NodeCliAuthorizationClient } from "@itwin/node-cli-authorization";
import { Reporter } from "@itwin/perf-tools";
import { ReporterInfo } from "./ReporterUtils";
import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import { TestTransformerModule } from "./TestTransformerModule";
import { TransformerLoggerCategory } from "@itwin/imodel-transformer";
import { filterIModels, initOutputFile, preFetchAsyncIterator } from "./TestUtils";
import { getBranchName } from "./GitUtils";
import { getTestIModels } from "./TestContext";
import assert from "assert";
import nativeTransformerTestModule from "./transformers/NativeTransformer";
import rawForkCreateFedGuidsTestModule from "./transformers/RawForkCreateFedGuids";
import rawForkOperationsTestModule from "./transformers/RawForkOperations";
import rawInserts from "./rawInserts";

// cases
import identityTransformer from "./cases/identity-transformer";
import prepareFork from "./cases/prepare-fork";

const testCasesMap = new Map([
  ["identity transform", { testCase: identityTransformer, functionNameToValidate: "createIdentityTransform" }],
  ["prepare-fork", { testCase: prepareFork, functionNameToValidate: "createForkInitTransform" }],
]);

const loggerCategory = "Transformer Performance Regression Tests";
const outputDir = path.join(__dirname, ".output");

const loadTransformers = async () => {
  const modulePaths = process.env.EXTRA_TRANSFORMERS?.split(",").map((name) => name.trim()).filter(Boolean) ?? [];
  const envSpecifiedExtraTransformerCases = await Promise.all(
    modulePaths.map(async (m) => [m, (await import(m)).default])
  ) as [string, TestTransformerModule][];
  const transformerModules = new Map<string, TestTransformerModule>([
    ["NativeTransformer", nativeTransformerTestModule],
    ["RawForkOperations", rawForkOperationsTestModule],
    ["RawForkCreateFedGuids", rawForkCreateFedGuidsTestModule],
    ...envSpecifiedExtraTransformerCases,
  ]);
  return transformerModules;
};

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
  if (process.env.V2_CHECKPOINT_USER_NAME) {
    usrEmail = process.env.V2_CHECKPOINT_USER_NAME;
    usrPass = process.env.V2_CHECKPOINT_USER_PASSWORD;
  } else if (process.env.V1_CHECKPOINT_USER_NAME) {
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

  const hostConfig = new IModelHostConfiguration();
  hostConfig.authorizationClient = authClient;
  const hubClient = new IModelsClient({ api: { baseUrl: `https://${process.env.IMJS_URL_PREFIX}api.bentley.com/imodels` } });
  hostConfig.hubAccess = new BackendIModelsAccess(hubClient);
  await IModelHost.startup(hostConfig);

  return preFetchAsyncIterator(getTestIModels(filterIModels));
};

async function runRegressionTests() {
  const testIModels = await setupTestData();
  const transformerModules = await loadTransformers();
  const reporter = new Reporter();
  const reportPath = initOutputFile("report.csv", outputDir);
  const branchName = await getBranchName();

  describe("Transformer Regression Tests", function () {
    testIModels.forEach(async (iModel) => {
      let sourceDb: BriefcaseDb;
      let reportInfo: ReporterInfo;
      let sourceFileName: string;

      describe(`Transforms of ${iModel.name}`, async () => {
        before(async () => {
          Logger.logInfo(loggerCategory, `processing iModel '${iModel.name}' of size '${iModel.tShirtSize.toUpperCase()}'`);
          sourceFileName = await iModel.getFileName();
          sourceDb = await BriefcaseDb.open({
            fileName: sourceFileName,
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
          const toGb = (bytes: number) => `${(bytes / 1024 ** 3).toFixed(2)}Gb`;
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

        beforeEach(async () => {
          sourceDb = await BriefcaseDb.open({
            fileName: sourceFileName,
            readonly: true,
          });
        });

        afterEach(async () => {
          sourceDb.close(); // closing to ensure connection cache reusage doesn't affect results
        });

        testCasesMap.forEach(async ({testCase, functionNameToValidate}, key) => {
          transformerModules.forEach((transformerModule: TestTransformerModule, moduleName: string) => {
            const moduleFunc = transformerModule[functionNameToValidate as keyof TestTransformerModule];
            if (moduleFunc) {
              it(`${key} on ${moduleName}`, async () => {
                const addReport = (testName: string, iModelName: string, valDescription: string, value: number) => {
                  reporter.addEntry(testName, iModelName, valDescription, value, reportInfo);
                };
                await testCase({ sourceDb, transformerModule, addReport });
                // eslint-disable-next-line no-console
                console.log("Finished the test");
              }).timeout(0);
            }
          });
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
