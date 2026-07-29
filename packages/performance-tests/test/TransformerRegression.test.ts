/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import "./setup";
import * as fs from "node:fs";
import * as path from "node:path";
import assert from "node:assert";
import {
  BriefcaseDb,
  IModelHost,
  IModelHostConfiguration,
} from "@itwin/core-backend";
import { Logger, LogLevel } from "@itwin/core-bentley";
import { TransformerLoggerCategory } from "@itwin/imodel-transformer";
import { BackendIModelsAccess } from "@itwin/imodels-access-backend";
import { IModelsClient } from "@itwin/imodels-client-authoring";
import { NodeCliAuthorizationClient } from "@itwin/node-cli-authorization";
import {
  AzureClientStorage,
  BlockBlobClientWrapperFactory,
} from "@itwin/object-storage-azure";
import { Reporter } from "@itwin/perf-tools";
import { TestBrowserAuthorizationClient } from "@itwin/oidc-signin-tool";
import {
  CleanupTask,
  runCleanupTasks,
  runWithCleanup,
  throwAfterCleanup,
} from "./Cleanup";
import { getBranchName } from "./GitUtils";
import {
  getRegressionTestDefinitions,
  RegressionTestCase,
} from "./RegressionTestRegistration";
import { ReporterInfo } from "./ReporterUtils";
import { getTestIModels, TestIModel } from "./TestContext";
import { TestTransformerModule } from "./TestTransformerModule";
import {
  filterIModels,
  initOutputFile,
  preFetchAsyncIterator,
} from "./TestUtils";
import identityTransformer from "./cases/identity-transformer";
import prepareFork from "./cases/prepare-fork";
import rawInserts from "./rawInserts";
import nativeTransformerTestModule from "./transformers/NativeTransformer";
import rawForkOperationsTestModule from "./transformers/RawForkOperations";

type AuthorizationClient =
  | NodeCliAuthorizationClient
  | TestBrowserAuthorizationClient;
type PerformanceTestCase = typeof identityTransformer;

const testCasesMap = new Map<string, RegressionTestCase<PerformanceTestCase>>([
  [
    "identity transform (provenance)",
    {
      testCase: identityTransformer,
      functionNameToValidate: "createIdentityTransform",
    },
  ],
  [
    "prepare-fork",
    {
      testCase: prepareFork,
      functionNameToValidate: "createForkInitTransform",
    },
  ],
]);

const loggerCategory = "Transformer Performance Regression Tests";
const outputDir = path.join(__dirname, ".output");

vi.setConfig({ testTimeout: 0, hookTimeout: 0 });

class WorkerLifecycle {
  private _authClient?: AuthorizationClient;

  public setAuthClient(authClient: AuthorizationClient): void {
    this._authClient = authClient;
  }

  public async shutdown(): Promise<void> {
    const authClient = this._authClient;
    this._authClient = undefined;

    const cleanupTasks: CleanupTask[] = [];
    if (IModelHost.isValid) {
      cleanupTasks.push({
        name: "IModelHost shutdown",
        run: async () => IModelHost.shutdown(),
      });
    }
    if (authClient) {
      cleanupTasks.push({
        name: "authorization sign out",
        run: async () => authClient.signOut(),
      });
    }
    await runCleanupTasks(cleanupTasks);
  }
}

async function loadTransformers(): Promise<Map<string, TestTransformerModule>> {
  const modulePaths =
    process.env.EXTRA_TRANSFORMERS?.split(",")
      .map((name) => name.trim())
      .filter(Boolean) ?? [];
  const extraTransformerCases = (await Promise.all(
    modulePaths.map(async (modulePath) => [
      modulePath,
      (await import(modulePath)).default,
    ])
  )) as [string, TestTransformerModule][];
  return new Map<string, TestTransformerModule>([
    ["NativeTransformer", nativeTransformerTestModule],
    ["RawForkOperations", rawForkOperationsTestModule],
    ...extraTransformerCases,
  ]);
}

async function setupTestData(
  workerLifecycle: WorkerLifecycle
): Promise<TestIModel[]> {
  const logLevel = process.env.LOG_LEVEL
    ? Number(process.env.LOG_LEVEL)
    : LogLevel.Error;
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
  const user = { email: usrEmail, password: usrPass };

  assert(process.env.OIDC_CLIENT_ID, "OIDC_CLIENT_ID not set");
  assert(process.env.OIDC_REDIRECT, "OIDC_REDIRECT not set");
  assert(process.env.IMJS_URL_PREFIX, "IMJS_URL_PREFIX not set");
  assert(process.env.OIDC_SCOPES, "OIDC_SCOPES not set");

  const authClient =
    process.env.CI === "1"
      ? new TestBrowserAuthorizationClient(
          {
            clientId: process.env.OIDC_CLIENT_ID,
            redirectUri: process.env.OIDC_REDIRECT,
            scope: process.env.OIDC_SCOPES,
            authority: `https://${process.env.IMJS_URL_PREFIX}ims.bentley.com`,
          },
          user
        )
      : new NodeCliAuthorizationClient({
          clientId: process.env.OIDC_CLIENT_ID,
          redirectUri: process.env.OIDC_REDIRECT,
          scope: process.env.OIDC_SCOPES,
        });
  workerLifecycle.setAuthClient(authClient);
  await authClient.signIn();

  const hostConfig = new IModelHostConfiguration();
  hostConfig.authorizationClient = authClient;
  const hubClient = new IModelsClient({
    api: {
      baseUrl: `https://${process.env.IMJS_URL_PREFIX}api.bentley.com/imodels`,
    },
    cloudStorage: new AzureClientStorage(new BlockBlobClientWrapperFactory()),
  });
  hostConfig.hubAccess = new BackendIModelsAccess(hubClient);
  await IModelHost.startup(hostConfig);

  return preFetchAsyncIterator(getTestIModels(filterIModels));
}

async function collectRegressionInputs(workerLifecycle: WorkerLifecycle) {
  const testIModels = await setupTestData(workerLifecycle);
  const transformerModules = await loadTransformers();
  const reporter = new Reporter();
  const reportPath = initOutputFile("report.csv", outputDir);
  const branchName = await getBranchName();
  return {
    branchName,
    reportPath,
    reporter,
    testIModels,
    transformerModules,
  };
}

const lifecycle = new WorkerLifecycle();
const {
  branchName: currentBranchName,
  reportPath: csvReportPath,
  reporter: performanceReporter,
  testIModels: collectedIModels,
  transformerModules: loadedTransformerModules,
} = await collectRegressionInputs(lifecycle).catch(async (error: unknown) =>
  throwAfterCleanup(error, [
    { name: "worker lifecycle", run: async () => lifecycle.shutdown() },
  ])
);
const regressionTestDefinitions = getRegressionTestDefinitions(
  testCasesMap,
  loadedTransformerModules
);

describe("Transformer Regression Tests", () => {
  for (const iModel of collectedIModels) {
    describe(`Transforms of ${iModel.name}`, () => {
      let sourceDb: BriefcaseDb | undefined;
      let reportInfo: ReporterInfo;
      let sourceFileName: string;

      beforeAll(async () => {
        Logger.logInfo(
          loggerCategory,
          `processing iModel '${iModel.name}' of size '${iModel.tShirtSize.toUpperCase()}'`
        );
        sourceFileName = await iModel.getFileName();
        const metadataDb = await BriefcaseDb.open({
          fileName: sourceFileName,
          readonly: true,
        });
        await runWithCleanup(async () => {
          const fedGuidReader = metadataDb.createQueryReader(
            "SELECT CAST(SUM(IIF(FederationGuid IS NOT NULL, 1, 0)) AS DOUBLE)/COUNT(*) AS ratio FROM bis.Element",
            undefined,
            { usePrimaryConn: true }
          );
          const fedGuidSaturation = (await fedGuidReader.step())
            ? (fedGuidReader.current[0] as number)
            : 0;
          Logger.logInfo(
            loggerCategory,
            `Federation Guid Saturation '${fedGuidSaturation}'`
          );
          const sizeInGb = `${(
            fs.statSync(metadataDb.pathName).size /
            1024 ** 3
          ).toFixed(2)}Gb`;
          Logger.logInfo(loggerCategory, `loaded (${sizeInGb})'`);
          reportInfo = {
            Id: iModel.iModelId,
            "T-shirt size": iModel.tShirtSize,
            "Gb size": sizeInGb,
            "Branch Name": currentBranchName,
            "Federation Guid Saturation 0-1": fedGuidSaturation,
          };
        }, [
          {
            name: `${iModel.name} metadata briefcase`,
            run: () => metadataDb.close(),
          },
        ]);
      });

      beforeEach(async () => {
        sourceDb = await BriefcaseDb.open({
          fileName: sourceFileName,
          readonly: true,
        });
      });

      afterEach(() => {
        const dbToClose = sourceDb;
        sourceDb = undefined;
        dbToClose?.close();
      });

      for (const definition of regressionTestDefinitions) {
        test(`${definition.testCaseName} on ${definition.moduleName}`, async () => {
          assert(sourceDb, "source briefcase was not opened");
          const addReport = (
            iModelName: string,
            valDescription: string,
            value: number
          ) => {
            performanceReporter.addEntry(
              `${definition.testCaseName} ${definition.moduleName}`,
              iModelName,
              valDescription,
              value,
              reportInfo
            );
          };
          await definition.testCase({
            sourceDb,
            transformerModule: definition.transformerModule,
            addReport,
          });
        });
      }
    });
  }

  test("Transform vs raw inserts", async () => {
    await rawInserts(performanceReporter, currentBranchName);
  });
});

afterAll(async () => {
  await runCleanupTasks([
    {
      name: "report export",
      run: () => performanceReporter.exportCSV(csvReportPath),
    },
    {
      name: "worker lifecycle",
      run: async () => lifecycle.shutdown(),
    },
  ]);
});
