/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { BriefcaseDb, BriefcaseManager, IModelHost, RequestNewBriefcaseArg } from "@itwin/core-backend";
import { Logger } from "@itwin/core-bentley";
import { BriefcaseIdValue, IModelVersion } from "@itwin/core-common";
import { AccessTokenAdapter, BackendIModelsAccess } from "@itwin/imodels-access-backend";
import assert from "assert";

const loggerCategory = "TestContext";

export interface TestIModel {
  name: string;
  iModelId: string;
  iTwinId: string;
  tShirtSize: string;
  load: () => Promise<BriefcaseDb>;
}

const iTwinIdStr = process.env.ITWIN_IDS;
assert(iTwinIdStr, "no iTwins entered");
export const testITwinIds = iTwinIdStr.split(",");

type TShirtSize = "s" | "m" | "l" | "xl" | "unknown";

function getTShirtSizeFromName(name: string): TShirtSize {
  return /^(?<size>s|m|l|xl)\s*-/i.exec(name)?.groups?.size?.toLowerCase() as TShirtSize ?? "unknown";
}

export async function *getTestIModels(filter: (iModel: TestIModel) => boolean) {
  assert(IModelHost.authorizationClient !== undefined);
  // eslint-disable-next-line @typescript-eslint/dot-notation
  const hubClient = (IModelHost.hubAccess as BackendIModelsAccess)["_iModelsClient"];

  for (const iTwinId of testITwinIds) {
    const iModels = hubClient.iModels.getMinimalList({
      authorization: AccessTokenAdapter.toAuthorizationCallback(
        await IModelHost.authorizationClient.getAccessToken()
      ),
      urlParams: { projectId: iTwinId },
    });

    for await (const iModel of iModels) {
      const iModelId = iModel.id;
      const iModelToCheck: TestIModel = {
        name: iModel.displayName,
        iModelId,
        iTwinId,
        tShirtSize: getTShirtSizeFromName(iModel.displayName),
        load: async () => downloadAndOpenBriefcase({ iModelId, iTwinId }),
      };
      if(filter(iModelToCheck)){
        yield iModelToCheck;
      }
    }
  }
}

export async function downloadAndOpenBriefcase(briefcaseArg: Omit<RequestNewBriefcaseArg, "accessToken">): Promise<BriefcaseDb> {
  const PROGRESS_FREQ_MS = 2000;
  let nextProgressUpdate = Date.now() + PROGRESS_FREQ_MS;

  const asOf = briefcaseArg.asOf ?? IModelVersion.latest().toJSON();
  const changeset = await IModelHost.hubAccess.getChangesetFromVersion( {...briefcaseArg, version: IModelVersion.fromJSON(asOf) });

  assert(IModelHost.authorizationClient !== undefined, "auth client undefined");
  const briefcaseProps = BriefcaseManager.getCachedBriefcases(briefcaseArg.iModelId).find((b) => b.changeset.id === changeset.id);

  const briefcase = briefcaseProps ?? (await BriefcaseManager.downloadBriefcase({
    ...briefcaseArg,
    accessToken: await IModelHost.authorizationClient.getAccessToken(),
    onProgress(loadedBytes, totalBytes) {
      if (totalBytes !== 0 && Date.now() > nextProgressUpdate || loadedBytes === totalBytes) {
        if (loadedBytes === totalBytes)
          Logger.logTrace(loggerCategory, "Briefcase download completed");
        const asMb = (n: number) => (n / (1024*1024)).toFixed(2);
        if (loadedBytes < totalBytes)
          Logger.logTrace(loggerCategory, `Downloaded ${asMb(loadedBytes)} of ${asMb(totalBytes)}`);
        nextProgressUpdate = Date.now() + PROGRESS_FREQ_MS;
      }
      return 0;
    },
  }));

  return BriefcaseDb.open({
    fileName: briefcase.fileName,
    readonly: briefcaseArg.briefcaseId ? briefcaseArg.briefcaseId === BriefcaseIdValue.Unassigned : false,
  });
}
