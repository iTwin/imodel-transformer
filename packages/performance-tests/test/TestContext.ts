/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  BriefcaseManager,
  IModelHost,
  RequestNewBriefcaseArg,
} from "@itwin/core-backend";
import { Logger } from "@itwin/core-bentley";
import { IModelVersion, LocalBriefcaseProps } from "@itwin/core-common";
import {
  AccessTokenAdapter,
  BackendIModelsAccess,
} from "@itwin/imodels-access-backend";
import assert from "assert";
import { generateTestIModel } from "./iModelUtils";

const loggerCategory = "TestContext";

export interface TestIModel {
  name: string;
  iModelId: string;
  iTwinId: string;
  tShirtSize: string;
  getFileName(): Promise<string>;
}

const iTwinIdStr = process.env.ITWIN_IDS;
assert(iTwinIdStr, "no iTwins entered");
export const testITwinIds = iTwinIdStr.split(",");

type TShirtSize = "s" | "m" | "l" | "xl" | "unknown";

export function getTShirtSizeFromName(name: string): TShirtSize {
  return (
    (/^(?<size>s|m|l|xl)\s*-/i
      .exec(name)
      ?.groups?.size?.toLowerCase() as TShirtSize) ?? "unknown"
  );
}

export async function* getTestIModels(filter: (iModel: TestIModel) => boolean) {
  assert(IModelHost.authorizationClient !== undefined);
  // eslint-disable-next-line @typescript-eslint/dot-notation
  const hubClient = (IModelHost.hubAccess as BackendIModelsAccess)[
    "_iModelsClient"
  ];

  for (const iTwinId of testITwinIds) {
    const iModels = hubClient.iModels.getMinimalList({
      authorization: AccessTokenAdapter.toAuthorizationCallback(
        await IModelHost.authorizationClient.getAccessToken()
      ),
      urlParams: { iTwinId },
    });

    for await (const iModel of iModels) {
      const iModelId = iModel.id;
      const iModelToCheck: TestIModel = {
        name: iModel.displayName,
        iModelId,
        iTwinId,
        tShirtSize: getTShirtSizeFromName(iModel.displayName),
        async getFileName(): Promise<string> {
          const _briefcase = await downloadBriefcase({ iModelId, iTwinId }); // note not downloadAndOpen
          return _briefcase.fileName;
        },
      };
      if (filter(iModelToCheck)) {
        yield iModelToCheck;
      }
    }
  }

  yield generateTestIModel({
    numElements: 100_000,
    fedGuids: true,
    fileName: `testIModel-fedguids-true.bim`,
  });
  yield generateTestIModel({
    numElements: 100_000,
    fedGuids: false,
    fileName: `testIModel-fedguids-false.bim`,
  });
}

export async function downloadBriefcase(
  briefcaseArg: Omit<RequestNewBriefcaseArg, "accessToken">
): Promise<LocalBriefcaseProps> {
  const PROGRESS_FREQ_MS = 2000;
  let nextProgressUpdate = Date.now() + PROGRESS_FREQ_MS;

  const asOf = briefcaseArg.asOf ?? IModelVersion.latest().toJSON();
  const changeset = await IModelHost.hubAccess.getChangesetFromVersion({
    ...briefcaseArg,
    version: IModelVersion.fromJSON(asOf),
  });

  assert(IModelHost.authorizationClient !== undefined, "auth client undefined");
  const briefcaseProps = BriefcaseManager.getCachedBriefcases(
    briefcaseArg.iModelId
  ).find((b) => b.changeset.id === changeset.id);

  const briefcase =
    briefcaseProps ??
    (await BriefcaseManager.downloadBriefcase({
      ...briefcaseArg,
      accessToken: await IModelHost.authorizationClient.getAccessToken(),
      onProgress(loadedBytes, totalBytes) {
        if (
          (totalBytes !== 0 && Date.now() > nextProgressUpdate) ||
          loadedBytes === totalBytes
        ) {
          if (loadedBytes === totalBytes)
            Logger.logTrace(loggerCategory, "Briefcase download completed");
          const asMb = (n: number) => (n / (1024 * 1024)).toFixed(2);
          if (loadedBytes < totalBytes)
            Logger.logTrace(
              loggerCategory,
              `Downloaded ${asMb(loadedBytes)} of ${asMb(totalBytes)}`
            );
          nextProgressUpdate = Date.now() + PROGRESS_FREQ_MS;
        }
        return 0;
      },
    }));

  return briefcase;
}
