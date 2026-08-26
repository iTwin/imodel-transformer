/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as path from "node:path";
import { IModelHost } from "@itwin/core-backend";
import { disableGcsWorkspacesForTests } from "@itwin/imodel-transformer-test-utils";
import { quickTestHub } from "../fixtures/QuickTestHub.js";

export const quickIModelHostProfileName = `imodel-transformer-quick-${process.pid}`;

let ownedProfileDirectory: string | undefined;

export async function startQuickIModelHost(): Promise<void> {
  if (ownedProfileDirectory !== undefined || IModelHost.isValid)
    throw new Error("The quick performance IModelHost is already running");

  await IModelHost.startup({
    hubAccess: quickTestHub,
    profileName: quickIModelHostProfileName,
  });
  ownedProfileDirectory = IModelHost.profileDir;
  disableGcsWorkspacesForTests();
}

export async function shutdownQuickIModelHost(): Promise<void> {
  if (ownedProfileDirectory === undefined) {
    if (IModelHost.isValid)
      throw new Error(
        "Refusing to shut down an IModelHost not owned by the quick performance harness"
      );
    return;
  }

  if (!IModelHost.isValid)
    throw new Error(
      "The quick performance IModelHost stopped before its profile could be cleaned up"
    );

  const profileDirectory = ownedProfileDirectory;
  const ownsProfileDirectory =
    path.basename(profileDirectory) === quickIModelHostProfileName;
  await IModelHost.shutdown();
  ownedProfileDirectory = undefined;

  if (!ownsProfileDirectory)
    throw new Error(
      `Refusing to remove unexpected IModelHost profile directory: ${profileDirectory}`
    );
  fs.rmSync(profileDirectory, { recursive: true, force: true });
}
