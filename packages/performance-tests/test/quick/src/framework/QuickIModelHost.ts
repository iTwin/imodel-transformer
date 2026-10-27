/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { IModelHost } from "@itwin/core-backend";
import { disableGcsWorkspacesForTests } from "@itwin/imodel-transformer-test-utils";
import { quickTestHub } from "../fixtures/QuickTestHub.js";

export const quickIModelHostProfileName = `imodel-transformer-quick-${process.pid}`;

export async function startQuickIModelHost(): Promise<void> {
  // IModelHost.startup silently keeps an already-running host and its profile.
  if (IModelHost.isValid)
    throw new Error("An IModelHost is already running in this process");

  await IModelHost.startup({
    hubAccess: quickTestHub,
    profileName: quickIModelHostProfileName,
    disableThinnedNativeInstanceWorkflow: true,
  });
  disableGcsWorkspacesForTests();
}

export async function shutdownQuickIModelHost(): Promise<void> {
  if (!IModelHost.isValid) return;
  if (IModelHost.profileName !== quickIModelHostProfileName)
    throw new Error(
      "Refusing to shut down an IModelHost not started by the quick performance harness"
    );

  const profileDirectory = IModelHost.profileDir;
  await IModelHost.shutdown();
  fs.rmSync(profileDirectory, { recursive: true, force: true });
}
