/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelHost, SettingsPriority } from "@itwin/core-backend";

/**
 * Stop the running `IModelHost` from loading GCS workspaces, which opening any iModel
 * otherwise downloads from the public GCS container into the host's profile.
 */
export function disableGcsWorkspacesForTests(): void {
  IModelHost.appWorkspace.settings.addDictionary(
    {
      name: "imodel-transformer-tests-disable-gcs-workspaces",
      priority: SettingsPriority.application,
    },
    { "itwin/core/gcs/disableWorkspaces": true }
  );
}
