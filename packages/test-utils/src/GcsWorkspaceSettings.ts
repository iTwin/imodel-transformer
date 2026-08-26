/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelHost, SettingsPriority } from "@itwin/core-backend";

const testGcsSettings = {
  name: "imodel-transformer-tests-disable-gcs-workspaces",
};

export function disableGcsWorkspacesForTests(): void {
  IModelHost.appWorkspace.settings.addDictionary(
    { ...testGcsSettings, priority: SettingsPriority.application },
    { "itwin/core/gcs/disableWorkspaces": true }
  );
}

export function allowGcsWorkspacesForTests(): void {
  IModelHost.appWorkspace.settings.dropDictionary(testGcsSettings);
}
