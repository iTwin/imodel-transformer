/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createRequire } from "node:module";
import {
  ChangedInstanceIdsDependency,
  createChangesetScanningBenchmark,
} from "./changesetScanningFactory.js";

const workspaceRequire = createRequire(import.meta.url);

function workspaceChangedInstanceIds(): ChangedInstanceIdsDependency {
  return (
    workspaceRequire("@itwin/imodel-transformer") as {
      readonly ChangedInstanceIds: ChangedInstanceIdsDependency;
    }
  ).ChangedInstanceIds;
}

export const changesetScanningBenchmark = createChangesetScanningBenchmark(
  workspaceChangedInstanceIds()
);
export const changesetScanningScenario = changesetScanningBenchmark.scenario;
