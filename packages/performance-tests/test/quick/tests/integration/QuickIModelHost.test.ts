/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { IModelHost, SnapshotDb } from "@itwin/core-backend";
import { afterEach, describe, expect, it } from "vitest";
import {
  quickIModelHostProfileName,
  shutdownQuickIModelHost,
  startQuickIModelHost,
} from "../../src/framework/QuickIModelHost.js";

describe("quick IModelHost lifecycle", () => {
  afterEach(async () => {
    if (IModelHost.isValid) await shutdownQuickIModelHost();
  });

  it("uses and removes a process-private profile with GCS workspaces disabled", async () => {
    await startQuickIModelHost();
    const profileDirectory = IModelHost.profileDir;

    expect(IModelHost.profileName).toBe(quickIModelHostProfileName);
    expect(path.basename(profileDirectory)).toBe(quickIModelHostProfileName);
    expect(
      IModelHost.appWorkspace.settings.getBoolean(
        "itwin/core/gcs/disableWorkspaces",
        false
      )
    ).toBe(true);
    expect(fs.existsSync(profileDirectory)).toBe(true);

    // Opening any iModel loads the default GCS workspaces unless they are disabled.
    const snapshotPath = path.join(
      os.tmpdir(),
      `quick-imodelhost-${process.pid}.bim`
    );
    try {
      SnapshotDb.createEmpty(snapshotPath, {
        rootSubject: { name: "quick IModelHost lifecycle" },
      }).close();
    } finally {
      fs.rmSync(snapshotPath, { force: true });
    }
    expect(
      fs.existsSync(
        path.join(profileDirectory, "CloudCaches", "Workspace", "gcs")
      )
    ).toBe(false);

    await shutdownQuickIModelHost();

    expect(IModelHost.isValid).toBe(false);
    expect(fs.existsSync(profileDirectory)).toBe(false);
  });
});
