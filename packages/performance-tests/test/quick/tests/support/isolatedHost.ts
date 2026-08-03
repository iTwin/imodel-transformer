/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as fs from "node:fs";
import { IModelHost } from "@itwin/core-backend";
import { quickTestHub } from "../../src/fixtures/QuickTestHub.js";

/**
 * Start `IModelHost` on a profile private to this process.
 *
 * `IModelHost.startup()` takes an exclusive lock on its profile directory, and the default profile
 * is shared by every iTwin.js process on the machine. Two concurrent runs therefore fail with
 * `Db is busy: Profile [...] is already in use by another process` — which surfaces far from its
 * cause, as an unrelated seed or teardown error.
 *
 * The benchmark itself deliberately keeps the default profile: its cache state is part of what is
 * being measured, and changing that would move published numbers. These are correctness tests, so
 * isolation is free and makes them safe to run alongside a benchmark.
 */
export async function startIsolatedHost(): Promise<void> {
  await IModelHost.startup({
    profileName: `quick-integration-${process.pid}`,
    hubAccess: quickTestHub,
  });
}

/** Shut down and remove the private profile, so runs do not accumulate directories. */
export async function shutdownIsolatedHost(): Promise<void> {
  if (!IModelHost.isValid) return;
  const profileDir = IModelHost.profileDir;
  await IModelHost.shutdown();
  // Only ever remove a profile this helper created.
  if (/quick-integration-\d+/.test(profileDir))
    fs.rmSync(profileDir, { recursive: true, force: true });
}
