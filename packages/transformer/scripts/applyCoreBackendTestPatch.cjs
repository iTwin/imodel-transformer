/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const coreBackendRoot = path.dirname(
  require.resolve("@itwin/core-backend/package.json")
);
const declarationsPath = path.join(coreBackendRoot, "lib/cjs/IModelDb.d.ts");
if (fs.readFileSync(declarationsPath, "utf8").includes("getAspectsForElements"))
  process.exit(0);

const patchPath = path.resolve(
  __dirname,
  "../../../patches/@itwin__core-backend@5.10.3.patch"
);
const result = spawnSync("git", ["apply", "--no-index", patchPath], {
  cwd: coreBackendRoot,
  stdio: "inherit",
});
if (result.status !== 0)
  throw new Error(`Failed to apply test-only core-backend patch: ${patchPath}`);
