/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

// Cross-platform `rm -rf` for package scripts: node ../../scripts/rm.js <path>...
const { rmSync } = require("node:fs");

for (const path of process.argv.slice(2))
  rmSync(path, { recursive: true, force: true });
