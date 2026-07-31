/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

const fs = require("node:fs");
const path = require("node:path");

const source = path.resolve(__dirname, "../CHANGELOG.md");
const destination = path.resolve(
  __dirname,
  "../../../build/docs/reference/imodel-transformer/CHANGELOG.md"
);

fs.mkdirSync(path.dirname(destination), { recursive: true });
fs.copyFileSync(source, destination);
