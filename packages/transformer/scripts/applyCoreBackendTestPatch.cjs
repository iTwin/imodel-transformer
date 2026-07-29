/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

const fs = require("node:fs");
const path = require("node:path");

const packageJsonPath = require.resolve("@itwin/core-backend/package.json");
const coreBackendRoot = path.dirname(packageJsonPath);
const declarationsPath = path.join(coreBackendRoot, "lib/cjs/IModelDb.d.ts");
if (fs.readFileSync(declarationsPath, "utf8").includes("getAspectsForElements"))
  process.exit(0);

const { version } = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
const patchPath = path.resolve(
  __dirname,
  `../../../patches/@itwin__core-backend@${version}.patch`
);
if (!fs.existsSync(patchPath))
  throw new Error(`No test-only core-backend patch for version ${version}`);

const patchLines = fs.readFileSync(patchPath, "utf8").split("\n");
let filePath;
let fileLines;
let lineOffset = 0;
const saveFile = () => {
  if (filePath !== undefined)
    fs.writeFileSync(path.join(coreBackendRoot, filePath), fileLines.join("\n"));
};

for (let index = 0; index < patchLines.length; index++) {
  const line = patchLines[index];
  if (line.startsWith("+++ b/")) {
    saveFile();
    filePath = line.substring(6);
    fileLines = fs
      .readFileSync(path.join(coreBackendRoot, filePath), "utf8")
      .replaceAll("\r\n", "\n")
      .split("\n");
    lineOffset = 0;
    continue;
  }
  if (!line.startsWith("@@")) continue;

  const match = /^@@ -(\d+)(?:,\d+)? \+(?:\d+)(?:,\d+)? @@/.exec(line);
  if (match === null || fileLines === undefined)
    throw new Error(`Invalid patch hunk: ${line}`);
  const oldLines = [];
  const newLines = [];
  while (++index < patchLines.length) {
    const hunkLine = patchLines[index];
    if (hunkLine.startsWith("@@") || hunkLine.startsWith("diff --git")) {
      index--;
      break;
    }
    if (hunkLine.startsWith(" ") || hunkLine.startsWith("-"))
      oldLines.push(hunkLine.substring(1));
    if (hunkLine.startsWith(" ") || hunkLine.startsWith("+"))
      newLines.push(hunkLine.substring(1));
  }

  const start = Number(match[1]) - 1 + lineOffset;
  const actual = fileLines.slice(start, start + oldLines.length);
  if (actual.join("\n") !== oldLines.join("\n"))
    throw new Error(`Patch context did not match ${filePath}:${start + 1}`);
  fileLines.splice(start, oldLines.length, ...newLines);
  lineOffset += newLines.length - oldLines.length;
}
saveFile();
