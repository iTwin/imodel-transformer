/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  ".."
);
const packageJson = JSON.parse(
  readFileSync(path.join(packageRoot, "package.json"), "utf8")
);
const tempDirectory = mkdtempSync(
  path.join(os.tmpdir(), "imodel-transformer-esm-smoke-")
);

try {
  const packOutput = JSON.parse(
    execFileSync(
      "npm",
      ["pack", "--json", "--pack-destination", tempDirectory],
      { cwd: packageRoot, encoding: "utf8" }
    )
  )[0];
  const packedFiles = packOutput.files.map(({ path: filePath }) => filePath);
  assert(packedFiles.includes("lib/imodel-transformer.js"));
  assert(packedFiles.includes("lib/imodel-transformer.d.ts"));
  assert(!packedFiles.some((filePath) => filePath.startsWith("lib/cjs/")));
  assert(!packedFiles.some((filePath) => filePath.includes("/test/")));

  const installedPackageRoot = path.join(
    tempDirectory,
    "node_modules",
    "@itwin",
    "imodel-transformer"
  );
  mkdirSync(installedPackageRoot, { recursive: true });
  execFileSync(
    "tar",
    [
      "-xzf",
      path.join(tempDirectory, packOutput.filename),
      "--strip-components=1",
      "-C",
      installedPackageRoot,
    ],
    { cwd: tempDirectory }
  );

  const workspaceRequire = createRequire(
    path.join(packageRoot, "package.json")
  );
  const runtimeDependencies = {
    ...packageJson.dependencies,
    ...packageJson.peerDependencies,
  };
  for (const dependencyName of Object.keys(runtimeDependencies)) {
    const dependencyPackageJson = workspaceRequire.resolve(
      `${dependencyName}/package.json`
    );
    const dependencyRoot = path.dirname(dependencyPackageJson);
    const dependencyLink = path.join(
      tempDirectory,
      "node_modules",
      ...dependencyName.split("/")
    );
    mkdirSync(path.dirname(dependencyLink), { recursive: true });
    symlinkSync(dependencyRoot, dependencyLink, "junction");
  }

  writeFileSync(
    path.join(tempDirectory, "package.json"),
    JSON.stringify({ private: true, type: "module" })
  );
  writeFileSync(
    path.join(tempDirectory, "smoke.mjs"),
    `import assert from "node:assert/strict";
import { IModelCloneContext, IModelTransformer } from "@itwin/imodel-transformer";
import transformerPackage from "@itwin/imodel-transformer/package.json" with { type: "json" };
assert.equal(typeof IModelCloneContext, "function");
assert.equal(typeof IModelTransformer, "function");
assert.equal(transformerPackage.name, "@itwin/imodel-transformer");
`
  );
  execFileSync(process.execPath, ["smoke.mjs"], {
    cwd: tempDirectory,
    stdio: "inherit",
  });

  writeFileSync(
    path.join(tempDirectory, "smoke.cjs"),
    `const assert = require("node:assert/strict");
assert.throws(
  () => require("@itwin/imodel-transformer"),
  (error) => error?.code === "ERR_PACKAGE_PATH_NOT_EXPORTED" || error?.code === "ERR_REQUIRE_ESM"
);
`
  );
  execFileSync(process.execPath, ["smoke.cjs"], {
    cwd: tempDirectory,
    stdio: "inherit",
  });
} finally {
  rmSync(tempDirectory, { recursive: true, force: true });
}
