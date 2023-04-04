/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
export * from "./TransformerLoggerCategory";
export * from "./IModelExporter";
export * from "./IModelImporter";
export * from "./IModelTransformer";

import * as semver from "semver";
import { version as iTwinCoreBackendVersion } from "@itwin/core-backend/package.json";

// must use an untyped require to not hoist src into lib/cjs, also the compiled output will be in 'lib/cjs', not 'src' so use `../..` to reach package.json
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { version: ourVersion, name: ourName, peerDependencies } = require("../../package.json");

const ourITwinCoreBackendDepRange = peerDependencies["@itwin/core-backend"];

const noStrictDepCheckEnvVar = "TRANSFORMER_NO_STRICT_DEP_CHECK";
const suggestEnvVarName = "SUGGEST_TRANSFORMER_VERSIONS";

if (process.env[noStrictDepCheckEnvVar] !== "1" && !semver.satisfies(iTwinCoreBackendVersion, ourITwinCoreBackendDepRange)) {

  const errHeader =
    `${ourName}@${ourVersion} only supports @itwin/core-backend@${ourITwinCoreBackendDepRange}, `
    + `but @itwin/core-backend${iTwinCoreBackendVersion} was resolved when looking for the peer dependency.\n`
    + `If you know exactly what you are doing, you can disable this check by setting ${noStrictDepCheckEnvVar}=1 in the environment\n`;

  if (process.env[suggestEnvVarName]) {
    // let's not import https except in this case
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const https = require("https") as typeof import("https");
    https.get(`https://registry.npmjs.org/${ourName}`, async (resp) => {
      const chunks: string[] = [];
      const packumentSrc = await new Promise<string>((r) => resp.setEncoding("utf8").on("data", (d) => chunks.push(d)).on("end", () => r(chunks.join(""))));
      interface PackumentSubset {
        versions: Record<string, { peerDependencies?: { "@itwin/core-backend": string } }>;
      }
      const packumentJson = JSON.parse(packumentSrc) as PackumentSubset;
      const isTaglessVersion = (version: string) => version.includes("-");
      const latestFirstApplicableVersions
        = Object.entries(packumentJson.versions)
          .filter(([,v]) => semver.satisfies(iTwinCoreBackendVersion, v.peerDependencies?.["@itwin/core-backend"] ?? ""))
          .map(([k]) => k)
          .filter(isTaglessVersion)
          .reverse();

      throw Error([
        errHeader,
        `You have ${suggestEnvVarName}=1 set in the environment, so we suggest one of the following versions.`,
        `Be aware that older versions may be missing bug fixes.`,
        ...latestFirstApplicableVersions,
      ].join("\n"));
    });
  } else {
    throw Error(
      `${errHeader} You can rerun with the environment variable ${suggestEnvVarName}=1 to have this error suggest a version`
    );
  }
}

/** @docs-package-description
 * The @itwin/imodel-transformer package contains classes that [backend code]($docs/learning/backend/index.md) can use to
 * traverse iModels, as well as *transform* an iModel into another existing or empty one, by exporting elements from one during
 * traversal and importing them into another.
 *
 * You can read further in [iModelTransformation and Data Exchange]($docs/learning/transformer/index.md) here.
 */
/**
 * @docs-group-description iModels
 * Classes for working with [iModels]($docs/learning/iModels.md).
 * See [the learning article]($docs/learning/backend/index.md).
 */
/**
 * @docs-group-description Utils
 * Miscellaneous utility classes.
 */
/**
 * @docs-group-description Logging
 * Logger categories used by this package.
 */
