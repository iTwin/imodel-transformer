/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
export * from "./TransformerLoggerCategory.js";
export * from "./IModelExporter.js";
export * from "./IModelImporter.js";
export * from "./IModelTransformer.js";
export * from "./IModelCloneContext.js";
export * from "./IModelTransformerError.js";
export * from "./BranchProvenanceInitializer.js";

import * as https from "node:https";
import * as semver from "semver";
import { ITwinError } from "@itwin/core-bentley";
import coreBackendPackage from "@itwin/core-backend/package.json" with { type: "json" };
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError.js";
import { transformerPackageMetadata } from "./TransformerPackageMetadata.js";

const {
  version: ourVersion,
  name: ourName,
  peerDependencies,
} = transformerPackageMetadata;
const iTwinCoreBackendVersion = coreBackendPackage.version;

const ourITwinCoreBackendDepRange = peerDependencies["@itwin/core-backend"];

const noStrictDepCheckEnvVar = "TRANSFORMER_NO_STRICT_DEP_CHECK";
const suggestEnvVarName = "SUGGEST_TRANSFORMER_VERSIONS";

// warn if using a prerelease or dev version
if (semver.prerelease(iTwinCoreBackendVersion)) {
  // eslint-disable-next-line no-console
  console.warn(
    `Warning: dev version detected (${iTwinCoreBackendVersion}). ` +
      "This version is most likely fine, but it may introduce new behavior that could cause " +
      "unexpected issues or changes in the transformer's functionality. Please proceed with caution."
  );
}

if (
  process.env[noStrictDepCheckEnvVar] !== "1" &&
  !semver.satisfies(iTwinCoreBackendVersion, ourITwinCoreBackendDepRange, {
    includePrerelease: true,
  })
) {
  const errHeader =
    `${ourName}@${ourVersion} only supports @itwin/core-backend@${ourITwinCoreBackendDepRange}, ` +
    `but @itwin/core-backend${iTwinCoreBackendVersion} was resolved when looking for the peer dependency.\n` +
    `If you know exactly what you are doing, you can disable this check by setting ${noStrictDepCheckEnvVar}=1 in the environment\n`;

  if (process.env[suggestEnvVarName]) {
    https.get(`https://registry.npmjs.org/${ourName}`, async (resp) => {
      const chunks: string[] = [];
      const packumentSrc = await new Promise<string>((r) =>
        resp
          .setEncoding("utf8")
          .on("data", (d) => chunks.push(d))
          .on("end", () => r(chunks.join("")))
      );
      interface PackumentSubset {
        versions: Record<
          string,
          {
            peerDependencies?: {
              "@itwin/core-backend": string; // eslint-disable-line @typescript-eslint/naming-convention
            };
          }
        >;
      }

      const packumentJson = JSON.parse(packumentSrc) as PackumentSubset;
      const isTaglessVersion = (version: string) => version.includes("-");
      const latestFirstApplicableVersions = Object.entries(
        packumentJson.versions
      )
        .filter(([, v]) =>
          semver.satisfies(
            iTwinCoreBackendVersion,
            v.peerDependencies?.["@itwin/core-backend"] ?? ""
          )
        )
        .map(([k]) => k)
        .filter(isTaglessVersion)
        .reverse();

      ITwinError.throwError({
        iTwinErrorId: {
          scope: IModelTransformerErrorScope,
          key: IModelTransformerError.DependencyVersionMismatch,
        },
        message: [
          errHeader,
          `You have ${suggestEnvVarName}=1 set in the environment, so we suggest one of the following versions.`,
          "Be aware that older versions may be missing bug fixes.",
          ...latestFirstApplicableVersions,
        ].join("\n"),
      });
    });
  } else {
    ITwinError.throwError({
      iTwinErrorId: {
        scope: IModelTransformerErrorScope,
        key: IModelTransformerError.DependencyVersionMismatch,
      },
      message: `${errHeader}You can rerun with the environment variable ${suggestEnvVarName}=1 to have this error suggest a version`,
    });
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
/**
 * @docs-group-description ElementAspectExportCoordinator
 * Internal coordination for scoped, owner-batched ElementAspect export.
 */
/**
 * @docs-group-description ElementAspectExportProcessor
 * Internal source queries, filtering, and export callbacks for ElementAspects owned by accepted elements.
 */
/**
 * @docs-group-description IModelTransformerError
 * Stable identifiers for errors originating from this package.
 */
/**
 * @docs-group-description TransformerPackageMetadata
 * Internal metadata describing this package and its peer dependencies.
 */
