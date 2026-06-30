// @ts-check
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

// Creates the GitHub Release for a freshly-published @itwin/imodel-transformer version.
//
// Runs as the last step of the "Publish NPM packages" workflow, after `beachball publish`
// has bumped the version, regenerated CHANGELOG.md, and created+pushed the git tag
// `@itwin/imodel-transformer_v<version>`.
//
// On a MINOR or MAJOR release it also "promotes" the hand-authored release notes:
// docs/changehistory/NEXT_VERSION.md is archived to docs/changehistory/<version>.md, used as
// the release body, and reset to the empty template. Patch (and prerelease-only) releases skip
// this and get a body that just links to the CHANGELOG.
//
// Usage: node create-github-release.mjs <previousStableVersion>
//   <previousStableVersion> may be empty (first stable release).

import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const PACKAGE = "@itwin/imodel-transformer";
const CHANGEHISTORY_DIR = join(repoRoot, "docs", "changehistory");
const NEXT_VERSION_FILE = join(CHANGEHISTORY_DIR, "NEXT_VERSION.md");

const NEXT_VERSION_TEMPLATE = `# Next release notes

<!--
  Hand-author the release notes for the NEXT minor or major release of @itwin/imodel-transformer here.
  On the next minor/major "Publish NPM packages" run this file is archived to
  docs/changehistory/<version>.md, used as the GitHub Release body, then reset to this template.
  Patch (and dev/prerelease) publishes ignore this file.
-->

_No release notes yet._
`;

function run(cmd, args) {
  return execFileSync(cmd, args, { cwd: repoRoot, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] }).trim();
}

/** Parse the leading major/minor of a semver-ish string, ignoring any prerelease/build suffix. */
function majorMinor(version) {
  if (!version)
    return undefined;
  const match = /^v?(\d+)\.(\d+)\./.exec(version);
  if (!match)
    return undefined;
  return { major: Number(match[1]), minor: Number(match[2]) };
}

function isMinorOrMajor(prev, next) {
  const n = majorMinor(next);
  if (!n)
    return false;
  const p = majorMinor(prev);
  if (!p)
    return true; // no previous stable release -> treat the first one as significant
  return n.major > p.major || (n.major === p.major && n.minor > p.minor);
}

/** Strip the leading H1, HTML comments, and the "no notes" sentinel; return real authored prose or "". */
function extractAuthoredNotes(raw) {
  const withoutComments = raw.replace(/<!--[\s\S]*?-->/g, "");
  const withoutTitle = withoutComments.replace(/^\s*#\s.*\r?\n/, "");
  return withoutTitle.replace(/_No release notes yet\._/g, "").trim();
}

const prevVersion = process.argv[2] ?? "";
const pkgJson = JSON.parse(readFileSync(join(repoRoot, "packages", "transformer", "package.json"), "utf8"));
const newVersion = pkgJson.version;
const tag = `${PACKAGE}_v${newVersion}`;
const changelogUrl = `https://github.com/iTwin/imodel-transformer/blob/${tag}/packages/transformer/CHANGELOG.md`;

console.log(`Creating GitHub Release for ${tag} (previous stable: ${prevVersion || "<none>"})`);

const bodyParts = [];

if (isMinorOrMajor(prevVersion, newVersion)) {
  const authored = existsSync(NEXT_VERSION_FILE) ? extractAuthoredNotes(readFileSync(NEXT_VERSION_FILE, "utf8")) : "";
  if (authored) {
    bodyParts.push(authored);

    // Promote: archive the authored notes to a versioned changehistory page, then reset the template.
    const archivePath = join(CHANGEHISTORY_DIR, `${newVersion}.md`);
    writeFileSync(archivePath, `# ${newVersion} Change Notes\n\n${authored}\n`);
    writeFileSync(NEXT_VERSION_FILE, NEXT_VERSION_TEMPLATE);

    run("git", ["add", archivePath, NEXT_VERSION_FILE]);
    run("git", ["commit", "-m", `Promote release notes for ${newVersion} [skip actions]`]);
    run("git", ["push", "origin", `HEAD:${process.env.GITHUB_REF_NAME ?? "main"}`]);
    console.log(`Promoted NEXT_VERSION.md -> docs/changehistory/${newVersion}.md and reset the template.`);
  } else {
    console.log("Minor/major release but NEXT_VERSION.md has no authored notes; skipping promotion.");
  }
} else {
  console.log("Patch or prerelease publish; NEXT_VERSION.md is left untouched.");
}

bodyParts.push(`For the full list of changes, see the [CHANGELOG](${changelogUrl}).`);

const bodyFile = join(mkdtempSync(join(tmpdir(), "release-notes-")), "body.md");
writeFileSync(bodyFile, bodyParts.join("\n\n") + "\n");

run("gh", ["release", "create", tag, "--title", newVersion, "--notes-file", bodyFile, "--verify-tag"]);
console.log(`Published GitHub Release ${tag}.`);
