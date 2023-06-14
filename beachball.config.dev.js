/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
// @ts-check
const base = require("./beachball.config.js");

/** @type {import("beachball").BeachballConfig } */
module.exports = {
  ...base,
  tag: !process.env.SPECIAL_TAG || process.env.SPECIAL_TAG === "dev"
    ? "nightly"
    : process.env.SPECIAL_TAG,
  prereleasePrefix: process.env.SPECIAL_TAG || "dev",
  generateChangelog: false,
  gitTags: false,
};

if (!module.exports.tag)
  throw Error("Sanity Error: using dev config but prerelease tag wasn't set");
