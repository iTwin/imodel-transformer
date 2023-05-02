/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
// @ts-check
const base = require("./beachball.config.js");

/** @type {import("beachball").BeachballConfig } */
module.exports = {
  ...base,
  tag: "nightly",
  prereleasePrefix: process.env.PRERELEASE_PREFIX ?? "dev",
  generateChangelog: false,
  gitTags: false,
};
