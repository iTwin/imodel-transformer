/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import dotenv from "dotenv";

const { error } = dotenv.config();

if (error && !process.env.CI)
  throw new Error("no env file found, and not ran as a CI job");
