/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { loadEnvFile } from "node:process";

try {
  loadEnvFile();
} catch (error) {
  if (!process.env.CI) throw error;
}
