/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { BriefcaseDb } from "@itwin/core-backend";
import { TestTransformerModule } from "../TestTransformerModule";

type ReportCallback = (
  iModelName: string,
  valDescription: string,
  value: number
) => void;

export interface TestCaseContext {
  sourceDb: BriefcaseDb;
  transformerModule: TestTransformerModule;
  addReport: ReportCallback;
}
