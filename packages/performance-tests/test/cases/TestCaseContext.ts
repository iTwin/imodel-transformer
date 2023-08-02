import { BriefcaseDb } from "@itwin/core-backend";
import { TestTransformerModule } from "../TestTransformerModule";

type ReportCallback = (testName: string, iModelName: string, valDescription: string, value: number) => void;

export interface TestCaseContext {
  sourceDb: BriefcaseDb;
  transformerModule: TestTransformerModule;
  addReport: ReportCallback;
}
