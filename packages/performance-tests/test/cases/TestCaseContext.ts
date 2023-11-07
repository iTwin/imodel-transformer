import { BriefcaseDb } from "@itwin/core-backend";
import { TestTransformerModule } from "../TestTransformerModule";

type ReportCallback = (iModelName: string, valDescription: string, value: number) => void;

export interface TestCaseContext {
  sourceDb: BriefcaseDb;
  transformerModule: TestTransformerModule;
  addReport: ReportCallback;
}
