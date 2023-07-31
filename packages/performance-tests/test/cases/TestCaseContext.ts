import { BriefcaseDb } from "@itwin/core-backend";
import { TestTransformerModule } from "../TestTransformerNodule";

type ReportCallback =  (testName: string, iModelName: string, valDescription: string, value: number) => void;

export interface TestCaseContext {
  sourceDb: BriefcaseDb;
  transformerModule: TestTransformerModule;
  addReport: ReportCallback;
}
