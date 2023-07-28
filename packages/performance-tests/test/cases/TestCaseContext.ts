import { BriefcaseDb } from "@itwin/core-backend";
import { TestTransformerModule } from "../TestTransformerNodule";

export type TestCaseContext = {
    sourceDb: BriefcaseDb;
    transformerModule: TestTransformerModule;
    addReport: (...smallReportSubset: [testName: string, iModelName: string, valDescription: string, value: number]) => void;
};
