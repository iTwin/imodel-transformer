export interface ReporterInfo {
  /* eslint-disable @typescript-eslint/naming-convention */
  "Id": string;
  "T-shirt size": string;
  "Gb size": string;
  "Branch Name": string;
  "Federation Guid Saturation 0-1": number;
  /* eslint-enable @typescript-eslint/naming-convention */
}

export interface ReporterEntry {
  testSuite: string;
  testName: string;
  valueDescription: string;
  value: number;
  info?: ReporterInfo;
}