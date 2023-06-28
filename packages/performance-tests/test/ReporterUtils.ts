/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/
import * as fs from "fs-extra";
export interface ReporterInfo {
  "Id": string;
  "T-shirt size": string;
  "Gb size": string;
  "Branch Name": string;
  "Federation Guid Saturation 0-1": number;
}

export interface ReporterEntry {
  testName?: string;
  iModelName: string;
  branch: string;
  valueDescription?: string;
  value?: number;
  info?: object;
}

interface Entry {
  testName: string;
  iModelName: string;
  branch: string;
  valueDescription: string;
  value: number;
  date: string;
  info?: object;
}
export class Reporter {
  private _entries: Entry[] = [];

  public addEntry(reportEntry: ReporterEntry) {
    let entry: Entry;
    if(reportEntry.testName && reportEntry.value){
      entry = { 
                testName: reportEntry.testName, 
                iModelName: reportEntry.iModelName, 
                branch: reportEntry.branch, 
                valueDescription: reportEntry.valueDescription ? reportEntry.valueDescription : "time elapsed (seconds)", 
                value: reportEntry.value, 
                date: new Date().toISOString(),
                info: reportEntry.info,
              };
      this._entries.push(entry);
    }
  }

  /**
   * Clear entries to get a fresh start
   */
  public clearEntries() {
    this._entries = [];
  }

  /**
   * Create CSV file with report. Call after all test have run
   * @param fileName Name of the CSV file with or without .csv
   */
  public exportCSV(fileName: string) {
    let finalReport: string = "";
    if (!fileName.endsWith(".csv")) {
      fileName = `${fileName}.csv`;
    }
    if (!fs.existsSync(fileName)) {
      finalReport += "TestName,iModelName,Branch,ValueDescription,Value,Date,Info\n";
    }
    for (const entry of this._entries) {
      let info = JSON.stringify(entry.info) ?? "";
      info = info.replace(/\"/g, '""');
      info = `"${info}"`;
      finalReport += `${entry.testName},${entry.iModelName},${entry.branch},${entry.valueDescription},${entry.value},${entry.date},${info}\n`;
    }
    fs.appendFileSync(fileName, finalReport);
  }
}