/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

export const changesetScanPass = {
  changedInstanceIds: "ChangedInstanceIds.initialize",
  processChangesets: "IModelTransformer.processChangesets",
  singleScanner: "ChangesetScanner",
} as const;

export type ChangesetScanPass =
  (typeof changesetScanPass)[keyof typeof changesetScanPass];

export interface ChangesetScanPassMetrics {
  fileOpens: number;
  fileScans: number;
  unifiedRowCount: number;
  deletionRecordCount: number;
  wallTimeMs: number;
  filePaths: string[];
}

export interface ChangesetScanRunMetrics {
  wallTimeMs?: number;
  cpuUserMicros?: number;
  cpuSystemMicros?: number;
  rssBytesBefore?: number;
  rssBytesAfter?: number;
}

export interface ChangesetScanMetricsSnapshot {
  run?: ChangesetScanRunMetrics;
  passes: Record<string, ChangesetScanPassMetrics>;
}

interface ActivePass {
  metrics: ChangesetScanPassMetrics;
  startTime: number;
}

export class ChangesetScanMetrics {
  private readonly _passes = new Map<string, ActivePass>();
  private _runStartTime?: number;
  private _runCpuUsage?: NodeJS.CpuUsage;
  private _runRssBefore?: number;
  private _runMetrics?: ChangesetScanRunMetrics;

  public startRun(): void {
    this._runStartTime = performance.now();
    this._runCpuUsage = process.cpuUsage();
    this._runRssBefore = process.memoryUsage().rss;
  }

  public finishRun(): void {
    if (this._runStartTime === undefined || this._runCpuUsage === undefined)
      return;

    const cpuUsage = process.cpuUsage(this._runCpuUsage);
    this._runMetrics = {
      wallTimeMs: performance.now() - this._runStartTime,
      cpuUserMicros: cpuUsage.user,
      cpuSystemMicros: cpuUsage.system,
      rssBytesBefore: this._runRssBefore,
      rssBytesAfter: process.memoryUsage().rss,
    };
  }

  public startPass(pass: string): void {
    const existingPass = this._passes.get(pass);
    if (existingPass !== undefined)
      throw new Error(`Changeset scan pass '${pass}' is already active.`);

    this._passes.set(pass, {
      metrics: {
        fileOpens: 0,
        fileScans: 0,
        unifiedRowCount: 0,
        deletionRecordCount: 0,
        wallTimeMs: 0,
        filePaths: [],
      },
      startTime: performance.now(),
    });
  }

  public recordFileOpen(pass: string, filePath: string): void {
    const activePass = this.getPass(pass);
    activePass.metrics.fileOpens++;
    activePass.metrics.filePaths.push(filePath);
  }

  public recordFileScan(pass: string): void {
    this.getPass(pass).metrics.fileScans++;
  }

  public recordUnifiedRows(pass: string, count: number): void {
    this.getPass(pass).metrics.unifiedRowCount += count;
  }

  public recordDeletionRecords(pass: string, count: number): void {
    this.getPass(pass).metrics.deletionRecordCount += count;
  }

  public finishPass(pass: string): void {
    const activePass = this.getPass(pass);
    activePass.metrics.wallTimeMs += performance.now() - activePass.startTime;
  }

  public snapshot(): ChangesetScanMetricsSnapshot {
    return {
      run: this._runMetrics,
      passes: Object.fromEntries(
        [...this._passes].map(([pass, activePass]) => [
          pass,
          {
            ...activePass.metrics,
            filePaths: [...activePass.metrics.filePaths],
          },
        ])
      ),
    };
  }

  private getPass(pass: string): ActivePass {
    const activePass = this._passes.get(pass);
    if (activePass === undefined)
      throw new Error(`Changeset scan pass '${pass}' is not active.`);
    return activePass;
  }
}

let activeMetrics: ChangesetScanMetrics | undefined;

export function getActiveChangesetScanMetrics():
  | ChangesetScanMetrics
  | undefined {
  return activeMetrics;
}

export async function withChangesetScanInstrumentation<T>(
  metrics: ChangesetScanMetrics,
  callback: () => Promise<T>
): Promise<T> {
  const previousMetrics = activeMetrics;
  activeMetrics = metrics;
  metrics.startRun();
  try {
    return await callback();
  } finally {
    metrics.finishRun();
    activeMetrics = previousMetrics;
  }
}
