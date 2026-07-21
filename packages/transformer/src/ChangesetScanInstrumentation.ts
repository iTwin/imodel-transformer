/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/** Stable names for the changeset passes measured by the test instrumentation. */
export const changesetScanPass = {
  changedInstanceIds: "ChangedInstanceIds.initialize",
  processChangesets: "IModelTransformer.processChangesets",
  singleScanner: "ChangesetScanner",
} as const;

/** Name of a measured changeset scan pass. */
export type ChangesetScanPass =
  (typeof changesetScanPass)[keyof typeof changesetScanPass];

/** Counters and elapsed time collected for one changeset scan pass. */
export interface ChangesetScanPassMetrics {
  /** Number of changeset files opened. */
  fileOpens: number;
  /** Number of changeset files scanned to completion. */
  fileScans: number;
  /** Number of rows emitted by the change unifier. */
  unifiedRowCount: number;
  /** Number of deletion records retained for transformer processing. */
  deletionRecordCount: number;
  /** Elapsed wall time across this pass, in milliseconds. */
  wallTimeMs: number;
  /** Changeset paths opened by this pass, in scan order. */
  filePaths: string[];
}

/** Process measurements collected around an instrumented operation. */
export interface ChangesetScanRunMetrics {
  /** Total elapsed wall time, in milliseconds. */
  wallTimeMs?: number;
  /** User CPU time consumed, in microseconds. */
  cpuUserMicros?: number;
  /** System CPU time consumed, in microseconds. */
  cpuSystemMicros?: number;
  /** Resident set size before the operation, in bytes. */
  rssBytesBefore?: number;
  /** Resident set size after the operation, in bytes. */
  rssBytesAfter?: number;
}

/** Immutable representation of the measurements collected so far. */
export interface ChangesetScanMetricsSnapshot {
  /** Process measurements, available after the run finishes. */
  run?: ChangesetScanRunMetrics;
  /** Measurements keyed by scan pass name. */
  passes: Record<string, ChangesetScanPassMetrics>;
}

interface ActivePass {
  metrics: ChangesetScanPassMetrics;
  startTime: number;
}

/** Collects deterministic scan counters and optional process measurements for tests. */
export class ChangesetScanMetrics {
  private readonly _passes = new Map<string, ActivePass>();
  private _runStartTime?: number;
  private _runCpuUsage?: NodeJS.CpuUsage;
  private _runRssBefore?: number;
  private _runMetrics?: ChangesetScanRunMetrics;

  /** Starts process-level wall-time, CPU, and resident-memory measurement. */
  public startRun(): void {
    this._runStartTime = performance.now();
    this._runCpuUsage = process.cpuUsage();
    this._runRssBefore = process.memoryUsage().rss;
  }

  /** Finishes process-level measurement if a run is active. */
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

  /** Starts collecting measurements for a named scan pass. */
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

  /** Records a changeset file open for an active pass. */
  public recordFileOpen(pass: string, filePath: string): void {
    const activePass = this.getPass(pass);
    activePass.metrics.fileOpens++;
    activePass.metrics.filePaths.push(filePath);
  }

  /** Records a completed changeset file scan for an active pass. */
  public recordFileScan(pass: string): void {
    this.getPass(pass).metrics.fileScans++;
  }

  /** Adds emitted unifier rows to an active pass. */
  public recordUnifiedRows(pass: string, count: number): void {
    this.getPass(pass).metrics.unifiedRowCount += count;
  }

  /** Adds retained deletion records to an active pass. */
  public recordDeletionRecords(pass: string, count: number): void {
    this.getPass(pass).metrics.deletionRecordCount += count;
  }

  /** Adds elapsed wall time to an active pass. */
  public finishPass(pass: string): void {
    const activePass = this.getPass(pass);
    activePass.metrics.wallTimeMs += performance.now() - activePass.startTime;
  }

  /** Returns a detached copy of the measurements collected so far. */
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

/** Returns the metrics collector installed for the current synchronous context. */
export function getActiveChangesetScanMetrics():
  | ChangesetScanMetrics
  | undefined {
  return activeMetrics;
}

/**
 * Installs a metrics collector while an asynchronous operation runs.
 * The previous collector is restored when the operation completes or throws.
 */
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
