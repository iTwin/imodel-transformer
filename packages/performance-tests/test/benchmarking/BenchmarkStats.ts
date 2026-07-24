/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/** Collected performance statistics from a benchmarked transformation run. */
export interface BenchmarkStats {
  /** Total wall-clock time for processSchemas() in ms */
  schemaTotalMs: number;
  /** Number of schemas exported during processSchemas() */
  schemaCount: number;
  /** Total wall-clock time for process() in ms */
  processTotalMs: number;
  /** Number of elements exported */
  exportElementCount: number;
  /** Number of relationships exported */
  exportRelationshipCount: number;
}

export function createEmptyStats(): BenchmarkStats {
  return {
    schemaTotalMs: 0,
    schemaCount: 0,
    processTotalMs: 0,
    exportElementCount: 0,
    exportRelationshipCount: 0,
  };
}

export function printBenchmarkStats(stats: BenchmarkStats): void {
  const line = "─".repeat(50);

  // eslint-disable-next-line no-console
  const log = console.log.bind(console);

  log(`\n  Quick Perf Benchmark Results`);
  log(`  ${line}`);
  log(`    Schemas exported:       ${stats.schemaCount}`);
  log(`    processSchemas():       ${stats.schemaTotalMs.toFixed(2)} ms`);
  log(`  ${line}`);
  log(`    Elements exported:      ${stats.exportElementCount}`);
  log(`    Relationships exported: ${stats.exportRelationshipCount}`);
  log(`    process():              ${stats.processTotalMs.toFixed(2)} ms`);
  if (stats.exportElementCount > 0) {
    const avgPerElement = stats.processTotalMs / stats.exportElementCount;
    log(`    Avg per element:        ${avgPerElement.toFixed(4)} ms`);
  }
  log(`  ${line}\n`);
}
