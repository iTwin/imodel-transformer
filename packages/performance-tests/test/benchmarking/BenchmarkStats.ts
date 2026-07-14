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
  /** Per-schema export times in ms (indexed by schema name) */
  schemaExportTimes: Map<string, number>;
  /** Total wall-clock time for process() in ms */
  processTotalMs: number;
  /** Time spent in onExportElement calls in ms */
  exportElementMs: number;
  /** Number of elements exported */
  exportElementCount: number;
  /** Time spent in onExportRelationship calls in ms */
  exportRelationshipMs: number;
  /** Number of relationships exported */
  exportRelationshipCount: number;
  /** Time spent in onInsertElement calls in ms */
  importInsertElementMs: number;
  /** Number of elements inserted */
  importInsertElementCount: number;
  /** Time spent in onInsertRelationship calls in ms */
  importInsertRelationshipMs: number;
  /** Number of relationships inserted */
  importInsertRelationshipCount: number;
}

export function createEmptyStats(): BenchmarkStats {
  return {
    schemaTotalMs: 0,
    schemaCount: 0,
    schemaExportTimes: new Map(),
    processTotalMs: 0,
    exportElementMs: 0,
    exportElementCount: 0,
    exportRelationshipMs: 0,
    exportRelationshipCount: 0,
    importInsertElementMs: 0,
    importInsertElementCount: 0,
    importInsertRelationshipMs: 0,
    importInsertRelationshipCount: 0,
  };
}

export function printBenchmarkStats(stats: BenchmarkStats): void {
  const separator = "═".repeat(60);
  const line = "─".repeat(60);

  // eslint-disable-next-line no-console
  const log = console.log.bind(console);

  log(`\n╔${separator}╗`);
  log(`║  Quick Perf Benchmark Results${" ".repeat(30)}║`);
  log(`╠${separator}╣`);

  log(`║  Schema Processing${" ".repeat(41)}║`);
  log(`║${line}║`);
  log(
    `║    Total time:          ${stats.schemaTotalMs.toFixed(2).padStart(10)} ms${" ".repeat(21)}║`
  );
  log(
    `║    Schemas exported:    ${String(stats.schemaCount).padStart(10)}${" ".repeat(24)}║`
  );
  if (stats.schemaCount > 0) {
    const avgPerSchema = stats.schemaTotalMs / stats.schemaCount;
    log(
      `║    Avg per schema:      ${avgPerSchema.toFixed(2).padStart(10)} ms${" ".repeat(21)}║`
    );
  }
  log(`║${line}║`);

  log(`║  Element/Relationship Processing${" ".repeat(27)}║`);
  log(`║${line}║`);
  log(
    `║    process() total:     ${stats.processTotalMs.toFixed(2).padStart(10)} ms${" ".repeat(21)}║`
  );
  log(
    `║    Export elements:     ${stats.exportElementMs.toFixed(2).padStart(10)} ms  (${stats.exportElementCount} items)${" ".repeat(Math.max(0, 10 - String(stats.exportElementCount).length))}║`
  );
  log(
    `║    Export relationships:${stats.exportRelationshipMs.toFixed(2).padStart(10)} ms  (${stats.exportRelationshipCount} items)${" ".repeat(Math.max(0, 10 - String(stats.exportRelationshipCount).length))}║`
  );
  log(
    `║    Import elements:     ${stats.importInsertElementMs.toFixed(2).padStart(10)} ms  (${stats.importInsertElementCount} items)${" ".repeat(Math.max(0, 10 - String(stats.importInsertElementCount).length))}║`
  );
  log(
    `║    Import relationships:${stats.importInsertRelationshipMs.toFixed(2).padStart(10)} ms  (${stats.importInsertRelationshipCount} items)${" ".repeat(Math.max(0, 10 - String(stats.importInsertRelationshipCount).length))}║`
  );
  if (stats.exportElementCount > 0) {
    const avgPerElement = stats.processTotalMs / stats.exportElementCount;
    log(`║${line}║`);
    log(
      `║    Avg per element:     ${avgPerElement.toFixed(4).padStart(10)} ms${" ".repeat(21)}║`
    );
  }

  log(`╚${separator}╝\n`);

  // Print per-schema breakdown if available
  if (stats.schemaExportTimes.size > 0) {
    log(`  Per-schema export times:`);
    log(`  ${"─".repeat(50)}`);
    const sorted = [...stats.schemaExportTimes.entries()].sort(
      (a, b) => b[1] - a[1]
    );
    for (const [name, ms] of sorted) {
      log(`    ${name.padEnd(35)} ${ms.toFixed(2).padStart(8)} ms`);
    }
    log(`  ${"─".repeat(50)}\n`);
  }
}
