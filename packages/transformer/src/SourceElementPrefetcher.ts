/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */
import * as child_process from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Id64String, Logger } from "@itwin/core-bentley";
import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
} from "@itwin/core-backend";
import { ElementProps } from "@itwin/core-common";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";

const loggerCategory = TransformerLoggerCategory.IModelExporter;

/** Options for [[SourceElementPrefetcher]].
 * @alpha
 */
export interface SourceElementPrefetchOptions {
  /** Number of elements per prefetch message. @default 50 */
  batchSize?: number;
  /** Maximum number of unacknowledged batches the prefetch process may have
   * in flight. This is the credit window that bounds memory usage.
   * @default 8
   */
  maxPendingBatches?: number;
  /** Maximum number of prefetched elements held in the parent-side cache
   * before acknowledgements (and therefore further prefetching) are paused.
   * @default 4 * batchSize * maxPendingBatches
   */
  maxCacheEntries?: number;
}

interface BatchMessage {
  type: "batch";
  elems: ElementProps[];
}
interface DoneMessage {
  type: "done";
}
interface ErrorMessage {
  type: "error";
  error: string;
}
type PrefetchMessage = BatchMessage | DoneMessage | ErrorMessage;

/**
 * Prefetches source element props in a child process so that the main thread
 * does not need to perform synchronous source-db reads between target-db
 * writes (see issue #9, "don't wait for targetDb writes to perform sourceDb
 * reads").
 *
 * The child process opens the (read-only, file-backed) source iModel itself,
 * walks all elements in ECInstanceId order, and streams their props to the
 * parent in bounded batches with credit-based flow control: at most
 * `maxPendingBatches` batches are unacknowledged at any time and the parent
 * defers acknowledgements while its cache holds more than `maxCacheEntries`
 * elements, so memory use is bounded regardless of iModel size.
 *
 * The cache is purely speculative: a miss (or a disabled/crashed prefetcher)
 * falls back to the ordinary synchronous `getElement` read, so behavior is
 * identical with or without prefetching.
 * @alpha
 */
export class SourceElementPrefetcher {
  private readonly _sourceDb: IModelDb;
  private readonly _batchSize: number;
  private readonly _maxPendingBatches: number;
  private readonly _maxCacheEntries: number;
  private readonly _cache = new Map<Id64String, ElementProps>();
  private _child: child_process.ChildProcess | undefined;
  private _scriptPath: string | undefined;
  private _deferredAcks = 0;
  private _hits = 0;
  private _misses = 0;
  private _batches = 0;
  private _takesSinceYield = 0;
  private _lastSkipTo: bigint | undefined;
  private _consumerPos: bigint | undefined;
  private _startedAt = 0;
  private _firstBatchMs: number | undefined;

  public constructor(
    sourceDb: IModelDb,
    options: SourceElementPrefetchOptions = {}
  ) {
    this._sourceDb = sourceDb;
    this._batchSize = options.batchSize ?? 50;
    this._maxPendingBatches = options.maxPendingBatches ?? 8;
    this._maxCacheEntries =
      options.maxCacheEntries ?? 4 * this._batchSize * this._maxPendingBatches;
  }

  /** Whether prefetching supports the given source iModel: it must be a
   * local, file-backed snapshot or standalone db that a second process can
   * open read-only.
   */
  public static isSupported(sourceDb: IModelDb): boolean {
    return (
      (sourceDb.isSnapshotDb() || sourceDb.isStandaloneDb()) &&
      sourceDb.pathName !== "" &&
      fs.existsSync(sourceDb.pathName)
    );
  }

  /** Cache hit/miss statistics, for diagnostics. */
  public get stats(): {
    hits: number;
    misses: number;
    batches: number;
    firstBatchMs: number | undefined;
  } {
    return {
      hits: this._hits,
      misses: this._misses,
      batches: this._batches,
      firstBatchMs: this._firstBatchMs,
    };
  }

  /** Start the prefetch child process. */
  public start(wantGeometry: boolean): void {
    if (this._child !== undefined) return;
    this._scriptPath = path.join(
      fs.mkdtempSync(path.join(os.tmpdir(), "itwin-transformer-prefetch-")),
      "prefetchSourceElements.js"
    );
    fs.writeFileSync(this._scriptPath, prefetchWorkerScript);
    this._child = child_process.fork(this._scriptPath, [], {
      serialization: "advanced",
      stdio: ["ignore", "ignore", "inherit", "ipc"],
      env: {
        ...process.env,
        PREFETCH_BACKEND: require.resolve("@itwin/core-backend"),
        PREFETCH_SOURCE: this._sourceDb.pathName,
        PREFETCH_WANT_GEOMETRY: wantGeometry ? "1" : "0",
        PREFETCH_BATCH: String(this._batchSize),
        PREFETCH_MAX_PENDING: String(this._maxPendingBatches),
      },
    });
    this._startedAt = Date.now();
    this._child.on("message", (msg: PrefetchMessage) => {
      if (msg.type === "batch") {
        this._batches++;
        if (this._firstBatchMs === undefined)
          this._firstBatchMs = Date.now() - this._startedAt;
        for (const props of msg.elems) {
          if (props.id !== undefined) this._cache.set(props.id, props);
        }
        this._deferredAcks++;
        this.tryAck();
      } else if (msg.type === "error") {
        Logger.logWarning(
          loggerCategory,
          `source element prefetch process failed, falling back to synchronous reads: ${msg.error}`
        );
        this.shutdownChild();
      }
    });
    this._child.on("error", (err) => {
      Logger.logWarning(
        loggerCategory,
        `source element prefetch process error, falling back to synchronous reads: ${err.message}`
      );
      this.shutdownChild();
    });
    this._child.on("exit", (code, signal) => {
      Logger.logTrace(
        loggerCategory,
        `source element prefetch child exited: code=${code} signal=${signal}`
      );
      this._child = undefined;
    });
  }

  private tryAck(): void {
    while (
      this._deferredAcks > 0 &&
      this._cache.size <= this._maxCacheEntries &&
      this._child !== undefined &&
      this._child.connected
    ) {
      this._child.send({ type: "ack" });
      this._deferredAcks--;
    }
  }

  /** Take a prefetched element out of the cache and construct it, or return
   * `undefined` on a cache miss (the caller should then perform the ordinary
   * synchronous read).
   *
   * Periodically yields to the event loop: the export pipeline can run for
   * long stretches without leaving the microtask queue, in which case the
   * child's IPC messages would never be delivered. Awaiting `setImmediate`
   * forces a full event-loop turn (including the poll phase that drains IPC).
   */
  public async takeElement<T extends Element>(
    id: Id64String
  ): Promise<T | undefined> {
    if (this._child !== undefined && ++this._takesSinceYield >= 16) {
      this._takesSinceYield = 0;
      await new Promise((resolve) => setImmediate(resolve));
    }
    return this.tryTakeElement<T>(id);
  }

  /** Synchronous cache take; see [[takeElement]] for the yielding variant. */
  public tryTakeElement<T extends Element>(id: Id64String): T | undefined {
    try {
      const position = BigInt(id);
      if (this._consumerPos === undefined || position > this._consumerPos)
        this._consumerPos = position;
    } catch {
      // non-numeric id: position tracking unaffected
    }
    const props = this._cache.get(id);
    if (props === undefined) {
      this._misses++;
      this.resyncAfterMiss();
      return undefined;
    }
    this._cache.delete(id);
    this._hits++;
    this.tryAck();
    // constructs the same way getElement does after its native read
    return this._sourceDb.elements.createElement<T>(props);
  }

  /** A miss usually means the consumer overtook the prefetch stream. Tell the
   * child to skip ahead and, if stale entries are blocking acknowledgements,
   * evict everything behind the consumer's position, otherwise the credit
   * window would stay exhausted and prefetching would permanently stall.
   */
  private resyncAfterMiss(): void {
    const position = this._consumerPos;
    if (position === undefined || this._child === undefined) return;
    if (
      this._child.connected &&
      (this._lastSkipTo === undefined || position > this._lastSkipTo)
    ) {
      this._lastSkipTo = position;
      this._child.send({ type: "skipTo", id: `0x${position.toString(16)}` });
    }
    // only pay the eviction scan when stale entries are actually blocking acks
    if (this._deferredAcks > 0 && this._cache.size > this._maxCacheEntries) {
      for (const cachedId of this._cache.keys()) {
        try {
          if (BigInt(cachedId) <= position) this._cache.delete(cachedId);
        } catch {
          // leave unparseable ids alone
        }
      }
      this.tryAck();
    }
  }

  private shutdownChild(): void {
    this._cache.clear();
    if (this._child !== undefined) {
      this._child.removeAllListeners();
      this._child.kill();
      this._child = undefined;
    }
  }

  /** Stop the child process and release resources. Safe to call multiple times. */
  public dispose(): void {
    this.shutdownChild();
    if (this._scriptPath !== undefined) {
      try {
        fs.rmSync(path.dirname(this._scriptPath), {
          recursive: true,
          force: true,
        });
      } catch {
        // ignore temp cleanup failures
      }
      this._scriptPath = undefined;
    }
    Logger.logInfo(
      loggerCategory,
      `source element prefetch stats: ${this._hits} hits, ${this._misses} misses`
    );
  }
}

/**
 * The child-process worker. Kept as source text and written to a temp file so
 * that it works both from the compiled package and when the transformer is
 * run directly from TypeScript sources (e.g. under vitest).
 */
const prefetchWorkerScript = `"use strict";
const { IModelHost, SnapshotDb, StandaloneDb } = require(process.env.PREFETCH_BACKEND);
const BATCH = Number(process.env.PREFETCH_BATCH);
const WANT_GEOM = process.env.PREFETCH_WANT_GEOMETRY === "1";
const MAX_PENDING = Number(process.env.PREFETCH_MAX_PENDING);
let credits = MAX_PENDING;
let creditWaiter;
let skipTo;
process.on("message", (m) => {
  if (!m) return;
  if (m.type === "ack") {
    credits++;
    if (creditWaiter) {
      const wake = creditWaiter;
      creditWaiter = undefined;
      wake();
    }
  } else if (m.type === "skipTo") {
    // the consumer overtook us: skip everything at or before its position
    try {
      const pos = BigInt(m.id);
      if (skipTo === undefined || pos > skipTo) skipTo = pos;
    } catch {}
  }
});
async function acquireCredit() {
  while (credits <= 0) await new Promise((resolve) => (creditWaiter = resolve));
  credits--;
}
(async () => {
  const t0 = Date.now();
  // separate profile: the parent process holds the default profile lock
  await IModelHost.startup({ profileName: "transformer-prefetch-" + process.pid });
  let db;
  try {
    db = SnapshotDb.openFile(process.env.PREFETCH_SOURCE, { key: "prefetch-source" });
  } catch {
    db = StandaloneDb.openFile(process.env.PREFETCH_SOURCE, 1 /* OpenMode.Readonly */);
  }
  if (process.env.PREFETCH_DEBUG) console.error("[prefetch-child] db open at " + (Date.now() - t0) + "ms, skipTo=" + skipTo);
  const reader = db.createQueryReader(
    "SELECT ECInstanceId FROM bis.Element ORDER BY ECInstanceId",
    undefined,
    { usePrimaryConn: true }
  );
  let batch = [];
  let sent = 0;
  const sendBatch = async () => {
    if (batch.length === 0) return;
    await acquireCredit();
    process.send({ type: "batch", elems: batch });
    sent += batch.length;
    batch = [];
  };
  while (await reader.step()) {
    const id = reader.current[0];
    if (skipTo !== undefined) {
      try {
        if (BigInt(id) <= skipTo) continue;
      } catch {}
    }
    try {
      const el = db.elements.getElement({
        id,
        wantGeometry: WANT_GEOM,
        wantBRepData: WANT_GEOM,
      });
      batch.push(el.toJSON());
    } catch {
      // skip: the parent falls back to a synchronous read for this element
    }
    if (batch.length >= BATCH) await sendBatch();
  }
  await sendBatch();
  if (process.env.PREFETCH_DEBUG) console.error("[prefetch-child] done at " + (Date.now() - t0) + "ms, sent=" + sent + ", skipTo=" + skipTo);
  process.send({ type: "done" });
  db.close();
  await IModelHost.shutdown();
  process.exit(0);
})().catch((err) => {
  try {
    process.send({ type: "error", error: String((err && err.message) || err) });
  } catch {}
  process.exit(1);
});
`;
