/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export interface ProcessMemorySample {
  readonly peakRssBytes?: number;
  readonly sampleCount: number;
  readonly samplingIntervalMilliseconds: number;
}

export async function readProcessRssBytes(
  pid: number
): Promise<number | undefined> {
  if (process.platform === "linux") {
    try {
      const status = await fs.readFile(`/proc/${pid}/status`, "utf8");
      const match = /^VmRSS:\s+(\d+)\s+kB$/m.exec(status);
      return match === null ? undefined : Number(match[1]) * 1024;
    } catch {
      return undefined;
    }
  }

  if (process.platform === "darwin") {
    try {
      const { stdout } = await execFileAsync("/bin/ps", [
        "-o",
        "rss=",
        "-p",
        String(pid),
      ]);
      const rssKilobytes = Number(stdout.trim());
      return Number.isFinite(rssKilobytes) && rssKilobytes >= 0
        ? rssKilobytes * 1024
        : undefined;
    } catch {
      return undefined;
    }
  }

  return undefined;
}

/** Samples a worker from its parent process so synchronous native work cannot block observation. */
export class ProcessMemorySampler {
  private _inFlight?: Promise<void>;
  private _peakRssBytes?: number;
  private _sampleCount = 0;
  private _timer?: NodeJS.Timeout;

  public constructor(
    private readonly _pid: number,
    private readonly _samplingIntervalMilliseconds: number
  ) {}

  public start(): void {
    if (this._samplingIntervalMilliseconds === 0) return;
    void this.sample();
    this._timer = setInterval(
      () => void this.sample(),
      this._samplingIntervalMilliseconds
    );
    this._timer.unref();
  }

  public async stop(): Promise<ProcessMemorySample> {
    if (this._timer !== undefined) clearInterval(this._timer);
    if (this._inFlight !== undefined) await this._inFlight;
    if (this._samplingIntervalMilliseconds !== 0) await this.sample();
    return {
      peakRssBytes: this._peakRssBytes,
      sampleCount: this._sampleCount,
      samplingIntervalMilliseconds: this._samplingIntervalMilliseconds,
    };
  }

  private async sample(): Promise<void> {
    if (this._inFlight !== undefined) return this._inFlight;
    this._inFlight = (async () => {
      const rssBytes = await readProcessRssBytes(this._pid);
      if (rssBytes === undefined) return;
      this._sampleCount++;
      this._peakRssBytes = Math.max(this._peakRssBytes ?? 0, rssBytes);
    })();
    try {
      await this._inFlight;
    } finally {
      this._inFlight = undefined;
    }
  }
}
