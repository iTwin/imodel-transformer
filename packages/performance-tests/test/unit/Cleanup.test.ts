/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { describe, expect, it, vi } from "vitest";
import { runCleanupTasks, runWithCleanup, throwAfterCleanup } from "../Cleanup";

describe("cleanup", () => {
  it("runs every cleanup task when one fails", async () => {
    const first = vi.fn(() => {
      throw new Error("first cleanup");
    });
    const second = vi.fn();

    await expect(
      runCleanupTasks([
        { name: "first", run: first },
        { name: "second", run: second },
      ])
    ).rejects.toThrow("Cleanup failed: first");
    expect(first).toHaveBeenCalledOnce();
    expect(second).toHaveBeenCalledOnce();
  });

  it("retains the primary failure when cleanup also fails", async () => {
    const primaryError = new Error("primary");
    const cleanupError = new Error("cleanup");

    const error = await throwAfterCleanup(primaryError, [
      {
        name: "resource",
        run: () => {
          throw cleanupError;
        },
      },
    ]).catch((caughtError: unknown) => caughtError);

    expect(error).toBeInstanceOf(AggregateError);
    expect((error as AggregateError).errors[0]).toBe(primaryError);
    expect((error as Error).cause).toBe(primaryError);
    expect((error as AggregateError).errors[1]).toMatchObject({
      cause: cleanupError,
    });
  });

  it("cleans up after a successful operation", async () => {
    const cleanup = vi.fn();

    await expect(
      runWithCleanup(async () => 42, [{ name: "resource", run: cleanup }])
    ).resolves.toBe(42);
    expect(cleanup).toHaveBeenCalledOnce();
  });
});
