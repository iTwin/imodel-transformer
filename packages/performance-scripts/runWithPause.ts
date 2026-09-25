/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { createInterface } from "node:readline/promises";
import { runWithCleanup } from "./runWithCleanup";

export interface PauseProfilerOptions {
  label?: string;
  waitForInput?(message: string): Promise<void>;
  write?(message: string): void;
}

export async function waitForInput(message: string): Promise<void> {
  if (!process.stdin.isTTY || !process.stdout.isTTY)
    throw new Error("Pause profiling requires an interactive terminal");
  const readline = createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  try {
    await readline.question(`${message}\nPress Enter to continue.`);
  } finally {
    readline.close();
  }
}

export async function runWithPause<T>(
  operation: () => Promise<T>,
  {
    label = "profiled operation",
    waitForInput: pause = waitForInput,
    write = (message) => process.stdout.write(message),
  }: PauseProfilerOptions = {}
): Promise<T> {
  await pause(
    `Ready to profile ${label}; PID=${process.pid}. Attach and start the profiler now.`
  );
  write(`PROFILE START ${label} PID=${process.pid}\n`);
  return runWithCleanup(operation, [
    {
      name: "finish pause profiling interval",
      run: async () => {
        write(`PROFILE END ${label} PID=${process.pid}\n`);
        await pause("Stop or detach the profiler now.");
      },
    },
  ]);
}

export default function RunWithPause(funcData: { object: any; key: string }[]) {
  for (const { object, key } of funcData) {
    const original = object[key];
    object[key] = function (...args: any[]) {
      return runWithPause(
        async () => {
          const result = original.call(this, ...args);
          if (Promise.resolve(result) !== result)
            throw new Error(
              "Pause profiling only supports instrumenting async functions"
            );
          return result;
        },
        { label: key }
      );
    };
  }
}
