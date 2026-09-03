/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

export interface CleanupTask {
  name: string;
  run(): void | Promise<void>;
}

interface NamedCleanupError {
  name: string;
  error: unknown;
}

function toError({ name, error }: NamedCleanupError): Error {
  return new Error(`Cleanup failed: ${name}`, { cause: error });
}

async function collectCleanupErrors(
  cleanupTasks: CleanupTask[]
): Promise<Error[]> {
  const errors: NamedCleanupError[] = [];
  for (const cleanupTask of cleanupTasks) {
    try {
      await cleanupTask.run();
    } catch (error) {
      errors.push({ name: cleanupTask.name, error });
    }
  }
  return errors.map(toError);
}

export async function runCleanupTasks(
  cleanupTasks: CleanupTask[]
): Promise<void> {
  const errors = await collectCleanupErrors(cleanupTasks);
  if (errors.length === 1) {
    throw errors[0];
  }
  if (errors.length > 1) {
    throw new AggregateError(errors, "Multiple cleanup tasks failed");
  }
}

export async function throwAfterCleanup(
  primaryError: unknown,
  cleanupTasks: CleanupTask[]
): Promise<never> {
  const cleanupErrors = await collectCleanupErrors(cleanupTasks);
  if (cleanupErrors.length === 0) {
    throw primaryError;
  }
  throw new AggregateError(
    [primaryError, ...cleanupErrors],
    "Operation and cleanup both failed",
    { cause: primaryError }
  );
}

export async function runWithCleanup<T>(
  operation: () => Promise<T>,
  cleanupTasks: CleanupTask[]
): Promise<T> {
  let result!: T;
  try {
    result = await operation();
  } catch (error) {
    await throwAfterCleanup(error, cleanupTasks);
  }
  await runCleanupTasks(cleanupTasks);
  return result;
}
