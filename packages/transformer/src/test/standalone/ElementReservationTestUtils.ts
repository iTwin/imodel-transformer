/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { type GuidString, OpenMode } from "@itwin/core-bentley";
import { IModelDb, SchemaSync } from "@itwin/core-backend";
import { _nativeDb } from "@itwin/core-backend/lib/cjs/internal/Symbols.js";

/**
 * Enable the real SchemaSync reservation implementation against a local SQLite database.
 *
 * This deliberately replaces only the cloud-access seam and the native SchemaSync-enabled
 * predicate. The reservation table, validation, allocation, and native insert hook remain the
 * backend implementation under test.
 */
export async function useLocalReservations(
  db: IModelDb,
  syncPath: string
): Promise<{
  sync: SchemaSync.SchemaSyncDb;
  readonly calls: number;
  readonly lastError: unknown;
  dispose(): void;
}> {
  const nativeDb = db[_nativeDb];
  const originalNativeDescriptor = Object.getOwnPropertyDescriptor(
    nativeDb,
    "schemaSyncEnabled"
  );
  const originalCloudAccessDescriptor = Object.getOwnPropertyDescriptor(
    SchemaSync,
    "getCloudAccess"
  );

  if (originalCloudAccessDescriptor === undefined)
    throw new Error("SchemaSync.getCloudAccess has no property descriptor");

  const sync = new SchemaSync.SchemaSyncDb();
  let calls = 0;
  let lastError: unknown;
  let disposed = false;

  const access = {
    synchronizeWithCloud: () => undefined,
    close: () => undefined,
    getUri: () => `${syncPath}?vfs=test`,
    container: { hasLocalChanges: false },
    reader: {
      findReservedElement: (federationGuid: GuidString) =>
        sync.findReservedElement(federationGuid),
    },
    writeLocker: {
      reserveElements: async (
        elements: SchemaSync.ProposedElementReservation[]
      ) => {
        calls++;
        try {
          await sync.reserveElements(elements);
          sync.saveChanges();
        } catch (error) {
          lastError = error;
          // Preserve the backend error object. The local sync transaction is independent of
          // the caller-owned iModel transaction and must be abandoned on every failed batch.
          try {
            sync.abandonChanges();
          } catch {
            // Keep the reservation error as the one that escapes this adapter.
          }
          throw error;
        }
      },
    },
  };

  const restoreHooks = (): void => {
    if (originalNativeDescriptor === undefined) {
      if (!Reflect.deleteProperty(nativeDb, "schemaSyncEnabled"))
        throw new Error("Unable to restore native schemaSyncEnabled");
    } else {
      Object.defineProperty(
        nativeDb,
        "schemaSyncEnabled",
        originalNativeDescriptor
      );
    }
    Object.defineProperty(
      SchemaSync,
      "getCloudAccess",
      originalCloudAccessDescriptor
    );
  };

  const cleanup = (): void => {
    let cleanupError: unknown;

    // The target must close while SchemaSync.getCloudAccess still returns the local adapter.
    try {
      if (db.isOpen) db.close();
    } catch (error) {
      cleanupError = error;
    }

    try {
      restoreHooks();
    } catch (error) {
      cleanupError ??= error;
    }

    try {
      if (sync.isOpen) sync.closeDb();
    } catch (error) {
      cleanupError ??= error;
    }

    if (cleanupError instanceof Error) throw cleanupError;
    if (cleanupError !== undefined)
      throw new Error("Local reservation fixture cleanup failed");
  };

  try {
    SchemaSync.SchemaSyncDb.createNewDb(syncPath);
    sync.openDb(syncPath, OpenMode.ReadWrite);

    Object.defineProperty(nativeDb, "schemaSyncEnabled", {
      configurable: true,
      enumerable: false,
      writable: true,
      value: () => true,
    });
    Object.defineProperty(SchemaSync, "getCloudAccess", {
      ...originalCloudAccessDescriptor,
      value: async () =>
        access as unknown as Awaited<
          ReturnType<typeof SchemaSync.getCloudAccess>
        >,
    });

    await db.initializeSharedElementReservations();

    if (!SchemaSync.isEnabled(db))
      throw new Error("Local reservation fixture did not enable SchemaSync");
    if (!db.reservations.isServerBased)
      throw new Error("Local reservation fixture is not server-based");
    if (db.holdsSchemaLock)
      throw new Error(
        "Local reservation fixture unexpectedly holds the schema lock"
      );

    return {
      sync,
      get calls() {
        return calls;
      },
      get lastError() {
        return lastError;
      },
      dispose(): void {
        if (disposed) return;
        disposed = true;
        cleanup();
      },
    };
  } catch (error) {
    try {
      cleanup();
    } catch {
      // Preserve the setup error while still attempting every cleanup step.
    }
    throw error;
  }
}
