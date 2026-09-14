/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import type { ElementProps } from "@itwin/core-common";
import type { ChangedInstanceIds } from "./IModelExporter";

/** Private state shared at exporter and importer boundaries without supported extension hooks. */
export interface ReservationExportContext {
  changes?: ChangedInstanceIds;
  replaying: boolean;
}

const exportContexts = new WeakMap<object, ReservationExportContext>();
const insertionGuards = new WeakMap<object, (props: ElementProps) => void>();

export function getReservationExportContext(
  exporter: object
): ReservationExportContext | undefined {
  return exportContexts.get(exporter);
}

export function setReservationExportContext(
  exporter: object,
  context: ReservationExportContext | undefined
): void {
  if (context === undefined) exportContexts.delete(exporter);
  else exportContexts.set(exporter, context);
}

export function getElementReservationInsertionGuard(
  importer: object
): ((props: ElementProps) => void) | undefined {
  return insertionGuards.get(importer);
}

export function setElementReservationInsertionGuard(
  importer: object,
  guard: ((props: ElementProps) => void) | undefined
): void {
  if (guard === undefined) insertionGuards.delete(importer);
  else insertionGuards.set(importer, guard);
}
