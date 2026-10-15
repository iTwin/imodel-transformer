/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Id64String, ITwinError } from "@itwin/core-bentley";
import type { CodeProps, ElementProps } from "@itwin/core-common";
import type { ChangedInstanceIds } from "./IModelExporter";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";

/** Target identity only; model IDs and geometry are resolved during ordinary cloning. @internal */
export type ElementReservationIdentity = Pick<
  ElementProps,
  "classFullName" | "federationGuid" | "code"
>;

/** Compare codes the way a reservation does: stored whitespace matters, a missing value is empty. @internal */
export function isSameReservationCode(a: CodeProps, b: CodeProps): boolean {
  return (
    a.spec === b.spec &&
    a.scope === b.scope &&
    (a.value ?? "") === (b.value ?? "")
  );
}

/** Whether props still carry the class, GUID, and code that were reserved. @internal */
export function isSameReservationIdentity(
  props: ElementReservationIdentity,
  expected: ElementReservationIdentity
): boolean {
  return (
    props.classFullName === expected.classFullName &&
    props.federationGuid === expected.federationGuid &&
    isSameReservationCode(props.code, expected.code)
  );
}

/** Reject a case that preparation cannot plan without writing to the target first. @internal */
export function throwReservationUnsupported(
  sourceId: Id64String,
  reason: string
): never {
  ITwinError.throwError({
    iTwinErrorId: {
      scope: IModelTransformerErrorScope,
      key: IModelTransformerError.ElementReservationUnsupported,
    },
    message: `Cannot prepare reservation identity for ${sourceId}: ${reason}. Preparation cannot insert target dependencies first.`,
  });
}

/** Reject execution that diverged from the reserved plan. @internal */
export function throwReservationPlanChanged(
  sourceId: Id64String,
  reason: string
): never {
  ITwinError.throwError({
    iTwinErrorId: {
      scope: IModelTransformerErrorScope,
      key: IModelTransformerError.ElementReservationPlanChanged,
    },
    message: `Reservation plan changed for ${sourceId}: ${reason}.`,
  });
}

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
