/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementAspect,
  ElementMultiAspect,
  ExternalSourceAspect,
  IModelDb,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  ElementAspectProps,
  ExternalSourceAspectProps,
  QueryBinder,
} from "@itwin/core-common";
import { isSameClass } from "./ElementAspectCleanup";

/** Returns the target scope elements: those that own a Scope ExternalSourceAspect.
 * A transformation creates its target scope's Scope aspect before it exports any aspect, so the result includes the current target scope.
 * @internal
 */
export async function queryTargetScopeElementIds(
  db: IModelDb
): Promise<Set<Id64String>> {
  const ids = new Set<Id64String>();
  for await (const row of db.createQueryReader(
    `SELECT Element.Id id FROM ${ExternalSourceAspect.classFullName} WHERE Kind = :scopeKind`,
    new QueryBinder().bindString("scopeKind", ExternalSourceAspect.Kind.Scope),
    { usePrimaryConn: true }
  ))
    ids.add(row.id);
  return ids;
}

/** Whether an aspect is transformer provenance: a Scope ExternalSourceAspect, or an Element or Relationship ExternalSourceAspect scoped to one of `targetScopeElementIds`.
 * Every target scope counts, not just the current one: an element shared by several target scopes, such as one source imported in parts, carries each scope's provenance.
 * Cleanup never deletes transformer provenance.
 * @internal
 */
export function isTransformerProvenanceAspect(
  aspect: ElementAspect,
  targetScopeElementIds: ReadonlySet<Id64String>
): aspect is ExternalSourceAspect {
  if (!(aspect instanceof ExternalSourceAspect)) return false;
  if (aspect.kind === ExternalSourceAspect.Kind.Scope) return true;
  return (
    (aspect.kind === ExternalSourceAspect.Kind.Element ||
      aspect.kind === ExternalSourceAspect.Kind.Relationship) &&
    aspect.scope !== undefined &&
    targetScopeElementIds.has(aspect.scope.id)
  );
}

/** Returns the `importElementMultiAspects` filter for a transformation that includes source provenance.
 * Source provenance cloned from another transformation looks exactly like another target scope's provenance, so a transformer provenance aspect is only eligible for reuse when an incoming ExternalSourceAspect has the same kind, scope, and identifier.
 * The current scope's own provenance is never eligible: the transformer maintains it separately.
 * @internal
 */
export function sourceProvenanceMatchFilter(
  incoming: readonly ElementAspectProps[],
  targetScopeElementIds: ReadonlySet<Id64String>,
  currentScopeElementId: Id64String
): (aspect: ElementMultiAspect) => boolean {
  const incomingKeys = new Set(
    incoming
      .filter((props) => isSameClass(props, ExternalSourceAspect.classFullName))
      .map((props) =>
        externalSourceAspectKey(props as ExternalSourceAspectProps)
      )
  );
  return (aspect) => {
    if (!isTransformerProvenanceAspect(aspect, targetScopeElementIds))
      return true;
    const scopeElementId =
      aspect.kind === ExternalSourceAspect.Kind.Scope
        ? aspect.element.id
        : aspect.scope?.id;
    return (
      scopeElementId !== currentScopeElementId &&
      incomingKeys.has(externalSourceAspectKey(aspect))
    );
  };
}

/** The natural key of an ExternalSourceAspect on its owner. Kinds and IDs never contain `|`, and the identifier comes last, so distinct keys can't collide. */
function externalSourceAspectKey(
  aspect: Pick<ExternalSourceAspectProps, "kind" | "scope" | "identifier">
): string {
  return `${aspect.kind}|${aspect.scope?.id}|${aspect.identifier}`;
}
