/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module Utils
 * utilities that unify operations, especially CRUD operations, on entities
 * for entity-generic operations in the transformer
 */

import * as assert from "assert";
import {
  ConcreteEntityTypes,
  EntityReference,
  IModelError,
} from "@itwin/core-common";
import {
  ConcreteEntity,
  ConcreteEntityProps,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  EntityReferences,
  IModelDb,
  Relationship,
} from "@itwin/core-backend";
import { DbResult, Id64 } from "@itwin/core-bentley";

/** @internal */
export namespace EntityUnifier {
  export function getReadableType(entity: ConcreteEntity) {
    if (entity instanceof Element) return "element";
    else if (entity instanceof ElementAspect) return "element aspect";
    else if (entity instanceof Relationship) return "relationship";
    else return "unknown entity type";
  }

  type EntityUpdater = (entityProps: ConcreteEntityProps) => void;

  /** needs to return a widened type otherwise typescript complains when result is used with a narrow type */
  export function updaterFor(db: IModelDb, entity: ConcreteEntity) {
    if (entity instanceof Element)
      return db.elements.updateElement.bind(db.elements) as EntityUpdater;
    else if (entity instanceof Relationship)
      return db.relationships.updateInstance.bind(
        db.relationships
      ) as EntityUpdater;
    else if (entity instanceof ElementAspect)
      return db.elements.updateAspect.bind(db.elements) as EntityUpdater;
    else
      assert(
        false,
        `unreachable; entity was '${entity.constructor.name}' not an Element, Relationship, or ElementAspect`
      );
  }

  export function exists(
    db: IModelDb,
    arg: { entity: ConcreteEntity } | { entityReference: EntityReference }
  ) {
    const [type, id] =
      "entityReference" in arg
        ? EntityReferences.split(arg.entityReference)
        : [undefined, arg.entity.id];
    const classFullName =
      "entityReference" in arg
        ? ConcreteEntityTypes.toBisCoreRootClassFullName(type!)
        : `[${arg.entity.schemaName}].[${arg.entity.className}]`;

    if (id === undefined || Id64.isInvalid(id)) return false;

    return db.withPreparedStatement(
      `SELECT 1 FROM ${classFullName} WHERE ECInstanceId=?`,
      (stmt) => {
        stmt.bindId(1, id);
        const matchesResult = stmt.step();
        if (matchesResult === DbResult.BE_SQLITE_ROW) return true;
        if (matchesResult === DbResult.BE_SQLITE_DONE) return false;
        else throw new IModelError(matchesResult, "query failed");
      }
    );
  }
}
