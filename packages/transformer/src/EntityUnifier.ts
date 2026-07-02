/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module Utils
 * utilities that unify operations, especially CRUD operations, on entities
 * for entity-generic operations in the transformer
 */

import {
  ConcreteEntityTypes,
  EntityReference,
  QueryBinder,
} from "@itwin/core-common";
import {
  ConcreteEntity,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  EntityReferences,
  IModelDb,
  Relationship,
} from "@itwin/core-backend";
import { Id64 } from "@itwin/core-bentley";

/** @internal */
export namespace EntityUnifier {
  export function getReadableType(entity: ConcreteEntity) {
    if (entity instanceof Element) return "element";
    else if (entity instanceof ElementAspect) return "element aspect";
    else if (entity instanceof Relationship) return "relationship";
    else return "unknown entity type";
  }

  export async function exists(
    db: IModelDb,
    arg: { entity: ConcreteEntity } | { entityReference: EntityReference }
  ) {
    const [type, id] =
      "entityReference" in arg
        ? EntityReferences.split(arg.entityReference)
        : [undefined, arg.entity.id];
    const classFullName =
      "entityReference" in arg
        ? // eslint-disable-next-line @itwin/no-internal, @typescript-eslint/no-non-null-assertion
          ConcreteEntityTypes.toBisCoreRootClassFullName(type!)
        : `[${arg.entity.schemaName}].[${arg.entity.className}]`;

    if (id === undefined || Id64.isInvalid(id)) return false;

    const query = `SELECT 1 FROM ${classFullName} WHERE ECInstanceId=:id`;
    const params = new QueryBinder().bindId("id", id);
    const reader = db.createQueryReader(query, params, {
      usePrimaryConn: true,
    });
    return reader.step();
  }
}
