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
  Entity,
  EntityReferences,
  IModelDb,
  Model,
  Relationship,
} from "@itwin/core-backend";
import { Id64, Id64String } from "@itwin/core-bentley";

const bisCoreRootClasses: Record<ConcreteEntityTypes, typeof Entity> = {
  [ConcreteEntityTypes.Model]: Model,
  [ConcreteEntityTypes.Element]: Element,
  [ConcreteEntityTypes.ElementAspect]: ElementAspect,
  [ConcreteEntityTypes.Relationship]: Relationship,
};

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
    let classFullName: string;
    let id: Id64String;
    if ("entityReference" in arg) {
      const [type, entityId] = EntityReferences.split(arg.entityReference);
      classFullName = bisCoreRootClasses[type].classFullName;
      id = entityId;
    } else {
      classFullName = `[${arg.entity.schemaName}].[${arg.entity.className}]`;
      id = arg.entity.id;
    }

    if (Id64.isInvalid(id)) return false;

    const query = `SELECT 1 FROM ${classFullName} WHERE ECInstanceId=:id`;
    const params = new QueryBinder().bindId("id", id);
    const reader = db.createQueryReader(query, params, {
      usePrimaryConn: true,
    });
    return reader.step();
  }
}
