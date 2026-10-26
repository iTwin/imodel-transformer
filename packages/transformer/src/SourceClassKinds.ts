/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { IModelDb } from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";

/**
 * What instances of a source EC class are to change processing.
 * - `relationship`: a `BisCore:ElementRefersToElements` relationship.
 * - `skippedRelationship`: a `BisCore:ElementDrivesElement` relationship, which change processing ignores.
 * - `externalSourceAspect`: exactly `BisCore:ExternalSourceAspect`, not a subclass.
 * @internal
 */
export type SourceClassKind =
  | "codeSpec"
  | "model"
  | "element"
  | "aspect"
  | "externalSourceAspect"
  | "relationship"
  | "skippedRelationship";

/**
 * The [[SourceClassKind]] of every class in an iModel's current schemas that change processing handles.
 * @internal
 */
export class SourceClassKinds {
  private readonly _kinds: ReadonlyMap<Id64String, SourceClassKind>;

  private constructor(kinds: ReadonlyMap<Id64String, SourceClassKind>) {
    this._kinds = kinds;
  }

  public static async query(iModel: IModelDb): Promise<SourceClassKinds> {
    const classIds = async (ecsql: string) => {
      const ids: Id64String[] = [];
      for await (const row of iModel.createQueryReader(ecsql, undefined, {
        usePrimaryConn: true,
      }))
        ids.push(row.ECInstanceId);
      return ids;
    };
    const subclassIds = async (baseClass: string) =>
      classIds(
        `SELECT ECInstanceId FROM ECDbMeta.ECClassDef WHERE ECInstanceId IS (${baseClass})`
      );
    const [
      codeSpecs,
      models,
      elements,
      uniqueAspects,
      multiAspects,
      externalSourceAspects,
      relationships,
      skippedRelationships,
    ] = await Promise.all([
      subclassIds("BisCore.CodeSpec"),
      subclassIds("BisCore.Model"),
      subclassIds("BisCore.Element"),
      subclassIds("BisCore.ElementUniqueAspect"),
      subclassIds("BisCore.ElementMultiAspect"),
      classIds(
        "SELECT c.ECInstanceId FROM ECDbMeta.ECClassDef c JOIN ECDbMeta.ECSchemaDef s ON c.Schema.Id = s.ECInstanceId WHERE s.Name = 'BisCore' AND c.Name = 'ExternalSourceAspect'"
      ),
      subclassIds("BisCore.ElementRefersToElements"),
      subclassIds("BisCore.ElementDrivesElement"),
    ]);

    const kinds = new Map<Id64String, SourceClassKind>();
    // Later entries refine earlier ones: ExternalSourceAspect is an aspect, and ElementDrivesElement is an ElementRefersToElements.
    const entries: [Id64String[], SourceClassKind][] = [
      [codeSpecs, "codeSpec"],
      [models, "model"],
      [elements, "element"],
      [uniqueAspects, "aspect"],
      [multiAspects, "aspect"],
      [externalSourceAspects, "externalSourceAspect"],
      [relationships, "relationship"],
      [skippedRelationships, "skippedRelationship"],
    ];
    for (const [ids, kind] of entries)
      for (const id of ids) kinds.set(id, kind);
    return new SourceClassKinds(kinds);
  }

  /** Returns undefined for classes that change processing doesn't handle and for classes missing from the schemas. */
  public kindOf(ecClassId: Id64String): SourceClassKind | undefined {
    return this._kinds.get(ecClassId);
  }
}
