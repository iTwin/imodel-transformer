/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { Id64String, Logger } from "@itwin/core-bentley";
import {
  BriefcaseDb,
  ElementRefersToElements,
  IModelDb,
  Relationship,
} from "@itwin/core-backend";
import {
  PropertyType,
  RelationshipClass,
  SchemaContext,
} from "@itwin/ecschema-metadata";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";

const loggerCategory: string = TransformerLoggerCategory.IModelImporter;

/** Property types whose ECSQL row values `Relationships.tryGetInstance` passes to the entity unchanged.
 * Classes with any other property type, such as points, binary, navigation, struct, or array properties, are not preloaded.
 */
const passThroughPropertyTypes: ReadonlySet<PropertyType> = new Set([
  PropertyType.Boolean,
  PropertyType.DateTime,
  PropertyType.Double,
  PropertyType.Integer,
  PropertyType.Integer_Enumeration,
  PropertyType.Long,
  PropertyType.String,
  PropertyType.String_Enumeration,
]);

/** Returned by [[TargetRelationshipLookup.find]] when `Relationships.tryGetInstance` must look up the relationship. */
export const lookUpWithCore = Symbol("lookUpWithCore");

/** A target relationship row read by a class preload. */
interface PreloadedRelationship {
  readonly id: Id64String;
  readonly classFullName: string;
  /** Values of [[PreloadedClass.propertyJsNames]], in the same order. */
  readonly values: readonly unknown[];
}

/** The target relationships of one relationship class and its subclasses, keyed by source and target id. */
interface PreloadedClass {
  readonly propertyJsNames: readonly string[];
  readonly relationships: Map<
    string,
    PreloadedRelationship | typeof lookUpWithCore
  >;
}

const noValues: readonly unknown[] = [];

function sourceAndTargetKey(sourceId: Id64String, targetId: Id64String) {
  return `${sourceId}/${targetId}`;
}

function lowerFirstChar(name: string): string {
  return name.length === 0 ? name : `${name[0].toLowerCase()}${name.slice(1)}`;
}

/** Finds existing target relationships for [IModelImporter.importRelationship]($transformer) without one ECSQL query per relationship.
 *
 * The first lookup of an ElementRefersToElements class reads every target relationship of that class and its subclasses
 * with one polymorphic query on the primary connection, so it sees changes in the current transaction. Later lookups of
 * that class return the same relationship that `Relationships.tryGetInstance(classFullName, { sourceId, targetId })`
 * would construct, or `undefined` when it would find none.
 *
 * Lookups return [[lookUpWithCore]] for any case that the preloaded rows cannot answer exactly: classes outside the
 * ElementRefersToElements hierarchy, classes with properties that core reshapes, classes that cannot be resolved,
 * source and target pairs with more than one relationship, and relationships that the importer has since inserted,
 * updated, or deleted.
 *
 * The importer reports its own relationship writes through [[markWritten]] and [[markDeleted]], and calls [[clear]]
 * after element and model deletes, which can delete relationships through cascades. The lookup also clears itself after
 * schema changes, undo or redo, and applied changesets. It does not see other writes to relationships of a preloaded
 * class, such as writes made directly through the target `EditTxn` during an import, or abandoned changes.
 * @internal
 */
export class TargetRelationshipLookup {
  private readonly _classes = new Map<string, PreloadedClass | undefined>();
  private readonly _deletedIds = new Set<Id64String>();
  private _schemaContext: SchemaContext | undefined;
  private _removeListeners: Array<() => void> = [];
  /** Incremented by [[clear]], so a preload that a clear interrupts is discarded. */
  private _generation = 0;

  public constructor(private readonly _db: IModelDb) {}

  /** Find the target relationship of `classFullName` or one of its subclasses with the specified source and target. */
  public async find(
    classFullName: string,
    sourceId: Id64String,
    targetId: Id64String
  ): Promise<Relationship | undefined | typeof lookUpWithCore> {
    if (
      this._schemaContext !== undefined &&
      this._schemaContext !== this._db.schemaContext
    )
      this.clear();
    let preloaded = this._classes.get(classFullName);
    if (!this._classes.has(classFullName)) {
      const generation = this._generation;
      preloaded = await this.preload(classFullName);
      if (generation !== this._generation) return lookUpWithCore;
      this._classes.set(classFullName, preloaded);
    }
    if (preloaded === undefined) return lookUpWithCore;
    const relationship = preloaded.relationships.get(
      sourceAndTargetKey(sourceId, targetId)
    );
    if (relationship === undefined) return undefined;
    if (
      relationship === lookUpWithCore ||
      this._deletedIds.has(relationship.id)
    )
      return lookUpWithCore;
    const props: Record<string, unknown> = {
      id: relationship.id,
      classFullName: relationship.classFullName,
      sourceId,
      targetId,
    };
    for (let i = 0; i < relationship.values.length; i++) {
      const value = relationship.values[i];
      if (value !== undefined && value !== null)
        props[preloaded.propertyJsNames[i]] = value;
    }
    return this._db.constructEntity<Relationship>(props as any);
  }

  /** Record that the importer inserted or updated a relationship with the specified source and target. */
  public markWritten(sourceId: Id64String, targetId: Id64String): void {
    if (this._classes.size === 0) return;
    const key = sourceAndTargetKey(sourceId, targetId);
    // A written row is looked up with core, because the stored values can differ from the supplied props.
    for (const preloaded of this._classes.values())
      preloaded?.relationships.set(key, lookUpWithCore);
  }

  /** Record that the importer deleted a relationship. */
  public markDeleted(relationshipId: Id64String): void {
    if (this._classes.size > 0) this._deletedIds.add(relationshipId);
  }

  /** Discard all preloaded relationships. Classes are preloaded again when they are next looked up. */
  public clear(): void {
    this._generation++;
    this._classes.clear();
    this._deletedIds.clear();
    this._schemaContext = undefined;
    for (const removeListener of this._removeListeners) removeListener();
    this._removeListeners = [];
  }

  private resolveClass(classFullName: string): RelationshipClass | undefined {
    let relClass;
    try {
      relClass = this._db.schemaContext.getSchemaItemSync(classFullName);
    } catch {
      return undefined;
    }
    if (
      !RelationshipClass.isRelationshipClass(relClass) ||
      !relClass.isSync(ElementRefersToElements.className, "BisCore")
    )
      return undefined;
    return relClass;
  }

  private async preload(
    classFullName: string
  ): Promise<PreloadedClass | undefined> {
    // Resolution results, including unsupported classes, are discarded when the schema context changes.
    this._schemaContext = this._db.schemaContext;
    const relClass = this.resolveClass(classFullName);
    if (relClass === undefined) return undefined;
    const propertyNames: string[] = [];
    for (const property of relClass.getPropertiesSync()) {
      if (!passThroughPropertyTypes.has(property.propertyType))
        return undefined;
      propertyNames.push(property.name);
    }

    this.watchForExternalChanges();
    const startTime = performance.now();
    // Like core's `SELECT * FROM <class>`, this query is polymorphic and returns only the properties of the queried class.
    const selectProperties = propertyNames
      .map((name) => `, [${name}]`)
      .join("");
    const ecsql = `SELECT ECInstanceId, ec_classname(ECClassId, 's:c'), SourceECInstanceId, TargetECInstanceId${selectProperties} FROM [${relClass.schema.name}].[${relClass.name}]`;
    const relationships = new Map<
      string,
      PreloadedRelationship | typeof lookUpWithCore
    >();
    const classFullNames = new Map<string, string>();
    for await (const row of this._db.createQueryReader(ecsql, undefined, {
      usePrimaryConn: true,
    })) {
      const rowClassFullName: string = row[1];
      let interned = classFullNames.get(rowClassFullName);
      if (interned === undefined) {
        interned = rowClassFullName;
        classFullNames.set(interned, interned);
      }
      let values = noValues;
      if (propertyNames.length > 0) {
        const rowValues = new Array<unknown>(propertyNames.length);
        for (let i = 0; i < propertyNames.length; i++)
          rowValues[i] = row[4 + i];
        values = rowValues;
      }
      const key = sourceAndTargetKey(row[2], row[3]);
      // Core returns the first of several matching rows in an order that depends on the query plan, so it decides.
      relationships.set(
        key,
        relationships.has(key)
          ? lookUpWithCore
          : { id: row[0], classFullName: interned, values }
      );
    }
    Logger.logTrace(
      loggerCategory,
      `Preloaded ${relationships.size} target relationship source and target pairs of ${classFullName} in ${Math.round(
        performance.now() - startTime
      )} ms`
    );
    return {
      propertyJsNames: propertyNames.map(lowerFirstChar),
      relationships,
    };
  }

  /** Clear the lookup when the target changes in ways the importer does not report. */
  private watchForExternalChanges(): void {
    if (this._removeListeners.length > 0) return;
    const clear = () => this.clear();
    this._removeListeners.push(this._db.onChangesetApplied.addListener(clear));
    if (this._db instanceof BriefcaseDb) {
      this._removeListeners.push(
        this._db.txns.onChangesApplied.addListener(clear),
        this._db.txns.onAfterUndoRedo.addListener(clear)
      );
    }
  }
}
