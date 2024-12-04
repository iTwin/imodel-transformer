/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import {
  DbResult,
  Logger,
  StopWatch,
  TupleKeyedMap,
} from "@itwin/core-bentley";
import {
  ConcreteEntityTypes,
  IModelError,
  RelTypeInfo,
} from "@itwin/core-common";
import {
  ECClass,
  Mixin,
  RelationshipClass,
  RelationshipConstraint,
  Schema,
  SchemaKey,
  SchemaLoader,
  StrengthDirection,
} from "@itwin/ecschema-metadata";
import * as assert from "assert";
import { IModelDb } from "@itwin/core-backend";
import { TransformerLoggerCategory } from "./TransformerLoggerCategory";

/** The context for transforming a *source* Element to a *target* Element and remapping internal identifiers to the target iModel.
 * @internal
 */
export class SchemaNotInCacheErr extends Error {
  public constructor() {
    super("Schema was not in cache, initialize that schema");
  }
}

/**
 * A cache of the entity types referenced by navprops in ecchemas, as well as the source and target entity types of
 * The transformer needs the referenced type to determine how to resolve references.
 *
 * Using multiple of these usually performs redundant computation, for static schemas at least. A possible future optimization
 * would be to seed the computation from a global cache of non-dynamic schemas, but dynamic schemas can collide willy-nilly
 * @internal
 */
export class ECReferenceTypesCache {
  /** nesting based tuple map keyed by qualified property path tuple [schemaName, className, propName] */
  private _propQualifierToRefType = new TupleKeyedMap<
    [string, string, string],
    ConcreteEntityTypes
  >();
  private _relClassNameEndToRefTypes = new TupleKeyedMap<
    [string, string],
    RelTypeInfo
  >();
  private _initedSchemas = new Map<string, SchemaKey>();

  private _cumulativeTimeSpentCheckingForQueryView: number = 0;
  private _classesCheckedForQueryView: number = 0;

  private static bisRootClassToRefType: Record<
    string,
    ConcreteEntityTypes | undefined
  > = {
    /* eslint-disable quote-props, @typescript-eslint/naming-convention */
    Element: ConcreteEntityTypes.Element,
    Model: ConcreteEntityTypes.Model,
    ElementAspect: ConcreteEntityTypes.ElementAspect,
    ElementRefersToElements: ConcreteEntityTypes.Relationship,
    ElementDrivesElement: ConcreteEntityTypes.Relationship,
    // code spec is technically a potential root class but it is ignored currently
    // see [ConcreteEntityTypes]($common)
    /* eslint-enable quote-props, @typescript-eslint/naming-convention */
  };

  private async getRootBisClass(ecclass: ECClass) {
    let bisRootForConstraint: ECClass = ecclass;
    await ecclass.traverseBaseClasses((baseClass) => {
      // The depth first traversal will descend all the way to the root class before making any lateral traversal
      // of mixin hierarchies, (or if the constraint is a mixin, it will traverse to the root of the mixin hierarchy)
      // Once we see that we've moved laterally, we can terminate early
      const isFirstTest = bisRootForConstraint === ecclass;
      const traversalSwitchedRootPath =
        baseClass.name !== bisRootForConstraint.baseClass?.name;
      const stillTraversingRootPath = isFirstTest || !traversalSwitchedRootPath;
      if (!stillTraversingRootPath) return true; // stop traversal early
      bisRootForConstraint = baseClass;
      return false;
    });
    // if the root class of the constraint was a mixin, use its AppliesToEntityClass
    if (bisRootForConstraint instanceof Mixin) {
      assert(
        bisRootForConstraint.appliesTo !== undefined,
        "The referenced AppliesToEntityClass could not be found, how did it pass schema validation?"
      );
      bisRootForConstraint = await this.getRootBisClass(
        await bisRootForConstraint.appliesTo
      );
    }
    return bisRootForConstraint;
  }

  private async getAbstractConstraintClass(
    constraint: RelationshipConstraint
  ): Promise<ECClass> {
    // constraint classes must share a base so we can get the root from any of them, just use the first
    const ecclass = await (constraint.constraintClasses?.[0] ||
      constraint.abstractConstraint);
    assert(
      ecclass !== undefined,
      "At least one constraint class or an abstract constraint must have been defined, the constraint is not valid"
    );
    return ecclass;
  }

  /** initialize from an imodel with metadata */
  public async initAllSchemasInIModel(imodel: IModelDb): Promise<void> {
    const schemaLoader = new SchemaLoader((name: string) =>
      imodel.getSchemaProps(name)
    );
    await imodel.withPreparedStatement(
      `
      WITH RECURSIVE refs(SchemaId) AS (
        SELECT ECInstanceId FROM ECDbMeta.ECSchemaDef WHERE Name='BisCore'
        UNION ALL
        SELECT sr.SourceECInstanceId
        FROM ECDbMeta.SchemaHasSchemaReferences sr
        JOIN refs ON sr.TargetECInstanceId = refs.SchemaId
      )
      SELECT s.Name
      FROM refs
      JOIN ECDbMeta.ECSchemaDef s ON refs.SchemaId=s.ECInstanceId
      -- ensure schema dependency order
      ORDER BY ECInstanceId
    `,
      async (stmt) => {
        let status: DbResult;
        while ((status = stmt.step()) === DbResult.BE_SQLITE_ROW) {
          const schemaName = stmt.getValue(0).getString();
          const schema = schemaLoader.getSchema(schemaName);
          await this.considerInitSchema(schema);
        }
        if (status !== DbResult.BE_SQLITE_DONE)
          throw new IModelError(status, "unexpected query failure");
      }
    );
    Logger.logInfo(
      TransformerLoggerCategory.IModelTransformer,
      `Checked ${this._classesCheckedForQueryView} classes for QueryView custom attribute. This took ${this._cumulativeTimeSpentCheckingForQueryView}ms.`
    );
  }

  /**
   *
   * @param cls the class to check for custom attributes
   * @param isSource whether the class is the source or target class of a relationship
   * @returns true if the class has a custom attribute that is a QueryView and no type found in the rootClassToRefType map
   */
  private hasQueryViewCustomAttributeAndNoRootClass(
    cls: ECClass,
    isSource: boolean,
    rootClassToRefType?: ConcreteEntityTypes
  ): boolean {
    const stopwatch = new StopWatch(
      "hasQueryViewCustomAttributeAndNoRootClass",
      true
    );
    const sourceCAs = cls.getCustomAttributesSync();
    this._classesCheckedForQueryView++;
    for (const [customAttributeName, _customAttribute] of sourceCAs) {
      /*
       * Since queryViews are typically associated with abstract classes and have no specific table in the database, we can ignore them.
       */
      if (
        customAttributeName.toLowerCase() === "ecdbmap.queryview" &&
        rootClassToRefType === undefined
      ) {
        // no rootClassToRefType means the rootBisClass.name is not in the bisRootClassToRefType map.
        Logger.logInfo(
          TransformerLoggerCategory.IModelTransformer,
          `${isSource ? "sourceClass" : "targetClass"}: ${cls.schema.name}:${cls.name} has customAttribute which is QueryView and no type found in the rootClassToRefType map`
        );
        this._cumulativeTimeSpentCheckingForQueryView +=
          stopwatch.elapsed.milliseconds;
        return true;
      }
    }
    this._cumulativeTimeSpentCheckingForQueryView +=
      stopwatch.elapsed.milliseconds;
    return false;
  }

  private async considerInitSchema(schema: Schema): Promise<void> {
    if (this._initedSchemas.has(schema.name)) {
      const cachedSchemaKey = this._initedSchemas.get(schema.name);
      assert(cachedSchemaKey !== undefined);
      const incomingSchemaIsEqualOrOlder =
        schema.schemaKey.compareByVersion(cachedSchemaKey) <= 0;
      if (incomingSchemaIsEqualOrOlder) {
        return;
      }
    }
    return this.initSchema(schema);
  }

  private async initSchema(schema: Schema): Promise<void> {
    for (const ecclass of schema.getClasses()) {
      for (const prop of await ecclass.getProperties()) {
        if (!prop.isNavigation()) continue;
        const relClass = await prop.relationshipClass;
        const relInfo = await this.relInfoFromRelClass(relClass);
        if (relInfo === undefined) continue;
        const navPropRefType =
          prop.direction === StrengthDirection.Forward
            ? relInfo.target
            : relInfo.source;
        this._propQualifierToRefType.set(
          [
            schema.name.toLowerCase(),
            ecclass.name.toLowerCase(),
            prop.name.toLowerCase(),
          ],
          navPropRefType
        );
      }

      if (ecclass instanceof RelationshipClass) {
        const relInfo = await this.relInfoFromRelClass(ecclass);
        if (relInfo)
          this._relClassNameEndToRefTypes.set(
            [schema.name.toLowerCase(), ecclass.name.toLowerCase()],
            relInfo
          );
      }
    }

    this._initedSchemas.set(schema.name, schema.schemaKey);
  }

  private async relInfoFromRelClass(
    ecclass: RelationshipClass
  ): Promise<RelTypeInfo | undefined> {
    assert(ecclass.source.constraintClasses !== undefined);
    assert(ecclass.target.constraintClasses !== undefined);
    const [
      [sourceClass, sourceRootBisClass],
      [targetClass, targetRootBisClass],
    ] = await Promise.all([
      this.getAbstractConstraintClass(ecclass.source).then(
        async (constraintClass) => [
          constraintClass,
          await this.getRootBisClass(constraintClass),
        ]
      ),
      this.getAbstractConstraintClass(ecclass.target).then(
        async (constraintClass) => [
          constraintClass,
          await this.getRootBisClass(constraintClass),
        ]
      ),
    ]);

    if (
      sourceRootBisClass.name === "CodeSpec" ||
      targetRootBisClass.name === "CodeSpec"
    )
      return undefined;
    const sourceType =
      ECReferenceTypesCache.bisRootClassToRefType[sourceRootBisClass.name];
    const targetType =
      ECReferenceTypesCache.bisRootClassToRefType[targetRootBisClass.name];

    if (
      this.hasQueryViewCustomAttributeAndNoRootClass(
        sourceClass,
        true,
        sourceType
      )
    )
      return undefined;
    if (
      this.hasQueryViewCustomAttributeAndNoRootClass(
        targetClass,
        false,
        targetType
      )
    )
      return undefined;

    const makeAssertMsg = (root: ECClass, cls: ECClass) =>
      [
        `An unknown root class '${root.fullName}' was encountered while populating`,
        `the nav prop reference type cache for ${cls.fullName}.`,
        "This is a bug.",
      ].join("\n");
    assert(
      sourceType !== undefined,
      makeAssertMsg(sourceRootBisClass, sourceClass)
    );
    assert(
      targetType !== undefined,
      makeAssertMsg(targetRootBisClass, targetClass)
    );
    return { source: sourceType, target: targetType };
  }

  public getNavPropRefType(
    schemaName: string,
    className: string,
    propName: string
  ): undefined | ConcreteEntityTypes {
    if (!this._initedSchemas.has(schemaName)) throw new SchemaNotInCacheErr();
    return this._propQualifierToRefType.get([
      schemaName.toLowerCase(),
      className.toLowerCase(),
      propName.toLowerCase(),
    ]);
  }

  public getRelationshipEndType(
    schemaName: string,
    className: string
  ): undefined | RelTypeInfo {
    if (!this._initedSchemas.has(schemaName)) throw new SchemaNotInCacheErr();
    return this._relClassNameEndToRefTypes.get([
      schemaName.toLowerCase(),
      className.toLowerCase(),
    ]);
  }

  public clear() {
    this._initedSchemas.clear();
    this._propQualifierToRefType.clear();
  }
}
