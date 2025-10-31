/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { DbResult, Logger, TupleKeyedMap } from "@itwin/core-bentley";
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

  // Performance optimization caches
  private _rootBisClassCache = new Map<string, ECClass>();
  private _relationshipInfoCache = new Map<string, RelTypeInfo | undefined>();
  private _constraintClassCache = new Map<string, ECClass>();

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
    const cacheKey = ecclass.fullName;
    const cached = this._rootBisClassCache.get(cacheKey);
    if (cached) {
      return cached;
    }

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

    this._rootBisClassCache.set(cacheKey, bisRootForConstraint);
    return bisRootForConstraint;
  }

  private async getAbstractConstraintClass(
    constraint: RelationshipConstraint
  ): Promise<ECClass> {
    const cacheKey = `${constraint.fullName}_${constraint.constraintClasses?.[0]?.fullName || "abstract"}`;
    const cached = this._constraintClassCache.get(cacheKey);
    if (cached) {
      return cached;
    }

    // constraint classes must share a base so we can get the root from any of them, just use the first
    const ecclass = await (constraint.constraintClasses?.[0] ||
      constraint.abstractConstraint);
    assert(
      ecclass !== undefined,
      "At least one constraint class or an abstract constraint must have been defined, the constraint is not valid"
    );

    this._constraintClassCache.set(cacheKey, ecclass);
    return ecclass;
  }

  /** initialize from an imodel with metadata */
  public async initAllSchemasInIModel(imodel: IModelDb): Promise<void> {
    let totalSchemaCount = 0;
    let schemaCompletedCount = 0;

    const initStartTime = performance.now();

    const query = `
      WITH RECURSIVE refs(SchemaId) AS (
        SELECT ECInstanceId FROM ECDbMeta.ECSchemaDef WHERE Name='BisCore'
        UNION ALL
        SELECT sr.SourceECInstanceId
        FROM ECDbMeta.SchemaHasSchemaReferences sr
        JOIN refs ON sr.TargetECInstanceId = refs.SchemaId
      )
      SELECT DISTINCT s.Name as name
      FROM refs
      JOIN ECDbMeta.ECSchemaDef s ON refs.SchemaId=s.ECInstanceId
      -- ensure schema dependency order
      ORDER BY s.ECInstanceId
    `;

    for await (const row of imodel.createQueryReader(query)) {
      const schemaName = row.name;
      const startTime = performance.now();
      Logger.logTrace(
        TransformerLoggerCategory.ECReferenceTypesCache,
        `Loading schema: ${schemaName}`
      );
      const schemaItemKey = new SchemaKey(schemaName);
      const schema = await imodel.schemaContext.getSchema(schemaItemKey);
      if (schema) {
        await this.considerInitSchema(schema);
        const endTime = performance.now();
        Logger.logTrace(
          TransformerLoggerCategory.ECReferenceTypesCache,
          `Completed schema: ${schemaName} in ${(endTime - startTime).toFixed(2)}ms`
        );
        schemaCompletedCount++;
      } else {
        Logger.logInfo(
          TransformerLoggerCategory.ECReferenceTypesCache,
          `Did not load schema: ${schemaName}`
        );
      }

      totalSchemaCount++;
    }

    const initEndTime = performance.now();
    Logger.logTrace(
      TransformerLoggerCategory.ECReferenceTypesCache,
      `Schemas completed out of total: ${schemaCompletedCount} / ${totalSchemaCount} in ${(initEndTime - initStartTime).toFixed(2)}ms`
    );
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
    Logger.logInfo(
      TransformerLoggerCategory.ECReferenceTypesCache,
      `Init Schema: ${schema.name}`
    );
    const schemaNameLower = schema.name.toLowerCase();

    // Pre-collect all items to reduce iterator overhead
    const allItems = Array.from(schema.getItems());
    const ecClasses: ECClass[] = [];
    const relationshipClasses: RelationshipClass[] = [];

    // Single pass through items with type checking
    for (const item of allItems) {
      // eslint-disable-next-line @itwin/no-internal
      if (!ECClass.isECClass(item)) continue;
      ecClasses.push(item);
      if (item instanceof RelationshipClass) {
        relationshipClasses.push(item);
      }
    }

    // Process relationship classes in parallel and populate global cache
    const relInfoPromises = relationshipClasses.map(async (relClass) => {
      const relInfo = await this.relInfoFromRelClass(relClass);
      this._relationshipInfoCache.set(relClass.fullName, relInfo);
      if (relInfo) {
        this._relClassNameEndToRefTypes.set(
          [schemaNameLower, relClass.name.toLowerCase()],
          relInfo
        );
      }
      return relInfo;
    });

    // Wait for all relationship info to be cached
    await Promise.all(relInfoPromises);

    // Process navigation properties with optimized batching
    const propertyBatchSize = 25;
    for (let i = 0; i < ecClasses.length; i += propertyBatchSize) {
      const classBatch = ecClasses.slice(i, i + propertyBatchSize);

      const classPromises = classBatch.map(async (ecclass) => {
        const properties = await ecclass.getProperties();
        if (!properties) return;

        const classNameLower = ecclass.name.toLowerCase();

        // Efficiently filter navigation properties
        const navProps = Array.from(properties).filter((prop) =>
          prop.isNavigation()
        );
        if (navProps.length === 0) return;

        const navPropPromises = navProps.map(async (prop) => {
          const relClass = await prop.relationshipClass;

          // Use cached relation info
          let relInfo = this._relationshipInfoCache.get(relClass.fullName);
          if (
            relInfo === undefined &&
            !this._relationshipInfoCache.has(relClass.fullName)
          ) {
            relInfo = await this.relInfoFromRelClass(relClass);
            this._relationshipInfoCache.set(relClass.fullName, relInfo);
          }

          if (relInfo === undefined) return;

          const navPropRefType =
            prop.direction === StrengthDirection.Forward
              ? // eslint-disable-next-line @itwin/no-internal
                relInfo.target
              : // eslint-disable-next-line @itwin/no-internal
                relInfo.source;

          this._propQualifierToRefType.set(
            [schemaNameLower, classNameLower, prop.name.toLowerCase()],
            navPropRefType
          );
        });

        await Promise.all(navPropPromises);
      });

      await Promise.all(classPromises);
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
      (!sourceType &&
        sourceRootBisClass.customAttributes?.has("ECDbMap.QueryView")) ||
      (!targetType &&
        targetRootBisClass.customAttributes?.has("ECDbMap.QueryView"))
    ) {
      // ECView elements are not "real" data and transformer does not need to process them.
      // Relationships that point to ECView elements can only be used in ECViews so the mapping is not needed.
      return undefined;
    }

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
    this._relClassNameEndToRefTypes.clear();
    this._rootBisClassCache.clear();
    this._relationshipInfoCache.clear();
    this._constraintClassCache.clear();
  }
}
