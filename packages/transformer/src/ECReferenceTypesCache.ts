/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { Logger, TupleKeyedMap } from "@itwin/core-bentley";
import { ConcreteEntityTypes, RelTypeInfo } from "@itwin/core-common";
import {
  ECClass,
  Mixin,
  Property,
  RelationshipClass,
  RelationshipConstraint,
  Schema,
  SchemaKey,
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

  private static bisRootClassToRefType: Record<
    string,
    ConcreteEntityTypes | undefined
  > = {
    Element: ConcreteEntityTypes.Element,
    Model: ConcreteEntityTypes.Model,
    ElementAspect: ConcreteEntityTypes.ElementAspect,
    ElementRefersToElements: ConcreteEntityTypes.Relationship,
    ElementDrivesElement: ConcreteEntityTypes.Relationship,
    // code spec is technically a potential root class but it is ignored currently
    // see [ConcreteEntityTypes]($common)
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
    // const schemaLoader = new SchemaLoader((name: string) =>
    //   imodel.getSchemaProps(name)
    // );
    // Issue for `createQueryReader` reported: https://github.com/iTwin/itwinjs-core/issues/7984
    const query = `
      WITH RECURSIVE refs(SchemaId) AS (
        SELECT ECInstanceId FROM ECDbMeta.ECSchemaDef WHERE Name='BisCore'
        UNION
        SELECT sr.SourceECInstanceId
        FROM ECDbMeta.SchemaHasSchemaReferences sr
        JOIN refs ON sr.TargetECInstanceId = refs.SchemaId
      )
      SELECT s.Name as name
      FROM refs
      JOIN ECDbMeta.ECSchemaDef s ON refs.SchemaId=s.ECInstanceId
      -- ensure schema dependency order
      ORDER BY s.ECInstanceId
    `;

    for await (const row of imodel.createQueryReader(query, undefined, {
      usePrimaryConn: true,
    })) {
      const schemaName = row.name;
      const startTime = performance.now();
      Logger.logTrace(
        TransformerLoggerCategory.ECReferenceTypesCache,
        `Loading schema: ${schemaName}`
      );
      const schemaItemKey = new SchemaKey(schemaName);
      const schema = await imodel.schemaContext.getSchema(schemaItemKey);
      if (!schema) {
        throw new Error(`Failed to load schema: ${schemaName}`);
      }
      await this.considerInitSchema(schema);
      const endTime = performance.now();
      Logger.logTrace(
        TransformerLoggerCategory.ECReferenceTypesCache,
        `IModelTransformer Completed schema: ${schemaName} in ${(endTime - startTime).toFixed(2)}ms`
      );
    }

    Logger.logInfo(
      TransformerLoggerCategory.IModelImporter,
      "IModelTransformer Init All Schemas In IModel Complete -- Use Schema Context"
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
    const schemaNameLower = schema.name.toLowerCase();

    // Local dedup map — scoped to this single initSchema call, not persisted
    const localRelInfoMap = new Map<string, Promise<RelTypeInfo | undefined>>();

    const getRelInfo = async (relClass: RelationshipClass) => {
      let promise = localRelInfoMap.get(relClass.fullName);
      if (!promise) {
        promise = this.relInfoFromRelClass(relClass);
        localRelInfoMap.set(relClass.fullName, promise);
      }
      return promise;
    };

    // Process all classes concurrently
    const classPromises = Array.from(schema.getItems())
      .filter((item): item is ECClass => ECClass.isECClass(item))
      .map(async (ecclass) => {
        // Handle relationship end types
        if (ecclass instanceof RelationshipClass) {
          const relInfo = await getRelInfo(ecclass);
          if (relInfo) {
            this._relClassNameEndToRefTypes.set(
              [schemaNameLower, ecclass.name.toLowerCase()],
              relInfo
            );
          }
        }

        // Handle nav props
        const properties = await ecclass.getProperties();
        const classNameLower = ecclass.name.toLowerCase();

        const navPropPromises = Array.from(properties)
          .filter((prop: Property) => prop.isNavigation())
          .map(async (prop: Property) => {
            if (!prop.isNavigation()) return;
            const relClass = await prop.relationshipClass;
            const relInfo = await getRelInfo(relClass);
            if (relInfo === undefined) return;

            const navPropRefType =
              prop.direction === StrengthDirection.Forward
                ? relInfo.target
                : relInfo.source;

            this._propQualifierToRefType.set(
              [schemaNameLower, classNameLower, prop.name.toLowerCase()],
              navPropRefType
            );
          });

        await Promise.all(navPropPromises);
      });

    await Promise.all(classPromises);
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
  }
}
