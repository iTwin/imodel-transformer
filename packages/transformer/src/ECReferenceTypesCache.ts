/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { Logger } from "@itwin/core-bentley";
import { ConcreteEntityTypes, RelTypeInfo } from "@itwin/core-common";
import {
  ECClass,
  Mixin,
  NavigationProperty,
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
 * A cache of the entity types referenced by navprops in ecschemas, as well as the source and target entity types of
 * relationship classes. The transformer needs the referenced type to determine how to resolve references.
 *
 * Using multiple of these usually performs redundant computation, for static schemas at least. A possible future optimization
 * would be to seed the computation from a global cache of non-dynamic schemas, but dynamic schemas can collide willy-nilly
 * @internal
 */
export class ECReferenceTypesCache {
  /** Flat map keyed by "\0"-separated lowercase [schemaName, className, propName] */
  private _propQualifierToRefType = new Map<string, ConcreteEntityTypes>();
  /** Flat map keyed by "\0"-separated lowercase [schemaName, className] */
  private _relClassNameEndToRefTypes = new Map<string, RelTypeInfo>();
  private _initedSchemas = new Map<string, SchemaKey>();

  /**
   * Pre-computed via SQL: maps ECClass fullName ("SchemaName.ClassName") to BIS root class name.
   * Built from ECDbMeta.ClassHasAllBaseClasses in a single query, eliminating the need
   * for expensive traverseBaseClasses() calls on every class.
   */
  private _classToRootBisName = new Map<string, string>();

  // Caches for the slow-path fallback (rare cases: mixins, QueryView classes)
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

  private static navPropKey(
    schemaName: string,
    className: string,
    propName: string
  ): string {
    return `${schemaName}\0${className}\0${propName}`;
  }

  private static relClassKey(
    schemaName: string,
    className: string
  ): string {
    return `${schemaName}\0${className}`;
  }

  /**
   * Pre-compute the BIS root class for every class in the iModel using a single SQL query.
   * Uses ECDbMeta.ClassHasAllBaseClasses to map each class to its root BIS type
   * (Element, Model, ElementAspect, ElementRefersToElements, ElementDrivesElement, or CodeSpec).
   * This replaces the expensive per-class traverseBaseClasses() approach.
   */
  private async preComputeRootBisClasses(imodel: IModelDb): Promise<void> {
    const query = `
      SELECT
        ec_className(hab.SourceECInstanceId, 's') || '.' || ec_className(hab.SourceECInstanceId, 'c') AS classFullName,
        bisRoot.Name AS rootName
      FROM ECDbMeta.ClassHasAllBaseClasses hab
      JOIN ECDbMeta.ECClassDef bisRoot ON hab.TargetECInstanceId = bisRoot.ECInstanceId
      JOIN ECDbMeta.ECSchemaDef bisSchema ON bisRoot.Schema.Id = bisSchema.ECInstanceId
      WHERE bisSchema.Name = 'BisCore'
        AND bisRoot.Name IN ('Element', 'Model', 'ElementAspect', 'ElementRefersToElements', 'ElementDrivesElement', 'CodeSpec')
    `;

    for await (const row of imodel.createQueryReader(query, undefined, {
      usePrimaryConn: true,
    })) {
      // BIS root hierarchies are mutually exclusive, so each class maps to at most one root.
      if (!this._classToRootBisName.has(row.classFullName)) {
        this._classToRootBisName.set(row.classFullName, row.rootName);
      }
    }
  }

  /**
   * Fast path: resolve the BIS root class NAME for an ECClass using the pre-computed SQL mapping.
   * For mixins (not in the SQL mapping since they don't derive from entity roots),
   * resolves via the appliesTo chain.
   * Returns undefined only for classes not deriving from any known BIS root (e.g., QueryView classes).
   */
  private async getRootBisClassNameFast(
    ecclass: ECClass
  ): Promise<string | undefined> {
    const preComputed = this._classToRootBisName.get(ecclass.fullName);
    if (preComputed !== undefined) return preComputed;

    // Mixins don't appear in ClassHasAllBaseClasses with entity roots;
    // resolve via appliesTo chain
    if (ecclass instanceof Mixin) {
      assert(
        ecclass.appliesTo !== undefined,
        "The referenced AppliesToEntityClass could not be found, how did it pass schema validation?"
      );
      return this.getRootBisClassNameFast(await ecclass.appliesTo);
    }

    return undefined;
  }

  /**
   * Slow-path fallback: resolves the actual ECClass object for the BIS root.
   * Only used for rare edge cases (QueryView detection) and test compatibility.
   */
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

  /**
   * Resolve the ConcreteEntityType for a constraint class.
   * Uses the fast pre-computed SQL mapping for the common case,
   * and falls back to the schema object model only for rare edge cases (QueryView).
   */
  private async resolveConstraintType(
    constraintClass: ECClass
  ): Promise<ConcreteEntityTypes | "skip" | undefined> {
    // Fast path: pre-computed SQL mapping (handles 99%+ of cases)
    const rootName = await this.getRootBisClassNameFast(constraintClass);

    if (rootName === "CodeSpec") return "skip";
    if (rootName !== undefined) {
      const type = ECReferenceTypesCache.bisRootClassToRefType[rootName];
      if (type !== undefined) return type;
    }

    // Slow-path fallback for classes not in the pre-computed map (e.g., QueryView)
    const rootClass = await this.getRootBisClass(constraintClass);
    if (rootClass.name === "CodeSpec") return "skip";
    const type = ECReferenceTypesCache.bisRootClassToRefType[rootClass.name];
    if (type !== undefined) return type;

    if (rootClass.customAttributes?.has("ECDbMap.QueryView")) return "skip";

    assert(
      false,
      [
        `An unknown root class '${rootClass.fullName}' was encountered while populating`,
        `the nav prop reference type cache for ${constraintClass.fullName}.`,
        "This is a bug.",
      ].join("\n")
    );
    return undefined;
  }

  /** initialize from an imodel with metadata */
  public async initAllSchemasInIModel(imodel: IModelDb): Promise<void> {
    const initStartTime = performance.now();

    // Pre-compute all root BIS class mappings in a single SQL query.
    // This replaces all per-class traverseBaseClasses() calls.
    await this.preComputeRootBisClasses(imodel);

    // Collect all BisCore-referencing schema names up front
    const schemaQuery = `
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

    const schemaNames: string[] = [];
    for await (const row of imodel.createQueryReader(schemaQuery, undefined, {
      usePrimaryConn: true,
    })) {
      schemaNames.push(row.name);
    }

    let schemaCount = 0;
    for (const schemaName of schemaNames) {
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
        `Completed schema: ${schemaName} in ${(endTime - startTime).toFixed(2)}ms`
      );
      schemaCount++;
    }

    const initEndTime = performance.now();
    Logger.logTrace(
      TransformerLoggerCategory.ECReferenceTypesCache,
      `Completed ${schemaCount} schemas in ${(initEndTime - initStartTime).toFixed(2)}ms`
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

    // Single pass: classify items and process relationship classes
    const ecClasses: ECClass[] = [];
    const relInfoPromises: Promise<void>[] = [];

    for (const item of schema.getItems()) {
      // eslint-disable-next-line @itwin/no-internal
      if (!ECClass.isECClass(item)) continue;
      ecClasses.push(item);

      if (item instanceof RelationshipClass) {
        const relClass = item;
        relInfoPromises.push(
          this.relInfoFromRelClass(relClass).then((relInfo) => {
            this._relationshipInfoCache.set(relClass.fullName, relInfo);
            if (relInfo) {
              this._relClassNameEndToRefTypes.set(
                ECReferenceTypesCache.relClassKey(
                  schemaNameLower,
                  relClass.name.toLowerCase()
                ),
                relInfo
              );
            }
          })
        );
      }
    }

    // Wait for all relationship info to be cached (nav props depend on them)
    await Promise.all(relInfoPromises);

    // Process navigation properties using synchronous property access
    const navPropPromises: Promise<void>[] = [];
    for (const ecclass of ecClasses) {
      // Use sync API: avoids async overhead and leverages internal property cache
      const properties = ecclass.getPropertiesSync();
      const classNameLower = ecclass.name.toLowerCase();

      for (const prop of properties) {
        if (!prop.isNavigation()) continue;

        // Use sync relationship class access when possible
        const relClassSync = (
          prop as NavigationProperty
        ).getRelationshipClassSync();
        if (relClassSync) {
          // Fully synchronous path: resolve nav prop type without any awaits
          let relInfo = this._relationshipInfoCache.get(relClassSync.fullName);
          if (
            relInfo === undefined &&
            !this._relationshipInfoCache.has(relClassSync.fullName)
          ) {
            // Relationship from another schema not yet cached; queue for async resolution
            navPropPromises.push(
              this.resolveAndCacheNavProp(
                prop as NavigationProperty,
                schemaNameLower,
                classNameLower
              )
            );
            continue;
          }

          if (relInfo === undefined) continue;

          const navPropRefType =
            prop.direction === StrengthDirection.Forward
              ? // eslint-disable-next-line @itwin/no-internal
              relInfo.target
              : // eslint-disable-next-line @itwin/no-internal
              relInfo.source;

          this._propQualifierToRefType.set(
            ECReferenceTypesCache.navPropKey(
              schemaNameLower,
              classNameLower,
              prop.name.toLowerCase()
            ),
            navPropRefType
          );
        } else {
          // Async fallback: relationship class not yet resolved synchronously
          navPropPromises.push(
            this.resolveAndCacheNavProp(
              prop as NavigationProperty,
              schemaNameLower,
              classNameLower
            )
          );
        }
      }
    }

    if (navPropPromises.length > 0) {
      await Promise.all(navPropPromises);
    }

    this._initedSchemas.set(schema.name, schema.schemaKey);
  }

  /** Async helper: resolve a nav prop's relationship info and cache the result */
  private async resolveAndCacheNavProp(
    prop: NavigationProperty,
    schemaNameLower: string,
    classNameLower: string
  ): Promise<void> {
    const relClass = await prop.relationshipClass;

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
      ECReferenceTypesCache.navPropKey(
        schemaNameLower,
        classNameLower,
        prop.name.toLowerCase()
      ),
      navPropRefType
    );
  }

  private async relInfoFromRelClass(
    ecclass: RelationshipClass
  ): Promise<RelTypeInfo | undefined> {
    assert(ecclass.source.constraintClasses !== undefined);
    assert(ecclass.target.constraintClasses !== undefined);

    const [sourceClass, targetClass] = await Promise.all([
      this.getAbstractConstraintClass(ecclass.source),
      this.getAbstractConstraintClass(ecclass.target),
    ]);

    // Use the fast pre-computed SQL mapping for constraint type resolution
    const [sourceType, targetType] = await Promise.all([
      this.resolveConstraintType(sourceClass),
      this.resolveConstraintType(targetClass),
    ]);

    if (sourceType === "skip" || targetType === "skip") return undefined;
    if (sourceType === undefined || targetType === undefined) return undefined;

    return { source: sourceType, target: targetType };
  }

  public getNavPropRefType(
    schemaName: string,
    className: string,
    propName: string
  ): undefined | ConcreteEntityTypes {
    if (!this._initedSchemas.has(schemaName)) throw new SchemaNotInCacheErr();
    return this._propQualifierToRefType.get(
      ECReferenceTypesCache.navPropKey(
        schemaName.toLowerCase(),
        className.toLowerCase(),
        propName.toLowerCase()
      )
    );
  }

  public getRelationshipEndType(
    schemaName: string,
    className: string
  ): undefined | RelTypeInfo {
    if (!this._initedSchemas.has(schemaName)) throw new SchemaNotInCacheErr();
    return this._relClassNameEndToRefTypes.get(
      ECReferenceTypesCache.relClassKey(
        schemaName.toLowerCase(),
        className.toLowerCase()
      )
    );
  }

  public clear() {
    this._initedSchemas.clear();
    this._propQualifierToRefType.clear();
    this._relClassNameEndToRefTypes.clear();
    this._classToRootBisName.clear();
    this._rootBisClassCache.clear();
    this._relationshipInfoCache.clear();
    this._constraintClassCache.clear();
  }
}
