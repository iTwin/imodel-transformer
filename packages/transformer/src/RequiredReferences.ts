import {
  Element,
  Entity,
  GeometricElement,
  SpatialViewDefinition,
  ViewDefinition,
} from "@itwin/core-backend";
import * as BackendExports from "@itwin/core-backend";
import { ConcreteEntityTypes } from "@itwin/core-common";

// NOTE: lodash has this same thing but probably with prototype pollution detection,
// but we only use it on a known set of paths for now, barring someone loading a class
// with requiredReferenceKeys = ["__proto__"]
export function readPropPath(obj: any, path: string): any {
  for (const prop of path.split(".")) obj = obj[prop];
  return obj;
}

type RequiredReferences = Record<string, { type: ConcreteEntityTypes }>;

/** list of requiredReferenceKeys introduced by each class. This is used
 * to generate the full inherited list of requiredReferenceKeys for every class
 */
const classSpecificRequiredReferenceKeys = new Map<
  abstract new (...a: any[]) => Entity,
  RequiredReferences
>([
  [
    Element,
    {
     "parent": { type: ConcreteEntityTypes.Element },
     "model": { type: ConcreteEntityTypes.Model },
     "code.scope": { type: ConcreteEntityTypes.Element },
     "code.spec": { type: ConcreteEntityTypes.Element },
    },
  ],
  [GeometricElement, { "category": { type: ConcreteEntityTypes.Element } }],
  [
    ViewDefinition as any, // typescript no-likey protected constructor https://github.com/microsoft/TypeScript/issues/30991
    {
      "categorySelectorId": { type: ConcreteEntityTypes.Element },
      "displayStyleId": { type: ConcreteEntityTypes.Element },
    },
  ],
  [
    SpatialViewDefinition,
    {
      "modelSelectorId": { type: ConcreteEntityTypes.Element },
    }
  ],
]);

/** inherited reference keys for each bis core class */
const requiredReferenceKeys = new Map<abstract new (...args: any[]) => Entity, RequiredReferences>([]);

export const RequiredReferenceKeys = {
  get(entityClass: abstract new (...args: any[]) => Entity): RequiredReferences {
    const cached = requiredReferenceKeys.get(entityClass);
    if (cached) return cached;
    return cacheRequiredReferenceKeys(entityClass as typeof Entity);
  }
};

// NOTE: this currently ignores the ability that core's requiredReferenceKeys has to allow custom js classes
// to introduce new requiredReferenceKeys, which means if that were used, this would break. Currently un used however.
function cacheRequiredReferenceKeys(cls: typeof Entity) {
  const classRequiredReferenceKeys = {};

  let baseClass = cls;
  while (baseClass !== null) {
    const baseClassRequiredRefs = classSpecificRequiredReferenceKeys.get(cls);
    Object.assign(classRequiredReferenceKeys, baseClassRequiredRefs);
    baseClass = Object.getPrototypeOf(baseClass);
  }
  for (const [maybeBaseClass, baseClassRequiredRefs] of classSpecificRequiredReferenceKeys.entries())
    if (cls.is(maybeBaseClass as typeof Entity))
      Object.assign(classRequiredReferenceKeys, baseClassRequiredRefs);

  requiredReferenceKeys.set(cls, classRequiredReferenceKeys);

  return classRequiredReferenceKeys;
}

const bisCoreClasses = Object.values(BackendExports).filter(
  (v): v is typeof Element => v instanceof Element.constructor
);

for (const bisCoreClass of bisCoreClasses) {
  cacheRequiredReferenceKeys(bisCoreClass)
}

