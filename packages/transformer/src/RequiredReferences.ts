import {
  Element,
  Entity,
  GeometricElement,
  SpatialViewDefinition,
  ViewDefinition,
} from "@itwin/core-backend";
import * as BackendExports from "@itwin/core-backend";
import { ConcreteEntityTypes } from "@itwin/core-common";

const bisCoreClasses = Object.values(BackendExports).filter(
  (v): v is typeof Element => v instanceof Element.constructor
);

// NOTE: lodash has this same thing but probably with prototype pollution detection
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
export const requiredReferenceKeys = new Map<abstract new (...args: any[]) => Entity, RequiredReferences>([]);


function populateRequiredReferenceKeys() {
  for (const bisCoreClass of bisCoreClasses) {
    const classRequiredReferenceKeys = {};

    for (const [maybeBaseClass, baseClassRequiredRefs] of classSpecificRequiredReferenceKeys.entries())
      if (bisCoreClass.is(maybeBaseClass as typeof Entity))
        Object.assign(classRequiredReferenceKeys, baseClassRequiredRefs);

    requiredReferenceKeys.set(bisCoreClass, classRequiredReferenceKeys);
  }
}

populateRequiredReferenceKeys();

