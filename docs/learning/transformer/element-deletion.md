# Deleting elements

`IModelImporter.deleteElements()` deletes a set of target elements together with everything that depends on them. It keeps any element that something outside the deletion still uses, logs a warning, and deletes the rest; see [Elements that are still in use](#elements-that-are-still-in-use). When every requested element is skipped or no longer exists, it returns without deleting anything. An unexpected native failure throws, as described in [Handling deletion errors](#handling-deletion-errors).

## Where deletions come from

During change processing, `IModelExporter.exportChanges()` handles deletions after inserts and updates. It deletes models first, then passes every deleted source element ID to `IModelExportHandler.onDeleteElements()` in one set, and then deletes relationships.

`IModelTransformer.onDeleteElements()` maps the source IDs to target IDs and calls `IModelImporter.deleteElements()` once. It skips source elements with no target element and target elements that were remapped by code. `IModelImporter.deleteElement(elementId)` sends its ID through the same path as a one-element set.

The importer skips requested elements that it doesn't update: those in `IModelImporter.doNotUpdateElementIds`, and the iModel's root elements (the root subject, the dictionary model's element, and the reality data source link partition) unless `IModelImportOptions.skipPropagateChangesToRootElements` is `false`. That option defaults to `true`. A skipped element can still be deleted as part of another requested element's tree.

## What gets deleted

Each requested element is the root of a deletion tree. The tree contains:

- the element's child elements, recursively;
- when the element is modeled by a sub-model, every element in that model, recursively;
- every top-level element whose code scope is an element in the tree. That element becomes the root of its own tree, so its children, sub-model contents, and code-scope dependents are deleted too.

A requested element whose parent, or the element that owns its model, is also requested is not a root of its own, because its ancestor's tree already contains it. The importer ignores requested IDs that no longer exist.

## How the importer deletes

```mermaid
flowchart TD
    A["Requested target element IDs"] --> B["Skip elements the importer doesn't update"]
    B --> C["Find deletion trees:<br/>children, sub-model contents, code-scope dependents"]
    C --> D["Find elements that use a category in the trees<br/>or have a code scope in them"]
    D --> E{"Is any of them outside the trees?"}
    E -->|"Yes"| F["Keep the elements they use<br/>and what those elements need"]
    F -->|"Plan again"| C
    E -->|"No"| G{"Does an element in the trees use a category in the trees?"}
    G -->|"No"| H["One native call with every root"]
    G -->|"Yes"| S{"Is a category user in a tree with a used category,<br/>or does an earlier tree scope a later element's code?"}
    S -->|"No"| I["Call 1: roots whose trees contain no used category<br/>Call 2: the remaining roots"]
    S -->|"Yes"| U["Call 1: category users and their code-scope dependents<br/>Call 2: the remaining roots"]
    H --> J{"Did every native call succeed?"}
    I --> J
    U --> J
    J -->|"Yes"| K["Done"]
    J -->|"No"| L["Throw ElementBulkDeleteFailed<br/>Earlier deletions stay pending"]
```

Before each native call, iTwin.js core checks every root against the iModel as it was before that call. It refuses a root whose tree contains a category that another element in the same call uses, even when that element is also being deleted. Deleting the users of a category in an earlier call avoids this. Code scopes need no ordering, because core accepts an element and its code-scope dependents in the same call.

Deleting whole trees in two calls doesn't work when an element that uses a category is itself in a tree that contains a used category, such as a `Subject` whose tree contains both a definition model with the category and a physical model with the user. It also doesn't work when a first-call tree is the code scope of an element in a second-call tree. In those cases, the first call deletes the category users as roots of their own, together with every element whose code they scope, including elements that have a parent, and the second call deletes the remaining roots.

The check before deletion covers the BisCore references that block deletion and aren't already inside the trees: the category of 2D and 3D geometric elements, and code scopes. Parent and model references need no check, because the trees contain every child and sub-model element. References from domain schemas aren't checked in advance. Core still validates them, and a refused root makes the importer throw `ElementBulkDeleteFailed`.

## Elements that are still in use

When an element outside the deletion trees still uses a category in them, or has its code scoped by an element in them, the importer keeps the element it uses instead of deleting it. For example, a category stays when a target element that didn't come from the source still uses it. The importer also keeps:

- the kept element's child elements and sub-model contents, such as a category's sub-categories;
- every element whose deletion would delete a kept element or leave its parent, model, or code scope dangling, such as the definition model that contains a kept category.

Other requested elements are still deleted, including other requested contents of a kept definition model. A kept element can itself use an element that is being deleted, such as its category, so the importer plans again until nothing outside the trees uses an element in them. It then logs one warning in the `imodel-transformer.IModelImporter` category that lists up to ten kept elements, each with one element that still uses it.

The importer keeps core's reference validation on for every native call and never passes `skipFKConstraintValidations`. With that option, core can delete a category that another element still uses.

## Handling deletion errors

A native call can still fail, for example because of a reference from a domain schema. The importer then throws an error with scope `IModelTransformerErrorScope`:

| Key                                              | What changed in the target                                                   | What to do                                                                                                                                    |
| ------------------------------------------------ | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `IModelTransformerError.ElementBulkDeleteFailed` | Deletions from this and earlier native calls are pending in the transaction. | Abandon the transaction before correcting the dependency and retrying. `status`, `sqlDeleteStatus`, and `failedIds` describe the failed call. |

```ts
[[include:ErrorHandling.handle-bulk-delete-errors]]
```

See [Error handling in imodel-transformer](./error-handling.md) for the general error contract.

## Customizing deletion

A custom `IModelImporter` can override `onDeleteElements(elementIds: ReadonlySet<Id64String>)`. The override receives the requested roots after the importer skips the elements it doesn't update, not the expanded trees. Complete any work that needs the elements to exist, then call and await `super.onDeleteElements()` exactly once. That call plans the deletion, makes one or two native calls, and resolves after every tree is deleted.

A custom `IModelTransformer` can override `onDeleteElements(sourceElementIds: ReadonlySet<Id64String>)` to see the deleted source IDs before they are mapped. Call and await `super.onDeleteElements()` to map them and pass them to the importer.
