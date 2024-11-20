# Change Log - @itwin/imodel-transformer

<!-- This log was last generated on Mon, 04 Nov 2024 14:45:46 GMT and should not be manually modified. -->

<!-- Start content -->

## 1.0.0

Mon, 04 Nov 2024 14:45:46 GMT

### Patches

- skipPropagateChangesToRootElements should be default true ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- Fixed BigMap class to implement Map (instead of extending it). Added missing method implementations. ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))

### Changes

- save reverse sync version for process all transformations ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- remove deprecated for 3.x, 0.1.x, 0.1.0 ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- 1.0.0-dev.0 ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- move to one getter for synchronizationVersion, bust a potential cached synchronizationVersion whenever we make changes to targetscopeprovenance, and only save changes if handleUnsafeMigrate makes changes not just if unsafe migrate is enabled ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- Revert "save and push changes after transformer is complete (#153)" ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- add more cases to support preserveElementIdsForFiltering options ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- In itwinjs versions 4.6.0 and greater DefinitionContainers are now deleted as if they were DefinitionPartitions. Made a change to onDeleteModel to treat definitinocontainers as if they were definitionpartitions ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- Support setting the reversesync and sync version when unsafe-migrate is set. Also fix bug with haselementchangedcache ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- add alternative schemaToXmlString ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- no longer track partially commited element references ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- only store deleted ESAS of kind 'Element' in processChangesets ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- improve error messages for mismatching peer dependencies ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- Make processAll and processChanges private, expose a public process function and let people pass isSync to constructor of imodeltransformer ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
-  ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- in findTargetEntityId, special case the repositoryModel such that we ignore any remappings and just set the target to the repositoryModelId ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))
- add extract-api.yaml ([commit](https://github.com/iTwin/transformer/commit/f91dcaa8e6d560b288a7ea3ed9203182bb870740))

## 0.4.3

Wed, 06 Dec 2023 15:24:30 GMT

### Patches

- Handle max path limit on windows for schema names ([commit](https://github.com/iTwin/transformer/commit/30ca2d43bdfbbfe1f7713fb82ecaaa4ea95f7b4c))

## 0.4.2

Tue, 26 Sep 2023 16:19:57 GMT

### Patches

- Fix aspect queries when class name is reserved SQLite keyword ([commit](https://github.com/iTwin/transformer/commit/dc462020a152694640355f06d6263aae464c52f6))

## 0.4.1

Wed, 20 Sep 2023 15:35:21 GMT

### Patches

- revert to original behavior of provenance ExternalSourceAspect version behavior ([commit](https://github.com/iTwin/transformer/commit/46373c33920c763ba3eb866fc415e433aa0952e6))

## 0.4.0

Mon, 11 Sep 2023 12:37:44 GMT

### Minor changes

- Add detached ElementAspect exporting ([commit](https://github.com/iTwin/transformer/commit/4c404f3980ec7f4e6a3f3a0b746701e4c6f77d92))

## 0.3.2

Fri, 18 Aug 2023 23:12:28 GMT

### Patches

- Added a fix for "Missing id" and "ForeignKey constraint" errors while using onDeleteModel ([commit](https://github.com/iTwin/transformer/commit/8cda406d158b46f57acfc97d7f4be03a4143414f))
- bump dependencies to allow all itwin.js 4.x ([commit](https://github.com/iTwin/transformer/commit/8cda406d158b46f57acfc97d7f4be03a4143414f))

## 0.3.1

Thu, 27 Jul 2023 13:07:39 GMT

### Patches

- Changed shouldDetectDeletes from private to protected ([commit](https://github.com/iTwin/transformer/commit/88fd8d15b82bc45e962eedd6fe16323498aa732f))

## 0.3.0

Tue, 11 Jul 2023 18:59:25 GMT

### Minor changes

- Add pending reference resolution when referenced element is not exported ([commit](https://github.com/iTwin/transformer/commit/c9e2ecdd80df3fd155111313f2abdc82963775fd))

### Patches

- Start using in BigMap instead of Map to overcome size limits ([commit](https://github.com/iTwin/transformer/commit/c9e2ecdd80df3fd155111313f2abdc82963775fd))
- add BranchProvenanceInitializer functions ([commit](https://github.com/iTwin/transformer/commit/c9e2ecdd80df3fd155111313f2abdc82963775fd))

## 0.2.1

Mon, 26 Jun 2023 13:40:00 GMT

### Patches

- Added ElementCascadingDeleter to fix FK errors while deleting element which is referenced in code scopes of other elements ([commit](https://github.com/iTwin/transformer/commit/c82f3b93754787392bff3f1e66023058e65d219f))

## 0.2.0

Sat, 24 Jun 2023 03:30:05 GMT

### Minor changes

- Added new functions overloads for IModelTransformer.processChanges and IModelExporter.exportChanges. Deprecated old overloads, they still work. ([commit](https://github.com/iTwin/transformer/commit/085590025bddffbf95dbfb6092f6b14c99fb8bcf))

### Patches

- Changed sourceDb to targetDb in IModelCloneContext.findTargetEntityId ([commit](https://github.com/iTwin/transformer/commit/085590025bddffbf95dbfb6092f6b14c99fb8bcf))

### Changes

- Started using provenanceSourceDb instead of sourceDb in initElementProvenance ([commit](https://github.com/iTwin/transformer/commit/085590025bddffbf95dbfb6092f6b14c99fb8bcf))

## 0.1.16

Fri, 09 Jun 2023 13:24:23 GMT

### Patches

- fixed findTargetEntityId when searching for relationship that points to non-existing element in targetIModel ([commit](https://github.com/iTwin/transformer/commit/d27dc1f156b72a10acfb1fc717606364f651f662))

## 0.1.14

Thu, 01 Jun 2023 22:51:33 GMT

### Patches

- update deps to support 3.6-4.0 ([commit](https://github.com/iTwin/transformer/commit/5a175aa5b15fb48e747cccd18be5886727fecb6a))

## 0.1.12

Wed, 31 May 2023 13:40:07 GMT

### Patches

- add checks in EntityUnifier.exists for id validity ([commit](https://github.com/iTwin/transformer/commit/e7528fea595d9d1668154c0245abe6458789e5f1))

## 0.1.10

Tue, 30 May 2023 13:03:51 GMT

### Patches

- fix detectElementDeletes since importer.deleteElement change ([commit](https://github.com/iTwin/transformer/commit/b248d238de2da7dae5ebc5b2609d0d79890811d6))

## 0.1.8

Tue, 02 May 2023 18:28:36 GMT

### Patches

- rerelease again ([commit](https://github.com/iTwin/transformer/commit/3b6ad3fbf7bfe36dfe63da7f8d6f9e5572793f05))

## 0.1.3

Thu, 20 Apr 2023 12:20:33 GMT

### Patches

- Fixed the change of code scope when the code spec is of type Repository and code scope is not root subject ([commit](https://github.com/iTwin/transformer/commit/db9ba2c5d706506210a6eae49229dc3d031d4567))

## 0.1.2

Tue, 18 Apr 2023 14:12:45 GMT

### Patches

- Modified query for getting all relationships ([commit](https://github.com/iTwin/transformer/commit/18c92c334e312b9c5b8f254dec66941c23ee3c0b))

## 0.1.1

Tue, 04 Apr 2023 15:43:37 GMT

### Patches

- rename docs artifact ([commit](https://github.com/iTwin/transformer/commit/35c2188ee72beaab88c26d68bd6b2f03336e63bf))
- add option to disable strict version dep checking ([commit](https://github.com/iTwin/transformer/commit/35c2188ee72beaab88c26d68bd6b2f03336e63bf))

## 0.1.0

Fri, 31 Mar 2023 20:21:28 GMT

### Minor changes

- version reset ([commit](https://github.com/iTwin/transformer/commit/f88a868c8dfbbf6bae42840d9210eb2c0f00359d))
