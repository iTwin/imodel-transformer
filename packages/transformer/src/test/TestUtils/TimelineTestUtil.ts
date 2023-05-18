/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import { assert, expect } from "chai";
import {
  BriefcaseDb,
  ECSqlStatement,
  ExternalSourceAspect, IModelDb, IModelHost, PhysicalModel,
  PhysicalObject, PhysicalPartition, SpatialCategory,
} from "@itwin/core-backend";

import { DbResult, Id64, Id64String } from "@itwin/core-bentley";
import { ChangesetIdWithIndex, Code, ElementProps, IModel, PhysicalElementProps, SubCategoryAppearance } from "@itwin/core-common";
import { Point3d, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelTransformer } from "../../transformer";
import { HubWrappers, IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { IModelTestUtils } from "./IModelTestUtils";

const { count, saveAndPushChanges } = IModelTestUtils;

export const deleted = Symbol('DELETED');

// NOTE: this is not done optimally
export function getIModelState(db: IModelDb): TimelineIModelElemState {
  const result = {} as TimelineIModelElemState;

  const elemIds = db.withPreparedStatement(`
    SELECT ECInstanceId
    FROM Bis.Element
    WHERE ECInstanceId>${IModelDb.dictionaryId}
      -- ignore the known required elements set in 'populateTimelineSeed'
      AND CodeValue NOT IN ('SpatialCategory', 'PhysicalModel')
  `, (s) => [...s].map((row) => row.id));

  for (const elemId of elemIds) {
    const elem = db.elements.getElement(elemId);
    if (elem.userLabel && elem.userLabel in result)
      throw Error("timelines only support iModels with unique user labels");
    const isSimplePhysicalObject = elem.jsonProperties.updateState !== undefined;

    result[elem.userLabel ?? elem.id]
      = isSimplePhysicalObject
      ? elem.jsonProperties.updateState
      : elem.toJSON();
  }
  return result;
}

export function applyDelta(state: TimelineIModelElemState, patch: TimelineIModelElemStateDelta): TimelineIModelElemState {
  const patched = { ...state, ...patch };

  for (const key in patched) {
    const value = patched[key];
    if (value === deleted) delete patched[key];
  }

  return patched as TimelineIModelElemState;
}

export function populateTimelineSeed(db: IModelDb, state?: TimelineIModelElemStateDelta): void {
  SpatialCategory.insert(db, IModel.dictionaryId, "SpatialCategory", new SubCategoryAppearance());
  PhysicalModel.insert(db, IModel.rootSubjectId, "PhysicalModel");
  if (state)
    maintainPhysicalObjects(db, state);
  db.performCheckpoint();
}

export function assertElemState(db: IModelDb, state: TimelineIModelElemStateDelta, { subset = false } = {}): void {
  expect(getIModelState(db)).to.deep.subsetEqual(state, { useSubsetEquality: subset });
}

const deferred = Symbol('DEFERRED');

interface DeferredProp {
  userLabel: string;
  get: ($: any) => any;
  [deferred]: undefined
}

/**
 * create a deferred property, useful for getting ids of elements in the same
 * timeline time point
 */
export function defer(userLabel: string, get: ($: any) => any): any {
  return {
    userLabel: userLabel,
    get,
    [deferred]: undefined,
  } as DeferredProp;
}

function topologicalSortDeltaByDefers(delta: TimelineIModelElemStateDelta): TimelineElemDelta[] {
  function getDeferredRefs(rootElemDef: TimelineElemDelta): DeferredProp[] {
    if (typeof rootElemDef === "number" || rootElemDef === deleted)
      return [];

    const result: DeferredProp[] = [];

    function impl(obj: any) {
      for (const key in obj) {
        const value = obj[key];
        if (typeof value === "object" && value !== null) {
          if (deferred in value)
            result.push(value);
          else
            impl(value);
        }
      }
    }

    impl(rootElemDef);
    return result;
  }

  const topoSortedResult: TimelineElemDelta[] = [];

  function traverseDeferredRefs(elemState: TimelineElemDelta) {
    if (topoSortedResult.includes(elemState)) return;

    const deferredRefs = getDeferredRefs(elemState);
    for (const deferredRef of deferredRefs) {
      const predecessor = delta[deferredRef.userLabel];
      traverseDeferredRefs(predecessor);
    }

    topoSortedResult.push(elemState);
  }

  for (const key in delta) {
    const elemState = delta[key];
    traverseDeferredRefs(elemState);
  }

  return topoSortedResult;
}

export function maintainPhysicalObjects(iModelDb: IModelDb, delta: TimelineIModelElemStateDelta): void {
  const modelId = iModelDb.elements.queryElementIdByCode(PhysicalPartition.createCode(iModelDb, IModel.rootSubjectId, "PhysicalModel"))!;
  const categoryId = iModelDb.elements.queryElementIdByCode(SpatialCategory.createCode(iModelDb, IModel.dictionaryId, "SpatialCategory"))!;

  // set userLabel to the key from the delta
  for (const elemName in delta) {
    const elemDelta = delta[elemName];
    if (typeof elemDelta === "object" && elemDelta !== null) {
      (elemDelta as ElementProps).userLabel = elemName;
    }
  }

  const elemDeltas = topologicalSortDeltaByDefers(delta);

  for (const upsertVal of elemDeltas) {
    const elemName = typeof upsertVal === "object" ? (upsertVal as ElementProps).userLabel : upsertVal.toString();
    const [id] = iModelDb.queryEntityIds({ from: "Bis.Element", where: "UserLabel=?", bindings: [elemName] })

    if (upsertVal === deleted) {
      assert(id, "tried to delete an element that wasn't in the database");
      iModelDb.elements.deleteElement(id);
      continue;
    }

    const props: ElementProps | PhysicalElementProps
      = typeof upsertVal !== "number"
      ? upsertVal
      : {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: elemName }),
        userLabel: elemName,
        geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
        placement: {
          origin: Point3d.create(0, 0, 0),
          angles: YawPitchRollAngles.createDegrees(0, 0, 0),
        },
        jsonProperties: {
          updateState: upsertVal,
        },
      };

    props.id = id;

    if (id === undefined)
      iModelDb.elements.insertElement(props);
    else
      iModelDb.elements.updateElement(props);
  }

  // TODO: iModelDb.performCheckpoint?
  iModelDb.saveChanges();
}

export type TimelineElemState = number | Omit<ElementProps, "userLabel">;
export type TimelineElemDelta = number | TimelineElemState | typeof deleted;

export interface TimelineIModelElemStateDelta {
  [name: string]: TimelineElemDelta;
}

export interface TimelineIModelElemState {
  [name: string]: TimelineElemState;
}

export interface TimelineIModelState {
  state: TimelineIModelElemState;
  id: string;
  db: BriefcaseDb;
}

export type TimelineStateChange =
  // update the state of that model to match and push a changeset
  | TimelineIModelElemStateDelta
  // create a new iModel from a seed
  | { seed: TimelineIModelState }
  // create a branch from an existing iModel with a given name
  | { branch: string }
  // synchronize with the changes in an iModel of a given name from a starting timeline point
  | { sync: [string, number] };

/** an object that helps resolve ids from names */
export type TimelineReferences = Record<string, ElementProps>;

/**
 * A small tree-sitter inspired DSL for building timelines of iModel updates and branching/forking events
 * The `$` can be used to refer to props that aren't resolved until later (e.g. the id of inserted elements)
 *
 * For each step in timeline, an object of iModels mapping to the event that occurs for them:
 * - a 'seed' event with an iModel to seed from, creating the iModel
 * - a 'branch' event with the name of an iModel to seed from, creating the iModel
 * - a 'sync' event with the name of an iModel and timeline point to sync from
 * - an object containing a mapping of user labels to elements to be upserted or to the `deleted` symbol
*    for elements to be deleted. If the iModel doesn't exist, it is created.
 * - an 'assert' function to run on the state of all the iModels in the timeline
 *
 * @note because the timeline manages PhysicalObjects for the state, any seed must contain the necessary
 *       model and category, which can be added to your seed by calling @see populateTimelineSeed
 */
export type Timeline = Record<number, {
  assert?: (imodels: Record<string, TimelineIModelState>) => void;
  [modelName: string]: | undefined // only necessary for the previous optional properties
  | ((imodels: Record<string, TimelineIModelState>) => void) // only necessary for the assert property
  | TimelineStateChange;
}>;

export interface TestContextOpts {
  iTwinId: string;
  accessToken: string;
}

/**
 * Run the branching and synchronization events in a @see Timeline object
 * you can print additional debug info from this by setting in your env TRANSFORMER_BRANCH_TEST_DEBUG=1
 */
export async function runTimeline(timeline: Timeline, { iTwinId, accessToken }: TestContextOpts) {
  const trackedIModels = new Map<string, TimelineIModelState>();
  const masterOfBranch = new Map<string, string>();

  /* eslint-disable @typescript-eslint/indent */
  const timelineStates = new Map<
    number,
    {
      states: { [iModelName: string]: TimelineIModelElemStateDelta };
      changesets: { [iModelName: string]: ChangesetIdWithIndex };
    }
  >();
  /* eslint-enable @typescript-eslint/indent */

  const getSeed = (model: TimelineStateChange): TimelineIModelState | undefined => (model as { seed: TimelineIModelState }).seed;
  const getBranch = (model: TimelineStateChange): string | undefined => (model as { branch: string }).branch;
  const getSync = (model: TimelineStateChange): [string, number] | undefined => (model as { sync: [string, number] }).sync;

  for (let i = 0; i < Object.values(timeline).length; ++i) {
    const pt = timeline[i];
    const iModelChanges = Object.entries(pt)
      .filter((entry): entry is [string, TimelineStateChange] => entry[0] !== "assert" && trackedIModels.has(entry[0]));

    const newIModels = Object.keys(pt).filter((s) => s !== "assert" && !trackedIModels.has(s));

    for (const newIModelName of newIModels) {
      assert(newIModelName !== "assert", "should have already been filtered out");

      const newIModelEvent = pt[newIModelName];
      assert(typeof newIModelEvent === "object");
      assert(!("sync" in newIModelEvent), "cannot sync an iModel that hasn't been created yet!");
      assert(!Object.values(newIModelEvent).includes(deleted), "cannot delete elements in an iModel that you are creating now!");

      const seed = (
        getSeed(newIModelEvent)
        ?? (getBranch(newIModelEvent) && trackedIModels.get(getBranch(newIModelEvent)!)))
        || undefined;

      const newIModelId = await IModelHost.hubAccess.createNewIModel({ iTwinId, iModelName: newIModelName, version0: seed?.db.pathName, noLocks: true });

      const newIModelDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: newIModelId });
      assert.isTrue(newIModelDb.isBriefcaseDb());
      assert.equal(newIModelDb.iTwinId, iTwinId);

      trackedIModels.set(newIModelName, {
        state: seed?.state ?? newIModelEvent as TimelineIModelElemState,
        db: newIModelDb,
        id: newIModelId,
      });

      const isNewBranch = "branch" in newIModelEvent;
      if (isNewBranch) {
        assert(seed);
        masterOfBranch.set(newIModelName, newIModelEvent.branch as string);
        const master = seed;
        const branchDb = newIModelDb;
        // record branch provenance
        const provenanceInserter = new IModelTransformer(master.db, branchDb, { wasSourceIModelCopiedToTarget: true });
        await provenanceInserter.processAll();
        provenanceInserter.dispose();
        assert.equal(count(master.db, ExternalSourceAspect.classFullName), 0);
        assert.isAbove(count(branchDb, ExternalSourceAspect.classFullName), Object.keys(master.state).length);
        await saveAndPushChanges(accessToken, branchDb, "initialized branch provenance");
      } else if ("seed" in newIModelEvent) {
        await saveAndPushChanges(accessToken, newIModelDb, `seeded from '${getSeed(newIModelEvent)!.id}' at point ${i}`);
      } else {
        populateTimelineSeed(newIModelDb, newIModelEvent);
        await saveAndPushChanges(accessToken, newIModelDb, `new with state [${newIModelEvent}] at point ${i}`);
      }

      if (seed) {
        assertElemState(newIModelDb, seed.state);
      }
    }

    for (const [iModelName, event] of iModelChanges) {
      if ("branch" in event || "seed" in event) {
        // "branch" and "seed" event has already been handled in the new imodels loop above
        continue;
      } else if ("sync" in event) {
        const [syncSource, startIndex] = getSync(event)!;
        // if the synchronization source is master, it's a normal sync
        const isForwardSync = masterOfBranch.get(iModelName) === syncSource;
        const target = trackedIModels.get(iModelName)!;
        const source = trackedIModels.get(syncSource)!;

        let targetStateBefore: TimelineIModelElemState | undefined;
        if (process.env.TRANSFORMER_BRANCH_TEST_DEBUG) targetStateBefore = getIModelState(target.db);

        const syncer = new IModelTransformer(source.db, target.db, { isReverseSynchronization: !isForwardSync });
        const startChangesetId = timelineStates.get(startIndex)?.changesets[syncSource].id;
        await syncer.processChanges(accessToken, startChangesetId);
        syncer.dispose();

        const stateMsg = `synced changes from ${syncSource} to ${iModelName} at ${i}`;
        if (process.env.TRANSFORMER_BRANCH_TEST_DEBUG) {
          /* eslint-disable no-console */
          console.log(stateMsg);
          console.log(` source range state: ${JSON.stringify(source.state)}`);
          const targetState = getIModelState(target.db);
          console.log(`target before state: ${JSON.stringify(targetStateBefore!)}`);
          console.log(` target after state: ${JSON.stringify(targetState)}`);
          /* eslint-enable no-console */
        }

        // subset because we don't care about elements that the target added itself
        assertElemState(target.db, source.state, { subset: true });
        target.state = source.state; // update the tracking state

        await saveAndPushChanges(accessToken, target.db, stateMsg);
      } else {
        const delta = event;
        const alreadySeenIModel = trackedIModels.get(iModelName)!;
        const fullState = applyDelta(alreadySeenIModel.state, delta);
        alreadySeenIModel.state = fullState;

        const stateMsg =
            `${iModelName} becomes: ${JSON.stringify(fullState)}, `
          + `delta: [${JSON.stringify(delta)}], at ${i}`;
        if (process.env.TRANSFORMER_BRANCH_TEST_DEBUG) {
          console.log(stateMsg); // eslint-disable-line no-console
        }

        maintainPhysicalObjects(alreadySeenIModel.db, delta);
        await saveAndPushChanges(accessToken, alreadySeenIModel.db, stateMsg);
      }
    }

    if (pt.assert) {
      pt.assert(Object.fromEntries(trackedIModels));
    }

    timelineStates.set(
      i,
      {
        changesets: Object.fromEntries([...trackedIModels].map(([name, state]) => [name, state.db.changeset])),
        states: Object.fromEntries([...trackedIModels].map(([name, state]) => [name, state.state])),
      }
    );
  }

  return {
    trackedIModels,
    timelineStates,
    tearDown: async () => {
      for (const [, state] of trackedIModels) {
        state.db.close();
        await IModelHost.hubAccess.deleteIModel({ iTwinId, iModelId: state.id });
      }
    },
  };
}

