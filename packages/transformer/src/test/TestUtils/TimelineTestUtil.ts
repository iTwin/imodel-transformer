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

export function getPhysicalObjects(iModelDb: IModelDb): TimelineIModelContentsState {
  return iModelDb.withPreparedStatement(
    `SELECT UserLabel, JsonProperties FROM ${PhysicalObject.classFullName}`,
    (s) =>
      Object.fromEntries(
        [...s].map((r) => [r.userLabel, r.jsonProperties && JSON.parse(r.jsonProperties).updateState])
      )
  );
}

export function populateTimelineSeed(db: IModelDb, state?: TimelineIModelContentsState): void {
  SpatialCategory.insert(db, IModel.dictionaryId, "SpatialCategory", new SubCategoryAppearance());
  PhysicalModel.insert(db, IModel.rootSubjectId, "PhysicalModel");
  if (state)
    maintainPhysicalObjects(db, state);
  db.performCheckpoint();
}

export function assertPhysicalObjects(iModelDb: IModelDb, numbers: TimelineIModelContentsState, { subset = false } = {}): void {
  if (subset) {
    for (const n in numbers) {
      if (typeof n !== "string")
        continue;
      assertPhysicalObjectExists(iModelDb, n);
    }
  } else {
    assert.deepEqual(getPhysicalObjects(iModelDb), numbers);
  }
}

export function assertPhysicalObjectExists(iModelDb: IModelDb, key: string): void {
  const physicalObjectId = getPhysicalObjectId(iModelDb, key);
  if (key.startsWith("-")) {
    assert.isTrue(Id64.isValidId64(physicalObjectId), `Expected element ${key} to exist`);
  } else {
    assert.equal(physicalObjectId, Id64.invalid, `Expected element ${key} to not exist`); // negative "n" means element was deleted
  }
}

export function getPhysicalObjectId(iModelDb: IModelDb, nkey: string): Id64String {
  const sql = `SELECT ECInstanceId FROM ${PhysicalObject.classFullName} WHERE UserLabel=:userLabel`;
  return iModelDb.withPreparedStatement(sql, (statement: ECSqlStatement): Id64String => {
    statement.bindString("userLabel", nkey);
    return DbResult.BE_SQLITE_ROW === statement.step() ? statement.getValue(0).getId() : Id64.invalid;
  });
}

export function maintainPhysicalObjects(iModelDb: IModelDb, state: TimelineIModelContentsState): void {
  const modelId = iModelDb.elements.queryElementIdByCode(PhysicalPartition.createCode(iModelDb, IModel.rootSubjectId, "PhysicalModel"))!;
  const categoryId = iModelDb.elements.queryElementIdByCode(SpatialCategory.createCode(iModelDb, IModel.dictionaryId, "SpatialCategory"))!;
  const currentObjs = getPhysicalObjects(iModelDb);
  const objsToDelete = Object.keys(currentObjs).filter((n) => !(n in state));
  for (const obj of objsToDelete) {
    const id = getPhysicalObjectId(iModelDb, obj);
    iModelDb.elements.deleteElement(id);
  }
  for (const i in state) {
    const value = state[i];
    const physicalObjectId = getPhysicalObjectId(iModelDb, i);
    if (Id64.isValidId64(physicalObjectId)) { // if element exists, update it
      const physicalObject = iModelDb.elements.getElement(physicalObjectId, PhysicalObject);
      physicalObject.jsonProperties.updateState = value;
      physicalObject.update();
    } else { // if element does not exist, insert it
      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: new Code({ spec: IModelDb.rootSubjectId, scope: IModelDb.rootSubjectId, value: i }),
        userLabel: i,
        geom: IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1)),
        placement: {
          origin: Point3d.create(0, 0, 0),
          angles: YawPitchRollAngles.createDegrees(0, 0, 0),
        },
        jsonProperties: {
          updateState: value,
        },
      };
      iModelDb.elements.insertElement(physicalObjectProps);
    }
  }
  // TODO: iModelDb.performCheckpoint?
  iModelDb.saveChanges();
}

export interface TimelineIModelContentsState {
  [name: number]: number | ElementProps;
}

export interface TimelineIModelState {
  state: TimelineIModelContentsState;
  id: string;
  db: BriefcaseDb;
}

export type TimelineStateChange =
  // update the state of that model to match and push a changeset
  | TimelineIModelContentsState
  // create a new iModel from a seed
  | { seed: TimelineIModelState }
  // create a branch from an existing iModel with a given name
  | { branch: string }
  // synchronize with the changes in an iModel of a given name from a starting timeline point
  // to the given ending point, inclusive. (end defaults to current point in time)
  | { sync: [string, number] };

/** For each step in timeline, an object of iModels mapping to the event that occurs for them:
 * - a 'seed' event with an iModel to seed from, creating the iModel
 * - a 'branch' event with the name of an iModel to seed from, creating the iModel
 * - a 'sync' event with the name of an iModel and timeline point to sync from
 * - an object containing the content of the iModel that it updates to,
 *   creating the iModel with this initial state if it didn't exist before
 * - an 'assert' function to run on the state of all the iModels in the timeline
 *
 * @note because the timeline manages PhysicalObjects for the state, any seed must contain the necessary
 * model and category, which can be added to your seed by calling @see populateTimelineSeed
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
      states: { [iModelName: string]: TimelineIModelContentsState };
      changesets: { [iModelName: string]: ChangesetIdWithIndex };
    }
  >();
  /* eslint-enable @typescript-eslint/indent */

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

      const seed
        = "seed" in newIModelEvent
          ? newIModelEvent.seed
          : "branch" in newIModelEvent
            ? trackedIModels.get(newIModelEvent.branch)!
            : undefined;

      const newIModelId = await IModelHost.hubAccess.createNewIModel({ iTwinId, iModelName: newIModelName, version0: seed?.db.pathName, noLocks: true });

      const newIModelDb = await HubWrappers.downloadAndOpenBriefcase({ accessToken, iTwinId, iModelId: newIModelId });
      assert.isTrue(newIModelDb.isBriefcaseDb());
      assert.equal(newIModelDb.iTwinId, iTwinId);

      trackedIModels.set(newIModelName, {
        state: seed?.state ?? newIModelEvent as number[],
        db: newIModelDb,
        id: newIModelId,
      });

      const isNewBranch = "branch" in newIModelEvent;
      if (isNewBranch) {
        assert(seed);
        masterOfBranch.set(newIModelName, newIModelEvent.branch);
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
        await saveAndPushChanges(accessToken, newIModelDb, `seeded from '${newIModelEvent.seed.id}' at point ${i}`);
      } else {
        populateTimelineSeed(newIModelDb, newIModelEvent);
        await saveAndPushChanges(accessToken, newIModelDb, `new with state [${newIModelEvent}] at point ${i}`);
      }

      if (seed) {
        assertPhysicalObjects(newIModelDb, seed.state);
      }
    }

    for (const [iModelName, event] of iModelChanges) {
      if ("branch" in event || "seed" in event) {
        // "branch" and "seed" event has already been handled in the new imodels loop above
        continue;
      } else if ("sync" in event) {
        const [syncSource, startIndex] = event.sync;
        // if the synchronization source is master, it's a normal sync
        const isForwardSync = masterOfBranch.get(iModelName) === syncSource;
        const target = trackedIModels.get(iModelName)!;
        const source = trackedIModels.get(syncSource)!;
        const targetStateBefore = getPhysicalObjects(target.db);
        const syncer = new IModelTransformer(source.db, target.db, { isReverseSynchronization: !isForwardSync });
        const startChangesetId = timelineStates.get(startIndex)?.changesets[syncSource].id;
        await syncer.processChanges(accessToken, startChangesetId);
        syncer.dispose();

        const stateMsg = `synced changes from ${syncSource} to ${iModelName} at ${i}`;
        if (process.env.TRANSFORMER_BRANCH_TEST_DEBUG) {
          /* eslint-disable no-console */
          console.log(stateMsg);
          console.log(` source range state: ${JSON.stringify(source.state)}`);
          const targetState = getPhysicalObjects(target.db);
          console.log(`target before state: ${JSON.stringify(targetStateBefore)}`);
          console.log(` target after state: ${JSON.stringify(targetState)}`);
          /* eslint-enable no-console */
        }
        // subset because we don't care about elements that the target added itself
        assertPhysicalObjects(target.db, source.state, { subset: true });
        target.state = source.state; // update the tracking state

        await saveAndPushChanges(accessToken, target.db, stateMsg);
      } else {
        const newState = event;
        const alreadySeenIModel = trackedIModels.get(iModelName)!;
        const prevState = alreadySeenIModel.state;
        alreadySeenIModel.state = event;
        // `(maintain|assert)PhysicalObjects` use negative to mean deleted
        const additions = Object.keys(newState).filter((s) => !(s in prevState)).map(Number);
        const deletions = Object.keys(prevState).filter((s) => !(s in newState)).map(Number);
        const delta = [...additions, ...deletions.map((d) => -d)];

        const stateMsg = `${iModelName} becomes: ${JSON.stringify(event)}, delta: [${delta}], at ${i}`;
        if (process.env.TRANSFORMER_BRANCH_TEST_DEBUG) {
          console.log(stateMsg); // eslint-disable-line no-console
        }

        maintainPhysicalObjects(alreadySeenIModel.db, newState);
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

