"""Tests for the concept Workflows, one per primitive."""

import uuid

import pytest
from temporalio.worker import Replayer, Worker

from concepts import (
    CONCEPT_ACTIVITIES,
    CONCEPT_WORKFLOWS,
    CounterWorkflow,
    DeterminismWorkflow,
    LocalActivityWorkflow,
    ParentWorkflow,
    VersionedWorkflow,
)


def worker_for(env, task_queue: str) -> Worker:
    return Worker(
        env.client,
        task_queue=task_queue,
        workflows=CONCEPT_WORKFLOWS,
        activities=CONCEPT_ACTIVITIES,
    )


async def test_update_returns_a_value_and_validator_rejects_bad_input(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, task_queue):
        handle = await env.client.start_workflow(
            CounterWorkflow.run, 0, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )

        assert await handle.execute_update(CounterWorkflow.add, 5) == 5
        assert await handle.execute_update(CounterWorkflow.add, 7) == 12

        # The validator rejects before anything is written to the history.
        with pytest.raises(Exception):
            await handle.execute_update(CounterWorkflow.add, -1)

        # Rejection left the state untouched.
        assert await handle.query(CounterWorkflow.total) == 12

        await handle.signal(CounterWorkflow.finish)
        assert await handle.result() == 12


async def test_continue_as_new_starts_a_new_run_preserving_state(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, task_queue):
        workflow_id = f"wf-{uuid.uuid4()}"
        handle = await env.client.start_workflow(
            CounterWorkflow.run, 0, id=workflow_id, task_queue=task_queue
        )
        first_run_id = handle.first_execution_run_id

        # Each Update adds events; eventually the Workflow rolls over.
        for _ in range(25):
            await handle.execute_update(CounterWorkflow.add, 1)

        fresh = env.client.get_workflow_handle(workflow_id)
        description = await fresh.describe()
        assert description.run_id != first_run_id, "expected a continue-as-new"

        # State carried across the rollover, ID unchanged.
        assert await fresh.query(CounterWorkflow.total) == 25
        await fresh.signal(CounterWorkflow.finish)


async def test_child_workflows_run_and_return_in_argument_order(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, task_queue):
        result = await env.client.execute_workflow(
            ParentWorkflow.run,
            ["ada", "alan", "grace"],
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert result == ["hello ada", "hello alan", "hello grace"]


async def test_deterministic_helpers_are_stable_across_replay(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, task_queue):
        workflow_id = f"wf-{uuid.uuid4()}"
        first = await env.client.execute_workflow(
            DeterminismWorkflow.run, id=workflow_id, task_queue=task_queue
        )

        # Replaying the recorded history must reproduce the same uuid/random,
        # which is the entire reason workflow.uuid4() exists. Replay raises a
        # NondeterminismError if the values came out differently.
        history = await env.client.get_workflow_handle(workflow_id).fetch_history()
        await Replayer(workflows=CONCEPT_WORKFLOWS).replay_workflow(history)
        assert first["uuid"] and first["random"]


async def test_local_activity_runs_without_a_task_queue_round_trip(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, task_queue):
        assert (
            await env.client.execute_workflow(
                LocalActivityWorkflow.run,
                args=[2, 40],
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            == 42
        )


async def test_patched_workflow_takes_the_new_branch_on_a_fresh_run(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, task_queue):
        result = await env.client.execute_workflow(
            VersionedWorkflow.run,
            "ada",
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert result == "hello ADA"
