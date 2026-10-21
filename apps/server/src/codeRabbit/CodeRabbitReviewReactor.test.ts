import { assert, describe, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Checkpoint,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RunStatus,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as CodeRabbitReviewReactor from "./CodeRabbitReviewReactor.ts";

const threadId = ThreadId.make("review-thread");
const runId = RunId.make("review-run");
const messageId = MessageId.make("user-message");
const at = DateTime.makeUnsafe("2026-10-21T12:00:00.000Z");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };

const checkpointEvent = (
  overrides: Partial<OrchestrationV2Checkpoint> = {},
): Extract<OrchestrationV2DomainEvent, { type: "checkpoint.captured" }> => ({
  id: EventId.make("checkpoint-event"),
  type: "checkpoint.captured",
  threadId,
  occurredAt: at,
  payload: {
    id: CheckpointId.make("review-checkpoint"),
    threadId,
    runId,
    scopeId: CheckpointScopeId.make("review-scope"),
    nodeId: NodeId.make("review-node"),
    parentCheckpointId: null,
    ordinalWithinScope: 1,
    appRunOrdinal: 1,
    ref: CheckpointRef.make("refs/t3/review"),
    status: "ready",
    files: [{ path: "src/index.ts", kind: "modified", additions: 2, deletions: 1 }],
    capturedAt: at,
    ...overrides,
  },
});

const seed = Effect.fnUntraced(function* (
  options: {
    readonly thread?: Partial<OrchestrationV2AppThread>;
    readonly createdBy?: "user" | "agent";
    readonly runStatus?: OrchestrationV2RunStatus;
  } = {},
) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  yield* store.apply({
    id: EventId.make("thread-event"),
    type: "thread.created",
    threadId,
    occurredAt: at,
    payload: {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId: ProjectId.make("review-project"),
      title: "CodeRabbit review",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "feature",
      worktreePath: "/workspace",
      activeProviderThreadId: null,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      codeRabbitReview: true,
      createdAt: at,
      updatedAt: at,
      archivedAt: null,
      deletedAt: null,
      settledAt: null,
      settledOverride: null,
      lastVisitedAt: null,
      ...options.thread,
    },
  });
  yield* store.apply({
    id: EventId.make("run-event"),
    type: "run.created",
    threadId,
    occurredAt: at,
    payload: {
      id: runId,
      threadId,
      ordinal: 1,
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: messageId,
      rootNodeId: null,
      activeAttemptId: null,
      status: options.runStatus ?? "completed",
      requestedAt: at,
      startedAt: at,
      completedAt: at,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  yield* store.apply({
    id: EventId.make("message-event"),
    type: "message.updated",
    threadId,
    occurredAt: at,
    payload: {
      createdBy: options.createdBy ?? "user",
      creationSource: options.createdBy === "agent" ? "server" : "web",
      id: messageId,
      threadId,
      runId,
      nodeId: null,
      role: "user",
      text: "Implement the change.",
      attachments: [],
      streaming: false,
      createdAt: at,
      updatedAt: at,
    },
  });
});

const harness = () => {
  const commands: Parameters<Orchestrator.OrchestratorV2Shape["dispatch"]>[0][] = [];
  const layer = CodeRabbitReviewReactor.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        ProjectionStore.layerMemory,
        Layer.mock(Orchestrator.OrchestratorV2)({
          dispatch: (command) =>
            Effect.sync(() => {
              commands.push(command);
              return { sequence: commands.length, storedEvents: [] };
            }),
        }),
      ),
    ),
  );
  return { commands, layer };
};

describe("CodeRabbitReviewReactor.handle", () => {
  it.effect("queues a review for a user turn with changed files", () => {
    const fixture = harness();
    return Effect.gen(function* () {
      yield* seed();
      const reactor = yield* CodeRabbitReviewReactor.CodeRabbitReviewReactor;
      yield* reactor.handle(checkpointEvent());
      assert.equal(fixture.commands.length, 1);
      const command = fixture.commands[0];
      assert.equal(command?.type, "message.dispatch");
      if (command?.type !== "message.dispatch") return;
      assert.equal(command.commandId, CommandId.make(`server:coderabbit-review:${runId}`));
      assert.equal(command.threadId, threadId);
      assert.equal(command.messageId, MessageId.make(`message:coderabbit-review:${runId}`));
      assert.include(command.text, "$code-review ");
      assert.equal(command.createdBy, "agent");
      assert.equal(command.creationSource, "server");
      assert.deepEqual(command.dispatchMode, { type: "queue_after_active" });
      yield* reactor.handle(checkpointEvent());
      assert.deepEqual(fixture.commands[1], command);
    }).pipe(Effect.provide(fixture.layer));
  });

  it.effect.each([
    ["flag off", { codeRabbitReview: false }],
    ["flag absent", { codeRabbitReview: undefined }],
    ["archived", { archivedAt: at }],
    ["deleted", { deletedAt: at }],
    [
      "subagent",
      {
        lineage: {
          rootThreadId: ThreadId.make("parent"),
          parentThreadId: ThreadId.make("parent"),
          relationshipToParent: "subagent",
        },
      },
    ],
  ] satisfies ReadonlyArray<readonly [string, Partial<OrchestrationV2AppThread>]>)(
    "skips an ineligible thread: %s",
    ([, thread]) => {
      const fixture = harness();
      return Effect.gen(function* () {
        yield* seed({ thread });
        const reactor = yield* CodeRabbitReviewReactor.CodeRabbitReviewReactor;
        yield* reactor.handle(checkpointEvent());
        assert.deepEqual(fixture.commands, []);
      }).pipe(Effect.provide(fixture.layer));
    },
  );

  it.effect.each([
    ["no files", { files: [] }],
    ["no run", { runId: null }],
    ["missing checkpoint", { status: "missing" }],
    ["unknown run", { runId: RunId.make("unknown") }],
  ] satisfies ReadonlyArray<readonly [string, Partial<OrchestrationV2Checkpoint>]>)(
    "skips an ineligible checkpoint: %s",
    ([, checkpoint]) => {
      const fixture = harness();
      return Effect.gen(function* () {
        yield* seed();
        const reactor = yield* CodeRabbitReviewReactor.CodeRabbitReviewReactor;
        yield* reactor.handle(checkpointEvent(checkpoint));
        assert.deepEqual(fixture.commands, []);
      }).pipe(Effect.provide(fixture.layer));
    },
  );

  it.effect("skips an agent-created user message to prevent recursive reviews", () => {
    const fixture = harness();
    return Effect.gen(function* () {
      yield* seed({ createdBy: "agent" });
      const reactor = yield* CodeRabbitReviewReactor.CodeRabbitReviewReactor;
      yield* reactor.handle(checkpointEvent());
      assert.deepEqual(fixture.commands, []);
    }).pipe(Effect.provide(fixture.layer));
  });

  it.effect.each([
    "preparing",
    "queued",
    "starting",
    "running",
    "waiting",
    "interrupted",
    "failed",
    "cancelled",
    "rolled_back",
  ] satisfies ReadonlyArray<OrchestrationV2RunStatus>)(
    "skips a run that did not complete: %s",
    (runStatus) => {
      const fixture = harness();
      return Effect.gen(function* () {
        yield* seed({ runStatus });
        const reactor = yield* CodeRabbitReviewReactor.CodeRabbitReviewReactor;
        yield* reactor.handle(checkpointEvent());
        assert.deepEqual(fixture.commands, []);
      }).pipe(Effect.provide(fixture.layer));
    },
  );
});
