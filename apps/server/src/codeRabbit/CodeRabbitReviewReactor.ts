import {
  CODERABBIT_REVIEW_SKILL,
  CommandId,
  MessageId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { forkParked } from "../serverActivation.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";

const CODERABBIT_REVIEW_MESSAGE = `$${CODERABBIT_REVIEW_SKILL} The user turned on CodeRabbit review for this thread. Use the CodeRabbit code-review skill to review the changes you just made with the CodeRabbit CLI, adding --include-untracked when you created new files, then fix the critical and major findings you verify against the code yourself. If you changed code, run the review once more with --fresh to confirm, and end your turn with the findings you left alone and why.`;

export class CodeRabbitReviewReactor extends Context.Service<
  CodeRabbitReviewReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly handle: (event: OrchestrationV2DomainEvent) => Effect.Effect<void>;
  }
>()("t3/codeRabbit/CodeRabbitReviewReactor") {}

const make = Effect.gen(function* () {
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;

  const handle = Effect.fn("CodeRabbitReviewReactor.handle")(
    function* (event: OrchestrationV2DomainEvent) {
      if (event.type !== "checkpoint.captured") return;
      const checkpoint = event.payload;
      if (checkpoint.runId === null || checkpoint.status !== "ready") return;
      if (checkpoint.files.length === 0) return;
      const runId = checkpoint.runId;
      const records = yield* projections.getThreadRecords(
        checkpoint.threadId,
        ["runs", "messages"],
        { runIds: [runId], messageRunIds: [runId], messageRoles: ["user"] },
      );
      const thread = records.thread;
      if (
        thread.codeRabbitReview !== true ||
        thread.archivedAt !== null ||
        thread.deletedAt !== null ||
        thread.lineage.relationshipToParent === "subagent"
      ) {
        return;
      }
      const run = records.runs.find((candidate) => candidate.id === runId);
      if (run?.status !== "completed") return;
      const message = records.messages.find((candidate) => candidate.id === run.userMessageId);
      // Only turns the user started: the review turn itself, and other server or agent wakes,
      // must not start another review.
      if (message?.createdBy !== "user") return;
      yield* engine.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`server:coderabbit-review:${runId}`),
        threadId: thread.id,
        messageId: MessageId.make(`message:coderabbit-review:${runId}`),
        text: CODERABBIT_REVIEW_MESSAGE,
        notification: {
          source: { kind: "monitor" },
          outcome: "updated",
          summary: "CodeRabbit review",
        },
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      });
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("starting a CodeRabbit review failed", {
            threadId: event.threadId,
            cause,
          }),
        ),
      ),
  );

  const start: CodeRabbitReviewReactor["Service"]["start"] = () =>
    forkParked(
      Stream.runForEach(engine.streamDomainEvents, handle).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("CodeRabbit review event stream failed", { cause }),
        ),
      ),
    );

  return { start, handle } satisfies CodeRabbitReviewReactor["Service"];
});

export const layer = Layer.effect(CodeRabbitReviewReactor, make);
