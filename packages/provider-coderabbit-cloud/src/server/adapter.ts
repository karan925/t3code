import {
  ProviderDriverKind,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ExecutionNode,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
} from "@t3tools/provider-core/server/adapterDriver";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { CodeRabbitCloudSettings } from "../settings.ts";
import { CommandError, runCodeCommand, type CodeEvent, type PendingQuestion } from "./protocol.ts";
import { verifyCloudWorkspace } from "./workspace.ts";

const CODERABBIT_CLOUD_PROVIDER = ProviderDriverKind.make("coderabbit-cloud");
const DRIVER = CODERABBIT_CLOUD_PROVIDER;
const decodeSettings = Schema.decodeSync(CodeRabbitCloudSettings);
const encodeAnswers = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      interactionId: Schema.String,
      answers: Schema.Record(Schema.String, Schema.String),
    }),
  ),
);

const codeRabbitCloudCapabilities = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: false,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: false,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: false,
    streamsReasoning: false,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: false,
    emitsToolStarted: false,
    emitsToolCompleted: false,
    emitsToolOutput: false,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: false,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: 48_000,
  },
  checkpointing: {
    appCanCheckpointFilesystem: false,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

const ref = (nativeId: string) => ({ driver: DRIVER, nativeId, strength: "strong" as const });
const protocolError = (detail: string) =>
  new ProviderAdapter.ProviderAdapterProtocolError({ driver: DRIVER, detail });

interface ActiveTurn {
  readonly input: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly receipt: Deferred.Deferred<string | null>;
  wake: Deferred.Deferred<void>;
  follower: Fiber.Fiber<void> | null;
  taskId: string | null;
  taskURL: string | null;
  operationId: string | null;
  acceptedMessageSeq: number | null;
  awaitingMessageReceipt: boolean;
  deferredMessage: string | null;
  interruptRequested: boolean;
  discardedUnsentMessage: boolean;
  lastSeq: number;
  lastAssistantText: string | null;
  nextOrdinal: number;
  finished: boolean;
  resolvedQuestions: Set<string>;
  pending: {
    readonly question: PendingQuestion;
    readonly answerable: boolean;
    readonly request: OrchestrationV2RuntimeRequest;
    readonly node: OrchestrationV2ExecutionNode;
    readonly item: Extract<OrchestrationV2TurnItem, { type: "user_input_request" }>;
  } | null;
}

export interface CodeRabbitCloudAdapterOptions {
  readonly instanceId: ProviderInstanceId;
  readonly settings: CodeRabbitCloudSettings;
  readonly environment: NodeJS.ProcessEnv;
}

export const makeCodeRabbitCloudAdapterV2 = Effect.fn("makeCodeRabbitCloudAdapterV2")(function* (
  options: CodeRabbitCloudAdapterOptions,
) {
  const ids = yield* IdAllocator.IdAllocatorV2;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DRIVER,
    getCapabilities: () => Effect.succeed(codeRabbitCloudCapabilities),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("CodeRabbitCloud.openSession")(function* (input) {
      if (input.runtimePolicy.runtimeMode !== "full-access") {
        return yield* protocolError(
          "CodeRabbit Cloud runs remotely and only supports full-access mode",
        );
      }
      if (!input.runtimePolicy.cwd)
        return yield* protocolError("CodeRabbit Cloud requires a repository directory");
      const cwd = input.runtimePolicy.cwd;
      const scope = yield* Effect.scope;
      const now = yield* DateTime.now;
      let session: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: DRIVER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: "default",
        capabilities: codeRabbitCloudCapabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const eventPermit = yield* Semaphore.make(1);
      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);
      let thread: OrchestrationV2ProviderThread | null = null;
      let active: ActiveTurn | null = null;
      let reattachOnNextTurn =
        input.resumeFromSession?.status === "running" ||
        input.resumeFromSession?.status === "waiting";
      const command = (
        args: ReadonlyArray<string>,
        onEvent: (event: CodeEvent) => Effect.Effect<void, CommandError>,
        stdin?: string,
      ) =>
        runCodeCommand({
          binaryPath: options.settings.binaryPath,
          args: ["code", ...args, "--agent"],
          cwd,
          env: options.environment,
          onEvent,
          ...(stdin === undefined ? {} : { stdin }),
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      const updateSession = Effect.fnUntraced(function* (
        status: OrchestrationV2ProviderSession["status"],
      ) {
        session = { ...session, status, updatedAt: yield* DateTime.now };
        yield* emit({ type: "provider_session.updated", driver: DRIVER, providerSession: session });
      });
      const updateThread = Effect.fnUntraced(function* (
        patch: Partial<OrchestrationV2ProviderThread>,
      ) {
        if (!thread) return;
        thread = { ...thread, ...patch, updatedAt: yield* DateTime.now };
        yield* emit({ type: "provider_thread.updated", driver: DRIVER, providerThread: thread });
      });
      const itemBase = (turn: ActiveTurn, nativeId: string, at: DateTime.Utc) => ({
        id: ids.derive.turnItemFromProviderItem({ driver: DRIVER, nativeItemId: nativeId }),
        threadId: turn.input.threadId,
        runId: turn.input.runId,
        nodeId: turn.input.rootNodeId,
        providerThreadId: turn.input.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: ref(nativeId),
        parentItemId: null,
        ordinal: turn.nextOrdinal++,
        title: null,
        startedAt: at,
        completedAt: at,
        updatedAt: at,
      });
      const say = Effect.fnUntraced(function* (turn: ActiveTurn, key: string, text: string) {
        if (!text.trim()) return;
        turn.lastAssistantText = text;
        const at = yield* DateTime.now;
        const nativeId = `${turn.providerTurn.id}:${key}`;
        const messageId = ids.derive.messageFromProviderItem({
          driver: DRIVER,
          nativeItemId: nativeId,
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DRIVER,
          turnItem: {
            ...itemBase(turn, nativeId, at),
            type: "assistant_message",
            messageId,
            text,
            status: "completed",
            streaming: false,
          },
        });
        yield* emit({
          type: "message.updated",
          driver: DRIVER,
          message: {
            id: messageId,
            threadId: turn.input.threadId,
            runId: turn.input.runId,
            nodeId: turn.input.rootNodeId,
            role: "assistant",
            text,
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
            createdAt: at,
            updatedAt: at,
          },
        });
      });
      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        outcome: "completed" | "failed" | "cancelled" | "interrupted",
        message?: string,
      ) {
        if (turn.finished) return;
        turn.finished = true;
        const at = yield* DateTime.now;
        if (turn.pending) {
          yield* emit({
            type: "runtime_request.updated",
            driver: DRIVER,
            threadId: turn.input.threadId,
            runtimeRequest: { ...turn.pending.request, status: "cancelled", resolvedAt: at },
          });
          yield* emit({
            type: "node.updated",
            driver: DRIVER,
            node: { ...turn.pending.node, status: "cancelled", completedAt: at },
          });
          yield* emit({
            type: "turn_item.updated",
            driver: DRIVER,
            turnItem: { ...turn.pending.item, status: "cancelled", completedAt: at, updatedAt: at },
          });
          turn.pending = null;
        }
        yield* emit({
          type: "provider_turn.updated",
          driver: DRIVER,
          threadId: turn.input.threadId,
          providerTurn: { ...turn.providerTurn, status: outcome, completedAt: at },
        });
        yield* updateThread({ status: "idle" });
        yield* updateSession(outcome === "failed" ? "error" : "ready");
        if (outcome === "failed") {
          const failure = makeProviderFailure({
            message: message ?? "CodeRabbit Cloud failed. Open the cloud task to inspect it.",
            retryable: false,
          });
          const ordinal = turn.nextOrdinal;
          yield* emit({
            type: "turn_item.updated",
            driver: DRIVER,
            turnItem: {
              ...itemBase(turn, `${turn.providerTurn.id}:failure`, at),
              type: "error",
              status: "failed",
              failure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: DRIVER,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status: "failed",
            failure,
            failureItemOrdinal: ordinal,
            threadDisposition: "reusable",
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: DRIVER,
            providerThreadId: turn.input.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.input.runOrdinal,
            status: outcome,
            failure: null,
            threadDisposition: "reusable",
          });
        }
        yield* Deferred.succeed(turn.receipt, turn.taskId);
        if (active === turn) active = null;
      }, Effect.uninterruptible);
      const ask = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        event: Extract<CodeEvent, { type: "action_required"; action: "answer_question" }>,
      ) {
        if (
          turn.finished ||
          turn.resolvedQuestions.has(event.question.interactionId) ||
          turn.pending?.question.interactionId === event.question.interactionId
        )
          return;
        if (turn.pending) yield* resolveQuestion(turn);
        const at = yield* DateTime.now;
        const requestId = yield* ids.allocate.runtimeRequest({
          driver: DRIVER,
          providerTurnId: turn.providerTurn.id,
          nativeRequestId: event.question.interactionId,
        });
        const nodeId = ids.derive.approvalNode({ requestId });
        const answerable =
          event.answerTemplate !== null && !event.question.questions.some((q) => q.isSensitive);
        const request: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId: turn.providerTurn.id,
          nativeRequestRef: ref(event.question.interactionId),
          kind: "user_input",
          status: "pending",
          createdAt: at,
          resolvedAt: null,
          responseCapability: answerable
            ? { type: "live", providerSessionId: input.providerSessionId }
            : {
                type: "not_resumable",
                reason: "Answer this question directly in the CodeRabbit Cloud task.",
              },
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId: turn.input.threadId,
          runId: turn.input.runId,
          parentNodeId: turn.input.rootNodeId,
          rootNodeId: turn.input.rootNodeId,
          kind: "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId: turn.input.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: ref(event.question.interactionId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: at,
          completedAt: null,
        };
        const item: Extract<OrchestrationV2TurnItem, { type: "user_input_request" }> = {
          ...itemBase(turn, `${turn.providerTurn.id}:question:${event.question.interactionId}`, at),
          id: ids.derive.approvalTurnItem({ requestId }),
          nodeId,
          completedAt: null,
          type: "user_input_request",
          status: "waiting",
          requestId,
          questions: event.question.questions.map((q) => ({
            id: q.id,
            header: q.header || "Question",
            question: q.question || "Open CodeRabbit Cloud to answer.",
            allowCustomAnswer: q.isOther || q.options === null,
            options: (q.options ?? []).map((o) => ({
              label: o.label,
              description: o.description || o.label,
            })),
          })),
        };
        turn.pending = { question: event.question, answerable, request, node, item };
        yield* emit({
          type: "runtime_request.updated",
          driver: DRIVER,
          threadId: turn.input.threadId,
          runtimeRequest: request,
        });
        yield* emit({ type: "node.updated", driver: DRIVER, node });
        yield* emit({ type: "turn_item.updated", driver: DRIVER, turnItem: item });
        if (!answerable)
          yield* say(
            turn,
            `question-link:${event.question.interactionId}`,
            `Answer this question directly in CodeRabbit Cloud: ${turn.taskURL ?? "open the task in the web app"}`,
          );
        yield* updateSession("waiting");
      });
      const resolveQuestion = Effect.fnUntraced(function* (turn: ActiveTurn) {
        const pending = turn.pending;
        if (!pending) return;
        turn.resolvedQuestions.add(pending.question.interactionId);
        turn.pending = null;
        const at = yield* DateTime.now;
        yield* emit({
          type: "runtime_request.updated",
          driver: DRIVER,
          threadId: turn.input.threadId,
          runtimeRequest: { ...pending.request, status: "resolved", resolvedAt: at },
        });
        yield* emit({
          type: "node.updated",
          driver: DRIVER,
          node: { ...pending.node, status: "completed", completedAt: at },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DRIVER,
          turnItem: { ...pending.item, status: "completed", completedAt: at, updatedAt: at },
        });
        yield* updateSession("running");
      });
      const handle = Effect.fnUntraced(function* (turn: ActiveTurn, event: CodeEvent) {
        if (turn.finished) return;
        if (turn.discardedUnsentMessage) {
          yield* finish(turn, "cancelled");
          return;
        }
        if ("taskId" in event && event.taskId) {
          if (turn.taskId !== null && turn.taskId !== event.taskId)
            return yield* new CommandError({ operation: "correlate task" });
          turn.taskId = event.taskId;
          if ("url" in event && event.url) turn.taskURL = event.url;
          if (thread?.nativeThreadRef?.nativeId !== event.taskId) {
            yield* updateThread({ nativeThreadRef: ref(event.taskId) });
            yield* say(
              turn,
              "cloud-task",
              `Working in CodeRabbit Cloud: ${turn.taskURL ?? event.taskId}\nChanges stay in the cloud until explicitly delivered.`,
            );
          }
          yield* Deferred.succeed(turn.receipt, event.taskId);
        }
        if (event.type === "status" && event.status === "message_sent") {
          turn.operationId = event.clientOperationId ?? null;
          turn.awaitingMessageReceipt = false;
          turn.deferredMessage = null;
          if (turn.operationId !== null)
            yield* updateThread({ nativeConversationHeadRef: ref(turn.operationId) });
        }
        if (event.type === "trace") {
          if (event.seq <= turn.lastSeq) return;
          turn.lastSeq = event.seq;
          if (
            event.kind === "user_message" &&
            event.queueItemId === turn.operationId &&
            turn.operationId !== null
          )
            turn.acceptedMessageSeq = event.seq;
          if (
            event.kind === "answer_submitted" &&
            event.interactionId === turn.pending?.question.interactionId
          )
            yield* resolveQuestion(turn);
          if (event.kind === "agent_message") yield* say(turn, `trace:${event.seq}`, event.text);
          else if (event.kind === "plan") yield* say(turn, `trace:${event.seq}`, event.markdown);
          else if (event.kind === "command" && event.status !== "started") {
            const at = yield* DateTime.now;
            yield* emit({
              type: "turn_item.updated",
              driver: DRIVER,
              turnItem: {
                ...itemBase(turn, `${turn.providerTurn.id}:trace:${event.seq}`, at),
                type: "command_execution",
                input: event.command,
                title: "Cloud command",
                status: event.status,
                completedAt: at,
                ...(event.exitCode === null ? {} : { exitCode: event.exitCode }),
              },
            });
          }
          return;
        }
        if (event.type === "action_required") {
          if (event.action === "answer_question") {
            yield* ask(turn, event).pipe(
              Effect.mapError(
                (cause) => new CommandError({ operation: "publish question", cause }),
              ),
            );
          } else if (event.action === "authenticate") {
            yield* finish(
              turn,
              "failed",
              "CodeRabbit CLI needs authentication. Run cr auth login, then resume the existing cloud task.",
            );
          }
          return;
        }
        if ((event.type === "complete" || event.type === "error") && "turn" in event) {
          if (turn.awaitingMessageReceipt || turn.deferredMessage !== null) return;
          if (
            turn.operationId !== null &&
            event.clientOperationId !== turn.operationId &&
            event.turn.queueItemId !== turn.operationId &&
            !(
              turn.acceptedMessageSeq !== null &&
              event.turn.startSeq !== null &&
              event.turn.endSeq !== null &&
              event.turn.startSeq <= turn.acceptedMessageSeq &&
              turn.acceptedMessageSeq <= event.turn.endSeq
            )
          )
            return;
          const text = event.turn.finalReply ?? event.turn.errorMessage;
          if (text && text !== turn.lastAssistantText) {
            yield* say(
              turn,
              "result",
              `${text}${turn.taskURL ? `\n\nCloud task: ${turn.taskURL}` : ""}`,
            );
          }
          const outcome = event.turn.outcome === "canceled" ? "cancelled" : event.turn.outcome;
          yield* finish(turn, outcome, event.turn.errorMessage ?? undefined);
        } else if (event.type === "error") {
          if (["message_dropped", "turn_not_running", "agent_not_running"].includes(event.status)) {
            yield* finish(
              turn,
              "failed",
              `CodeRabbit Cloud could not deliver this message (${event.status}). Open the cloud task before retrying.`,
            );
            return;
          }
          return yield* new CommandError({ operation: "cloud command", cause: event });
        }
      });
      const follow = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        args: ReadonlyArray<string>,
        stdin?: string,
      ) {
        let nextArgs = args;
        let nextInput = stdin;
        let failures = 0;
        let questionWaitSeconds = 10;
        let resolvedQuestionWaits = 0;
        let unrelatedSettlements = 0;
        const pause = Effect.fnUntraced(function* (seconds: number) {
          yield* Effect.raceFirst(Effect.sleep(`${seconds} seconds`), Deferred.await(turn.wake));
          turn.wake = yield* Deferred.make<void>();
        });
        while (!turn.finished) {
          const sendingMessage = nextArgs.includes("--message");
          let sawResolvedQuestion = false;
          const result = yield* command(
            nextArgs,
            (event) =>
              Effect.gen(function* () {
                if (
                  event.type === "action_required" &&
                  event.action === "answer_question" &&
                  sendingMessage &&
                  turn.awaitingMessageReceipt
                ) {
                  // The CLI explicitly declined to send this message while blocked.
                  if (turn.interruptRequested) {
                    turn.discardedUnsentMessage = true;
                    turn.awaitingMessageReceipt = false;
                    turn.deferredMessage = null;
                    yield* finish(turn, "cancelled");
                    return;
                  }
                  turn.deferredMessage = nextInput ?? null;
                }
                if (
                  event.type === "action_required" &&
                  event.action === "answer_question" &&
                  turn.resolvedQuestions.has(event.question.interactionId)
                )
                  sawResolvedQuestion = true;
                const sameQuestion =
                  event.type === "action_required" &&
                  event.action === "answer_question" &&
                  event.question.interactionId === turn.pending?.question.interactionId;
                const progress =
                  (event.type === "trace" && event.seq > turn.lastSeq) ||
                  ((event.type === "complete" || event.type === "error") && "turn" in event) ||
                  (event.type === "status" &&
                    ["turn_started", "waiting", "answer_sent"].includes(event.status)) ||
                  (event.type === "action_required" && event.action === "resume");
                // Reconnect preloads past answers without replaying their trace.
                const pendingBefore = turn.pending;
                const previousSeq = turn.lastSeq;
                const previousOperation = turn.operationId;
                if (turn.pending && !sameQuestion && progress) yield* resolveQuestion(turn);
                yield* handle(turn, event);
                if (
                  turn.lastSeq > previousSeq ||
                  turn.pending !== pendingBefore ||
                  turn.operationId !== previousOperation
                ) {
                  failures = 0;
                  resolvedQuestionWaits = 0;
                }
              }).pipe(eventPermit.withPermits(1), Effect.uninterruptible),
            nextInput,
          ).pipe(Effect.result);
          if (turn.finished) return;
          if (
            turn.deferredMessage !== null &&
            !turn.pending &&
            !sawResolvedQuestion &&
            !turn.interruptRequested &&
            turn.taskId
          ) {
            nextArgs = ["resume", turn.taskId, "--message", "-"];
            nextInput = turn.deferredMessage;
            continue;
          }
          if (result._tag === "Failure") {
            if (turn.taskId && !turn.awaitingMessageReceipt && failures < 3) {
              yield* pause(2 ** failures++);
              nextArgs = ["resume", turn.taskId];
              nextInput = undefined;
              continue;
            }
            return yield* result.failure;
          }
          const code = result.success;
          if (code === 3 && sawResolvedQuestion && turn.taskId) {
            if (++resolvedQuestionWaits > 8)
              return yield* new CommandError({
                operation: "answer acknowledgement did not reach the cloud trace",
              });
            yield* pause(Math.min(10, 2 ** (resolvedQuestionWaits - 1)));
          } else if (code === 3 && turn.pending && turn.taskId) {
            yield* pause(questionWaitSeconds);
            questionWaitSeconds = Math.min(60, questionWaitSeconds * 2);
          } else if (code === 4 && turn.taskId) {
            failures = 0;
          } else if (code === 0 && turn.taskId && turn.operationId !== null) {
            // Plain resume may report the earlier turn while our message is queued.
            if (++unrelatedSettlements > 20)
              return yield* new CommandError({
                operation: "queued message settlement could not be correlated",
              });
            yield* pause(Math.min(30, 2 ** Math.min(unrelatedSettlements - 1, 5)));
          } else if (turn.taskId && !turn.awaitingMessageReceipt && failures < 3) {
            yield* pause(2 ** failures++);
          } else {
            return yield* new CommandError({ operation: "follow ended without a terminal result" });
          }
          if (!turn.taskId)
            return yield* new CommandError({ operation: "follow has no task receipt" });
          nextArgs = ["resume", turn.taskId];
          nextInput = undefined;
        }
      });
      const launch = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        args: ReadonlyArray<string>,
        stdin?: string,
      ) {
        turn.follower = yield* follow(turn, args, stdin).pipe(
          Effect.catch(() =>
            finish(
              turn,
              "failed",
              `CodeRabbit Cloud connection failed.${turn.taskId ? ` The task may still be running: ${turn.taskURL ?? turn.taskId}. Resume that task instead of starting another.` : " Check CodeRabbit Cloud before retrying; task admission could not be confirmed."}`,
            ).pipe(eventPermit.withPermits(1)),
          ),
          Effect.forkIn(scope),
        );
      });
      const wakeFollower = (turn: ActiveTurn) =>
        turn.finished ? Effect.void : Deferred.succeed(turn.wake, undefined).pipe(Effect.asVoid);
      // A control command has its own local observer. Its rejection never ends
      // the healthy cloud follower or replays a previously submitted message.
      const control = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        action: "steer" | "answer",
        text: string,
      ) {
        if (turn.finished || !turn.taskId)
          return yield* new CommandError({ operation: "control settled turn" });
        const ack = yield* Deferred.make<void, CommandError>();
        let submitted = false;
        let operationId: string | null = null;
        const send = command(
          ["resume", turn.taskId, `--${action}`, "-"],
          (event) =>
            Effect.gen(function* () {
              if (event.type === "error" && !("turn" in event)) {
                yield* Deferred.fail(
                  ack,
                  new CommandError({
                    operation: `${action} ${submitted ? "delivery unconfirmed" : "rejected"}`,
                    cause: event,
                  }),
                );
                return;
              }
              if (event.type === "action_required" && event.action === "answer_question") {
                if (event.question.interactionId !== turn.pending?.question.interactionId) {
                  yield* ask(turn, event).pipe(
                    Effect.mapError(
                      (cause) => new CommandError({ operation: "publish question", cause }),
                    ),
                  );
                }
                if (!submitted)
                  yield* Deferred.fail(
                    ack,
                    new CommandError({ operation: `${action} requires an answer` }),
                  );
              }
              if (event.type === "status" && event.status === `${action}_sent`) {
                submitted = true;
                operationId = event.clientOperationId ?? null;
                if (action === "answer") {
                  yield* resolveQuestion(turn);
                  yield* Deferred.succeed(ack, undefined);
                }
              }
              if (
                action === "steer" &&
                event.type === "trace" &&
                event.kind === "steer_result" &&
                event.reservationId === operationId &&
                submitted
              ) {
                if (event.result === "accepted") yield* Deferred.succeed(ack, undefined);
                else
                  yield* Deferred.fail(ack, new CommandError({ operation: "steer not delivered" }));
              }
              if (
                (event.type === "complete" || event.type === "error") &&
                "turn" in event &&
                submitted
              ) {
                // The CLI waits for deferred steering before emitting this settlement.
                yield* Deferred.succeed(ack, undefined);
              }
              if (event.type === "action_required" && event.action === "resume") {
                yield* Deferred.fail(
                  ack,
                  new CommandError({ operation: `${action} delivery unconfirmed` }),
                );
              }
            }).pipe(eventPermit.withPermits(1), Effect.uninterruptible),
          text,
        ).pipe(
          Effect.flatMap(() =>
            Deferred.fail(
              ack,
              new CommandError({
                operation: `${action} ${submitted ? "delivery unconfirmed" : "ended without receipt"}`,
              }),
            ),
          ),
          Effect.catch((cause) =>
            Deferred.fail(
              ack,
              submitted
                ? new CommandError({ operation: `${action} delivery unconfirmed`, cause })
                : cause,
            ),
          ),
          Effect.andThen(Deferred.await(ack)),
        );
        yield* Effect.raceFirst(send, Deferred.await(ack));
      });
      const register = Effect.fnUntraced(function* (
        request: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (active)
          return yield* protocolError("Cannot switch a CodeRabbit Cloud task while it is running");
        const at = yield* DateTime.now;
        const existing = request.existingProviderThread;
        thread = existing
          ? {
              ...existing,
              providerSessionId: input.providerSessionId,
              status: "idle",
              updatedAt: at,
            }
          : {
              id: ids.derive.providerThread({
                driver: DRIVER,
                providerInstanceId: options.instanceId,
                nativeThreadId: request.threadId,
              }),
              driver: DRIVER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: request.threadId,
              ownerNodeId: null,
              nativeThreadRef: input.initialNativeThreadId
                ? ref(input.initialNativeThreadId)
                : null,
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [],
              createdAt: at,
              updatedAt: at,
            };
        yield* emit({ type: "provider_thread.updated", driver: DRIVER, providerThread: thread });
        return thread;
      });
      const unsupported = () =>
        Effect.fail(protocolError("CodeRabbit Cloud does not support this operation"));
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DRIVER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return session;
        },
        events: Stream.fromQueue(events),
        ensureThread: register,
        resumeThread: (request) =>
          register({
            threadId: request.threadId ?? request.providerThread.appThreadId ?? input.threadId,
            modelSelection: request.modelSelection ?? input.modelSelection,
            runtimePolicy: request.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: request.providerThread,
          }),
        startTurn: Effect.fn("CodeRabbitCloud.startTurn")(function* (request) {
          if (active) return yield* protocolError("A CodeRabbit Cloud turn is already running");
          if (!thread || thread.id !== request.providerThread.id)
            return yield* protocolError("CodeRabbit Cloud thread is not registered");
          if (
            request.runtimePolicy.runtimeMode !== "full-access" ||
            request.runtimePolicy.interactionMode !== "default"
          ) {
            return yield* protocolError(
              "CodeRabbit Cloud supports only full-access mode with the default interaction mode",
            );
          }
          if (request.modelSelection.model !== "default")
            return yield* protocolError(
              "CodeRabbit Cloud chooses its model; select the default model",
            );
          if (request.message.attachments.length)
            return yield* protocolError(
              "CodeRabbit Cloud CLI does not accept attachments; include text or repository paths",
            );
          const text = [
            ...(request.appContext ?? []).map((c) => c.text),
            request.message.text,
          ].join("\n\n");
          if (
            !text.trim() ||
            text.length > 65_536 ||
            new TextEncoder().encode(text).byteLength > 262_144
          )
            return yield* protocolError(
              "CodeRabbit Cloud requires a nonempty prompt of at most 65,536 characters",
            );
          const taskId = thread.nativeThreadRef?.nativeId ?? null;
          const reattach =
            taskId !== null &&
            (reattachOnNextTurn || request.restartContinuationOfRunId !== undefined);
          const savedHead = thread.nativeConversationHeadRef?.nativeId ?? null;
          reattachOnNextTurn = false;
          if (taskId === null)
            yield* verifyCloudWorkspace(cwd, options.environment).pipe(
              Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              Effect.mapError((cause) => protocolError(cause.message)),
            );
          const at = yield* DateTime.now;
          const providerTurn: OrchestrationV2ProviderTurn = {
            id: ids.derive.providerTurn({
              driver: DRIVER,
              nativeTurnId: `${request.providerThread.id}:${request.attemptId}`,
            }),
            providerThreadId: request.providerThread.id,
            nodeId: request.rootNodeId,
            runAttemptId: request.attemptId,
            nativeTurnRef: { ...ref(request.attemptId), strength: "weak" },
            ordinal: request.providerTurnOrdinal,
            status: "running",
            startedAt: at,
            completedAt: null,
          };
          const turn: ActiveTurn = {
            input: request,
            providerTurn,
            receipt: yield* Deferred.make<string | null>(),
            wake: yield* Deferred.make<void>(),
            follower: null,
            taskId,
            taskURL: taskId
              ? `https://app.coderabbit.ai/code/tasks/${encodeURIComponent(taskId)}`
              : null,
            operationId: reattach && !savedHead?.startsWith("initial:") ? savedHead : null,
            acceptedMessageSeq: null,
            awaitingMessageReceipt: taskId !== null && !reattach,
            deferredMessage: null,
            interruptRequested: false,
            discardedUnsentMessage: false,
            lastSeq: -1,
            lastAssistantText: null,
            nextOrdinal: 1,
            finished: false,
            resolvedQuestions: new Set(),
            pending: null,
          };
          active = turn;
          if (taskId) yield* Deferred.succeed(turn.receipt, taskId);
          yield* updateSession("running");
          yield* updateThread({
            status: "active",
            ...(reattach
              ? {}
              : {
                  nativeConversationHeadRef: ref(
                    `${taskId ? "pending" : "initial"}:${request.attemptId}`,
                  ),
                }),
            firstRunOrdinal: thread.firstRunOrdinal ?? request.runOrdinal,
            lastRunOrdinal: request.runOrdinal,
          });
          yield* emit({
            type: "provider_turn.updated",
            driver: DRIVER,
            threadId: request.threadId,
            providerTurn,
          });
          if (reattach && (savedHead === null || savedHead.startsWith("pending:"))) {
            yield* finish(
              turn,
              "failed",
              "CodeRabbit Cloud message delivery was not confirmed before the restart. Open the existing cloud task to check it before sending another message.",
            );
            return;
          }
          yield* launch(
            turn,
            taskId
              ? reattach
                ? ["resume", taskId]
                : ["resume", taskId, "--message", "-"]
              : ["new", "--resume", "-"],
            reattach ? undefined : text,
          );
        }),
        steerTurn: Effect.fn("CodeRabbitCloud.steerTurn")(function* (request) {
          const turn = active;
          if (
            !turn ||
            turn.finished ||
            turn.providerTurn.id !== request.providerTurnId ||
            !turn.taskId
          )
            return yield* protocolError("No running CodeRabbit Cloud task is available to steer");
          if (turn.awaitingMessageReceipt || turn.deferredMessage !== null)
            return yield* protocolError(
              "Wait for CodeRabbit Cloud to accept the message before steering",
            );
          if (turn.pending)
            return yield* protocolError("Answer the pending CodeRabbit question before steering");
          const text = request.message.text;
          if (
            request.message.attachments.length ||
            !text.trim() ||
            text.length > 65_536 ||
            new TextEncoder().encode(text).byteLength > 262_144
          )
            return yield* protocolError(
              "CodeRabbit steering requires text without attachments within the prompt limit",
            );
          yield* control(turn, "steer", text).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver: DRIVER,
                  providerThreadId: request.providerThread.id,
                  providerTurnId: request.providerTurnId,
                  cause,
                }),
            ),
          );
        }),
        interruptTurn: Effect.fn("CodeRabbitCloud.interruptTurn")(function* (request) {
          const turn = active;
          if (!turn || turn.finished || turn.providerTurn.id !== request.providerTurnId) return;
          const taskId =
            turn.taskId ??
            (yield* Deferred.await(turn.receipt).pipe(
              Effect.timeout("30 seconds"),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: DRIVER,
                    providerThreadId: request.providerThread.id,
                    providerTurnId: request.providerTurnId,
                    cause,
                  }),
              ),
            ));
          if (!taskId || turn.finished) return;
          yield* Effect.sync(() => {
            turn.interruptRequested = true;
            if (turn.deferredMessage !== null) {
              turn.discardedUnsentMessage = true;
              turn.deferredMessage = null;
              turn.awaitingMessageReceipt = false;
            }
          }).pipe(eventPermit.withPermits(1));
          const code = yield* command(["cancel", taskId], (event) =>
            Effect.gen(function* () {
              if (event.type === "error") return yield* new CommandError({ operation: "cancel" });
              if (event.type === "complete" && event.status === "task_canceled")
                yield* finish(turn, "cancelled");
            }).pipe(eventPermit.withPermits(1), Effect.uninterruptible),
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterInterruptError({
                  driver: DRIVER,
                  providerThreadId: request.providerThread.id,
                  providerTurnId: request.providerTurnId,
                  cause,
                }),
            ),
          );
          if (code !== 0)
            return yield* new ProviderAdapter.ProviderAdapterInterruptError({
              driver: DRIVER,
              providerThreadId: request.providerThread.id,
              providerTurnId: request.providerTurnId,
            });
          // An unsent local message can be cancelled immediately. Admitted work
          // still needs a cloud terminal; cancel_pending is only an acknowledgement.
          if (turn.discardedUnsentMessage)
            yield* finish(turn, "cancelled").pipe(eventPermit.withPermits(1));
          else if (!turn.finished) yield* wakeFollower(turn);
        }),
        respondToRuntimeRequest: Effect.fn("CodeRabbitCloud.respondToQuestion")(
          function* (request) {
            const turn = active;
            const pending = turn?.pending;
            if (
              !turn ||
              turn.finished ||
              !pending ||
              pending.request.id !== request.requestId ||
              !turn.taskId
            )
              return yield* protocolError("No matching CodeRabbit Cloud question is pending");
            if (!pending.answerable)
              return yield* protocolError(
                "This question must be answered directly in CodeRabbit Cloud",
              );
            const answers: Record<string, string> = {};
            const questionIds = new Set(pending.question.questions.map((question) => question.id));
            if (Object.keys(request.answers ?? {}).some((key) => !questionIds.has(key)))
              return yield* protocolError(
                "CodeRabbit Cloud answers contain an unknown question ID",
              );
            for (const question of pending.question.questions) {
              const value = request.answers?.[question.id];
              const answer =
                typeof value === "string"
                  ? value
                  : Array.isArray(value) && value.length === 1 && typeof value[0] === "string"
                    ? value[0]
                    : null;
              if (answer === null || !answer.trim())
                return yield* protocolError(
                  "Each CodeRabbit Cloud question requires one nonempty answer",
                );
              if (
                question.options !== null &&
                !question.isOther &&
                !question.options.some((option) => option.label === answer)
              )
                return yield* protocolError(
                  "Choose one of the CodeRabbit Cloud question’s listed options",
                );
              answers[question.id] = answer;
            }
            const payload = yield* encodeAnswers({
              interactionId: pending.question.interactionId,
              answers,
            }).pipe(
              Effect.mapError(() => protocolError("Could not encode CodeRabbit Cloud answers")),
            );
            if (new TextEncoder().encode(payload).byteLength > 262_144)
              return yield* protocolError("CodeRabbit Cloud answers exceed the input limit");
            yield* control(turn, "answer", payload).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: DRIVER,
                    requestId: request.requestId,
                    cause,
                  }),
              ),
            );
            yield* wakeFollower(turn);
          },
        ),
        readThreadSnapshot: unsupported,
        rollbackThread: unsupported,
        forkThread: unsupported,
      };
      yield* emit({ type: "provider_session.updated", driver: DRIVER, providerSession: session });
      // Closing a local follower never cancels remote work; cancellation is explicit.
      yield* Effect.addFinalizer(() => Queue.shutdown(events));
      return runtime;
    }),
  });
});

export type CodeRabbitCloudAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | IdAllocator.IdAllocatorV2;
export const CodeRabbitCloudAdapterV2Driver: ProviderAdapterDriver<
  CodeRabbitCloudSettings,
  CodeRabbitCloudAdapterV2DriverEnv
> = {
  driverKind: DRIVER,
  configSchema: CodeRabbitCloudSettings,
  defaultConfig: () => decodeSettings({}),
  create: (input) =>
    Effect.gen(function* () {
      const environment = yield* mergeProviderInstanceEnvironment(input.environment);
      return yield* makeCodeRabbitCloudAdapterV2({
        instanceId: input.instanceId,
        settings: input.config,
        environment,
      });
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterDriverCreateError({
            driver: DRIVER,
            instanceId: input.instanceId,
            detail: "Could not create CodeRabbit Cloud adapter.",
            cause,
          }),
      ),
    ),
};
