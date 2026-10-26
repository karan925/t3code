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
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import { ProviderContinuationRequests } from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { CodeRabbitSettings } from "../settings.ts";
import {
  CommandError,
  isKnownCodeEvent,
  type CodeEvent,
  type PendingQuestion,
  type TaskDetail,
  type Turn,
} from "./protocol.ts";
import { openInteractiveSession, type InteractiveSession } from "./session.ts";
import { createTask, fetchLatestTurn } from "./tasks.ts";
import { verifyCloudWorkspace } from "./workspace.ts";

const CODERABBIT_PROVIDER = ProviderDriverKind.make("coderabbit");
const DRIVER = CODERABBIT_PROVIDER;
const decodeSettings = Schema.decodeSync(CodeRabbitSettings);
const encodeAnswers = Schema.encodeEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)),
);
const codeRabbitCapabilities = {
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
  readonly messageReceipt: Deferred.Deferred<void>;
  taskId: string | null;
  createdTask: boolean;
  taskURL: string | null;
  operationId: string | null;
  acceptedSeq: number;
  awaitingMessageReceipt: boolean;
  submitted: boolean;
  stopRequested: boolean;
  admittedAt: number | null;
  runningObserved: boolean;
  messageObserved: boolean;
  wakeCursor: number | null;
  latestInFlight: boolean;
  latestPending: boolean;
  latestStatusKey: string | null;
  readonly text: string;
  readonly previousHead: OrchestrationV2ProviderThread["nativeConversationHeadRef"];
  buffered: CodeEvent[];
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

export interface CodeRabbitAdapterOptions {
  readonly instanceId: ProviderInstanceId;
  readonly settings: CodeRabbitSettings;
  readonly environment: NodeJS.ProcessEnv;
}

export const makeCodeRabbitAdapterV2 = Effect.fn("makeCodeRabbitAdapterV2")(function* (
  options: CodeRabbitAdapterOptions,
) {
  const ids = yield* IdAllocator.IdAllocatorV2;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const continuationRequests = yield* ProviderContinuationRequests;
  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DRIVER,
    getCapabilities: () => Effect.succeed(codeRabbitCapabilities),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("CodeRabbit.openSession")(function* (input) {
      if (input.runtimePolicy.runtimeMode !== "full-access")
        return yield* protocolError("CodeRabbit runs remotely and only supports full-access mode");
      if (!input.runtimePolicy.cwd)
        return yield* protocolError("CodeRabbit requires a repository directory");
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
        capabilities: codeRabbitCapabilities,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const eventPermit = yield* Semaphore.make(1);
      const connectionPermit = yield* Semaphore.make(1);
      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);
      let thread: OrchestrationV2ProviderThread | null = null;
      let active: ActiveTurn | null = null;
      let interactive: InteractiveSession | null = null;
      let follower: Fiber.Fiber<void> | null = null;
      let connectionReady = yield* Deferred.make<InteractiveSession, CommandError>();
      let admissionUncertain = false;
      let foreignMessages: CodeEvent[] = [];
      let pendingWake: { events: CodeEvent[]; cursor: number; completed: Turn | undefined } | null =
        null;
      let lastSettled: { queueItemId: string | null; endSeq: number; at: number } | null = null;
      let wakeProbeInFlight = false;
      let wakeProbeKey: string | null = null;
      let lastSeq = -1;
      let workVersion = 0;
      let task: TaskDetail | null = null;
      let closed = false;
      let imported = input.initialNativeThreadId !== undefined;
      let reattachOnNextTurn =
        input.resumeFromSession?.status === "running" ||
        input.resumeFromSession?.status === "waiting";
      const taskOptions = {
        binaryPath: options.settings.binaryPath,
        cwd,
        env: options.environment,
      };
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
      const offerWake = Effect.fn("CodeRabbit.offerWake")(function* (
        frames: CodeEvent[],
        cursor: number,
        completed?: Turn,
      ) {
        if (!thread?.appThreadId || closed || frames.length === 0) return;
        workVersion++;
        const wake = { events: frames, cursor, completed };
        pendingWake = wake;
        yield* updateSession("running");
        yield* continuationRequests.offer({
          threadId: thread.appThreadId,
          providerThreadId: thread.id,
          driver: DRIVER,
          detail: null,
          delivery: "adapter_buffered",
          dispatchIfCurrent: (dispatch) =>
            eventPermit.withPermits(1)(
              Effect.suspend(() =>
                pendingWake === wake && !closed
                  ? dispatch.pipe(Effect.map(Option.some))
                  : Effect.succeed(Option.none()),
              ),
            ),
          clearIfCurrent: () =>
            eventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (pendingWake !== wake) return;
                pendingWake = null;
                yield* updateSession("ready");
              }),
            ),
        });
      });
      const finish = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        outcome: "completed" | "failed" | "cancelled" | "interrupted",
        message?: string,
      ) {
        if (turn.finished) return;
        turn.finished = true;
        if (turn.taskId === null || (!turn.submitted && turn.awaitingMessageReceipt))
          yield* updateThread({ nativeConversationHeadRef: turn.previousHead });
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
            message: message ?? "CodeRabbit failed. Open the cloud task to inspect it.",
            retryable: false,
            class: /quota|usage limit|rate limit|billing/i.test(message ?? "")
              ? "usage_limit"
              : "unknown",
          });
          const ordinal = turn.nextOrdinal;
          yield* emit({
            type: "turn_item.updated",
            driver: DRIVER,
            turnItem: {
              ...itemBase(turn, `terminal-failure:${turn.providerTurn.id}`, at),
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
        yield* Deferred.succeed(turn.messageReceipt, undefined);
        if (active === turn) active = null;
        if (foreignMessages.length) {
          const queued = foreignMessages;
          foreignMessages = [];
          yield* offerWake(queued, lastSettled?.endSeq ?? lastSeq);
        }
      }, Effect.uninterruptible);
      const ask = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        event: { readonly question: PendingQuestion; readonly answerTemplate?: unknown },
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
                reason: "Answer this question directly in the CodeRabbit task.",
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
            question: q.question || "Open CodeRabbit to answer.",
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
            `Answer this question directly in CodeRabbit: ${turn.taskURL ?? "open the task in the web app"}`,
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
      const terminal = (status: string | null | undefined) =>
        status !== null &&
        status !== undefined &&
        ["completed", "failed", "canceled", "cancelled", "interrupted"].includes(status);
      const settle = Effect.fn("CodeRabbit.settle")(function* (
        turn: ActiveTurn,
        result: Turn,
        detail: TaskDetail | null,
      ) {
        if (turn.finished) return;
        workVersion++;
        lastSeq = Math.max(lastSeq, result.endSeq ?? lastSeq);
        lastSettled = {
          queueItemId: result.queueItemId,
          endSeq: result.endSeq ?? lastSeq,
          at: DateTime.toEpochMillis(yield* DateTime.now),
        };
        if (interactive) {
          const response = yield* interactive.request({ command: "show" }).pipe(
            Effect.timeoutOrElse({
              duration: "5 seconds",
              orElse: () => Effect.succeed(null),
            }),
            Effect.catch((cause) =>
              Effect.logDebug("CodeRabbit delivery refresh failed", cause).pipe(Effect.as(null)),
            ),
          );
          if (response?.ok && response.task) detail = response.task;
        }

        const text = result.finalReply ?? result.errorMessage;
        if (text && text !== turn.lastAssistantText) yield* say(turn, "result", text);
        yield* say(
          turn,
          "delivery",
          [
            `CodeRabbit task: ${detail?.url ?? turn.taskURL}`,
            `Working branch: ${detail?.workingBranchName ?? "pending"}`,
            `Patch: ${detail?.patchStatus ?? "unknown"}; delivery: ${detail?.deliveryStatus ?? "unknown"}.`,
            `Results stay in the cloud. Deliver with \`cr code push ${turn.taskId}\` or the web app.`,
          ].join("\n"),
        );
        yield* finish(
          turn,
          result.outcome === "canceled" ? "cancelled" : result.outcome,
          result.errorMessage ?? undefined,
        );
      });
      // A status wake may miss user_message history; only its end cursor proves new work.
      const matchesTurn = (turn: ActiveTurn, result: Turn) =>
        turn.operationId !== null
          ? result.queueItemId === turn.operationId ||
            (turn.createdTask &&
              result.queueItemId === null &&
              result.startSeq !== null &&
              result.startSeq >= turn.acceptedSeq)
          : turn.wakeCursor !== null
            ? result.endSeq !== null && result.endSeq > turn.wakeCursor
            : result.startSeq !== null && result.startSeq >= turn.acceptedSeq;
      const latest = Effect.fn("CodeRabbit.latest")(function* (turn: ActiveTurn) {
        if (!turn.taskId || turn.finished) return;
        if (turn.latestInFlight) {
          turn.latestPending = true;
          return;
        }
        turn.latestInFlight = true;
        const taskId = turn.taskId;
        yield* Effect.gen(function* () {
          do {
            turn.latestPending = false;
            yield* Effect.gen(function* () {
              const result = yield* fetchLatestTurn({ ...taskOptions, taskId }).pipe(
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              );
              yield* Effect.gen(function* () {
                if (turn.finished) return;
                task = result.task;
                if (matchesTurn(turn, result.turn)) yield* settle(turn, result.turn, result.task);
              }).pipe(eventPermit.withPermits(1));
            }).pipe(
              Effect.catch((cause) => Effect.logDebug("CodeRabbit latest turn unavailable", cause)),
            );
          } while (!turn.finished && turn.latestPending);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              turn.latestInFlight = false;
            }),
          ),
          Effect.forkIn(scope),
        );
      });
      // Trace handlers share one serialized boundary; helpers inside it do not create extra spans.
      const handle = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        frame: CodeEvent,
        replay = false,
      ) {
        if (turn.finished) return;
        if (!isKnownCodeEvent(frame)) {
          yield* Effect.logDebug("Ignoring unknown CodeRabbit frame", { type: frame.type });
          return;
        }
        if (turn.awaitingMessageReceipt) {
          turn.buffered.push(frame);
          return;
        }
        if (frame.type === "trace") {
          if (!replay && frame.seq <= lastSeq) return;
          const continuation =
            turn.input.message.createdBy === "agent" &&
            turn.input.message.creationSource === "provider";
          if (frame.kind === "user_message" && continuation && turn.operationId === null) {
            turn.operationId = frame.queueItemId ?? null;
            turn.acceptedSeq = frame.seq;
            turn.wakeCursor = null;
          }
          if (
            frame.kind === "user_message" &&
            frame.queueItemId != null &&
            (turn.operationId !== null
              ? frame.queueItemId !== turn.operationId
              : !continuation && frame.text !== turn.text)
          ) {
            foreignMessages.push(frame);
            return;
          }
          lastSeq = Math.max(lastSeq, frame.seq);
          switch (frame.kind) {
            case "agent_message":
              yield* say(turn, `trace:${frame.seq}`, frame.text);
              break;
            case "user_message":
              if (
                frame.queueItemId === turn.operationId ||
                (turn.operationId === null && frame.text === turn.text)
              ) {
                turn.operationId = frame.queueItemId ?? null;
                turn.acceptedSeq = frame.seq;
                turn.wakeCursor = null;
                turn.messageObserved = true;
              }
              if (
                turn.input.message.createdBy === "agent" &&
                turn.input.message.creationSource === "provider"
              )
                yield* say(
                  turn,
                  `web-message:${frame.seq}`,
                  `Message sent from the CodeRabbit web app: ${frame.text}`,
                );
              break;
            case "plan":
              yield* say(turn, `trace:${frame.seq}`, frame.markdown);
              break;
            case "command": {
              if (frame.status === "started") break;
              const at = yield* DateTime.now;
              yield* emit({
                type: "turn_item.updated",
                driver: DRIVER,
                turnItem: {
                  ...itemBase(turn, `${turn.providerTurn.id}:trace:${frame.seq}`, at),
                  type: "command_execution",
                  input: frame.command,
                  title: "Cloud command",
                  status: frame.status,
                  ...(frame.exitCode == null ? {} : { exitCode: frame.exitCode }),
                },
              });
              break;
            }
            case "question":
              yield* ask(turn, { question: frame.question });
              break;
            case "answer_submitted":
              if (frame.interactionId === turn.pending?.question.interactionId)
                yield* resolveQuestion(turn);
              break;
            case "steer_result":
              if (frame.result !== "accepted")
                yield* say(
                  turn,
                  `steer:${frame.seq}`,
                  `CodeRabbit did not accept steering: ${frame.reason ?? frame.result}`,
                );
              break;
            case "turn_end":
              if (matchesTurn(turn, frame.turn)) yield* settle(turn, frame.turn, task);
              break;
            case "error":
              if (frame.retryable === false) yield* finish(turn, "failed", frame.message);
              break;
          }
          return;
        }
        if (frame.type === "action_required") {
          if (frame.action === "answer_question") yield* ask(turn, frame);
          else if (frame.action === "authenticate")
            yield* finish(turn, "failed", "Run `cr auth login` on the T3 server host.");
        } else if (frame.type === "status" && frame.task !== undefined) {
          task = frame.task;
          if (frame.status === "task_updated") {
            if (frame.task.turnStatus === "running" && (!replay || turn.messageObserved)) {
              turn.runningObserved = true;
              turn.latestStatusKey = null;
            }
            const updatedAt = frame.task.updatedAt ? Date.parse(frame.task.updatedAt) : NaN;
            const newer = turn.admittedAt !== null && updatedAt > turn.admittedAt;
            if (terminal(frame.task.turnStatus) && (turn.runningObserved || newer)) {
              const key = `${frame.task.turnStatus}:${frame.task.updatedAt ?? ""}`;
              if (turn.latestStatusKey !== key) {
                turn.latestStatusKey = key;
                yield* latest(turn);
              }
            }
          }
        } else if (frame.type === "error") {
          yield* finish(
            turn,
            "failed",
            /auth/i.test(frame.status)
              ? "Run `cr auth login` on the T3 server host."
              : frame.message,
          );
        }
      });
      const probeWake = Effect.fn("CodeRabbit.probeWake")(function* (detail: TaskDetail) {
        const settled = lastSettled;
        const taskId = thread?.nativeThreadRef?.nativeId;
        if (!settled || !taskId) return;
        const key = `${settled.endSeq}:${detail.turnStatus}:${detail.updatedAt ?? ""}`;
        if (wakeProbeKey === key) return;
        wakeProbeKey = key;
        if (wakeProbeInFlight) return;
        wakeProbeInFlight = true;
        yield* Effect.gen(function* () {
          while (true) {
            const probeKey = wakeProbeKey;
            yield* Effect.gen(function* () {
              const result = yield* fetchLatestTurn({ ...taskOptions, taskId }).pipe(
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              );
              yield* Effect.gen(function* () {
                if (
                  closed ||
                  active ||
                  pendingWake ||
                  lastSettled !== settled ||
                  result.turn.endSeq === null ||
                  result.turn.endSeq <= settled.endSeq ||
                  (result.turn.queueItemId !== null &&
                    result.turn.queueItemId === settled.queueItemId)
                )
                  return;
                task = result.task;
                yield* offerWake(
                  [
                    {
                      phase: "code",
                      type: "trace",
                      taskId,
                      seq: result.turn.endSeq,
                      createdAt: DateTime.formatIso(yield* DateTime.now),
                      kind: "turn_end",
                      turn: result.turn,
                    },
                  ],
                  settled.endSeq,
                  result.turn,
                );
              }).pipe(eventPermit.withPermits(1));
            }).pipe(
              Effect.catch((cause) => {
                if (wakeProbeKey === probeKey) wakeProbeKey = null;
                return Effect.logDebug("CodeRabbit wake lookup unavailable", cause);
              }),
            );
            if (
              closed ||
              active ||
              pendingWake ||
              lastSettled !== settled ||
              wakeProbeKey === null ||
              wakeProbeKey === probeKey
            )
              break;
          }
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              wakeProbeInFlight = false;
            }),
          ),
          Effect.forkIn(scope),
        );
      });
      const route = Effect.fnUntraced(function* (frame: CodeEvent) {
        if (closed) return;
        if (!isKnownCodeEvent(frame)) {
          yield* Effect.logDebug("Ignoring unknown CodeRabbit frame", { type: frame.type });
          return;
        }
        if (active) {
          yield* handle(active, frame);
          return;
        }
        if (pendingWake) {
          pendingWake.events.push(frame);
          return;
        }
        if (!thread?.appThreadId) return;
        if (
          lastSettled &&
          frame.type === "status" &&
          frame.task &&
          terminal(frame.task.turnStatus)
        ) {
          yield* probeWake(frame.task);
          return;
        }
        const foreign =
          (frame.type === "trace" && frame.kind === "user_message" && frame.seq > lastSeq) ||
          (frame.type === "trace" &&
            frame.kind === "turn_end" &&
            lastSettled !== null &&
            frame.turn.endSeq !== null &&
            frame.turn.endSeq > lastSettled.endSeq &&
            (frame.turn.queueItemId === null ||
              frame.turn.queueItemId !== lastSettled.queueItemId)) ||
          (frame.type === "status" &&
            frame.task !== undefined &&
            frame.task.turnStatus === "running");
        if (!foreign) return;
        if (
          frame.type === "status" &&
          frame.task &&
          lastSettled &&
          lastSeq <= lastSettled.endSeq &&
          !(frame.task?.updatedAt && Date.parse(frame.task.updatedAt) > lastSettled.at)
        ) {
          // Without a newer timestamp, queue identity distinguishes a stale status from new work.
          yield* probeWake(frame.task);
          return;
        }
        yield* offerWake([frame], lastSettled?.endSeq ?? lastSeq);
      });
      const follow = Effect.fn("CodeRabbit.follow")(function* () {
        const retrySchedule = Schedule.max([Schedule.exponential("2 seconds"), Schedule.recurs(3)]);
        let backoff = yield* Schedule.toStepWithSleep(retrySchedule);
        let accountedWorkVersion = workVersion;
        while (true) {
          if (closed) return;
          const taskId = thread?.nativeThreadRef?.nativeId;
          if (!taskId) break;
          let processedWork = false;
          yield* Effect.scoped(
            Effect.gen(function* () {
              const child = yield* openInteractiveSession({ ...taskOptions, taskId, spawner });
              yield* Stream.runForEach(
                child.frames.pipe(
                  Stream.takeUntil(
                    (frame) =>
                      isKnownCodeEvent(frame) &&
                      frame.type === "complete" &&
                      frame.status === "session_closed",
                  ),
                ),
                (frame) =>
                  Effect.gen(function* () {
                    if (closed) return;
                    if (
                      isKnownCodeEvent(frame) &&
                      frame.type === "session" &&
                      frame.status === "ready"
                    ) {
                      task = frame.task;
                      lastSeq = Math.max(lastSeq, frame.cursor ?? -1);
                      interactive = child;
                      if (active && !active.awaitingMessageReceipt && terminal(task.turnStatus))
                        yield* latest(active);
                      yield* Deferred.succeed(connectionReady, child);
                    } else {
                      if (isKnownCodeEvent(frame) && frame.type === "trace" && frame.seq > lastSeq)
                        processedWork = true;
                      yield* route(frame);
                    }
                  }).pipe(eventPermit.withPermits(1)),
              ).pipe(Effect.catch((cause) => Effect.logDebug("CodeRabbit stream ended", cause)));
              // Pre-ready failures retain their waiters; completed generations get fresh readiness.
              if (yield* Deferred.isDone(connectionReady))
                connectionReady = yield* Deferred.make<InteractiveSession, CommandError>();
              interactive = null;
            }),
          ).pipe(Effect.catch((cause) => Effect.logDebug("CodeRabbit connection ended", cause)));
          if (closed) return;
          if (processedWork || workVersion !== accountedWorkVersion)
            backoff = yield* Schedule.toStepWithSleep(retrySchedule);
          accountedWorkVersion = workVersion;
          const retry = yield* backoff(undefined).pipe(Effect.result);
          if (retry._tag === "Failure") break;
        }
        const message = `CodeRabbit connection lost; task may still be running: ${task?.url ?? thread?.nativeThreadRef?.nativeId}`;
        if (active) yield* finish(active, "failed", message).pipe(eventPermit.withPermits(1));
        else {
          session = { ...session, lastError: message };
          yield* updateSession("error");
        }
        yield* Deferred.fail(
          connectionReady,
          new CommandError({ operation: "connection lost", detail: message }),
        );
      });
      const ensureInteractive = Effect.fn("CodeRabbit.ensureInteractive")(function* () {
        while (true) {
          if (closed) return yield* new CommandError({ operation: "session closed" });
          if (interactive) return interactive;
          if (follower === null) {
            connectionReady = yield* Deferred.make<InteractiveSession, CommandError>();
            follower = yield* follow().pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  follower = null;
                }),
              ),
              Effect.forkIn(scope),
            );
          }
          const child = yield* Deferred.await(connectionReady).pipe(
            Effect.timeoutOrElse({
              duration: "60 seconds",
              orElse: () => Effect.fail(new CommandError({ operation: "wait for ready" })),
            }),
          );
          if (child === interactive) return child;
        }
      }, connectionPermit.withPermits(1));
      const register = Effect.fn("CodeRabbit.register")(function* (
        request: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (active)
          return yield* protocolError("Cannot switch a CodeRabbit task while it is running");
        const at = yield* DateTime.now;
        thread = request.existingProviderThread
          ? {
              ...request.existingProviderThread,
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
              contextUsage: null,
              nativeMetadata: null,
              goal: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [],
              createdAt: at,
              updatedAt: at,
            };
        if (thread.firstRunOrdinal !== null) imported = false;
        admissionUncertain =
          thread.firstRunOrdinal !== null &&
          thread.nativeConversationHeadRef?.nativeId?.startsWith("pending:") === true;
        yield* emit({ type: "provider_thread.updated", driver: DRIVER, providerThread: thread });
        return thread;
      });
      const unsupported = () =>
        Effect.fail(protocolError("CodeRabbit does not support this operation"));
      const detach = Effect.fn("CodeRabbit.detach")(function* () {
        closed = true;
        const child = interactive;
        interactive = null;
        if (child) yield* child.close.pipe(Effect.ignore);
        if (follower) yield* Fiber.interrupt(follower);
        pendingWake = null;
      });
      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DRIVER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return session;
        },
        events: Stream.fromQueue(events),
        hasPendingBackgroundWork: Effect.sync(() => pendingWake !== null),
        hasPendingBackgroundWorkForThread: (target) =>
          Effect.sync(() => pendingWake !== null && thread?.id === target.id),
        ensureThread: register,
        resumeThread: (request) =>
          register({
            threadId: request.threadId ?? request.providerThread.appThreadId ?? input.threadId,
            modelSelection: request.modelSelection ?? input.modelSelection,
            runtimePolicy: request.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: request.providerThread,
          }),
        unloadThread: () => detach(),
        startTurn: Effect.fn("CodeRabbit.startTurn")(function* (request) {
          if (closed) return yield* protocolError("CodeRabbit session is closed");
          if (active) return yield* protocolError("A CodeRabbit turn is already running");
          if (!thread || thread.id !== request.providerThread.id)
            return yield* protocolError("CodeRabbit thread is not registered");
          if (
            request.runtimePolicy.runtimeMode !== "full-access" ||
            request.runtimePolicy.interactionMode !== "default"
          )
            return yield* protocolError(
              "CodeRabbit supports only full-access mode with the default interaction mode",
            );
          if (request.modelSelection.model !== "default")
            return yield* protocolError("CodeRabbit chooses its model; select the default model");
          if (request.message.attachments.length)
            return yield* protocolError(
              "CodeRabbit CLI does not accept attachments; include text or repository paths",
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
              "CodeRabbit requires a nonempty prompt of at most 65,536 characters",
            );
          let recovered: { turn: Turn; task: TaskDetail } | null = null;
          const uncertainTaskId = thread.nativeThreadRef?.nativeId;
          if (!uncertainTaskId) admissionUncertain = false;
          if (admissionUncertain && uncertainTaskId) {
            const child = yield* ensureInteractive().pipe(Effect.option);
            const shown = Option.isSome(child)
              ? yield* child.value.request({ command: "show" }).pipe(Effect.option)
              : Option.none();
            const history =
              Option.isSome(shown) && shown.value.ok && terminal(shown.value.task?.turnStatus)
                ? yield* fetchLatestTurn({
                    ...taskOptions,
                    taskId: uncertainTaskId,
                  }).pipe(
                    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                    Effect.option,
                  )
                : Option.none();
            admissionUncertain = false;
            yield* updateThread({
              nativeConversationHeadRef: Option.isSome(history)
                ? ref(
                    history.value.turn.queueItemId ??
                      `accepted:${history.value.turn.startSeq ?? 0}`,
                  )
                : null,
            });
            if (Option.isSome(history)) recovered = history.value;
            if (Option.isNone(history)) {
              reattachOnNextTurn = false;
              return yield* protocolError(
                `CodeRabbit message delivery is unconfirmed. Check the task before sending again to avoid a duplicate: ${task?.url ?? `https://app.coderabbit.ai/code/tasks/${encodeURIComponent(uncertainTaskId)}`}. After checking, send again to continue.`,
              );
            }
          }
          const taskId = thread.nativeThreadRef?.nativeId ?? null;
          const continuation =
            request.message.createdBy === "agent" && request.message.creationSource === "provider";
          if (continuation && !pendingWake)
            return yield* protocolError("CodeRabbit continuation has no buffered work");
          const reattach =
            taskId !== null &&
            (reattachOnNextTurn || request.restartContinuationOfRunId !== undefined);
          reattachOnNextTurn = false;
          const at = yield* DateTime.now;
          const providerTurn: OrchestrationV2ProviderTurn = {
            id: ids.derive.providerTurn({
              driver: DRIVER,
              nativeTurnId: `${thread.id}:${request.attemptId}`,
            }),
            providerThreadId: thread.id,
            nodeId: request.rootNodeId,
            runAttemptId: request.attemptId,
            nativeTurnRef: { ...ref(request.attemptId), strength: "weak" },
            ordinal: request.providerTurnOrdinal,
            status: "running",
            startedAt: at,
            completedAt: null,
          };
          const savedHead = thread.nativeConversationHeadRef?.nativeId ?? null;
          const turn: ActiveTurn = {
            input: request,
            providerTurn,
            messageReceipt: yield* Deferred.make<void>(),
            taskId,
            createdTask: false,
            taskURL: taskId
              ? `https://app.coderabbit.ai/code/tasks/${encodeURIComponent(taskId)}`
              : null,
            operationId:
              reattach &&
              savedHead &&
              !savedHead.startsWith("initial:") &&
              !savedHead.startsWith("pending:") &&
              !savedHead.startsWith("accepted:")
                ? savedHead
                : null,
            acceptedSeq:
              reattach && savedHead?.startsWith("accepted:")
                ? Number(savedHead.slice("accepted:".length))
                : lastSeq + 1,
            awaitingMessageReceipt: !continuation && !reattach,
            submitted: false,
            stopRequested: false,
            admittedAt: continuation || reattach ? DateTime.toEpochMillis(at) : null,
            runningObserved: continuation,
            messageObserved: continuation || reattach,
            wakeCursor: continuation ? (pendingWake?.cursor ?? lastSeq) : null,
            latestInFlight: false,
            latestPending: false,
            latestStatusKey: null,
            text,
            previousHead: thread.nativeConversationHeadRef,
            buffered: [],
            lastAssistantText: null,
            nextOrdinal: 1,
            finished: false,
            resolvedQuestions: new Set(),
            pending: null,
          };
          yield* Effect.gen(function* () {
            if (continuation && !pendingWake)
              return yield* protocolError("CodeRabbit continuation has no buffered work");
            active = turn;
            yield* updateSession("running");
            yield* updateThread({
              status: "active",
              firstRunOrdinal: request.providerThread.firstRunOrdinal ?? request.runOrdinal,
              lastRunOrdinal: request.runOrdinal,
              ...(reattach || continuation
                ? {}
                : {
                    nativeConversationHeadRef: ref(
                      `${taskId ? "pending" : "initial"}:${request.attemptId}`,
                    ),
                  }),
            });
            yield* emit({
              type: "provider_turn.updated",
              driver: DRIVER,
              threadId: request.threadId,
              providerTurn,
            });
            if (continuation) {
              const wake = pendingWake;
              pendingWake = null;
              const message = wake?.events.find(
                (frame) =>
                  isKnownCodeEvent(frame) &&
                  frame.type === "trace" &&
                  frame.kind === "user_message",
              );
              if (
                message &&
                isKnownCodeEvent(message) &&
                message.type === "trace" &&
                message.kind === "user_message"
              ) {
                turn.operationId = message.queueItemId ?? null;
                turn.acceptedSeq = message.seq;
                turn.wakeCursor = null;
              }
              yield* Deferred.succeed(turn.messageReceipt, undefined);
              if (wake?.completed) yield* settle(turn, wake.completed, task);
              if (wake)
                for (const frame of wake.events) {
                  if (turn.finished) yield* route(frame);
                  else yield* handle(turn, frame, true);
                }
            }
          }).pipe(
            eventPermit.withPermits(1),
            Effect.mapError((cause) => protocolError(cause.message)),
          );
          const start = Effect.gen(function* () {
            if (turn.finished) return;
            if (continuation) {
              if (!turn.finished) yield* ensureInteractive();
              return;
            }
            if (reattach) {
              if (recovered) {
                yield* settle(turn, recovered.turn, recovered.task).pipe(
                  eventPermit.withPermits(1),
                );
                return;
              }
              // A pending receipt is ambiguous after a crash; never resend or cancel possibly admitted work.
              if (savedHead === null || savedHead.startsWith("pending:")) {
                admissionUncertain = true;
                yield* finish(
                  turn,
                  "failed",
                  "CodeRabbit message delivery was not confirmed before the restart. Open the cloud task to check it before sending another message.",
                );
                return;
              }
              yield* Deferred.succeed(turn.messageReceipt, undefined);
              yield* ensureInteractive();
              return;
            }
            if (taskId === null) {
              const workspace = yield* verifyCloudWorkspace(
                cwd,
                options.environment,
                options.settings,
              ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
              if (turn.stopRequested || turn.finished) return;
              turn.submitted = true;
              task = yield* createTask({ ...taskOptions, prompt: text }).pipe(
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              );
              turn.createdTask = true;
              turn.taskId = task.taskId;
              turn.taskURL = task.url;
              turn.awaitingMessageReceipt = false;
              turn.admittedAt = DateTime.toEpochMillis(yield* DateTime.now);
              imported = false;
              yield* updateThread({ nativeThreadRef: ref(task.taskId) });
              yield* say(
                turn,
                "cloud-task",
                [
                  `Working in CodeRabbit: ${task.url}`,
                  ...(workspace.pushed
                    ? [
                        `Pushed \`${workspace.branch}\` to origin so CodeRabbit could start from it.`,
                      ]
                    : []),
                  ...(workspace.localDiffers
                    ? [
                        `CodeRabbit started from \`${workspace.branch}\` on origin, which differs from your local copy; unpushed local commits are not included.`,
                      ]
                    : []),
                  ...(workspace.dirty ? ["Local uncommitted changes were not uploaded."] : []),
                ].join("\n"),
              );
              yield* Deferred.succeed(turn.messageReceipt, undefined);
              yield* ensureInteractive();
              return;
            }
            if (imported) {
              const preview = yield* fetchLatestTurn({ ...taskOptions, taskId }).pipe(
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              );
              task = preview.task;
              yield* say(
                turn,
                "import-preview",
                `Latest reply from CodeRabbit: ${preview.turn.finalReply ?? preview.turn.errorMessage ?? "No reply available."}\n\nEarlier history is only in the web app: ${preview.task.url}`,
              );
              imported = false;
            }
            const child = yield* ensureInteractive();
            // Events can arrive before the correlated response. Keep them until admission is known.
            turn.acceptedSeq = lastSeq + 1;
            turn.submitted = true;
            // The cloud can finish before its message receipt reaches us.
            turn.admittedAt = DateTime.toEpochMillis(yield* DateTime.now);
            const response = yield* child.request({ command: "message", text });
            if (!response.ok) {
              yield* updateThread({ nativeConversationHeadRef: savedHead ? ref(savedHead) : null });
              yield* finish(turn, "failed", response.error.message);
              return;
            }
            yield* Effect.gen(function* () {
              // Client operation ids belong to requests, not the queue ids in turn traces.
              turn.operationId = response.queueItemId ?? null;
              turn.awaitingMessageReceipt = false;
              yield* updateThread({
                nativeConversationHeadRef: ref(turn.operationId ?? `accepted:${turn.acceptedSeq}`),
              });
              yield* Deferred.succeed(turn.messageReceipt, undefined);
              const buffered = turn.buffered;
              turn.buffered = [];
              for (const frame of buffered) {
                if (turn.finished) yield* route(frame);
                else yield* handle(turn, frame, true);
              }
            }).pipe(eventPermit.withPermits(1));
          }).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                if (turn.finished) return;
                if (turn.taskId !== null && turn.submitted && turn.awaitingMessageReceipt) {
                  const proof = turn.buffered.find(
                    (frame) =>
                      isKnownCodeEvent(frame) &&
                      frame.type === "trace" &&
                      frame.kind === "user_message" &&
                      frame.text === text &&
                      frame.seq >= turn.acceptedSeq,
                  );
                  if (
                    proof &&
                    isKnownCodeEvent(proof) &&
                    proof.type === "trace" &&
                    proof.kind === "user_message"
                  ) {
                    yield* Effect.gen(function* () {
                      turn.operationId = proof.queueItemId ?? null;
                      turn.acceptedSeq = proof.seq;
                      turn.awaitingMessageReceipt = false;
                      yield* updateThread({
                        nativeConversationHeadRef: ref(
                          turn.operationId ?? `accepted:${turn.acceptedSeq}`,
                        ),
                      });
                      yield* Deferred.succeed(turn.messageReceipt, undefined);
                      const frames = turn.buffered;
                      turn.buffered = [];
                      for (const frame of frames) {
                        if (turn.finished) yield* route(frame);
                        else yield* handle(turn, frame, true);
                      }
                    }).pipe(eventPermit.withPermits(1));
                    return;
                  }
                  admissionUncertain = true;
                  yield* finish(
                    turn,
                    "failed",
                    `CodeRabbit message delivery is unconfirmed; the cloud task may still be running: ${turn.taskURL}. Continue in the CodeRabbit web app to avoid sending a duplicate message.`,
                  );
                  return;
                }
                yield* finish(
                  turn,
                  "failed",
                  cause._tag === "CodeRabbitWorkspaceError"
                    ? cause.message
                    : /auth/i.test(cause.message)
                      ? "Run `cr auth login` on the T3 server host."
                      : turn.taskId === null && turn.submitted
                        ? `${cause.message} Check your CodeRabbit task list at app.coderabbit.ai before retrying.`
                        : cause.message,
                );
              }),
            ),
          );
          yield* start.pipe(Effect.forkIn(scope));
        }),
        steerTurn: Effect.fn("CodeRabbit.steerTurn")(function* (request) {
          const turn = active;
          if (
            !turn ||
            turn.finished ||
            turn.providerTurn.id !== request.providerTurnId ||
            turn.awaitingMessageReceipt ||
            !interactive
          )
            return yield* protocolError(
              "Wait for CodeRabbit to accept the message before steering",
            );
          if (turn.pending)
            return yield* protocolError("Answer the pending CodeRabbit question before steering");
          const text = request.message.text;
          if (request.message.attachments.length || !text.trim() || text.length > 65_536)
            return yield* protocolError(
              "CodeRabbit steering requires text without attachments within the prompt limit",
            );
          yield* interactive.request({ command: "steer", text }).pipe(
            Effect.flatMap((response) =>
              response.ok
                ? Effect.void
                : Effect.fail(
                    new CommandError({ operation: "request", detail: response.error.message }),
                  ),
            ),
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
        interruptTurn: Effect.fn("CodeRabbit.interruptTurn")(function* (request) {
          const turn = active;
          if (!turn || turn.finished || turn.providerTurn.id !== request.providerTurnId) return;
          turn.stopRequested = true;
          if (turn.taskId === null && !turn.submitted) {
            yield* finish(turn, "cancelled").pipe(eventPermit.withPermits(1));
            return;
          }
          // Cancellation before admission can miss the queued message entirely.
          yield* Deferred.await(turn.messageReceipt);
          if (admissionUncertain)
            return yield* new ProviderAdapter.ProviderAdapterInterruptError({
              driver: DRIVER,
              providerThreadId: request.providerThread.id,
              providerTurnId: request.providerTurnId,
              cause: new CommandError({
                operation: "cancel",
                detail: `Message delivery is unconfirmed; inspect the cloud task: ${turn.taskURL}`,
              }),
            });
          if (turn.finished) return;
          const child = yield* ensureInteractive().pipe(
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
          yield* child.request({ command: "cancel" }).pipe(
            Effect.flatMap((response) =>
              !response.ok
                ? Effect.fail(
                    new CommandError({ operation: "request", detail: response.error.message }),
                  )
                : [
                      "not_running",
                      "turn_not_running",
                      "agent_not_running",
                      "turn_finished",
                    ].includes(response.status)
                  ? finish(turn, "cancelled").pipe(eventPermit.withPermits(1))
                  : Effect.void,
            ),
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
        }),
        respondToRuntimeRequest: Effect.fn("CodeRabbit.respondToQuestion")(function* (request) {
          const turn = active;
          const pending = turn?.pending;
          if (
            !turn ||
            turn.finished ||
            !pending ||
            pending.request.id !== request.requestId ||
            !turn.taskId
          )
            return yield* protocolError("No matching CodeRabbit question is pending");
          if (!pending.answerable)
            return yield* protocolError("This question must be answered directly in CodeRabbit");
          const answers: Record<string, string> = {};
          const questionIds = new Set(pending.question.questions.map((question) => question.id));
          if (Object.keys(request.answers ?? {}).some((key) => !questionIds.has(key)))
            return yield* protocolError("CodeRabbit answers contain an unknown question ID");
          for (const question of pending.question.questions) {
            const value = request.answers?.[question.id];
            const answer =
              typeof value === "string"
                ? value
                : Array.isArray(value) && value.length === 1 && typeof value[0] === "string"
                  ? value[0]
                  : null;
            if (answer === null || !answer.trim())
              return yield* protocolError("Each CodeRabbit question requires one nonempty answer");
            if (
              question.options !== null &&
              !question.isOther &&
              !question.options.some((option) => option.label === answer)
            )
              return yield* protocolError("Choose one of the CodeRabbit question’s listed options");
            answers[question.id] = answer;
          }
          const payload = yield* encodeAnswers(answers).pipe(
            Effect.mapError(() => protocolError("Could not encode CodeRabbit answers")),
          );
          if (new TextEncoder().encode(payload).byteLength > 262_144)
            return yield* protocolError("CodeRabbit answers exceed the input limit");
          if (!interactive) return yield* protocolError("CodeRabbit session is disconnected");
          yield* interactive
            .request({ command: "answer", interactionId: pending.question.interactionId, answers })
            .pipe(
              Effect.flatMap((response) =>
                response.ok
                  ? Effect.suspend(() =>
                      turn.pending?.request.id === pending.request.id
                        ? resolveQuestion(turn)
                        : Effect.void,
                    ).pipe(eventPermit.withPermits(1))
                  : Effect.fail(
                      new CommandError({ operation: "request", detail: response.error.message }),
                    ),
              ),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: DRIVER,
                    requestId: request.requestId,
                    cause,
                  }),
              ),
            );
        }),
        readThreadSnapshot: unsupported,
        rollbackThread: unsupported,
        forkThread: unsupported,
      };
      yield* emit({ type: "provider_session.updated", driver: DRIVER, providerSession: session });
      yield* Effect.addFinalizer(() =>
        detach().pipe(Effect.andThen(Queue.shutdown(events)), Effect.asVoid),
      );
      return runtime;
    }),
  });
});

export type CodeRabbitAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | IdAllocator.IdAllocatorV2;
export const CodeRabbitAdapterV2Driver: ProviderAdapterDriver<
  CodeRabbitSettings,
  CodeRabbitAdapterV2DriverEnv
> = {
  driverKind: DRIVER,
  configSchema: CodeRabbitSettings,
  defaultConfig: () => decodeSettings({}),
  create: Effect.fn("CodeRabbitAdapterV2Driver.create")(
    function* (input) {
      const environment = yield* mergeProviderInstanceEnvironment(input.environment);
      return yield* makeCodeRabbitAdapterV2({
        instanceId: input.instanceId,
        settings: input.config,
        environment,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: DRIVER,
              instanceId: input.instanceId,
              detail: "Could not create CodeRabbit adapter.",
              cause,
            }),
        ),
      ),
  ),
};
