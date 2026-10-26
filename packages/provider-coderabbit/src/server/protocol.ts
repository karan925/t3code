import { collectUint8StreamText } from "@t3tools/provider-core/server/collectStreamText";
import {
  isWindowsCommandNotFound,
  ProviderCommandNotFoundError,
} from "@t3tools/provider-core/server/snapshotProbe";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

const nullableString = Schema.NullOr(Schema.String);
const optionalString = Schema.optional(nullableString);
export const PendingQuestion = Schema.Struct({
  interactionId: Schema.String,
  isBlocking: Schema.Boolean,
  questions: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      header: Schema.String,
      question: Schema.String,
      options: Schema.NullOr(
        Schema.Array(Schema.Struct({ label: Schema.String, description: optionalString })),
      ),
      isOther: Schema.Boolean,
      isSensitive: Schema.Boolean,
    }),
  ),
});
export type PendingQuestion = typeof PendingQuestion.Type;

export const TaskDetail = Schema.StructWithRest(
  Schema.Struct({
    taskId: Schema.String,
    url: Schema.String,
    status: Schema.String,
    title: optionalString,
    taskStatus: optionalString,
    turnStatus: optionalString,
    patchStatus: optionalString,
    planStatus: optionalString,
    deliveryStatus: optionalString,
    resultSummary: optionalString,
    errorSummary: optionalString,
    cancellationPending: Schema.optional(Schema.NullOr(Schema.Boolean)),
    collaborationMode: optionalString,
    createdAt: optionalString,
    updatedAt: optionalString,
    completedAt: optionalString,
    archivedAt: optionalString,
    branchName: optionalString,
    workingBranchName: optionalString,
    baseBranchName: optionalString,
    repository: Schema.optional(
      Schema.NullOr(
        Schema.Struct({
          provider: optionalString,
          repoId: optionalString,
          name: optionalString,
        }),
      ),
    ),
    resultPullRequest: Schema.optional(
      Schema.NullOr(
        Schema.Struct({
          number: Schema.optional(Schema.NullOr(Schema.Number)),
          url: optionalString,
          status: optionalString,
        }),
      ),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type TaskDetail = typeof TaskDetail.Type;

export const Turn = Schema.Struct({
  startSeq: Schema.NullOr(Schema.Number),
  endSeq: Schema.NullOr(Schema.Number),
  queueItemId: nullableString,
  outcome: Schema.Literals(["completed", "failed", "canceled", "interrupted"]),
  outputKind: nullableString,
  finalReply: nullableString,
  errorMessage: nullableString,
});
export type Turn = typeof Turn.Type;
const base = { phase: Schema.Literal("code") };
const taskRef = { taskId: Schema.String, url: Schema.String };
const traceBase = {
  ...base,
  type: Schema.Literal("trace"),
  taskId: Schema.String,
  seq: Schema.Number,
  createdAt: Schema.String,
};
const trace = <const K extends string, F extends Schema.Struct.Fields>(kind: K, fields: F) =>
  Schema.Struct({ ...traceBase, kind: Schema.Literal(kind), ...fields });
const terminalTurn = {
  ...base,
  ...taskRef,
  clientOperationId: optionalString,
  turn: Turn,
  agentMinutes: Schema.optional(Schema.NullOr(Schema.Number)),
  task: TaskDetail,
};
const warnings = Schema.optional(
  Schema.Array(Schema.Struct({ code: Schema.String, message: Schema.String })),
);

const responseBase = {
  ...base,
  type: Schema.Literal("response"),
  id: Schema.String,
  taskId: Schema.optional(Schema.String),
  clientOperationId: optionalString,
  queueItemId: optionalString,
  task: Schema.optional(TaskDetail),
};
export const CodeResponse = Schema.Union([
  Schema.Struct({ ...responseBase, ok: Schema.Literal(true), status: Schema.String }),
  Schema.Struct({
    ...responseBase,
    ok: Schema.Literal(false),
    error: Schema.Struct({ status: Schema.String, message: Schema.String }),
  }),
]);
export type CodeResponse = typeof CodeResponse.Type;
export const isCodeResponse = Schema.is(CodeResponse);
export const TaskListLine = Schema.Struct({
  ...base,
  type: Schema.Literal("task"),
  task: TaskDetail,
});
export type TaskListLine = typeof TaskListLine.Type;

export const KnownCodeEvent = Schema.Union([
  Schema.Struct({
    ...base,
    type: Schema.Literal("session"),
    status: Schema.Literal("ready"),
    taskId: Schema.String,
    task: TaskDetail,
    protocolVersion: Schema.optional(Schema.Number),
    commands: Schema.optional(Schema.Array(Schema.String)),
    maxRequests: Schema.optional(Schema.Number),
    cursor: Schema.optional(Schema.Number),
    url: Schema.optional(Schema.String),
  }),
  CodeResponse,
  TaskListLine,
  Schema.Struct({
    ...base,
    type: Schema.Literal("status"),
    status: Schema.String,
    message: Schema.optional(Schema.String),
    taskId: Schema.optional(Schema.String),
    url: Schema.optional(Schema.String),
    clientOperationId: optionalString,
    task: Schema.optional(TaskDetail),
  }),
  trace("agent_message", { text: Schema.String }),
  trace("user_message", {
    text: Schema.String,
    source: optionalString,
    queueItemId: optionalString,
  }),
  trace("command", {
    command: Schema.String,
    status: Schema.Literals(["started", "completed", "failed"]),
    exitCode: Schema.optional(Schema.NullOr(Schema.Number)),
    durationMs: Schema.optional(Schema.NullOr(Schema.Number)),
  }),
  trace("file_change", {
    status: optionalString,
    files: Schema.Array(Schema.Struct({ path: Schema.String, change: optionalString })),
    truncated: Schema.optional(Schema.Boolean),
  }),
  trace("plan", { markdown: Schema.String }),
  trace("todo_list", {
    items: Schema.Array(Schema.Struct({ text: Schema.String, completed: Schema.Boolean })),
  }),
  trace("question", { question: PendingQuestion }),
  trace("answer_submitted", { interactionId: optionalString }),
  trace("steer_result", {
    result: Schema.String,
    reservationId: optionalString,
    reason: optionalString,
  }),
  trace("turn_end", { turn: Turn }),
  trace("error", {
    message: Schema.String,
    retryable: Schema.optional(Schema.NullOr(Schema.Boolean)),
  }),
  trace("notice", { eventType: Schema.String, message: Schema.String }),
  trace("git_operation", {
    operationId: Schema.String,
    deliveryMode: optionalString,
    step: optionalString,
    status: Schema.String,
    message: optionalString,
  }),
  Schema.Struct({
    ...terminalTurn,
    type: Schema.Literal("complete"),
    status: Schema.Literal("turn_completed"),
  }),
  Schema.Struct({
    ...terminalTurn,
    type: Schema.Literal("error"),
    status: Schema.Literals(["turn_failed", "turn_canceled", "turn_interrupted"]),
    message: Schema.String,
  }),
  Schema.Struct({
    ...base,
    ...taskRef,
    type: Schema.Literal("complete"),
    status: Schema.Literal("task_submitted"),
    created: Schema.optional(Schema.Boolean),
    task: TaskDetail,
    warnings,
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("complete"),
    status: Schema.Literals([
      "task_canceled",
      "turn_finished",
      "cancel_pending",
      "not_running",
      "task_shown",
    ]),
    task: TaskDetail,
    warnings,
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("complete"),
    status: Schema.Literal("session_closed"),
    taskId: Schema.optional(Schema.String),
    cursor: Schema.optional(Schema.Number),
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("complete"),
    status: Schema.Literal("tasks_listed"),
    tasks: Schema.Array(TaskDetail),
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("complete"),
    status: Schema.Literals(["plan_shown", "plan_approved"]),
    plan: Schema.Struct({
      ...taskRef,
      version: Schema.Number,
      sha256: Schema.String,
      createdAt: Schema.String,
      byteLength: Schema.Number,
      markdown: Schema.String,
      approvals: Schema.Array(
        Schema.Struct({
          userId: Schema.String,
          displayName: Schema.String,
          handle: optionalString,
          approvedAt: Schema.String,
        }),
      ),
    }),
    viewer: Schema.Struct({ isAuthor: Schema.Boolean, hasApproved: Schema.Boolean }),
  }),
  Schema.Struct({
    ...base,
    ...taskRef,
    type: Schema.Literal("action_required"),
    action: Schema.Literal("resume"),
    message: Schema.String,
    command: Schema.String,
    clientOperationId: optionalString,
  }),
  Schema.Struct({
    ...base,
    ...taskRef,
    type: Schema.Literal("action_required"),
    action: Schema.Literal("answer_question"),
    message: Schema.String,
    command: nullableString,
    question: PendingQuestion,
    answerTemplate: Schema.NullOr(
      Schema.Struct({
        interactionId: Schema.String,
        answers: Schema.Record(Schema.String, Schema.String),
      }),
    ),
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("action_required"),
    action: Schema.Literal("authenticate"),
    message: Schema.String,
    command: Schema.String,
    detail: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("error"),
    status: Schema.String.check(
      Schema.makeFilter(
        (status) => !["turn_failed", "turn_canceled", "turn_interrupted"].includes(status),
      ),
    ),
    message: Schema.String,
  }),
]);
export type KnownCodeEvent = typeof KnownCodeEvent.Type;
export const isKnownCodeEvent = Schema.is(KnownCodeEvent);

// Unknown protocol extensions remain observable without widening the known event union.
export const UnknownFrame = Schema.StructWithRest(Schema.Struct({ type: Schema.String }), [
  Schema.Record(Schema.String, Schema.Unknown),
]);
export const CodeEvent = Schema.Union([KnownCodeEvent, UnknownFrame]);
export type CodeEvent = typeof CodeEvent.Type;

export const CodeRequest = Schema.Union([
  Schema.Struct({ command: Schema.Literals(["message", "steer"]), text: Schema.String }),
  Schema.Struct({
    command: Schema.Literal("answer"),
    interactionId: Schema.String,
    answers: Schema.Record(Schema.String, Schema.String),
  }),
  Schema.Struct({ command: Schema.Literals(["cancel", "show", "close"]) }),
]);
export type CodeRequest = typeof CodeRequest.Type;
const WireRequest = Schema.Union([
  Schema.Struct({
    id: Schema.String,
    command: Schema.Literals(["message", "steer"]),
    text: Schema.String,
  }),
  Schema.Struct({
    id: Schema.String,
    command: Schema.Literal("answer"),
    interactionId: Schema.String,
    answers: Schema.Record(Schema.String, Schema.String),
  }),
  Schema.Struct({ id: Schema.String, command: Schema.Literals(["cancel", "show", "close"]) }),
]);
const encodeWireRequest = Schema.encodeEffect(Schema.fromJsonString(WireRequest), {
  onExcessProperty: "error",
});
const validateRequest = Schema.decodeUnknownEffect(CodeRequest, { onExcessProperty: "error" });
export const encodeCodeRequest = Effect.fn("CodeRabbit.encodeCodeRequest")(function* (
  request: CodeRequest,
  id: string,
) {
  const validated = yield* validateRequest(request).pipe(
    Effect.mapError((cause) => new CommandError({ operation: "encode request", cause })),
  );
  return yield* encodeWireRequest({ ...validated, id }).pipe(
    Effect.mapError((cause) => new CommandError({ operation: "encode request", cause })),
  );
});

export class CommandError extends Schema.TaggedError<CommandError>()("CodeRabbitCommandError", {
  operation: Schema.String,
  status: Schema.optional(Schema.String),
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() {
    return this.detail ?? `CodeRabbit ${this.operation} failed.`;
  }
}
const decodeEvent = Schema.decodeUnknownEffect(CodeEvent);
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const isCommandError = Schema.is(CommandError);
const MAX_FRAME_BYTES = 1_048_576;

// Frames and trace chunks are frequent; a span per frame would dominate tracing.
export const consumeCodeEvents = Effect.fnUntraced(function* <E, R>(
  stream: Stream.Stream<Uint8Array, E, R>,
  onEvent: (event: CodeEvent) => Effect.Effect<void, CommandError, R>,
) {
  let decoder = new TextDecoder("utf-8", { fatal: true });
  let buffered = "";
  let bytes = 0;
  const emit = Effect.fnUntraced(function* () {
    const text = buffered.trim();
    buffered = "";
    bytes = 0;
    if (!text) return;
    yield* decodeJson(text).pipe(
      Effect.flatMap(decodeEvent),
      Effect.matchEffect({
        onFailure: () =>
          Effect.logDebug("Skipping undecodable CodeRabbit stdout line", {
            characters: text.length,
          }),
        onSuccess: onEvent,
      }),
    );
  });
  yield* Stream.runForEach(stream, (chunk) =>
    Effect.gen(function* () {
      let start = 0;
      for (let index = 0; index <= chunk.length; index++) {
        if (index !== chunk.length && chunk[index] !== 10) continue;
        const part = chunk.subarray(start, index);
        bytes += part.length;
        if (bytes > MAX_FRAME_BYTES)
          return yield* new CommandError({ operation: "read oversized frame" });
        buffered += yield* Effect.try({
          try: () => decoder.decode(part, { stream: true }),
          catch: (cause) => new CommandError({ operation: "decode UTF-8", cause }),
        });
        if (index < chunk.length) {
          buffered += yield* Effect.try({
            try: () => decoder.decode(),
            catch: (cause) => new CommandError({ operation: "decode UTF-8", cause }),
          });
          decoder = new TextDecoder("utf-8", { fatal: true });
          yield* emit();
        }
        start = index + 1;
      }
    }),
  );
  buffered += yield* Effect.try({
    try: () => decoder.decode(),
    catch: (cause) => new CommandError({ operation: "decode UTF-8", cause }),
  });
  yield* emit();
});

export const collectCodeRabbitCommand = Effect.fn("CodeRabbit.collectCommand")(function* (
  binaryPath: string,
  command: ChildProcess.Command,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(command);
  const [stdout, stderr, code] = yield* Effect.all(
    [
      collectUint8StreamText({ stream: child.stdout, maxBytes: 1_048_576 }),
      collectUint8StreamText({ stream: child.stderr, maxBytes: 65_536 }),
      child.exitCode.pipe(Effect.map(Number)),
    ],
    { concurrency: "unbounded" },
  );
  if (yield* isWindowsCommandNotFound(code, stderr.text)) {
    return yield* new ProviderCommandNotFoundError({
      binaryPath,
      exitCode: code,
      stdoutLength: stdout.bytes,
      stderrLength: stderr.bytes,
    });
  }
  // A valid-looking prefix must never stand in for the CLI's full structured output.
  if (stdout.truncated || stdout.invalidUtf8) {
    return yield* new CommandError({ operation: "read command output" });
  }
  return { stdout: stdout.text, stderr: stderr.text, code };
}, Effect.scoped);

export interface RunCodeCommandOptions {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly stdin?: string;
  readonly timeout?: Duration.Input;
  readonly onEvent: (event: CodeEvent) => Effect.Effect<void, CommandError>;
}
export const runCodeCommand = Effect.fn("CodeRabbit.runCodeCommand")(function* (
  options: RunCodeCommandOptions,
) {
  return yield* Effect.gen(function* () {
    const spawn = yield* resolveSpawnCommand(options.binaryPath, [...options.args], {
      env: options.env,
    }).pipe(Effect.mapError((cause) => new CommandError({ operation: "resolve command", cause })));
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawn.command, spawn.args, {
          cwd: options.cwd,
          env: options.env,
          extendEnv: false,
          shell: spawn.shell,
          forceKillAfter: "1 second",
          stdin: Stream.make(new TextEncoder().encode(options.stdin ?? "")),
        }),
      )
      .pipe(Effect.mapError((cause) => new CommandError({ operation: "spawn", cause })));
    const [, , code] = yield* Effect.all(
      [
        consumeCodeEvents(child.stdout, options.onEvent),
        Stream.runDrain(child.stderr),
        child.exitCode,
      ],
      { concurrency: "unbounded" },
    ).pipe(
      Effect.mapError((cause) =>
        isCommandError(cause) ? cause : new CommandError({ operation: "stream command", cause }),
      ),
    );
    return Number(code);
  }).pipe(
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: options.timeout ?? "60 seconds",
      orElse: () => Effect.fail(new CommandError({ operation: "command timeout" })),
    }),
  );
});
