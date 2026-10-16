import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

const nullableString = Schema.NullOr(Schema.String);
export const PendingQuestion = Schema.Struct({
  interactionId: Schema.String,
  isBlocking: Schema.Boolean,
  questions: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      header: Schema.String,
      question: Schema.String,
      options: Schema.NullOr(
        Schema.Array(Schema.Struct({ label: Schema.String, description: nullableString })),
      ),
      isOther: Schema.Boolean,
      isSensitive: Schema.Boolean,
    }),
  ),
});
export type PendingQuestion = typeof PendingQuestion.Type;
const TaskDetail = Schema.Struct({
  taskId: Schema.String,
  url: Schema.String,
  status: Schema.String,
  turnStatus: nullableString,
  resultSummary: nullableString,
  errorSummary: nullableString,
  cancellationPending: Schema.Boolean,
  collaborationMode: nullableString,
  resultPullRequest: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        number: Schema.NullOr(Schema.Number),
        url: nullableString,
        status: nullableString,
      }),
    ),
  ),
});
const turn = Schema.Struct({
  startSeq: Schema.NullOr(Schema.Number),
  endSeq: Schema.NullOr(Schema.Number),
  queueItemId: nullableString,
  outcome: Schema.Literals(["completed", "failed", "canceled", "interrupted"]),
  outputKind: nullableString,
  finalReply: nullableString,
  errorMessage: nullableString,
});
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
  clientOperationId: nullableString,
  turn,
  agentMinutes: Schema.NullOr(Schema.Number),
  task: TaskDetail,
};

export const CodeEvent = Schema.Union([
  Schema.Struct({
    ...base,
    type: Schema.Literal("status"),
    status: Schema.String,
    message: Schema.String,
    taskId: Schema.optional(Schema.String),
    url: Schema.optional(Schema.String),
    clientOperationId: Schema.optional(nullableString),
  }),
  trace("agent_message", { text: Schema.String }),
  trace("user_message", {
    text: Schema.String,
    source: nullableString,
    queueItemId: nullableString,
  }),
  trace("command", {
    command: Schema.String,
    status: Schema.Literals(["started", "completed", "failed"]),
    exitCode: Schema.NullOr(Schema.Number),
    durationMs: Schema.NullOr(Schema.Number),
  }),
  trace("file_change", {
    status: nullableString,
    files: Schema.Array(Schema.Struct({ path: Schema.String, change: nullableString })),
    truncated: Schema.Boolean,
  }),
  trace("plan", { markdown: Schema.String }),
  trace("todo_list", {
    items: Schema.Array(Schema.Struct({ text: Schema.String, completed: Schema.Boolean })),
  }),
  trace("question", { question: PendingQuestion }),
  trace("answer_submitted", { interactionId: nullableString }),
  trace("steer_result", {
    result: Schema.String,
    reservationId: nullableString,
    reason: nullableString,
  }),
  trace("turn_end", { turn }),
  trace("error", { message: Schema.String, retryable: Schema.NullOr(Schema.Boolean) }),
  trace("notice", { eventType: Schema.String, message: Schema.String }),
  trace("git_operation", {
    operationId: Schema.String,
    deliveryMode: nullableString,
    step: nullableString,
    status: Schema.String,
    message: nullableString,
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
    created: Schema.Boolean,
    task: TaskDetail,
    warnings: Schema.Array(Schema.Struct({ code: Schema.String, message: Schema.String })),
  }),
  Schema.Struct({
    ...base,
    ...taskRef,
    type: Schema.Literal("complete"),
    status: Schema.Literals(["task_canceled", "turn_finished", "cancel_pending", "not_running"]),
    task: TaskDetail,
  }),
  Schema.Struct({
    ...base,
    type: Schema.Literal("complete"),
    status: Schema.Literal("task_shown"),
    task: TaskDetail,
    warnings: Schema.Array(Schema.Struct({ code: Schema.String, message: Schema.String })),
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
          handle: nullableString,
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
    clientOperationId: nullableString,
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
export type CodeEvent = typeof CodeEvent.Type;

export class CommandError extends Schema.TaggedError<CommandError>()(
  "CodeRabbitCloudCommandError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `CodeRabbit Cloud ${this.operation} failed.`;
  }
}

const decodeEvent = Schema.decodeUnknownEffect(CodeEvent);
const isCommandError = Schema.is(CommandError);
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const MAX_FRAME_BYTES = 1_048_576;

/** Keeps receipts observable before following requests fail, without retaining the trace. */
export const consumeCodeEvents = Effect.fnUntraced(function* <E>(
  stream: Stream.Stream<Uint8Array, E>,
  onEvent: (event: CodeEvent) => Effect.Effect<void, CommandError>,
) {
  let decoder = new TextDecoder("utf-8", { fatal: true });
  let buffered = "";
  let bytes = 0;
  const emit = Effect.fnUntraced(function* () {
    const text = buffered.trim();
    buffered = "";
    bytes = 0;
    if (!text) return;
    const value = yield* decodeJson(text).pipe(
      Effect.mapError(() => new CommandError({ operation: "decode JSON" })),
    );
    const event = yield* decodeEvent(value).pipe(
      Effect.mapError(() => new CommandError({ operation: "decode event" })),
    );
    yield* onEvent(event);
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
          catch: () => new CommandError({ operation: "decode UTF-8" }),
        });
        if (index < chunk.length) {
          buffered += yield* Effect.try({
            try: () => decoder.decode(),
            catch: () => new CommandError({ operation: "decode UTF-8" }),
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
    catch: () => new CommandError({ operation: "decode UTF-8" }),
  });
  yield* emit();
});

export interface RunCodeCommandOptions {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly stdin?: string;
  readonly onEvent: (event: CodeEvent) => Effect.Effect<void, CommandError>;
}

export const runCodeCommand = Effect.fn("CodeRabbitCloud.runCodeCommand")(function* (
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
      duration: "11 minutes",
      orElse: () =>
        Effect.fail(
          new CommandError({ operation: "follow timeout (cloud task may still be running)" }),
        ),
    }),
  );
});
