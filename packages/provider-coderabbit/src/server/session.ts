import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  CommandError,
  consumeCodeEvents,
  encodeCodeRequest,
  isCodeResponse,
  type CodeEvent,
  type CodeRequest,
  type CodeResponse,
} from "./protocol.ts";

const isCommandError = Schema.is(CommandError);
const encodeEventJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const MAX_BUFFERED_EVENTS = 1024;
const MAX_BUFFERED_EVENT_BYTES = 4 * 1024 * 1024;

export interface InteractiveSessionOptions {
  readonly binaryPath: string;
  readonly taskId: string;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
}

export interface InteractiveSession {
  readonly frames: Stream.Stream<CodeEvent, CommandError>;
  readonly request: (command: CodeRequest) => Effect.Effect<CodeResponse, CommandError>;
  readonly exitCode: Effect.Effect<number, CommandError>;
  readonly close: Effect.Effect<void, CommandError>;
}

export const openInteractiveSession = Effect.fn("CodeRabbit.openInteractiveSession")(function* (
  options: InteractiveSessionOptions,
) {
  const scope = yield* Effect.scope;
  const incoming = yield* Queue.dropping<
    { frame: CodeEvent; retainedBytes: number },
    CommandError | Cause.Done
  >(MAX_BUFFERED_EVENTS);
  const outgoing = yield* Queue.bounded<string, Cause.Done>(256);
  const endOutgoing = Queue.end(outgoing).pipe(
    Effect.andThen(Queue.shutdown(outgoing)),
    Effect.asVoid,
  );
  const pending = new Map<
    string,
    { response: Deferred.Deferred<CodeResponse, CommandError>; bytes: Uint8Array }
  >();
  let bufferedEventBytes = 0;
  const exited = yield* Deferred.make<number, CommandError>();
  let nextId = 0;
  let closed = false;
  let transportError: CommandError | undefined;
  const failPending = Effect.fn("CodeRabbit.failPending")(function* (error: CommandError) {
    transportError ??= error;
    const requests = Array.from(pending.values());
    pending.clear();
    for (const request of requests) {
      yield* Deferred.fail(request.response, error);
    }
  });
  const failTransport = Effect.fn("CodeRabbit.failTransport")(function* (error: CommandError) {
    yield* failPending(error);
    yield* endOutgoing;
    yield* Queue.fail(incoming, error);
    // Effect 4 preserves a closing queue's failure when shutdown discards its buffered frames.
    yield* Queue.shutdown(incoming);
    bufferedEventBytes = 0;
  });
  yield* Effect.addFinalizer(() =>
    failPending(new CommandError({ operation: "session closed" })).pipe(
      Effect.andThen(endOutgoing),
      Effect.andThen(Queue.shutdown(incoming)),
      Effect.asVoid,
    ),
  );
  const spawn = yield* resolveSpawnCommand(
    options.binaryPath,
    ["code", "resume", options.taskId, "--agent", "--interactive"],
    { env: options.env },
  ).pipe(Effect.mapError((cause) => new CommandError({ operation: "resolve command", cause })));
  const child = yield* options.spawner
    .spawn(
      ChildProcess.make(spawn.command, spawn.args, {
        cwd: options.cwd,
        env: options.env,
        extendEnv: false,
        shell: spawn.shell,
        forceKillAfter: "1 second",
      }),
    )
    .pipe(Effect.mapError((cause) => new CommandError({ operation: "spawn", cause })));

  yield* child.exitCode.pipe(
    Effect.matchEffect({
      onSuccess: (code) => Deferred.succeed(exited, Number(code)),
      onFailure: (cause) => Deferred.fail(exited, new CommandError({ operation: "exit", cause })),
    }),
    Effect.forkIn(scope),
  );
  yield* consumeCodeEvents(child.stdout, (frame) =>
    Effect.gen(function* () {
      // Correlate on id alone so response-shape drift fails the request now, not at the timeout.
      if (frame.type === "response" && typeof frame.id === "string") {
        const request = pending.get(frame.id);
        if (request === undefined) return;
        pending.delete(frame.id);
        yield* isCodeResponse(frame)
          ? Deferred.succeed(request.response, frame)
          : Deferred.fail(
              request.response,
              new CommandError({
                operation: "decode response",
                detail: "Unrecognized CodeRabbit response.",
              }),
            );
        return;
      }
      // UTF-16 serialized size bounds retained text; the count limit also bounds object overhead.
      const retainedBytes = encodeEventJson(frame).length * 2;
      if (
        bufferedEventBytes + retainedBytes > MAX_BUFFERED_EVENT_BYTES ||
        !(yield* Queue.offer(incoming, { frame, retainedBytes }))
      )
        return yield* new CommandError({
          operation: "read backlog overflow",
          detail: "CodeRabbit event backlog exceeded its count or retained size limit.",
        });
      bufferedEventBytes += retainedBytes;
    }),
  ).pipe(
    Effect.matchEffect({
      onSuccess: () =>
        failPending(
          new CommandError({ operation: "read", detail: "CodeRabbit session closed stdout." }),
        ).pipe(Effect.andThen(endOutgoing), Effect.andThen(Queue.end(incoming))),
      onFailure: (cause) =>
        failTransport(
          isCommandError(cause) ? cause : new CommandError({ operation: "read", cause }),
        ),
    }),
    Effect.forkIn(scope),
  );
  yield* Stream.runDrain(child.stderr).pipe(
    Effect.catch((cause) => failTransport(new CommandError({ operation: "stderr", cause }))),
    Effect.forkIn(scope),
  );
  // Pull one id at a time: a dequeued batch must not retain bytes past cancellation or teardown.
  yield* Stream.fromEffectRepeat(Queue.take(outgoing)).pipe(
    Stream.map((id) =>
      transportError === undefined && !closed ? pending.get(id)?.bytes : undefined,
    ),
    Stream.filter((bytes): bytes is Uint8Array => bytes !== undefined),
    Stream.run(child.stdin),
    Effect.catch((cause) => failTransport(new CommandError({ operation: "write", cause }))),
    Effect.forkIn(scope),
  );

  const request = Effect.fn("CodeRabbit.request")(function* (command: CodeRequest) {
    const id = `r${++nextId}`;
    const encoded = yield* encodeCodeRequest(command, id);
    const deferred = yield* Deferred.make<CodeResponse, CommandError>();
    if (transportError !== undefined) return yield* transportError;
    if (closed)
      return yield* new CommandError({
        operation: "request",
        detail: "CodeRabbit session is closed.",
      });
    pending.set(id, {
      response: deferred,
      bytes: new TextEncoder().encode(`${encoded}\n`),
    });
    return yield* Effect.gen(function* () {
      const accepted = yield* Queue.offer(outgoing, id);
      if (!accepted)
        return yield* new CommandError({
          operation: "write",
          detail: "CodeRabbit session is closed.",
        });
      return yield* Deferred.await(deferred);
    }).pipe(
      Effect.timeoutOrElse({
        duration: "60 seconds",
        orElse: () =>
          Effect.fail(
            new CommandError({
              operation: "request timeout",
              detail: `CodeRabbit ${command.command} request timed out after 60000ms.`,
            }),
          ),
      }),
      Effect.ensuring(
        Effect.sync(() => {
          pending.delete(id);
        }),
      ),
    );
  });

  // EOF detaches the local follower. It must never send a cancel or close command.
  const close = Effect.gen(function* () {
    closed = true;
    yield* failPending(new CommandError({ operation: "session closed" }));
    yield* endOutgoing;
    yield* Deferred.await(exited);
  }).pipe(
    Effect.timeoutOrElse({
      duration: "60 seconds",
      orElse: () => Effect.fail(new CommandError({ operation: "close timeout" })),
    }),
  );
  // Registered last so EOF reaches the CLI before its scoped fibers or process are stopped.
  yield* Effect.addFinalizer(() => close.pipe(Effect.ignore));
  return {
    frames: Stream.fromEffectRepeat(Queue.take(incoming)).pipe(
      Stream.map(({ frame, retainedBytes }) => {
        bufferedEventBytes -= retainedBytes;
        return frame;
      }),
    ),
    request,
    exitCode: Deferred.await(exited),
    close,
  } satisfies InteractiveSession;
});
