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
  const incoming = yield* Queue.unbounded<CodeEvent, CommandError | Cause.Done>();
  const outgoing = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const pending = new Map<string, Deferred.Deferred<CodeResponse, CommandError>>();
  const exited = yield* Deferred.make<number, CommandError>();
  let nextId = 0;
  let closed = false;
  let transportError: CommandError | undefined;
  const failPending = Effect.fn("CodeRabbit.failPending")(function* (error: CommandError) {
    transportError ??= error;
    for (const [id, deferred] of pending) {
      pending.delete(id);
      yield* Deferred.fail(deferred, error);
    }
  });
  const failTransport = Effect.fn("CodeRabbit.failTransport")(function* (error: CommandError) {
    yield* failPending(error);
    yield* Queue.end(outgoing);
    yield* Queue.fail(incoming, error);
  });
  yield* Effect.addFinalizer(() =>
    failPending(new CommandError({ operation: "session closed" })).pipe(
      Effect.andThen(Queue.end(outgoing)),
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
        const deferred = pending.get(frame.id);
        if (deferred === undefined) return;
        pending.delete(frame.id);
        yield* isCodeResponse(frame)
          ? Deferred.succeed(deferred, frame)
          : Deferred.fail(
              deferred,
              new CommandError({
                operation: "decode response",
                detail: "Unrecognized CodeRabbit response.",
              }),
            );
        return;
      }
      yield* Queue.offer(incoming, frame);
    }),
  ).pipe(
    Effect.matchEffect({
      onSuccess: () =>
        failPending(
          new CommandError({ operation: "read", detail: "CodeRabbit session closed stdout." }),
        ).pipe(Effect.andThen(Queue.end(incoming))),
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
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catch((cause) => failTransport(new CommandError({ operation: "write", cause }))),
    Effect.forkIn(scope),
  );

  const request = Effect.fn("CodeRabbit.request")(function* (command: CodeRequest) {
    if (transportError !== undefined) return yield* transportError;
    if (closed)
      return yield* new CommandError({
        operation: "request",
        detail: "CodeRabbit session is closed.",
      });
    const id = `r${++nextId}`;
    const encoded = yield* encodeCodeRequest(command, id);
    const deferred = yield* Deferred.make<CodeResponse, CommandError>();
    pending.set(id, deferred);
    return yield* Effect.gen(function* () {
      const accepted = yield* Queue.offer(outgoing, new TextEncoder().encode(`${encoded}\n`));
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
    yield* Queue.end(outgoing);
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
    frames: Stream.fromQueue(incoming),
    request,
    exitCode: Deferred.await(exited),
    close,
  } satisfies InteractiveSession;
});
