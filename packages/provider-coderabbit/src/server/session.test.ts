import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Queue from "effect/Queue";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { openInteractiveSession } from "./session.ts";

const task = {
  taskId: "task-1",
  url: "https://app.coderabbit.ai/code/tasks/task-1",
  status: "active",
};
const ready = { type: "session", phase: "code", status: "ready", taskId: "task-1", task };
const closed = { type: "complete", phase: "code", status: "session_closed", taskId: "task-1" };

it.live(
  "keeps one interactive process, correlates out-of-order replies, drains stderr and closes via EOF",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-session-" });
        const binaryPath = writeFakeCli({
          directory: cwd,
          name: "fake-cr",
          source: `
    import { createInterface } from 'node:readline';
    const args = process.argv.slice(2);
    if (JSON.stringify(args) !== JSON.stringify(['code','resume','task-1','--agent','--interactive'])) process.exit(91);
    const output = value => console.log(JSON.stringify(value));
    output(${JSON.stringify(ready)});
    process.stderr.write('x'.repeat(2_000_000));
    const requests = [];
    const input = createInterface({input:process.stdin});
    input.on('line', line => {
      const request = JSON.parse(line); requests.push(request);
      if (requests.length === 2) {
        if (JSON.stringify(requests) !== JSON.stringify([
          {id:'r1',command:'message',text:'hello'},
          {id:'r2',command:'answer',interactionId:'question-1',answers:{a:'yes'}}
        ])) process.exit(92);
        output({type:'response',phase:'code',id:'r2',ok:false,error:{status:'not_pending',message:'Question is no longer pending.'}});
        output({type:'trace',phase:'code',taskId:'task-1',seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Hello'});
        output({type:'response',phase:'code',id:'r1',ok:true,status:'message_submitted',queueItemId:'queue-1',clientOperationId:null});
      }
      if (requests.length > 2) process.exit(93);
    });
    input.on('close', () => { output(${JSON.stringify(closed)}); process.exitCode = requests.length === 2 ? 7 : 94; });
  `,
        });
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const session = yield* openInteractiveSession({
          binaryPath,
          taskId: "task-1",
          cwd,
          env: process.env,
          spawner,
        });
        const frames = yield* Stream.runCollect(session.frames).pipe(Effect.forkScoped);
        const first = yield* session
          .request({ command: "message", text: "hello" })
          .pipe(Effect.forkScoped);
        const second = yield* session
          .request({ command: "answer", interactionId: "question-1", answers: { a: "yes" } })
          .pipe(Effect.forkScoped);
        assert.deepEqual(yield* Fiber.join(first), {
          type: "response",
          phase: "code",
          id: "r1",
          ok: true,
          status: "message_submitted",
          queueItemId: "queue-1",
          clientOperationId: null,
        });
        assert.deepEqual(yield* Fiber.join(second), {
          type: "response",
          phase: "code",
          id: "r2",
          ok: false,
          error: { status: "not_pending", message: "Question is no longer pending." },
        });
        yield* session.close;
        assert.equal(yield* session.exitCode, 7);
        const observed = yield* Fiber.join(frames);
        assert.deepEqual(observed, [
          ready,
          {
            type: "trace",
            phase: "code",
            taskId: "task-1",
            seq: 1,
            createdAt: "2026-10-16",
            kind: "agent_message",
            text: "Hello",
          },
          closed,
        ]);
        const error = yield* session.request({ command: "show" }).pipe(Effect.flip);
        assert.equal(error.operation, "session closed");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("scope finalization gives stdin EOF before killing the child", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-scope-" });
      const marker = `${cwd}/eof.txt`;
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `
    import { writeFileSync } from 'node:fs';
    console.log(${JSON.stringify(JSON.stringify(ready))});
    process.stdin.resume();
    process.stdin.on('end', () => writeFileSync(${JSON.stringify(marker)}, 'EOF'));
  `,
      });
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* openInteractiveSession({
            binaryPath,
            taskId: "task-1",
            cwd,
            env: process.env,
            spawner,
          });
          yield* Stream.runHead(session.frames);
        }),
      );
      assert.equal(yield* fs.readFileString(marker), "EOF");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails pending requests when the CLI closes stdout without a response", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-stdout-" });
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `
    process.stdin.once('data', () => { process.stdout.end(); process.stdin.resume(); });
  `,
      });
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const session = yield* openInteractiveSession({
        binaryPath,
        taskId: "task-1",
        cwd,
        env: process.env,
        spawner,
      });
      const error = yield* session.request({ command: "show" }).pipe(Effect.flip);
      assert.equal(error.message, "CodeRabbit session closed stdout.");
      yield* session.close;
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails a request whose response does not match the known shape", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-drift-" });
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `
    process.stdin.once('data', (chunk) => {
      const { id } = JSON.parse(String(chunk).split('\\n')[0]);
      process.stdout.write(JSON.stringify({ type: 'response', id, shape: 'unexpected' }) + '\\n');
      process.stdin.resume();
    });
  `,
      });
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const session = yield* openInteractiveSession({
        binaryPath,
        taskId: "task-1",
        cwd,
        env: process.env,
        spawner,
      });
      const error = yield* session.request({ command: "show" }).pipe(Effect.flip);
      assert.equal(error.message, "Unrecognized CodeRabbit response.");
      yield* session.close;
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const response = (id: string) => ({
  type: "response",
  phase: "code",
  id,
  ok: true,
  status: "task_shown",
});
const makeSessionFixture = Effect.fn("test.makeSessionFixture")(function* (
  options: {
    startWriting?: Deferred.Deferred<void>;
    writePermits?: Queue.Queue<void>;
  } = {},
) {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const writes = yield* Queue.unbounded<string>();
  const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
  const eof = yield* Deferred.make<void>();
  const stdin = Sink.forEach((chunk: Uint8Array) =>
    Effect.gen(function* () {
      yield* Queue.offer(writes, new TextDecoder().decode(chunk));
      if (options.writePermits) yield* Queue.take(options.writePermits);
    }),
  ).pipe(
    Sink.mapEffect(() =>
      Effect.gen(function* () {
        yield* Deferred.succeed(eof, undefined);
        yield* Deferred.succeed(exited, ChildProcessSpawner.ExitCode(0));
        yield* Queue.end(stdout);
      }),
    ),
  );
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(123),
        exitCode: Deferred.await(exited),
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.unwrap(
          (options.startWriting ? Deferred.await(options.startWriting) : Effect.void).pipe(
            Effect.as(stdin),
          ),
        ),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
  const session = yield* openInteractiveSession({
    binaryPath: "/fake/cr",
    taskId: "task-1",
    cwd: "/fake",
    env: {},
    spawner,
  });
  const emit = (...frames: ReadonlyArray<unknown>) =>
    Queue.offer(
      stdout,
      new TextEncoder().encode(frames.map((frame) => encodeJson(frame)).join("\n") + "\n"),
    );
  return { session, writes, eof, emit };
});

it.effect("times out at 60 seconds, cleans pending requests and ignores late replies", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { session, writes, emit } = yield* makeSessionFixture();
      const first = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(writes), '{"id":"r1","command":"show"}\n');
      yield* TestClock.adjust("60 seconds");
      const error = yield* Fiber.join(first).pipe(Effect.flip);
      assert.equal(error.operation, "request timeout");
      const second = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(writes), '{"id":"r2","command":"show"}\n');
      yield* emit(response("r1"), response("r2"));
      assert.equal((yield* Fiber.join(second)).id, "r2");
      yield* session.close;
    }),
  ),
);

it.effect("discards expired and cancelled requests behind an already-pulled writer item", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startWriting = yield* Deferred.make<void>();
      const writePermits = yield* Queue.unbounded<void>();
      const { session, writes, emit } = yield* makeSessionFixture({ startWriting, writePermits });
      const first = yield* session
        .request({ command: "show" })
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const expired = yield* session
        .request({ command: "cancel" })
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const cancelled = yield* session
        .request({ command: "close" })
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.succeed(startWriting, undefined);
      assert.equal(yield* Queue.take(writes), '{"id":"r1","command":"show"}\n');
      yield* emit(response("r1"));
      yield* Fiber.join(first);
      yield* Fiber.interrupt(cancelled);
      yield* TestClock.adjust("60 seconds");
      assert.equal((yield* Fiber.join(expired).pipe(Effect.flip)).operation, "request timeout");
      const live = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      yield* Queue.offer(writePermits, undefined);
      assert.equal(yield* Queue.take(writes), '{"id":"r4","command":"show"}\n');
      yield* emit(response("r4"));
      yield* Fiber.join(live);
      yield* Queue.offer(writePermits, undefined);
      yield* session.close;
      assert.equal(yield* Queue.size(writes), 0);
    }),
  ),
);

it.effect("close discards unsent requests after a writer has pulled its first item", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const startWriting = yield* Deferred.make<void>();
      const writePermits = yield* Queue.unbounded<void>();
      const { session, writes, eof } = yield* makeSessionFixture({ startWriting, writePermits });
      const first = yield* session
        .request({ command: "show" })
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const unsent = yield* session
        .request({ command: "cancel" })
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.succeed(startWriting, undefined);
      assert.equal(yield* Queue.take(writes), '{"id":"r1","command":"show"}\n');
      const closing = yield* session.close.pipe(Effect.forkScoped);
      assert.equal((yield* Fiber.join(first).pipe(Effect.flip)).operation, "session closed");
      assert.equal((yield* Fiber.join(unsent).pipe(Effect.flip)).operation, "session closed");
      yield* Queue.offer(writePermits, undefined);
      yield* Deferred.await(eof);
      yield* Fiber.join(closing);
      assert.equal(yield* Queue.size(writes), 0);
    }),
  ),
);

it.effect(
  "correlates responses with a full event backlog and releases capacity as frames drain",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, writes, emit } = yield* makeSessionFixture();
        const first = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
        yield* Queue.take(writes);
        yield* emit(...Array.from({ length: 1024 }, () => ready), response("r1"));
        assert.equal((yield* Fiber.join(first)).id, "r1");
        const frames = yield* Stream.runCollect(session.frames.pipe(Stream.take(1024)));
        assert.equal(frames.length, 1024);
        const second = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
        yield* Queue.take(writes);
        yield* emit(ready, response("r2"));
        assert.equal((yield* Fiber.join(second)).id, "r2");
        yield* session.close;
        assert.deepEqual(yield* Stream.runCollect(session.frames), [ready]);
      }),
    ),
);

it.effect("fails and detaches when the event count exceeds its bound", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { session, writes, emit, eof } = yield* makeSessionFixture();
      const request = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      yield* Queue.take(writes);
      yield* emit(...Array.from({ length: 1025 }, () => ready));
      assert.equal(
        (yield* Fiber.join(request).pipe(Effect.flip)).operation,
        "read backlog overflow",
      );
      assert.equal(
        (yield* Stream.runCollect(session.frames).pipe(Effect.flip)).operation,
        "read backlog overflow",
      );
      yield* Deferred.await(eof);
      assert.equal(
        (yield* session.request({ command: "show" }).pipe(Effect.flip)).operation,
        "read backlog overflow",
      );
      yield* session.close;
    }),
  ),
);

it.effect("bounds retained event bytes even when each frame is below the frame limit", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { session, writes, emit, eof } = yield* makeSessionFixture();
      const request = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      yield* Queue.take(writes);
      const frame = {
        type: "trace",
        phase: "code",
        taskId: "task-1",
        seq: 1,
        createdAt: "2026-10-16",
        kind: "agent_message",
        text: "x".repeat(600_000),
      };
      yield* emit(frame, frame, frame, frame);
      assert.equal(
        (yield* Fiber.join(request).pipe(Effect.flip)).operation,
        "read backlog overflow",
      );
      assert.equal(
        (yield* Stream.runCollect(session.frames).pipe(Effect.flip)).operation,
        "read backlog overflow",
      );
      yield* Deferred.await(eof);
      yield* session.close;
    }),
  ),
);

it.live("scope cleanup force-kills an interactive child that resists SIGTERM", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-resistant-" });
      const marker = `${cwd}/sigterm.txt`;
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `
    import { writeFileSync } from 'node:fs';
    process.on('SIGTERM', () => writeFileSync(${JSON.stringify(marker)}, 'SIGTERM'));
    process.stdin.resume();
    setInterval(() => {}, 1_000);
    console.log(${JSON.stringify(JSON.stringify(ready))});
  `,
      });
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const processScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(processScope, Exit.void));
      const session = yield* openInteractiveSession({
        binaryPath,
        taskId: "task-1",
        cwd,
        env: process.env,
        spawner: ChildProcessSpawner.make((command) =>
          spawner.spawn(command).pipe(Effect.provideService(Scope.Scope, processScope)),
        ),
      });
      yield* Stream.runHead(session.frames);
      yield* Scope.close(processScope, Exit.void);
      const error = yield* session.exitCode.pipe(Effect.flip);
      assert.equal(error.operation, "exit");
      if ((yield* HostProcess.Platform) !== "win32") {
        assert.equal(yield* fs.readFileString(marker), "SIGTERM");
        assert.instanceOf(error.cause, PlatformError.PlatformError);
        assert.include(String(error.cause.reason.cause), "SIGKILL");
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("delivers already-buffered frames before a transport failure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* Deferred.make<void>();
      const logger = Logger.make(({ message }) => {
        if (encodeJson(message).includes("CodeRabbit transport failed"))
          Deferred.doneUnsafe(failed, Effect.void);
      });
      yield* Effect.gen(function* () {
        const { session, writes, emit } = yield* makeSessionFixture();
        const request = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
        yield* Queue.take(writes);
        yield* emit(ready, { type: "oversized", text: "x".repeat(1_048_576) });
        assert.equal(
          (yield* Fiber.join(request).pipe(Effect.flip)).operation,
          "read oversized frame",
        );
        yield* Deferred.await(failed);
        const observed: unknown[] = [];
        const error = yield* Stream.runForEach(session.frames, (frame) =>
          Effect.sync(() => observed.push(frame)),
        ).pipe(Effect.flip);
        assert.equal(error.operation, "read oversized frame");
        assert.deepEqual(observed, [ready]);
        yield* session.close;
      }).pipe(Effect.provide(Logger.layer([logger], { mergeWithExisting: false })));
    }),
  ),
);
