import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
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

it.effect("times out at 60 seconds, cleans pending requests and ignores late replies", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      const writes = yield* Queue.unbounded<string>();
      const exited = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const eof = yield* Deferred.make<void>();
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(123),
            exitCode: Deferred.await(exited),
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.forEach((chunk: Uint8Array) =>
              Queue.offer(writes, new TextDecoder().decode(chunk)),
            ).pipe(Sink.mapEffect(() => Deferred.succeed(eof, undefined).pipe(Effect.asVoid))),
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
      const first = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(writes), '{"id":"r1","command":"show"}\n');
      yield* TestClock.adjust("60 seconds");
      const error = yield* Fiber.join(first).pipe(Effect.flip);
      assert.equal(error.operation, "request timeout");
      const second = yield* session.request({ command: "show" }).pipe(Effect.forkScoped);
      assert.equal(yield* Queue.take(writes), '{"id":"r2","command":"show"}\n');
      yield* Queue.offer(
        stdout,
        new TextEncoder().encode(
          '{"type":"response","phase":"code","id":"r1","ok":true,"status":"late"}\n{"type":"response","phase":"code","id":"r2","ok":true,"status":"task_shown"}\n',
        ),
      );
      assert.equal((yield* Fiber.join(second)).id, "r2");
      const closing = yield* session.close.pipe(Effect.forkScoped);
      yield* Deferred.await(eof);
      yield* Deferred.succeed(exited, ChildProcessSpawner.ExitCode(0));
      yield* Queue.end(stdout);
      yield* Fiber.join(closing);
    }),
  ),
);
