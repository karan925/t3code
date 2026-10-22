import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import {
  consumeCodeEvents,
  encodeCodeRequest,
  isKnownCodeEvent,
  runCodeCommand,
  type CodeEvent,
} from "./protocol.ts";

const encode = (record: unknown) => new TextEncoder().encode(`${JSON.stringify(record)}\n`);
const receipt = {
  type: "status",
  phase: "code",
  status: "task_selected",
  message: "Following",
  taskId: "task-1",
};

it.effect("keeps prior receipts observable when a later JSON frame is malformed", () =>
  Effect.gen(function* () {
    const events: CodeEvent[] = [];
    const result = yield* consumeCodeEvents(
      Stream.make(encode(receipt), new TextEncoder().encode("not JSON\n")),
      (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
    assert.deepEqual(events, [receipt]);
  }),
);

it.effect("decodes split UTF-8, CRLF, multiple frames and an unterminated final frame", () =>
  Effect.gen(function* () {
    const record = {
      type: "trace",
      phase: "code",
      taskId: "task-1",
      seq: 1,
      createdAt: "2026-10-16",
      kind: "agent_message",
      text: "Working 🐰\u2028line",
    };
    const bytes = encode(record);
    const split = bytes.indexOf(0xf0) + 2;
    const events: CodeEvent[] = [];
    yield* consumeCodeEvents(
      Stream.make(
        bytes.subarray(0, split),
        bytes.subarray(split),
        new TextEncoder().encode(`\r\n${JSON.stringify(receipt)}`),
      ),
      (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
    );
    assert.deepEqual(events, [record, receipt]);
  }),
);

it.effect("preserves unknown frames and known-frame drift without losing typed guards", () =>
  Effect.gen(function* () {
    const future = { type: "trace", phase: "code", kind: "new_trace", payload: { version: 2 } };
    const drifted = {
      type: "trace",
      phase: "code",
      taskId: "task-1",
      seq: 2,
      createdAt: "2026-10-16",
      kind: "command",
      command: "future",
      status: "future_status",
    };
    const events: CodeEvent[] = [];
    yield* consumeCodeEvents(Stream.make(encode(future), encode(drifted)), (event) =>
      Effect.sync(() => {
        events.push(event);
      }),
    );
    assert.deepEqual(events, [future, drifted]);
    assert.isFalse(isKnownCodeEvent(events[0]));
    assert.isFalse(isKnownCodeEvent(events[1]));
  }),
);

it.effect("rejects objects without a frame type and bounds each frame in bytes", () =>
  Effect.gen(function* () {
    const missingType = yield* consumeCodeEvents(
      Stream.make(encode({ payload: "bad" })),
      () => Effect.void,
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(missingType));
    const oversized = yield* consumeCodeEvents(
      Stream.make(new Uint8Array(1_048_577).fill(32)),
      () => Effect.void,
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(oversized));
    let count = 0;
    yield* consumeCodeEvents(
      Stream.fromArray(Array.from({ length: 200 }, () => encode(receipt))),
      () =>
        Effect.sync(() => {
          count++;
        }),
    );
    assert.equal(count, 200);
  }),
);

it.effect("rejects malformed UTF-8 rather than silently changing text", () =>
  Effect.gen(function* () {
    const result = yield* consumeCodeEvents(
      Stream.make(new Uint8Array([0xc3, 0x28, 10])),
      () => Effect.void,
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(result));
  }),
);

it.effect("recognizes captured interactive and latest-turn shapes with nullable task extras", () =>
  Effect.gen(function* () {
    const task = {
      taskId: "task-1",
      url: "https://app.coderabbit.ai/code/tasks/task-1",
      title: "Integration smoke test",
      status: "no_file_changes",
      taskStatus: "active",
      turnStatus: "completed",
      patchStatus: "none",
      planStatus: "none",
      deliveryStatus: "none",
      repository: { provider: "github", repoId: "repo-1", name: "example" },
      workContextType: "branch",
      prNumber: null,
      branchName: "main",
      workingBranchName: "coderabbit/smoke/task-1",
      baseBranchName: "main",
      sourceType: "manual",
      deliveryMode: "proposed_patch",
      resultPullRequest: null,
      isPrivate: false,
      isPinned: false,
      requestedBy: { id: "user-1", login: "example", name: "Example" },
      createdAt: "2026-10-16T09:26:11.548Z",
      updatedAt: "2026-10-16T09:42:24.145Z",
      completedAt: "2026-10-16T09:42:24.145Z",
      archivedAt: null,
      collaborationMode: "code",
      remoteAccessMode: "workspace",
      goalState: null,
      patch: null,
      plan: null,
      result: null,
      resultSummary: null,
      errorSummary: null,
      pendingReason: null,
      cancellationPending: false,
      prHealth: null,
    };
    const turn = {
      startSeq: 33,
      endSeq: 50,
      queueItemId: "queue-1",
      outcome: "completed",
      outputKind: "patch",
      finalReply: "Follow-up verified.",
      errorMessage: null,
    };
    const frames = [
      {
        phase: "code",
        taskId: "task-1",
        type: "session",
        status: "ready",
        protocolVersion: 1,
        commands: ["message", "steer", "answer", "cancel", "show", "close"],
        maxRequests: 10000,
        url: task.url,
        cursor: 50,
        task,
      },
      { phase: "code", taskId: "task-1", type: "status", status: "task_updated", task },
      {
        phase: "code",
        taskId: "task-1",
        type: "response",
        id: "r1",
        ok: true,
        clientOperationId: null,
        status: "task_shown",
        task,
      },
      { phase: "code", taskId: "task-1", type: "complete", status: "session_closed", cursor: 50 },
      {
        phase: "code",
        taskId: "task-1",
        url: task.url,
        clientOperationId: null,
        turn,
        agentMinutes: null,
        task,
        type: "complete",
        status: "turn_completed",
      },
    ];
    const observed: CodeEvent[] = [];
    yield* consumeCodeEvents(Stream.fromArray(frames.map(encode)), (event) =>
      Effect.sync(() => {
        observed.push(event);
      }),
    );
    assert.deepEqual(observed, frames);
    assert.isTrue(observed.every(isKnownCodeEvent));
  }),
);

it.effect("encodes only the authoritative request fields and rejects extra keys", () =>
  Effect.gen(function* () {
    const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
    const encoded = yield* encodeCodeRequest(
      { command: "answer", interactionId: "question-1", answers: { a: "yes" } },
      "r1",
    );
    assert.deepEqual(yield* decodeJson(encoded), {
      id: "r1",
      command: "answer",
      interactionId: "question-1",
      answers: { a: "yes" },
    });
    const extra = { command: "cancel" as const, text: "must not send" };
    assert.isTrue(Exit.isFailure(yield* encodeCodeRequest(extra, "r2").pipe(Effect.exit)));
    const suppliedId = { command: "show" as const, id: "spoofed" };
    assert.isTrue(Exit.isFailure(yield* encodeCodeRequest(suppliedId, "r3").pipe(Effect.exit)));
  }),
);

it.live("closes stdin, drains large stderr, and returns nonzero exit codes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-protocol-" });
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `
      let input = ''; for await (const chunk of process.stdin) input += chunk;
      if (input !== 'test prompt') process.exit(99);
      process.stderr.write('x'.repeat(2_000_000));
      console.log(${JSON.stringify(JSON.stringify(receipt))});
      process.exitCode = 4;
    `,
      });
      const events: CodeEvent[] = [];
      const code = yield* runCodeCommand({
        binaryPath,
        args: ["code", "new", "-", "--agent"],
        cwd,
        env: process.env,
        stdin: "test prompt",
        onEvent: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      });
      assert.equal(code, 4);
      assert.deepEqual(events, [receipt]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("honors the caller's command timeout and releases the scoped process", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const released = yield* Deferred.make<void>();
    const spawner = ChildProcessSpawner.make(() =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Deferred.succeed(released, undefined));
        yield* Deferred.succeed(started, undefined);
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(123),
          exitCode: Effect.never,
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.never,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
    );
    const command = yield* runCodeCommand({
      binaryPath: "/fake/cr",
      cwd: "/fake",
      args: ["code", "ls", "--agent"],
      env: {},
      timeout: "2 seconds",
      onEvent: () => Effect.void,
    }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.forkChild,
    );
    yield* Deferred.await(started);
    yield* TestClock.adjust("2 seconds");
    const error = yield* Fiber.join(command).pipe(Effect.flip);
    assert.equal(error.operation, "command timeout");
    yield* Deferred.await(released);
  }),
);
