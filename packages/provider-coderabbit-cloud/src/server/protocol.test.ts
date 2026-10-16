import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import { consumeCodeEvents, runCodeCommand, type CodeEvent } from "./protocol.ts";

const encode = (record: unknown) => new TextEncoder().encode(`${JSON.stringify(record)}\n`);
const receipt = {
  type: "status",
  phase: "code",
  status: "task_submitted",
  message: "accepted",
  taskId: "task-1",
  url: "https://app.coderabbit.ai/code/tasks/task-1",
  clientOperationId: null,
} satisfies CodeEvent;

it.effect("delivers a submission receipt before a later malformed frame fails", () =>
  Effect.gen(function* () {
    const events: CodeEvent[] = [];
    const exit = yield* consumeCodeEvents(
      Stream.make(encode(receipt), new TextEncoder().encode("not JSON\n")),
      (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(exit));
    assert.deepEqual(events, [receipt]);
  }),
);

it.effect("decodes Unicode split across chunks and multiple LF frames", () =>
  Effect.gen(function* () {
    const record = {
      type: "trace",
      phase: "code",
      taskId: "task-1",
      seq: 1,
      createdAt: "2026-10-16",
      kind: "agent_message",
      text: "Working 🐰",
    } satisfies CodeEvent;
    const bytes = encode(record);
    const split = bytes.indexOf(0xf0) + 2;
    const events: CodeEvent[] = [];
    yield* consumeCodeEvents(
      Stream.make(
        bytes.subarray(0, split),
        bytes.subarray(split),
        encode({ type: "status", phase: "code", status: "future_progress", message: "Continuing" }),
      ),
      (event) =>
        Effect.sync(() => {
          events.push(event);
        }),
    );
    assert.equal(events.length, 2);
    assert.deepEqual(events[0], record);
  }),
);

it.effect("rejects malformed terminal errors instead of accepting their generic error fields", () =>
  Effect.gen(function* () {
    const exit = yield* consumeCodeEvents(
      Stream.make(
        encode({ type: "error", phase: "code", status: "turn_failed", message: "failed" }),
      ),
      () => Effect.void,
    ).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(exit));
  }),
);

it.effect("bounds individual frames while allowing many small frames", () =>
  Effect.gen(function* () {
    const oversized = new Uint8Array(1_048_577).fill(32);
    const exit = yield* consumeCodeEvents(Stream.make(oversized), () => Effect.void).pipe(
      Effect.exit,
    );
    assert.isTrue(Exit.isFailure(exit));
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

it.effect("preserves a sensitive question's null answer template and timeout resume action", () =>
  Effect.gen(function* () {
    const question = {
      type: "action_required",
      phase: "code",
      action: "answer_question",
      taskId: "task-1",
      url: receipt.url,
      message: "Answer in browser",
      command: null,
      answerTemplate: null,
      question: {
        interactionId: "i-1",
        isBlocking: true,
        questions: [
          {
            id: "secret",
            header: "Input",
            question: "Provide a secret",
            options: null,
            isOther: true,
            isSensitive: true,
          },
        ],
      },
    } satisfies CodeEvent;
    const resume = {
      type: "action_required",
      phase: "code",
      action: "resume",
      taskId: "task-1",
      url: receipt.url,
      message: "Still running",
      command: "coderabbit code resume task-1 --agent",
      clientOperationId: null,
    } satisfies CodeEvent;
    const events: CodeEvent[] = [];
    yield* consumeCodeEvents(Stream.make(encode(question), encode(resume)), (event) =>
      Effect.sync(() => {
        events.push(event);
      }),
    );
    assert.deepEqual(events, [question, resume]);
  }),
);

it.layer(NodeServices.layer)("CodeRabbit command runner", (it) => {
  it.effect("closes stdin, drains large stderr, and preserves the still-running exit code", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-protocol-" });
        const binaryPath = writeFakeCli({
          directory: cwd,
          name: "cr",
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
          args: ["code", "new", "--agent", "-"],
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
    ),
  );
});
