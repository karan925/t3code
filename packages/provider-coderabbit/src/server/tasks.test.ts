import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { createTask, fetchLatestTurn, listTasks } from "./tasks.ts";

const task = {
  taskId: "task-1",
  url: "https://app.coderabbit.ai/code/tasks/task-1",
  title: "A task",
  status: "no_file_changes",
  turnStatus: "completed",
  collaborationMode: null,
  resultPullRequest: null,
};
const turn = {
  startSeq: 33,
  endSeq: 50,
  queueItemId: "queue-1",
  outcome: "completed" as const,
  outputKind: "patch",
  finalReply: "Done",
  errorMessage: null,
};

it.live(
  "creates with prompt on stdin, lists own repository tasks, and reads the terminal latest turn",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-tasks-" });
        const binaryPath = writeFakeCli({
          directory: cwd,
          name: "fake-cr",
          source: `
      const args = process.argv.slice(2);
      const output = value => console.log(JSON.stringify({phase:'code', ...value}));
      if (args[1] === 'new') {
        if (JSON.stringify(args) !== JSON.stringify(['code','new','-','--agent'])) process.exit(91);
        let text = ''; for await (const chunk of process.stdin) text += chunk;
        if (text !== 'prompt\\nwith lines') process.exit(92);
        output({type:'complete',status:'task_submitted',taskId:'task-1',url:${JSON.stringify(task.url)},task:${JSON.stringify(task)},created:true});
      } else if (args[1] === 'ls') {
        if (JSON.stringify(args) !== JSON.stringify(['code','ls','--agent','--repo',${JSON.stringify(cwd)}])) process.exit(93);
        output({type:'task',task:${JSON.stringify(task)}});
        output({type:'complete',status:'tasks_listed',tasks:[${JSON.stringify(task)}]});
      } else if (args[1] === 'resume') {
        if (JSON.stringify(args) !== JSON.stringify(['code','resume','task-1','--agent'])) process.exit(94);
        output({type:'status',status:'task_selected',taskId:'task-1',url:${JSON.stringify(task.url)}});
        output({type:'complete',status:'turn_completed',taskId:'task-1',url:${JSON.stringify(task.url)},turn:${JSON.stringify(turn)},task:${JSON.stringify(task)},agentMinutes:null});
      } else process.exit(95);
    `,
        });
        const options = { binaryPath, cwd, env: process.env };
        assert.deepEqual(yield* createTask({ ...options, prompt: "prompt\nwith lines" }), task);
        assert.deepEqual(yield* listTasks(options), [task]);
        assert.deepEqual(yield* fetchLatestTurn({ ...options, taskId: "task-1" }), {
          turn,
          task,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each([[], [task]])(
  "supports valid terminal task lists with unrelated extensions (%j)",
  (tasks) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-list-" });
        const binaryPath = writeFakeCli({
          directory: cwd,
          name: "fake-cr",
          source: `
          console.log(JSON.stringify({type:'future',phase:'code',payload:42}));
          console.log(JSON.stringify({type:'trace',phase:'code',kind:'future_trace'}));
          console.log(JSON.stringify({type:'complete',phase:'code',status:'future_complete'}));
          console.log(JSON.stringify({type:'complete',phase:'code',status:'tasks_listed',tasks:${JSON.stringify(tasks)}}));
        `,
        });
        assert.deepEqual(yield* listTasks({ binaryPath, cwd, env: process.env }), tasks);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("surfaces the CLI's task-list error verbatim", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-list-error-" });
      const message =
        "Repository is not enabled for CodeRabbit Coding Agent. Enable it in repository settings.";
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `console.log(JSON.stringify({type:'error',phase:'code',status:'repository_not_enabled',message:${JSON.stringify(message)}}));process.exitCode=1;`,
      });
      const error = yield* listTasks({ binaryPath, cwd, env: process.env }).pipe(Effect.flip);
      assert.equal(error.message, message);
      assert.equal(error.status, "repository_not_enabled");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("returns failed terminal turns as history with their error details", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-turn-error-" });
      const failed = {
        ...turn,
        outcome: "failed" as const,
        errorMessage: "Cloud execution failed",
      };
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `console.log(JSON.stringify({type:'error',phase:'code',status:'turn_failed',message:'Cloud execution failed',taskId:'task-1',url:${JSON.stringify(task.url)},turn:${JSON.stringify(failed)},task:${JSON.stringify(task)}}));process.exitCode=1;`,
      });
      assert.deepEqual(
        yield* fetchLatestTurn({ binaryPath, cwd, env: process.env, taskId: "task-1" }),
        { turn: failed, task },
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("keeps a task submission receipt when the transport subsequently fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-receipt-" });
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "fake-cr",
        source: `
      process.stdin.resume();
      process.stdin.on('end', () => {
        console.log(JSON.stringify({phase:'code',type:'complete',status:'task_submitted',taskId:'task-1',url:${JSON.stringify(task.url)},task:${JSON.stringify(task)}}));
        console.log('broken trailing frame'); process.exitCode=1;
      });
    `,
      });
      assert.deepEqual(
        yield* createTask({ binaryPath, cwd, env: process.env, prompt: "Do work" }),
        task,
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

const listed = { type: "complete", phase: "code", status: "tasks_listed", tasks: [task] };

it.effect.each(
  (
    [
      ["missing output", []],
      ["an individual task without a receipt", [{ type: "task", phase: "code", task }]],
      ["an unrelated terminal", [{ type: "complete", phase: "code", status: "future_complete" }]],
      ["a missing task payload", [{ type: "task", phase: "code" }, listed]],
      [
        "a drifted task payload",
        [{ type: "task", phase: "code", task: { ...task, taskId: 123 } }, listed],
      ],
      ["a task with a wrong phase", [{ type: "task", phase: "future", task }, listed]],
      ["a missing tasks array", [{ type: "complete", phase: "code", status: "tasks_listed" }]],
      ["a drifted tasks array", [{ ...listed, tasks: [task, { ...task, url: null }] }]],
      ["a receipt with a wrong phase", [{ ...listed, phase: "future" }]],
      ["a receipt with a wrong type", [{ ...listed, type: "status" }]],
      ["a malformed task after the receipt", [listed, { type: "task", phase: "code", task: null }]],
      ["a malformed receipt after the receipt", [listed, { ...listed, tasks: null }]],
    ] as const
  ).map(([label, frames]) => ({ label, frames })),
)("rejects task lists with $label", ({ frames }) =>
  Effect.gen(function* () {
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(123),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.encodeText(
            Stream.fromArray(frames.map((frame) => JSON.stringify(frame) + "\n")),
          ),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
    );
    const error = yield* listTasks({ binaryPath: "/fake/cr", cwd: "/fake", env: {} }).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.provideService(HostProcess.Platform, "linux"),
      Effect.flip,
    );
    assert.equal(error._tag, "CodeRabbitCommandError");
  }),
);
