import * as Effect from "effect/Effect";

import {
  CommandError,
  isKnownCodeEvent,
  runCodeCommand,
  type CodeEvent,
  type TaskDetail,
  type Turn,
} from "./protocol.ts";

export interface TaskCommandOptions {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
}

const commandFailure = (event: CodeEvent): CommandError | undefined => {
  if (!isKnownCodeEvent(event)) return undefined;
  if (
    event.type === "error" ||
    (event.type === "action_required" && event.action === "authenticate")
  )
    return new CommandError({
      operation: "task command",
      detail: event.message,
      ...(event.type === "error" ? { status: event.status } : {}),
    });
  return undefined;
};

export const createTask = Effect.fn("CodeRabbit.createTask")(function* (
  options: TaskCommandOptions & { readonly prompt: string },
) {
  let task: TaskDetail | undefined;
  const result = yield* runCodeCommand({
    ...options,
    args: ["code", "new", "-", "--agent"],
    stdin: options.prompt,
    onEvent: Effect.fn("CodeRabbit.createTask.onEvent")(function* (event: CodeEvent) {
      const error = commandFailure(event);
      if (error) return yield* error;
      if (isKnownCodeEvent(event) && event.type === "complete" && event.status === "task_submitted")
        task = event.task;
    }),
  }).pipe(Effect.result);
  // A submission receipt is durable even if the local command subsequently loses its stream.
  if (task !== undefined) return task;
  if (result._tag === "Failure") return yield* result.failure;
  return yield* new CommandError({ operation: "create task" });
});

export const listTasks = Effect.fn("CodeRabbit.listTasks")(function* (options: TaskCommandOptions) {
  let tasks: ReadonlyArray<TaskDetail> = [];
  const code = yield* runCodeCommand({
    ...options,
    args: ["code", "ls", "--agent", "--repo", options.cwd],
    onEvent: Effect.fn("CodeRabbit.listTasks.onEvent")(function* (event: CodeEvent) {
      const error = commandFailure(event);
      if (error) return yield* error;
      if (isKnownCodeEvent(event)) {
        if (event.type === "task") tasks = [...tasks, event.task];
        if (event.type === "complete" && event.status === "tasks_listed") tasks = event.tasks;
      }
    }),
  });
  if (code !== 0) return yield* new CommandError({ operation: "list tasks" });
  // The CLI defaults to the caller's own tasks, newest first, with a 20-task limit.
  return tasks;
});

export const fetchLatestTurn = Effect.fn("CodeRabbit.fetchLatestTurn")(function* (
  options: TaskCommandOptions & { readonly taskId: string },
) {
  let latest: { readonly turn: Turn; readonly task: TaskDetail } | undefined;
  const code = yield* runCodeCommand({
    ...options,
    args: ["code", "resume", options.taskId, "--agent"],
    onEvent: Effect.fn("CodeRabbit.fetchLatestTurn.onEvent")(function* (event: CodeEvent) {
      // Failed terminal turns are valid history, rather than transport failures.
      if (
        isKnownCodeEvent(event) &&
        (event.type === "complete" || event.type === "error") &&
        "turn" in event
      )
        latest = { turn: event.turn, task: event.task };
      else {
        const error = commandFailure(event);
        if (error) return yield* error;
      }
    }),
  });
  if (latest !== undefined) return latest;
  return yield* new CommandError({
    operation: "fetch latest turn",
    detail: `CodeRabbit resume returned no terminal turn (exit ${code}).`,
  });
});
