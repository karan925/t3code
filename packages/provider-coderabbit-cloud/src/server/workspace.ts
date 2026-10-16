import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";

export class CloudWorkspaceError extends Schema.TaggedError<CloudWorkspaceError>()(
  "CodeRabbitCloudWorkspaceError",
  {
    reason: Schema.Literals(["git", "dirty", "unpublished"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    switch (this.reason) {
      case "dirty":
        return "CodeRabbit Cloud starts from origin. Commit and push local changes before delegating.";
      case "unpublished":
        return "CodeRabbit Cloud needs the current branch and commit published on origin. Push the branch before delegating.";
      case "git":
        return "Could not verify the CodeRabbit Cloud source branch. Check Git and origin access before delegating.";
    }
  }
}

const isCloudWorkspaceError = Schema.is(CloudWorkspaceError);

export const verifyCloudWorkspace = Effect.fn("CodeRabbitCloud.verifyWorkspace")(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
) {
  const git = Effect.fnUntraced(function* (args: ReadonlyArray<string>) {
    const spawn = yield* resolveSpawnCommand("git", args, { env });
    const result = yield* spawnAndCollect(
      "git",
      ChildProcess.make(spawn.command, spawn.args, {
        cwd,
        env: { ...env, GIT_TERMINAL_PROMPT: "0" },
        extendEnv: false,
        shell: spawn.shell,
        stdin: "ignore",
      }),
    );
    if (result.code !== 0) return yield* new CloudWorkspaceError({ reason: "git" });
    return result.stdout.trim();
  });
  return yield* Effect.gen(function* () {
    if ((yield* git(["status", "--porcelain", "--untracked-files=normal"])) !== "") {
      return yield* new CloudWorkspaceError({ reason: "dirty" });
    }
    const branch = yield* git(["symbolic-ref", "--quiet", "--short", "HEAD"]);
    const head = yield* git(["rev-parse", "HEAD"]);
    const remote = yield* git(["ls-remote", "--heads", "origin", `refs/heads/${branch}`]);
    if (remote.split(/\s+/u)[0] !== head) {
      return yield* new CloudWorkspaceError({ reason: "unpublished" });
    }
  }).pipe(
    Effect.timeout("30 seconds"),
    Effect.mapError((cause) =>
      isCloudWorkspaceError(cause) ? cause : new CloudWorkspaceError({ reason: "git", cause }),
    ),
  );
});
