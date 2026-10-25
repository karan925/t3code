import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";

export class CloudWorkspaceError extends Schema.TaggedError<CloudWorkspaceError>()(
  "CodeRabbitWorkspaceError",
  {
    reason: Schema.Literals(["git", "unpublished"]),
    branch: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    switch (this.reason) {
      case "unpublished":
        return this.branch
          ? `CodeRabbit needs the current branch and commit published on origin. Push \`${this.branch}\` first before delegating.`
          : "CodeRabbit needs a published branch. Check out a branch and push it to origin first before delegating.";
      case "git":
        return "Could not verify the CodeRabbit source branch. Check Git and origin access before delegating.";
    }
  }
}

const isCloudWorkspaceError = Schema.is(CloudWorkspaceError);

export const verifyCloudWorkspace = Effect.fn("CodeRabbit.verifyWorkspace")(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
) {
  const git = Effect.fn("CodeRabbit.workspaceGit")(function* (args: ReadonlyArray<string>) {
    const spawn = yield* resolveSpawnCommand("git", args, { env });
    return yield* spawnAndCollect(
      "git",
      ChildProcess.make(spawn.command, spawn.args, {
        cwd,
        env: { ...env, GIT_TERMINAL_PROMPT: "0" },
        extendEnv: false,
        shell: spawn.shell,
        stdin: "ignore",
      }),
    );
  });
  const requiredGit = Effect.fn("CodeRabbit.requiredWorkspaceGit")(function* (
    args: ReadonlyArray<string>,
  ) {
    const result = yield* git(args);
    if (result.code !== 0) return yield* new CloudWorkspaceError({ reason: "git" });
    return result.stdout.trim();
  });
  return yield* Effect.gen(function* () {
    const branchResult = yield* git(["symbolic-ref", "--quiet", "--short", "HEAD"]);
    if (branchResult.code === 1) {
      return yield* new CloudWorkspaceError({ reason: "unpublished" });
    }
    if (branchResult.code !== 0) return yield* new CloudWorkspaceError({ reason: "git" });
    const branch = branchResult.stdout.trim();
    const head = yield* requiredGit(["rev-parse", "HEAD"]);
    const remote = yield* requiredGit(["ls-remote", "--heads", "origin", `refs/heads/${branch}`]);
    if (remote.split(/\s+/u)[0] !== head) {
      return yield* new CloudWorkspaceError({ reason: "unpublished", branch });
    }
    const dirty =
      (yield* requiredGit(["status", "--porcelain", "--untracked-files=normal"])) !== "";
    return { dirty };
  }).pipe(
    Effect.timeout("30 seconds"),
    Effect.mapError((cause) =>
      isCloudWorkspaceError(cause) ? cause : new CloudWorkspaceError({ reason: "git", cause }),
    ),
  );
});
