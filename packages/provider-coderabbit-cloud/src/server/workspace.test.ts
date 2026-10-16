import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import { CloudWorkspaceError, verifyCloudWorkspace } from "./workspace.ts";

const isCloudWorkspaceError = Schema.is(CloudWorkspaceError);

const makeRepository = Effect.fnUntraced(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-cloud-workspace-" });
  const origin = path.join(directory, "origin.git");
  const local = path.join(directory, "local");
  const emptyConfig = path.join(directory, "gitconfig");
  yield* fs.writeFileString(emptyConfig, "");
  const env = {
    ...process.env,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: emptyConfig,
    GIT_TERMINAL_PROMPT: "0",
    GIT_AUTHOR_NAME: "Test",
    GIT_AUTHOR_EMAIL: "test@example.test",
    GIT_COMMITTER_NAME: "Test",
    GIT_COMMITTER_EMAIL: "test@example.test",
  };
  const git = Effect.fnUntraced(function* (cwd: string, args: ReadonlyArray<string>) {
    const result = yield* spawnAndCollect(
      "git",
      ChildProcess.make("git", args, {
        cwd,
        env,
        extendEnv: false,
        stdin: "ignore",
      }),
    );
    expect(result.code, result.stderr).toBe(0);
    return result.stdout.trim();
  });
  yield* git(directory, ["init", "--bare", "--initial-branch=main", origin]);
  yield* git(directory, ["clone", origin, local]);
  yield* fs.writeFileString(path.join(local, "tracked.txt"), "published\n");
  yield* git(local, ["add", "tracked.txt"]);
  yield* git(local, ["commit", "-m", "Published state"]);
  yield* git(local, ["push", "origin", "main"]);
  return { fs, path, directory, origin, local, env, git };
});

const expectRejection = Effect.fnUntraced(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
  reason: CloudWorkspaceError["reason"],
) {
  const result = yield* verifyCloudWorkspace(cwd, env).pipe(Effect.result);
  expect(Result.isFailure(result)).toBe(true);
  if (Result.isFailure(result)) {
    expect(isCloudWorkspaceError(result.failure)).toBe(true);
    expect(result.failure.reason).toBe(reason);
  }
});

it.layer(NodeServices.layer)("CodeRabbit Cloud source workspace", (it) => {
  it.effect("accepts a clean branch whose commit is published on origin", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* verifyCloudWorkspace(repo.local, repo.env);
      }),
    ),
  );

  it.effect("rejects modified tracked files", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.fs.writeFileString(repo.path.join(repo.local, "tracked.txt"), "local change\n");
        yield* expectRejection(repo.local, repo.env, "dirty");
      }),
    ),
  );

  it.effect("rejects untracked files that would not reach the cloud", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.fs.writeFileString(
          repo.path.join(repo.local, "untracked.txt"),
          "local change\n",
        );
        yield* expectRejection(repo.local, repo.env, "dirty");
      }),
    ),
  );

  it.effect("rejects an unpushed commit even with a clean working tree", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["commit", "--allow-empty", "-m", "Local commit"]);
        yield* expectRejection(repo.local, repo.env, "unpublished");
      }),
    ),
  );

  it.effect("rejects a branch that does not exist on origin", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["checkout", "-b", "unpublished-branch"]);
        yield* expectRejection(repo.local, repo.env, "unpublished");
      }),
    ),
  );

  it.effect("checks the remote branch rather than trusting a stale tracking ref", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const other = repo.path.join(repo.directory, "other");
        yield* repo.git(repo.directory, ["clone", repo.origin, other]);
        yield* repo.git(other, ["commit", "--allow-empty", "-m", "Remote change"]);
        yield* repo.git(other, ["push", "origin", "main"]);
        yield* expectRejection(repo.local, repo.env, "unpublished");
      }),
    ),
  );

  it.effect("rejects detached HEAD even when the commit is published", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["checkout", "--detach", "HEAD"]);
        yield* expectRejection(repo.local, repo.env, "git");
      }),
    ),
  );
});
