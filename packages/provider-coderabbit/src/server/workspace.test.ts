import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import { CodeRabbitSettings } from "../settings.ts";
import { CloudWorkspaceError, verifyCloudWorkspace } from "./workspace.ts";

const isCloudWorkspaceError = Schema.is(CloudWorkspaceError);
const decodeSettings = Schema.decodeEffect(CodeRabbitSettings);

const makeRepository = Effect.fn("CodeRabbitWorkspaceTest.makeRepository")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-workspace-" });
  const origin = path.join(directory, "origin.git");
  const local = path.join(directory, "local");
  const emptyConfig = path.join(directory, "gitconfig");
  yield* fs.writeFileString(emptyConfig, "");
  const settings = yield* decodeSettings({
    binaryPath: writeFakeCli({
      directory,
      name: "fake-cr",
      source: "throw new Error('Unexpected CodeRabbit CLI invocation');",
    }),
  });
  const env = {
    ...process.env,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: emptyConfig,
    GIT_CONFIG_COUNT: "0",
    GIT_TERMINAL_PROMPT: "0",
    GIT_AUTHOR_NAME: "Test",
    GIT_AUTHOR_EMAIL: "test@example.test",
    GIT_COMMITTER_NAME: "Test",
    GIT_COMMITTER_EMAIL: "test@example.test",
  };
  const git = Effect.fn("CodeRabbitWorkspaceTest.git")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
  ) {
    const result = yield* spawnAndCollect(
      "git",
      ChildProcess.make("git", args, { cwd, env, extendEnv: false, stdin: "ignore" }),
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
  return { fs, path, directory, origin, local, env, git, settings };
});

const expectRejection = Effect.fn("CodeRabbitWorkspaceTest.expectRejection")(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
  settings: Pick<CodeRabbitSettings, "binaryPath" | "autoPushBranch">,
  reason: CloudWorkspaceError["reason"],
) {
  const result = yield* verifyCloudWorkspace(cwd, env, settings).pipe(Effect.result);
  assert(Result.isFailure(result));
  assert(isCloudWorkspaceError(result.failure));
  expect(result.failure.reason).toBe(reason);
  if (reason === "unpublished") {
    expect(result.failure.message).toContain("branch");
    expect(result.failure.message.toLowerCase()).toContain("push");
    if (result.failure.branch)
      expect(result.failure.message).toContain(`Push \`${result.failure.branch}\` first`);
  }
  return result.failure;
});

describe("CodeRabbit source workspace", () => {
  it.live("accepts a clean branch whose commit is published on origin", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "main",
          dirty: false,
          pushed: false,
          localDiffers: false,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reports modified tracked files without rejecting the published branch", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.fs.writeFileString(repo.path.join(repo.local, "tracked.txt"), "local change\n");
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "main",
          dirty: true,
          pushed: false,
          localDiffers: false,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reports untracked files without rejecting the published branch", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.fs.writeFileString(
          repo.path.join(repo.local, "untracked.txt"),
          "local change\n",
        );
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "main",
          dirty: true,
          pushed: false,
          localDiffers: false,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("accepts an ahead branch without pushing its local commits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const remoteHead = yield* repo.git(repo.origin, ["rev-parse", "refs/heads/main"]);
        yield* repo.git(repo.local, ["commit", "--allow-empty", "-m", "Local commit"]);
        const localHead = yield* repo.git(repo.local, ["rev-parse", "HEAD"]);
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "main",
          dirty: false,
          pushed: false,
          localDiffers: true,
        });
        expect(yield* repo.git(repo.origin, ["rev-parse", "refs/heads/main"])).toBe(remoteHead);
        expect(yield* repo.git(repo.local, ["rev-parse", "HEAD"])).toBe(localHead);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("publishes a new branch with no new commits by default and sets its upstream", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const head = yield* repo.git(repo.local, ["rev-parse", "HEAD"]);
        yield* repo.git(repo.local, ["checkout", "-b", "unpublished-branch"]);
        expect(repo.settings.autoPushBranch).toBe(true);
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "unpublished-branch",
          dirty: false,
          pushed: true,
          localDiffers: false,
        });
        expect(yield* repo.git(repo.local, ["rev-parse", "HEAD"])).toBe(head);
        expect(yield* repo.git(repo.origin, ["rev-parse", "refs/heads/unpublished-branch"])).toBe(
          head,
        );
        expect(yield* repo.git(repo.local, ["rev-parse", "--abbrev-ref", "@{upstream}"])).toBe(
          "origin/unpublished-branch",
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("leaves a missing remote branch unpublished when auto-push is disabled", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["checkout", "-b", "unpublished-branch"]);
        const failure = yield* expectRejection(
          repo.local,
          repo.env,
          { ...repo.settings, autoPushBranch: false },
          "unpublished",
        );
        expect(failure.branch).toBe("unpublished-branch");
        expect(
          yield* repo.git(repo.local, [
            "ls-remote",
            "--heads",
            "origin",
            "refs/heads/unpublished-branch",
          ]),
        ).toBe("");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("accepts a behind branch while checking origin rather than a stale tracking ref", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const other = repo.path.join(repo.directory, "other");
        yield* repo.git(repo.directory, ["clone", repo.origin, other]);
        yield* repo.git(other, ["commit", "--allow-empty", "-m", "Remote change"]);
        yield* repo.git(other, ["push", "origin", "main"]);
        const remoteHead = yield* repo.git(other, ["rev-parse", "HEAD"]);
        const localHead = yield* repo.git(repo.local, ["rev-parse", "HEAD"]);
        expect(yield* repo.git(repo.local, ["rev-parse", "origin/main"])).toBe(localHead);
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "main",
          dirty: false,
          pushed: false,
          localDiffers: true,
        });
        expect(yield* repo.git(repo.origin, ["rev-parse", "refs/heads/main"])).toBe(remoteHead);
        expect(yield* repo.git(repo.local, ["rev-parse", "HEAD"])).toBe(localHead);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("accepts a diverged branch without changing local or remote history", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const other = repo.path.join(repo.directory, "other");
        yield* repo.git(repo.directory, ["clone", repo.origin, other]);
        yield* repo.git(other, ["commit", "--allow-empty", "-m", "Remote change"]);
        yield* repo.git(other, ["push", "origin", "main"]);
        yield* repo.git(repo.local, ["commit", "--allow-empty", "-m", "Local change"]);
        const remoteHead = yield* repo.git(other, ["rev-parse", "HEAD"]);
        const localHead = yield* repo.git(repo.local, ["rev-parse", "HEAD"]);
        expect(yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).toEqual({
          branch: "main",
          dirty: false,
          pushed: false,
          localDiffers: true,
        });
        expect(yield* repo.git(repo.origin, ["rev-parse", "refs/heads/main"])).toBe(remoteHead);
        expect(yield* repo.git(repo.local, ["rev-parse", "HEAD"])).toBe(localHead);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("rejects detached HEAD even when the commit is published", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["checkout", "--detach", "HEAD"]);
        yield* expectRejection(repo.local, repo.env, repo.settings, "unpublished");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reports Git failures for a directory outside a repository", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* expectRejection(repo.directory, repo.env, repo.settings, "git");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reports actionable push failures with capped Git stderr", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["checkout", "-b", "rejected-branch"]);
        const hooks = repo.path.join(repo.directory, "hooks");
        writeFakeCli({
          directory: hooks,
          name: "pre-push",
          platform: "linux",
          source: `
            process.stderr.write('verbose-prefix-' + 'x'.repeat(10000) + '\\nPermission denied by test hook\\n');
            process.exit(1);
          `,
        });
        yield* repo.git(repo.local, ["config", "core.hooksPath", hooks]);
        const failure = yield* expectRejection(repo.local, repo.env, repo.settings, "push_failed");
        expect(failure.branch).toBe("rejected-branch");
        expect(failure.message).toContain("Push `rejected-branch` manually");
        expect(failure.message).toContain("then send your message again");
        assert(failure.detail);
        expect(failure.detail.length).toBeLessThanOrEqual(300);
        expect(failure.detail).toContain("Permission denied by test hook");
        expect(failure.detail).not.toContain("verbose-prefix-");
        expect(failure.message).toContain(failure.detail);
        expect(
          yield* repo.git(repo.local, [
            "ls-remote",
            "--heads",
            "origin",
            "refs/heads/rejected-branch",
          ]),
        ).toBe("");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
