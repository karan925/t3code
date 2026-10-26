import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
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
  it.effect("gives push its full 60 seconds after slow authentication and remote lookup", () =>
    Effect.gen(function* () {
      const authenticated = yield* Deferred.make<void>();
      const lookedUp = yield* Deferred.make<void>();
      const completed = yield* Deferred.make<void>();
      const started = yield* Queue.unbounded<ReadonlyArray<string>>();
      const steps = [
        {
          command: "git",
          args: ["remote", "get-url", "origin"],
          stdout: "git@github.com:example/repo.git\n",
          exit: Effect.void,
        },
        {
          command: "/fake/cr",
          args: ["auth", "status", "--agent"],
          stdout: JSON.stringify({
            type: "status",
            phase: "auth",
            authenticated: true,
            provider: "github",
            currentOrg: { name: "example" },
          }),
          exit: Deferred.await(authenticated),
        },
        {
          command: "git",
          args: ["symbolic-ref", "--quiet", "HEAD"],
          stdout: "refs/heads/unpublished-branch\n",
          exit: Effect.void,
        },
        {
          command: "git",
          args: ["rev-parse", "HEAD"],
          stdout: "abc123\n",
          exit: Effect.void,
        },
        {
          command: "git",
          args: ["ls-remote", "--heads", "origin", "refs/heads/unpublished-branch"],
          stdout: "",
          exit: Deferred.await(lookedUp),
        },
        {
          command: "git",
          args: ["symbolic-ref", "--quiet", "HEAD"],
          stdout: "refs/heads/unpublished-branch\n",
          exit: Effect.void,
        },
        {
          command: "git",
          args: ["remote", "get-url", "origin"],
          stdout: "git@github.com:example/repo.git\n",
          exit: Effect.void,
        },
        {
          command: "git",
          args: ["remote", "get-url", "--push", "origin"],
          stdout: "https://github.com/Example/repo\n",
          exit: Effect.void,
        },
        {
          command: "git",
          args: [
            "push",
            "-u",
            "origin",
            "refs/heads/unpublished-branch:refs/heads/unpublished-branch",
          ],
          stdout: "",
          exit: Effect.never,
        },
      ];
      const commands: Array<{ command: string; args: ReadonlyArray<string> }> = [];
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          assert(command._tag === "StandardCommand");
          const step = steps[commands.length];
          assert(step, "Unexpected command after push");
          const invocation = { command: command.command, args: command.args };
          commands.push(invocation);
          expect(invocation).toEqual({ command: step.command, args: step.args });
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(123),
            exitCode: Queue.offer(started, command.args).pipe(
              Effect.andThen(step.exit),
              Effect.as(ChildProcessSpawner.ExitCode(0)),
            ),
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.encodeText(Stream.make(step.stdout)),
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          });
        }),
      );
      const verification = yield* verifyCloudWorkspace(
        "/fake/repo",
        {},
        {
          binaryPath: "/fake/cr",
          autoPushBranch: true,
        },
      ).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(HostProcess.Platform, "linux"),
        Effect.result,
        Effect.ensuring(Deferred.succeed(completed, undefined)),
        Effect.forkChild,
      );
      for (const step of steps.slice(0, 2)) {
        expect(yield* Queue.take(started)).toEqual(step.args);
      }
      yield* TestClock.adjust("9 seconds");
      yield* Deferred.succeed(authenticated, undefined);
      for (const step of steps.slice(2, 5)) {
        expect(yield* Queue.take(started)).toEqual(step.args);
      }
      yield* TestClock.adjust("29 seconds");
      yield* Deferred.succeed(lookedUp, undefined);
      for (const step of steps.slice(5, 9)) {
        expect(yield* Queue.take(started)).toEqual(step.args);
      }

      // Slow preflight must not consume the push's own 60-second budget.
      yield* TestClock.adjust("52 seconds");
      expect(yield* Deferred.isDone(completed)).toBe(false);
      yield* TestClock.adjust("7999 millis");
      expect(yield* Deferred.isDone(completed)).toBe(false);
      yield* TestClock.adjust("1 millis");
      expect(yield* Deferred.isDone(completed)).toBe(true);
      const result = yield* Fiber.join(verification);
      assert(Result.isFailure(result));
      assert(isCloudWorkspaceError(result.failure));
      expect(result.failure.reason).toBe("push_failed");
      expect(result.failure.branch).toBe("unpublished-branch");
      expect(result.failure.message).toContain("timed out");
      expect(result.failure.message).toContain("Push `unpublished-branch` manually");
      expect(result.failure.message).toContain("then send your message again");
      expect(commands).toEqual(steps.map(({ command, args }) => ({ command, args })));
    }),
  );

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

  it.live("uses an unambiguous branch ref when a tag has the same name", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["tag", "main"]);
        expect((yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings)).branch).toBe(
          "main",
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live.each(["temporary", "before-push"] as const)("publishes the renamed branch (%s)", (when) =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const initial = when === "temporary" ? "t3code/1234abcd" : "initial-name";
        yield* repo.git(repo.local, ["checkout", "-b", initial]);
        const original = yield* ChildProcessSpawner.ChildProcessSpawner;
        let reads = 0;
        const spawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            assert(command._tag === "StandardCommand");
            if (command.args[0] === "symbolic-ref") reads++;
            if (
              (when === "temporary" && reads === 2 && command.args[0] === "symbolic-ref") ||
              (when === "before-push" && command.args[0] === "ls-remote" && reads === 1)
            ) {
              yield* repo
                .git(repo.local, ["branch", "-m", "final-name"])
                .pipe(
                  Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, original),
                  Effect.orDie,
                );
            }
            return yield* original.spawn(command);
          }),
        );
        const result = yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        expect(result).toMatchObject({ branch: "final-name", pushed: true });
        expect(
          yield* repo.git(repo.local, ["ls-remote", "--heads", "origin", `refs/heads/${initial}`]),
        ).toBe("");
        expect(yield* repo.git(repo.local, ["rev-parse", "--abbrev-ref", "@{upstream}"])).toBe(
          "origin/final-name",
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("stops retrying when the branch changes again before the second push attempt", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        yield* repo.git(repo.local, ["checkout", "-b", "initial-name"]);
        const original = yield* ChildProcessSpawner.ChildProcessSpawner;
        let lookups = 0;
        const spawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            assert(command._tag === "StandardCommand");
            if (command.args[0] === "ls-remote") {
              lookups++;
              yield* repo
                .git(repo.local, ["branch", "-m", `renamed-${lookups}`])
                .pipe(
                  Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, original),
                  Effect.orDie,
                );
            }
            return yield* original.spawn(command);
          }),
        );
        const failure = yield* expectRejection(
          repo.local,
          repo.env,
          repo.settings,
          "branch_changed",
        ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
        expect(failure.message).toBe(
          "The branch changed while CodeRabbit was preparing the task. Send your message again.",
        );
        expect(lookups).toBe(2);
        expect(
          yield* repo.git(repo.local, ["ls-remote", "--heads", "origin", "refs/heads/renamed-*"]),
        ).toBe("");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("publishes a temporary branch after the deadline without setting an upstream", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const branch = "t3code/1234abcd";
        yield* repo.git(repo.local, ["checkout", "--no-track", "-b", branch]);
        expect(
          yield* verifyCloudWorkspace(repo.local, repo.env, repo.settings, {
            branchRenameWaitMs: 0,
          }),
        ).toMatchObject({ branch, pushed: true });
        yield* repo.git(repo.local, ["branch", "-m", "late-name"]);
        expect(
          yield* repo.git(repo.local, [
            "for-each-ref",
            "--format=%(upstream)",
            "refs/heads/late-name",
          ]),
        ).toBe("");
        expect(
          yield* repo.git(repo.local, ["ls-remote", "--heads", "origin", `refs/heads/${branch}`]),
        ).not.toBe("");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("refuses to auto-push when origin pushes to a different repository", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const pushOrigin = repo.path.join(repo.directory, "push.git");
        yield* repo.git(repo.directory, ["init", "--bare", pushOrigin]);
        yield* repo.git(repo.local, ["remote", "set-url", "--push", "origin", pushOrigin]);
        yield* repo.git(repo.local, ["checkout", "-b", "different-push-url"]);
        const failure = yield* expectRejection(
          repo.local,
          repo.env,
          repo.settings,
          "push_target_mismatch",
        );
        expect(failure.message).toContain("`different-push-url`");
        expect(yield* repo.git(pushOrigin, ["branch", "--list", "different-push-url"])).toBe("");
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

  it.live("keeps Git stderr out of push failure messages", () =>
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
            process.stderr.write('verbose-prefix-' + 'x'.repeat(10000) + '\\nhttps://user:' + 'private-password'.repeat(100) + '@example.test/repo?token=private-token\\nPermission denied by test hook\\n');
            process.exit(1);
          `,
        });
        yield* repo.git(repo.local, ["config", "core.hooksPath", hooks]);
        const failure = yield* expectRejection(repo.local, repo.env, repo.settings, "push_failed");
        expect(failure.branch).toBe("rejected-branch");
        expect(failure.message).toContain("Push `rejected-branch` manually");
        expect(failure.message).toContain("then send your message again");
        expect(failure.message).not.toContain("Permission denied by test hook");
        expect(failure.message).not.toContain("private-password");
        expect(failure.message).not.toContain("example.test");
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
