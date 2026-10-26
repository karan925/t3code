import { redactProviderFailureText } from "@t3tools/provider-core/server/failure";
import { isTemporaryWorktreeBranch } from "@t3tools/shared/git";
import * as Schedule from "effect/Schedule";
import * as Cause from "effect/Cause";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import type { CodeRabbitSettings } from "../settings.ts";
import { collectCodeRabbitCommand } from "./protocol.ts";
import { probeCodeRabbitOrganization } from "./status.ts";

export class CloudWorkspaceError extends Schema.TaggedError<CloudWorkspaceError>()(
  "CodeRabbitWorkspaceError",
  {
    reason: Schema.Literals([
      "git",
      "branch_changed",
      "unpublished",
      "push_failed",
      "push_unconfirmed",
      "push_target_mismatch",
      "org_mismatch",
    ]),
    branch: Schema.optional(Schema.String),
    timedOut: Schema.optional(Schema.Boolean),
    org: Schema.optional(Schema.String),
    owner: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    switch (this.reason) {
      case "unpublished":
        return this.branch
          ? `CodeRabbit needs the current branch published on origin. Push \`${this.branch}\` first before delegating.`
          : "CodeRabbit needs a published branch. Check out a branch and push it to origin first before delegating.";
      case "branch_changed":
        return "The branch changed while CodeRabbit was preparing the task. Send your message again.";
      case "git":
        return "Could not verify the CodeRabbit source branch. Check Git and origin access before delegating.";
      case "push_failed":
        return `CodeRabbit could not push \`${this.branch}\` to origin${this.timedOut ? " (timed out after 60 seconds)" : ""}. Push \`${this.branch}\` manually, then send your message again.`;
      case "push_unconfirmed":
        return `Pushed \`${this.branch}\` but origin does not show it yet (origin may push to a different URL). Check the remote and try again.`;
      case "push_target_mismatch":
        return `origin pushes to a different repository than it fetches from, so CodeRabbit won't push \`${this.branch}\` automatically. Push it to origin's fetch repository, then send your message again.`;
      case "org_mismatch":
        return `CodeRabbit CLI is using the \`${this.org}\` organization, but this repository belongs to \`${this.owner}\`. Run \`cr auth logout && cr auth login\` on this machine, choose \`${this.owner}\`, then send your message again.`;
    }
  }
}

const isCloudWorkspaceError = Schema.is(CloudWorkspaceError);

const githubOwner = (remote: string) =>
  /^(?:git@github\.com:|ssh:\/\/git@github\.com\/|https:\/\/github\.com\/)([^/\s]+)\/[^/\s]+\/?$/iu.exec(
    remote,
  )?.[1];

const githubRepository = (remote: string) => {
  const match =
    /^(?:git@github\.com:|ssh:\/\/git@github\.com\/|https:\/\/github\.com\/)([^/\s]+)\/([^/\s]+?)(?:\.git)?\/?$/iu.exec(
      remote,
    );
  return match ? `${match[1]}/${match[2]}`.toLowerCase() : undefined;
};

// SSH and HTTPS URLs for one GitHub repository are the same publication target.
const sameRepository = (fetchUrl: string, pushUrl: string) =>
  fetchUrl === pushUrl ||
  (githubRepository(fetchUrl) !== undefined &&
    githubRepository(fetchUrl) === githubRepository(pushUrl));

const workspaceGit = Effect.fn("CodeRabbit.workspaceGit")(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
  args: ReadonlyArray<string>,
  timeout = 30_000,
) {
  const spawn = yield* resolveSpawnCommand("git", args, { env });
  return yield* collectCodeRabbitCommand(
    "git",
    ChildProcess.make(spawn.command, spawn.args, {
      cwd,
      env: { ...env, GIT_TERMINAL_PROMPT: "0" },
      extendEnv: false,
      shell: spawn.shell,
      forceKillAfter: "1 second",
      stdin: "ignore",
    }),
  ).pipe(Effect.timeout(timeout));
});

export const readWorkspaceBranch = Effect.fn("CodeRabbit.workspaceBranch")(
  function* (cwd: string, env: NodeJS.ProcessEnv) {
    const result = yield* workspaceGit(cwd, env, ["symbolic-ref", "--quiet", "HEAD"]);
    if (result.code === 1 || (result.code === 0 && !result.stdout.startsWith("refs/heads/"))) {
      return yield* new CloudWorkspaceError({ reason: "unpublished" });
    }
    if (result.code !== 0) return yield* new CloudWorkspaceError({ reason: "git" });
    return result.stdout.trim().slice("refs/heads/".length);
  },
  (effect) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isCloudWorkspaceError(cause) ? cause : new CloudWorkspaceError({ reason: "git", cause }),
      ),
    ),
);

export const verifyCloudWorkspace = Effect.fn("CodeRabbit.verifyWorkspace")(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
  settings: Pick<CodeRabbitSettings, "binaryPath" | "autoPushBranch">,
  options: { readonly branchRenameWaitMs?: number } = {},
) {
  const git = (args: ReadonlyArray<string>, timeout?: number) =>
    workspaceGit(cwd, env, args, timeout);
  const requiredGit = Effect.fn("CodeRabbit.requiredWorkspaceGit")(function* (
    args: ReadonlyArray<string>,
  ) {
    const result = yield* git(args);
    if (result.code !== 0) return yield* new CloudWorkspaceError({ reason: "git" });
    return result.stdout.trim();
  });
  return yield* Effect.gen(function* () {
    const remoteUrl = yield* git(["remote", "get-url", "origin"]);
    const owner = remoteUrl.code === 0 ? githubOwner(remoteUrl.stdout.trim()) : undefined;
    if (owner) {
      const org = yield* probeCodeRabbitOrganization({ binaryPath: settings.binaryPath, cwd, env });
      if (org && org.toLowerCase() !== owner.toLowerCase()) {
        return yield* new CloudWorkspaceError({ reason: "org_mismatch", org, owner });
      }
    }
    const readBranch = () => readWorkspaceBranch(cwd, env);
    let branch = yield* readBranch();
    if (isTemporaryWorktreeBranch(branch) && options.branchRenameWaitMs !== 0) {
      // Thread launch names worktrees asynchronously; do not publish the placeholder first.
      yield* readBranch().pipe(
        Effect.repeat({ while: isTemporaryWorktreeBranch, schedule: Schedule.spaced("1 second") }),
        Effect.timeoutOption(options.branchRenameWaitMs ?? 30_000),
      );
      branch = yield* readBranch();
    }
    const publish = Effect.fn("CodeRabbit.publishWorkspaceBranch")(function* () {
      for (let attempt = 0; attempt < 2; attempt++) {
        const head = yield* requiredGit(["rev-parse", "HEAD"]);
        const remoteArgs = ["ls-remote", "--heads", "origin", `refs/heads/${branch}`];
        let remote = yield* requiredGit(remoteArgs);
        let pushed = false;
        if (!remote) {
          if (!settings.autoPushBranch) {
            return yield* new CloudWorkspaceError({ reason: "unpublished", branch });
          }
          const current = yield* readBranch();
          if (current !== branch) {
            branch = current;
            continue;
          }
          // The org check and ls-remote read the fetch URL; never publish somewhere else.
          const fetchUrl = yield* requiredGit(["remote", "get-url", "origin"]);
          // `git push` publishes to every push URL, so each one must match.
          const pushUrls = yield* requiredGit(["remote", "get-url", "--push", "--all", "origin"]);
          if (!pushUrls.split("\n").every((pushUrl) => sameRepository(fetchUrl, pushUrl.trim()))) {
            return yield* new CloudWorkspaceError({ reason: "push_target_mismatch", branch });
          }
          const pushError = (cause?: unknown, timedOut?: boolean) =>
            new CloudWorkspaceError({ reason: "push_failed", branch, cause, timedOut });
          // A late rename must not inherit an upstream pointing at a temporary branch.
          const upstream = isTemporaryWorktreeBranch(branch) ? [] : ["-u"];
          const push = yield* git(
            ["push", ...upstream, "origin", `refs/heads/${branch}:refs/heads/${branch}`],
            60_000,
          ).pipe(
            Effect.mapError((cause) => pushError(cause, Cause.isTimeoutError(cause) || undefined)),
          );
          if (push.code !== 0) {
            // Git output stays in the cause for diagnostics; the message is fixed text.
            return yield* pushError({
              exitCode: push.code,
              stderr: redactProviderFailureText(push.stderr),
            });
          }
          remote = yield* requiredGit(remoteArgs).pipe(
            Effect.mapError(
              (cause) => new CloudWorkspaceError({ reason: "push_unconfirmed", branch, cause }),
            ),
          );
          if (!remote)
            return yield* new CloudWorkspaceError({ reason: "push_unconfirmed", branch });
          pushed = true;
        }
        return { pushed, localDiffers: remote.split(/\s+/u)[0] !== head };
      }
      return yield* new CloudWorkspaceError({ reason: "branch_changed" });
    });
    const published = yield* publish();
    const dirty =
      (yield* requiredGit(["status", "--porcelain", "--untracked-files=normal"])) !== "";
    return { branch, dirty, ...published };
  }).pipe(
    Effect.mapError((cause) =>
      isCloudWorkspaceError(cause) ? cause : new CloudWorkspaceError({ reason: "git", cause }),
    ),
  );
});
