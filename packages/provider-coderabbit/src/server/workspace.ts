import { spawnAndCollect } from "@t3tools/provider-core/server/snapshotProbe";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import type { CodeRabbitSettings } from "../settings.ts";
import { probeCodeRabbitOrganization } from "./status.ts";

export class CloudWorkspaceError extends Schema.TaggedError<CloudWorkspaceError>()(
  "CodeRabbitWorkspaceError",
  {
    reason: Schema.Literals(["git", "unpublished", "push_failed", "org_mismatch"]),
    branch: Schema.optional(Schema.String),
    detail: Schema.optional(Schema.String),
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
      case "git":
        return "Could not verify the CodeRabbit source branch. Check Git and origin access before delegating.";
      case "push_failed":
        return `CodeRabbit could not push \`${this.branch}\` to origin. Push \`${this.branch}\` manually, then send your message again.${this.detail ? ` Git: ${this.detail}` : ""}`;
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

export const verifyCloudWorkspace = Effect.fn("CodeRabbit.verifyWorkspace")(function* (
  cwd: string,
  env: NodeJS.ProcessEnv,
  settings: Pick<CodeRabbitSettings, "binaryPath" | "autoPushBranch">,
) {
  const git = Effect.fn("CodeRabbit.workspaceGit")(function* (
    args: ReadonlyArray<string>,
    timeout = 30_000,
  ) {
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
    ).pipe(Effect.timeout(timeout));
  });
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
    const branchResult = yield* git(["symbolic-ref", "--quiet", "--short", "HEAD"]);
    if (branchResult.code === 1) {
      return yield* new CloudWorkspaceError({ reason: "unpublished" });
    }
    if (branchResult.code !== 0) return yield* new CloudWorkspaceError({ reason: "git" });
    const branch = branchResult.stdout.trim();
    const head = yield* requiredGit(["rev-parse", "HEAD"]);
    const remoteArgs = ["ls-remote", "--heads", "origin", `refs/heads/${branch}`];
    let remote = yield* requiredGit(remoteArgs);
    let pushed = false;
    if (!remote) {
      if (!settings.autoPushBranch) {
        return yield* new CloudWorkspaceError({ reason: "unpublished", branch });
      }
      const pushError = (cause?: unknown, detail?: string) =>
        new CloudWorkspaceError({ reason: "push_failed", branch, cause, detail });
      const push = yield* git(
        ["push", "-u", "origin", `refs/heads/${branch}:refs/heads/${branch}`],
        60_000,
      ).pipe(Effect.mapError((cause) => pushError(cause)));
      if (push.code !== 0) {
        const detail = makeProviderFailure({
          message: push.stderr.trim().slice(-300),
        }).message.slice(0, 300);
        return yield* pushError(undefined, push.stderr.trim() ? detail : undefined);
      }
      remote = yield* requiredGit(remoteArgs).pipe(Effect.mapError((cause) => pushError(cause)));
      if (!remote) return yield* pushError();
      pushed = true;
    }
    const dirty =
      (yield* requiredGit(["status", "--porcelain", "--untracked-files=normal"])) !== "";
    return { branch, dirty, pushed, localDiffers: remote.split(/\s+/u)[0] !== head };
  }).pipe(
    Effect.timeout("90 seconds"),
    Effect.mapError((cause) =>
      isCloudWorkspaceError(cause) ? cause : new CloudWorkspaceError({ reason: "git", cause }),
    ),
  );
});
