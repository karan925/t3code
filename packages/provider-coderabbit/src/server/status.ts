import type { ServerProviderModel } from "@t3tools/contracts";
import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  isCommandMissingCause,
  parseGenericCliVersion,
  spawnAndCollect,
  type ProviderProbeResult,
} from "@t3tools/provider-core/server/snapshotProbe";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as ChildProcess from "effect/process/ChildProcess";
import type { CodeRabbitSettings } from "../settings.ts";

const PRESENTATION = {
  displayName: "CodeRabbit",
  badgeLabel: "Beta",
  showInteractionModeToggle: false,
  reportsContextWindow: false,
  supportedRuntimeModes: ["full-access"],
  supportsConversationRollback: false,
} as const;

// CodeRabbit selects its runtime and model; this sentinel is never sent as a model ID.
const MODELS: ReadonlyArray<ServerProviderModel> = [
  { slug: "default", name: "CodeRabbit", isCustom: false, capabilities: null },
];

const AuthStatus = Schema.Struct({
  type: Schema.Literal("status"),
  phase: Schema.Literal("auth"),
  authenticated: Schema.Boolean,
});
const decodeAuthStatus = Schema.decodeUnknownOption(Schema.fromJsonString(AuthStatus));

const snapshotForProbe = (input: {
  readonly enabled: boolean;
  readonly checkedAt: string;
  readonly probe: ProviderProbeResult;
}) => ({
  ...buildServerProvider({ presentation: PRESENTATION, models: MODELS, ...input }),
  supportsTextGeneration: false,
  nativeSessions: {
    canList: input.enabled && input.probe.auth.status === "authenticated",
    canLoad: input.enabled && input.probe.auth.status === "authenticated",
    canResume: input.enabled && input.probe.auth.status === "authenticated",
    canDelete: false,
  },
});

export const makePendingCodeRabbitProvider = Effect.fn("makePendingCodeRabbitProvider")(function* (
  settings: CodeRabbitSettings,
) {
  return snapshotForProbe({
    enabled: settings.enabled,
    checkedAt: DateTime.formatIso(yield* DateTime.now),
    probe: {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: settings.enabled
        ? "Checking CodeRabbit CLI availability..."
        : "CodeRabbit is disabled in T3 Code settings.",
    },
  });
});

export const checkCodeRabbitProviderStatus = Effect.fn("checkCodeRabbitProviderStatus")(function* (
  settings: CodeRabbitSettings,
  environment?: NodeJS.ProcessEnv,
  cwd?: string,
) {
  if (!settings.enabled) return yield* makePendingCodeRabbitProvider(settings);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const snapshot = (probe: ProviderProbeResult) =>
    snapshotForProbe({ enabled: true, checkedAt, probe });
  const run = Effect.fn("CodeRabbit.statusProbe")(function* (args: ReadonlyArray<string>) {
    const spawn = yield* resolveSpawnCommand(
      settings.binaryPath,
      args,
      environment ? { env: environment } : undefined,
    );
    return yield* spawnAndCollect(
      settings.binaryPath,
      ChildProcess.make(spawn.command, spawn.args, {
        env: environment,
        shell: spawn.shell,
        stdin: "ignore",
        ...(cwd ? { cwd } : {}),
      }),
    );
  });
  const versionResult = yield* run(["--version"]).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(versionResult)) {
    const missing = isCommandMissingCause(versionResult.failure);
    return snapshot({
      installed: !missing,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: missing
        ? "CodeRabbit CLI (`cr`) was not found. Install it and run `cr auth login` on this T3 server host."
        : "Failed to execute CodeRabbit CLI. Check its binary path on this T3 server host.",
    });
  }
  if (Option.isNone(versionResult.success)) {
    return snapshot({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: "CodeRabbit CLI version check timed out.",
    });
  }
  const output = versionResult.success.value;
  const version = parseGenericCliVersion(output.stdout);
  if (output.code !== 0 || version === null) {
    return snapshot({
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message:
        "CodeRabbit CLI did not return a usable version. Check its installation on this T3 server host.",
    });
  }
  if (compareSemverVersions(version, "0.9.0") < 0) {
    return snapshot({
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message: "CodeRabbit requires CLI 0.9.0 or newer. Update it on this T3 server host.",
    });
  }
  const authResult = yield* run(["auth", "status", "--agent"]).pipe(
    Effect.timeoutOption(AUTH_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(authResult) || Option.isNone(authResult.success)) {
    return snapshot({
      installed: true,
      version,
      status: "warning",
      auth: { status: "unknown" },
      message:
        "Could not check CodeRabbit authentication. Run `cr auth status` on this T3 server host.",
    });
  }
  const auth = decodeAuthStatus(authResult.success.value.stdout.trim());
  if (Option.isNone(auth)) {
    return snapshot({
      installed: true,
      version,
      status: "warning",
      auth: { status: "unknown" },
      message:
        "CodeRabbit CLI returned an unsupported authentication response. Update the CLI on this T3 server host.",
    });
  }
  const authenticated = auth.value.authenticated && authResult.success.value.code === 0;
  return snapshot({
    installed: true,
    version,
    status: authenticated ? "ready" : "warning",
    auth: { status: authenticated ? "authenticated" : "unauthenticated" },
    ...(!authenticated
      ? { message: "Run `cr auth login` on this T3 server host to use CodeRabbit." }
      : {}),
  });
});
