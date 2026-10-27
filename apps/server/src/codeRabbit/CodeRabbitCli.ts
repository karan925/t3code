import {
  CodeRabbitError,
  type CodeRabbitStatus,
  defaultInstanceIdForDriver,
  ProviderDriverKind,
} from "@t3tools/contracts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { CodeRabbitSettings } from "@t3tools/provider-coderabbit/settings";
import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";

const DRIVER = ProviderDriverKind.make("coderabbit");
const INSTANCE_ID = defaultInstanceIdForDriver(DRIVER);
const decodeSettings = Schema.decodeUnknownEffect(CodeRabbitSettings);
const SIGNED_OUT: CodeRabbitStatus = { installed: false, authenticated: false, account: null };
const decodeJsonLine = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodeAuthStatus = Schema.decodeUnknownOption(
  Schema.Struct({
    authenticated: Schema.Boolean,
    user: Schema.optional(
      Schema.NullOr(
        Schema.Struct({
          username: Schema.optional(Schema.Unknown),
          email: Schema.optional(Schema.Unknown),
        }),
      ),
    ),
  }),
);

// `--agent` prints NDJSON; the last JSON line is the command's answer.
export function lastJsonLine(stdout: string): Record<string, unknown> | null {
  for (const line of stdout.split("\n").toReversed()) {
    const value = decodeJsonLine(line.trim());
    if (Option.isSome(value)) return value.value;
  }
  return null;
}

export function parseCodeRabbitAuthStatus(stdout: string): CodeRabbitStatus {
  const answer = Option.getOrNull(decodeAuthStatus(lastJsonLine(stdout)));
  if (answer?.authenticated !== true) return { ...SIGNED_OUT, installed: true };
  const account = [answer.user?.username, answer.user?.email]
    .filter((value): value is string => typeof value === "string")
    .map((value) => value.trim())
    .find((value) => value.length > 0);
  return { installed: true, authenticated: true, account: account ?? null };
}

export class CodeRabbitCli extends Context.Service<
  CodeRabbitCli,
  {
    readonly status: Effect.Effect<CodeRabbitStatus>;
    readonly installSkills: Effect.Effect<void, CodeRabbitError>;
  }
>()("t3/codeRabbit/CodeRabbitCli") {}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const processes = yield* ProcessRunner.ProcessRunner;
  const providers = yield* ProviderRegistry.ProviderRegistry;
  const settings = yield* ServerSettings.ServerSettingsService;
  const homeDirectory = yield* HostProcess.HomeDirectory;

  const run = Effect.fn("CodeRabbitCli.run")(function* (
    args: ReadonlyArray<string>,
    timeout: Duration.Input,
  ) {
    const current = yield* settings.getSettings;
    const entry = current.providerInstances[INSTANCE_ID];
    // Autofix uses this host-wide CLI even when CodeRabbit is disabled as a coding provider.
    const configured = yield* decodeSettings(entry?.driver === DRIVER ? (entry.config ?? {}) : {});
    return yield* processes.run({
      command: expandHomePath(configured.binaryPath, homeDirectory),
      args,
      cwd: config.cwd,
      timeout,
      maxOutputBytes: 256_000,
      outputMode: "truncate",
    });
  });

  const [status, invalidateStatus] = yield* Effect.cachedInvalidateWithTTL(
    run(["auth", "status", "--agent"], "10 seconds").pipe(
      Effect.map((output) => parseCodeRabbitAuthStatus(output.stdout)),
      // A missing executable fails to spawn; anything else means it is there but unusable.
      Effect.catchTags({ ProcessSpawnError: () => Effect.succeed(SIGNED_OUT) }),
      Effect.orElseSucceed(() => ({ ...SIGNED_OUT, installed: true })),
    ),
    "30 seconds",
  );

  const installSkills = Effect.gen(function* () {
    const preview = yield* run(["skills", "install", "--agent"], "60 seconds").pipe(
      Effect.mapError(
        (cause) =>
          new CodeRabbitError({
            reason: cause._tag === "ProcessSpawnError" ? "not_installed" : "plan_failed",
            cause,
          }),
      ),
    );
    const plan = lastJsonLine(preview.stdout);
    // The CLI previews a plan and applies only that exact plan by its hash.
    if (
      plan?.type === "action_required" &&
      typeof plan.planHash === "string" &&
      plan.planHash.trim().length > 0
    ) {
      const applied = yield* run(
        ["skills", "install", "--confirm", plan.planHash],
        "120 seconds",
      ).pipe(Effect.mapError((cause) => new CodeRabbitError({ reason: "install_failed", cause })));
      if (applied.code !== 0) {
        return yield* new CodeRabbitError({ reason: "install_failed", cause: applied });
      }
    } else if (preview.code !== 0 || plan?.type === "action_required") {
      return yield* new CodeRabbitError({ reason: "plan_failed", cause: preview });
    }
    yield* invalidateStatus;
    // Refresh so the new skills reach each thread's skill list right away.
    yield* providers.refresh();
  });

  return CodeRabbitCli.of({ status, installSkills });
});

export const layer = Layer.effect(CodeRabbitCli, make);
