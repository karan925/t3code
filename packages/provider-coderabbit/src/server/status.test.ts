import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { CodeRabbitSettings } from "../settings.ts";
import { checkCodeRabbitProviderStatus, probeCodeRabbitOrganization } from "./status.ts";

const settings = Schema.decodeSync(CodeRabbitSettings);
const fakeCli = Effect.fn("CodeRabbitStatusTest.fakeCli")(function* (
  auth: unknown,
  code = 0,
  version = "0.9.0",
  output: {
    readonly authPadding?: number;
    readonly versionPadding?: number;
    readonly stderrBytes?: number;
  } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-status-test-" });
  const log = path.join(directory, "commands.jsonl");
  const binaryPath = writeFakeCli({
    directory,
    name: "cr",
    source: `
      import { appendFileSync } from 'node:fs';
      const args = process.argv.slice(2);
      appendFileSync(${JSON.stringify(log)}, JSON.stringify(args) + '\\n');
      process.stderr.write('x'.repeat(${output.stderrBytes ?? 0}));
      if (args.join(' ') === '--version') {
        process.stdout.write(${JSON.stringify(version)} + ' '.repeat(${output.versionPadding ?? 0}));
      } else if (args.join(' ') === 'auth status --agent') {
        process.stdout.write(${JSON.stringify(JSON.stringify(auth))} + ' '.repeat(${output.authPadding ?? 0}));
        process.exitCode = ${code};
      } else {
        throw new Error('Unexpected billable command: ' + args.join(' '));
      }
    `,
  });
  return { binaryPath, log, fs };
});

describe("CodeRabbit status", () => {
  it("enables automatic branch publishing by default and preserves an explicit opt-out", () => {
    expect(settings({}).autoPushBranch).toBe(true);
    expect(settings({ autoPushBranch: false }).autoPushBranch).toBe(false);
  });

  it.live("skips the organization probe when the CLI cannot be executed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped();
        expect(
          yield* probeCodeRabbitOrganization({
            cwd,
            binaryPath: "/missing/cr",
            env: process.env,
          }),
        ).toBeUndefined();
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("does not execute the CLI while disabled", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli({ type: "status", phase: "auth", authenticated: true });
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ binaryPath: cli.binaryPath }),
        );
        expect(yield* cli.fs.exists(cli.log)).toBe(false);
        expect(result.status).toBe("disabled");
        expect(result.models.map((model) => [model.slug, model.name])).toEqual([
          ["default", "CodeRabbit"],
        ]);
        expect(result.supportedRuntimeModes).toEqual(["full-access"]);
        expect(result.supportsConversationRollback).toBe(false);
        expect(result.supportsTextGeneration).toBe(false);
        expect(result.nativeSessions).toEqual({
          canList: false,
          canLoad: false,
          canResume: false,
          canDelete: false,
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("reports missing binaries without requiring login", () =>
    Effect.gen(function* () {
      const result = yield* checkCodeRabbitProviderStatus(
        settings({ enabled: true, binaryPath: "/missing/cr" }),
      );
      expect(result.installed).toBe(false);
      expect(result.status).toBe("error");
      expect(result.nativeSessions.canList).toBe(false);
      expect(result.message).toContain("this T3 server host");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("probes only version and authentication without retaining account data", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli({
          type: "status",
          phase: "auth",
          authenticated: true,
          user: { email: "private@example.test" },
        });
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ enabled: true, binaryPath: cli.binaryPath }),
        );
        expect(yield* cli.fs.readFileString(cli.log)).toBe(
          '["--version"]\n["auth","status","--agent"]\n',
        );
        expect(result.status).toBe("ready");
        expect(result.auth).toEqual({ status: "authenticated" });
        expect(result.version).toBe("0.9.0");
        expect(result.supportsTextGeneration).toBe(false);
        expect(result.nativeSessions).toEqual({
          canList: true,
          canLoad: true,
          canResume: true,
          canDelete: false,
        });
        expect(JSON.stringify(result)).not.toContain("private@example.test");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("rejects an older CLI before checking authentication", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli(
          { type: "status", phase: "auth", authenticated: true },
          0,
          "0.8.0",
        );
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ enabled: true, binaryPath: cli.binaryPath }),
        );
        expect(yield* cli.fs.readFileString(cli.log)).toBe('["--version"]\n');
        expect(result.status).toBe("error");
        expect(result.auth.status).toBe("unknown");
        expect(result.nativeSessions.canResume).toBe(false);
        expect(result.message).toContain("0.9.0 or newer");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("gives login guidance for explicit unauthenticated status", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli({ type: "status", phase: "auth", authenticated: false }, 1);
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ enabled: true, binaryPath: cli.binaryPath }),
        );
        expect(result.auth.status).toBe("unauthenticated");
        expect(result.status).toBe("warning");
        expect(result.nativeSessions.canLoad).toBe(false);
        expect(result.message).toContain("cr auth login");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("does not trust authenticated output from a failed auth command", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli({ type: "status", phase: "auth", authenticated: true }, 1);
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ enabled: true, binaryPath: cli.binaryPath }),
        );
        expect(result.status).toBe("warning");
        expect(result.nativeSessions.canList).toBe(false);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("does not turn malformed auth responses or private errors into success", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli({ authenticated: true, secret: "private-token" });
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ enabled: true, binaryPath: cli.binaryPath }),
        );
        expect(result.auth.status).toBe("unknown");
        expect(result.status).toBe("warning");
        expect(result.nativeSessions.canList).toBe(false);
        expect(JSON.stringify(result)).not.toContain("private-token");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});

it.live("drains oversized stderr without preventing successful status probes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cli = yield* fakeCli(
        { type: "status", phase: "auth", authenticated: true },
        0,
        "0.9.0",
        { stderrBytes: 2_000_000 },
      );
      const result = yield* checkCodeRabbitProviderStatus(
        settings({ enabled: true, binaryPath: cli.binaryPath }),
      );
      expect(result.status).toBe("ready");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["version", "auth"] as const)(
  "rejects truncated %s output even with a valid prefix",
  (probe) =>
    Effect.scoped(
      Effect.gen(function* () {
        const cli = yield* fakeCli(
          { type: "status", phase: "auth", authenticated: true },
          0,
          "0.9.0",
          probe === "version" ? { versionPadding: 1_048_576 } : { authPadding: 1_048_576 },
        );
        const result = yield* checkCodeRabbitProviderStatus(
          settings({ enabled: true, binaryPath: cli.binaryPath }),
        );
        expect(result.status).toBe(probe === "version" ? "error" : "warning");
        expect(result.auth.status).toBe("unknown");
        expect(result.nativeSessions.canList).toBe(false);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("does not trust an organization from truncated structured output", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cli = yield* fakeCli(
        {
          type: "status",
          phase: "auth",
          authenticated: true,
          provider: "github",
          currentOrg: { name: "example" },
        },
        0,
        "0.9.0",
        { authPadding: 1_048_576 },
      );
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped();
      expect(
        yield* probeCodeRabbitOrganization({ binaryPath: cli.binaryPath, cwd, env: process.env }),
      ).toBeUndefined();
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["version", "auth", "organization"] as const)(
  "times out and releases a blocked %s probe",
  (probe) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          assert(command._tag === "StandardCommand");
          expect(command.options.forceKillAfter).toBe("1 second");
          const blocked = probe === "version" || command.args[0] === "auth";
          if (blocked) {
            yield* Effect.addFinalizer(() => Deferred.succeed(released, undefined));
          }
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(123),
            exitCode: blocked
              ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.succeed(ChildProcessSpawner.ExitCode(0)),
            isRunning: Effect.succeed(blocked),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: blocked ? Stream.never : Stream.encodeText(Stream.make("0.9.0")),
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          });
        }),
      );
      const check = Effect.gen(function* () {
        if (probe === "organization") {
          const result = yield* probeCodeRabbitOrganization({
            binaryPath: "/fake/cr",
            cwd: "/fake",
            env: {},
          });
          expect(result).toBeUndefined();
        } else {
          const result = yield* checkCodeRabbitProviderStatus(
            settings({ enabled: true, binaryPath: "/fake/cr" }),
            {},
            "/fake",
          );
          expect(result.status).toBe(probe === "version" ? "error" : "warning");
          expect(result.auth.status).toBe("unknown");
        }
      });
      const fiber = yield* check.pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(HostProcess.Platform, "linux"),
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust(probe === "version" ? "4 seconds" : "10 seconds");
      yield* Fiber.join(fiber);
      yield* Deferred.await(released);
    }),
);
