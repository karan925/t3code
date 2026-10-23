import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { CodeRabbitSettings } from "../settings.ts";
import { checkCodeRabbitProviderStatus } from "./status.ts";

const settings = Schema.decodeSync(CodeRabbitSettings);
const fakeCli = Effect.fn("CodeRabbitStatusTest.fakeCli")(function* (
  auth: unknown,
  code = 0,
  version = "0.9.0",
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
      if (args.join(' ') === '--version') {
        console.log(${JSON.stringify(version)});
      } else if (args.join(' ') === 'auth status --agent') {
        console.log(${JSON.stringify(JSON.stringify(auth))});
        process.exit(${code});
      } else {
        throw new Error('Unexpected billable command: ' + args.join(' '));
      }
    `,
  });
  return { binaryPath, log, fs };
});

describe("CodeRabbit status", () => {
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
