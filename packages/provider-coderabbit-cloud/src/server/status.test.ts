import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import { CodeRabbitCloudSettings } from "../settings.ts";
import { checkCodeRabbitCloudProviderStatus } from "./status.ts";

const settings = Schema.decodeSync(CodeRabbitCloudSettings);
const fakeCli = (auth: unknown, code = 0, version = "0.9.0") =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "coderabbit-status-test-" });
    return writeFakeCli({
      directory,
      name: "cr",
      source: `const args = process.argv.slice(2); if (args.join(' ') === '--version') { console.log(${JSON.stringify(version)}); } else if (args.join(' ') === 'auth status --agent') { console.log(${JSON.stringify(JSON.stringify(auth))}); process.exit(${code}); } else { throw new Error('Unexpected billable command: ' + args.join(' ')); }`,
    });
  });

it.layer(NodeServices.layer)("CodeRabbit Cloud status", (it) => {
  it.effect("does not execute the CLI while disabled", () =>
    Effect.gen(function* () {
      const result = yield* checkCodeRabbitCloudProviderStatus(
        settings({ binaryPath: "/missing/cr" }),
      );
      expect(result.status).toBe("disabled");
      expect(result.models.map((model) => model.slug)).toEqual(["default"]);
      expect(result.supportedRuntimeModes).toEqual(["full-access"]);
      expect(result.supportsConversationRollback).toBe(false);
    }),
  );

  it.effect("reports missing binaries without requiring login", () =>
    Effect.gen(function* () {
      const result = yield* checkCodeRabbitCloudProviderStatus(
        settings({ enabled: true, binaryPath: "/missing/cr" }),
      );
      expect(result.installed).toBe(false);
      expect(result.status).toBe("error");
      expect(result.message).toContain("this T3 server host");
    }),
  );

  it.effect("probes only version and authentication without retaining account data", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* fakeCli({
          type: "status",
          phase: "auth",
          authenticated: true,
          user: { email: "private@example.test" },
        });
        const result = yield* checkCodeRabbitCloudProviderStatus(
          settings({ enabled: true, binaryPath }),
        );
        expect(result.status).toBe("ready");
        expect(result.auth).toEqual({ status: "authenticated" });
        expect(result.version).toBe("0.9.0");
        expect(JSON.stringify(result)).not.toContain("private@example.test");
        expect(result.models.map((model) => model.slug)).toEqual(["default"]);
      }),
    ),
  );

  it.effect("rejects an older CLI before checking authentication", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* fakeCli(
          { type: "status", phase: "auth", authenticated: true },
          0,
          "0.8.0",
        );
        const result = yield* checkCodeRabbitCloudProviderStatus(
          settings({ enabled: true, binaryPath }),
        );
        expect(result.status).toBe("error");
        expect(result.auth.status).toBe("unknown");
        expect(result.message).toContain("0.9.0 or newer");
      }),
    ),
  );

  it.effect("gives login guidance for explicit unauthenticated status", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* fakeCli(
          { type: "status", phase: "auth", authenticated: false },
          1,
        );
        const result = yield* checkCodeRabbitCloudProviderStatus(
          settings({ enabled: true, binaryPath }),
        );
        expect(result.auth.status).toBe("unauthenticated");
        expect(result.status).toBe("warning");
        expect(result.message).toContain("cr auth login");
      }),
    ),
  );

  it.effect("does not turn malformed auth responses or private errors into success", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* fakeCli({ authenticated: true, secret: "private-token" });
        const result = yield* checkCodeRabbitCloudProviderStatus(
          settings({ enabled: true, binaryPath }),
        );
        expect(result.auth.status).toBe("unknown");
        expect(result.status).toBe("warning");
        expect(JSON.stringify(result)).not.toContain("private-token");
      }),
    ),
  );
});
