import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it, vi } from "@effect/vitest";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as CodeRabbitCli from "./CodeRabbitCli.ts";

const output = (stdout: string, code = 0): ProcessRunner.ProcessRunOutput => ({
  stdout,
  stderr: "",
  code: ChildProcessSpawner.ExitCode(code),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

describe("CodeRabbit CLI parsing", () => {
  it("reads authenticated accounts and prefers username over email", () => {
    assert.deepEqual(
      CodeRabbitCli.parseCodeRabbitAuthStatus(
        '{"authenticated":true,"user":{"username":" rabbit ","email":"rabbit@example.com"}}',
      ),
      { installed: true, authenticated: true, account: "rabbit" },
    );
    assert.deepEqual(
      CodeRabbitCli.parseCodeRabbitAuthStatus(
        '{"authenticated":true,"user":{"username":" ","email":" rabbit@example.com "}}',
      ),
      { installed: true, authenticated: true, account: "rabbit@example.com" },
    );
  });

  it.each([
    '{"authenticated":false,"user":{"username":"rabbit"}}',
    '{"authenticated":"true"}',
    "not JSON",
    "",
  ])("treats an unusable auth answer as signed out: %s", (stdout) => {
    assert.deepEqual(CodeRabbitCli.parseCodeRabbitAuthStatus(stdout), {
      installed: true,
      authenticated: false,
      account: null,
    });
  });

  it.each(['{"authenticated":true}', '{"authenticated":true,"user":null}'])(
    "supports an authenticated CLI without an account label: %s",
    (stdout) => {
      assert.deepEqual(CodeRabbitCli.parseCodeRabbitAuthStatus(stdout), {
        installed: true,
        authenticated: true,
        account: null,
      });
    },
  );

  it("uses the last JSON object while ignoring banners and malformed lines", () => {
    const stdout =
      'CodeRabbit CLI\n{"type":"progress"}\n{banner}\n {"authenticated":true,"user":{"username":"rabbit"}}\nAll done\n';
    assert.deepEqual(CodeRabbitCli.lastJsonLine(stdout), {
      authenticated: true,
      user: { username: "rabbit" },
    });
    assert.isTrue(CodeRabbitCli.parseCodeRabbitAuthStatus(stdout).authenticated);
    assert.deepEqual(
      CodeRabbitCli.lastJsonLine('{"authenticated":true}\n{"authenticated":false}'),
      {
        authenticated: false,
      },
    );
  });

  it.each(["garbage\n{broken", '[]\nnull\n"text"', ""])(
    "returns no answer when there is no JSON object: %s",
    (stdout) => {
      assert.isNull(CodeRabbitCli.lastJsonLine(stdout));
    },
  );
});

describe("CodeRabbitCli.status", () => {
  const harness = (preview = output('{"type":"success"}')) => {
    const probe = vi
      .fn(() => output('{"authenticated":true,"user":{"username":"rabbit"}}'))
      .mockReturnValueOnce(output('{"authenticated":false}'));
    const run: ProcessRunner.ProcessRunner["Service"]["run"] = (input) =>
      Effect.sync(() => {
        if (input.args[0] === "auth") return probe();
        return input.args.includes("--confirm") ? output('{"type":"success"}') : preview;
      });
    const layer = CodeRabbitCli.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          ServerConfig.layerTest("/workspace", { prefix: "t3-coderabbit-cli-" }).pipe(
            Layer.provide(NodeServices.layer),
          ),
          Layer.mock(ProcessRunner.ProcessRunner)({ run }),
          Layer.mock(ProviderRegistry.ProviderRegistry)({ refresh: () => Effect.succeed([]) }),
        ),
      ),
    );
    return { probe, layer };
  };

  it.effect("reuses status for 30 seconds, then reads the CLI again on demand", () => {
    const fixture = harness();
    return Effect.gen(function* () {
      const cli = yield* CodeRabbitCli.CodeRabbitCli;
      assert.equal(fixture.probe.mock.calls.length, 0);
      const initial = yield* cli.status;
      assert.deepEqual(initial, { installed: true, authenticated: false, account: null });
      assert.deepEqual(yield* cli.status, initial);
      yield* TestClock.adjust("29999 millis");
      assert.deepEqual(yield* cli.status, initial);
      assert.equal(fixture.probe.mock.calls.length, 1);
      yield* TestClock.adjust("1 millis");
      assert.equal(fixture.probe.mock.calls.length, 1);
      assert.deepEqual(yield* cli.status, {
        installed: true,
        authenticated: true,
        account: "rabbit",
      });
      assert.equal(fixture.probe.mock.calls.length, 2);
      yield* cli.status;
      assert.equal(fixture.probe.mock.calls.length, 2);
    }).pipe(Effect.provide(fixture.layer));
  });

  it.effect.each([
    ["already installed", output('{"type":"success"}')],
    ["confirmed plan", output('{"type":"action_required","planHash":"plan-123"}', 1)],
  ] as const)("invalidates status after skills installation: %s", ([, preview]) => {
    const fixture = harness(preview);
    return Effect.gen(function* () {
      const cli = yield* CodeRabbitCli.CodeRabbitCli;
      assert.isFalse((yield* cli.status).authenticated);
      yield* cli.installSkills;
      assert.equal(fixture.probe.mock.calls.length, 1);
      assert.deepEqual(yield* cli.status, {
        installed: true,
        authenticated: true,
        account: "rabbit",
      });
      assert.equal(fixture.probe.mock.calls.length, 2);
      yield* cli.status;
      assert.equal(fixture.probe.mock.calls.length, 2);
    }).pipe(Effect.provide(fixture.layer));
  });
});

describe("CodeRabbitCli.installSkills", () => {
  it.effect("confirms the previewed plan and refreshes skills after installation", () => {
    const run = vi.fn<ProcessRunner.ProcessRunner["Service"]["run"]>((input) =>
      Effect.succeed(
        input.args.includes("--confirm")
          ? output('{"type":"success"}')
          : output('Planning\n{"type":"action_required","planHash":"plan-123"}', 1),
      ),
    );
    const refresh = vi.fn(() => Effect.succeed([]));
    const layer = CodeRabbitCli.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          ServerConfig.layerTest("/workspace", { prefix: "t3-coderabbit-cli-" }).pipe(
            Layer.provide(NodeServices.layer),
          ),
          Layer.mock(ProcessRunner.ProcessRunner)({ run }),
          Layer.mock(ProviderRegistry.ProviderRegistry)({ refresh }),
        ),
      ),
    );
    return Effect.gen(function* () {
      const cli = yield* CodeRabbitCli.CodeRabbitCli;
      run.mockClear();
      yield* cli.installSkills;
      assert.deepEqual(
        run.mock.calls.map(([input]) => input.args),
        [
          ["skills", "install", "--agent"],
          ["skills", "install", "--confirm", "plan-123"],
        ],
      );
      assert.equal(refresh.mock.calls.length, 1);
      assert.isTrue(refresh.mock.invocationCallOrder[0]! > run.mock.invocationCallOrder[1]!);
    }).pipe(Effect.provide(layer));
  });

  it.effect("does not refresh providers or confirm a malformed plan", () => {
    const run = vi.fn(() => Effect.succeed(output('{"type":"action_required","planHash":""}')));
    const refresh = vi.fn(() => Effect.succeed([]));
    return Effect.gen(function* () {
      const cli = yield* CodeRabbitCli.CodeRabbitCli;
      run.mockClear();
      const error = yield* cli.installSkills.pipe(Effect.flip);
      assert.equal(error.reason, "plan_failed");
      assert.equal(run.mock.calls.length, 1);
      assert.equal(refresh.mock.calls.length, 0);
    }).pipe(
      Effect.provide(
        CodeRabbitCli.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              ServerConfig.layerTest("/workspace", { prefix: "t3-coderabbit-cli-" }).pipe(
                Layer.provide(NodeServices.layer),
              ),
              Layer.mock(ProcessRunner.ProcessRunner)({ run }),
              Layer.mock(ProviderRegistry.ProviderRegistry)({ refresh }),
            ),
          ),
        ),
      ),
    );
  });
});
