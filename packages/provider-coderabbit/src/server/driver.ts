import {
  AcpRegistryOperationError,
  ProviderDriverKind,
  TextGenerationError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import { CodeRabbitSettings } from "../settings.ts";
import { CodeRabbitAdapterV2Driver, type CodeRabbitAdapterV2DriverEnv } from "./adapter.ts";
import { checkCodeRabbitProviderStatus, makePendingCodeRabbitProvider } from "./status.ts";
import { listTasks } from "./tasks.ts";

const DRIVER_KIND = ProviderDriverKind.make("coderabbit");
const decodeSettings = Schema.decodeSync(CodeRabbitSettings);
const unsupportedTextGeneration = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail:
        "CodeRabbit does not support text generation helpers. Choose another provider for this operation.",
    }),
  );

const codeRabbitTextGeneration: ProviderTextGeneration = {
  generateCommitMessage: () => unsupportedTextGeneration("generateCommitMessage"),
  generatePrContent: () => unsupportedTextGeneration("generatePrContent"),
  generateBranchName: () => unsupportedTextGeneration("generateBranchName"),
  generateThreadTitle: () => unsupportedTextGeneration("generateThreadTitle"),
  generateReviewGuide: () => unsupportedTextGeneration("generateReviewGuide"),
};

const unsupportedManagement = (reason: AcpRegistryOperationError["reason"], message: string) =>
  Effect.fail(new AcpRegistryOperationError({ reason, message }));

export type CodeRabbitDriverEnv = CodeRabbitAdapterV2DriverEnv | ProviderHost.ProviderHost;

export const CodeRabbitDriver: ProviderDriver<CodeRabbitSettings, CodeRabbitDriverEnv> = {
  driverKind: DRIVER_KIND,
  // The CLI owns one host-wide login; separate instance labels would not isolate accounts.
  metadata: { displayName: "CodeRabbit", supportsMultipleInstances: false },
  configSchema: CodeRabbitSettings,
  defaultConfig: () => decodeSettings({}),
  create: Effect.fn("CodeRabbitDriver.create")(function* (input) {
    const { instanceId, displayName, accentColor, enabled } = input;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const host = yield* ProviderHost.ProviderHost;
    const environment = yield* mergeProviderInstanceEnvironment(input.environment);
    const config = {
      ...input.config,
      enabled,
      binaryPath: expandHomePath(input.config.binaryPath, yield* HostProcess.HomeDirectory),
    };
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER_KIND,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      driverKind: DRIVER_KIND,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const settings = yield* makeProviderSnapshotSettingsSource(config);
    const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<CodeRabbitSettings>>(
      {
        resolveMaintenance: () =>
          Effect.succeed(
            makeManualOnlyProviderMaintenanceCapabilities({
              provider: DRIVER_KIND,
              packageName: null,
            }),
          ),
        getSettings: settings.getSettings,
        streamSettings: settings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (current) =>
          makePendingCodeRabbitProvider(current.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkCodeRabbitProviderStatus(config, environment, host.paths.cwd).pipe(
          Effect.map(stampIdentity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
      },
    ).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build CodeRabbit snapshot.",
            cause,
          }),
      ),
    );
    const orchestrationAdapter = yield* CodeRabbitAdapterV2Driver.create({ ...input, config }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER_KIND,
            instanceId,
            detail: "Failed to build CodeRabbit orchestration adapter.",
            cause,
          }),
      ),
    );
    return {
      instanceId,
      driverKind: DRIVER_KIND,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      orchestrationAdapter,
      textGeneration: codeRabbitTextGeneration,
      acpSessionManagement: {
        listSessions: ({ cwd }) =>
          listTasks({ binaryPath: config.binaryPath, cwd, env: environment }).pipe(
            Effect.map((tasks) => ({
              sessions: tasks.map((task) => ({
                sessionId: task.taskId,
                cwd,
                additionalDirectories: [],
                title: task.title ?? task.taskId,
                updatedAt: task.updatedAt ?? null,
                importedThreadId: null,
              })),
              nextCursor: null,
              canLoad: true,
              canResume: true,
              canDelete: false,
            })),
            Effect.mapError(
              (cause) =>
                new AcpRegistryOperationError({
                  reason: "probe_failed",
                  message: cause.status ? `${cause.status}: ${cause.message}` : cause.message,
                  cause,
                }),
            ),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          ),
        logout: () =>
          unsupportedManagement(
            "logout_unsupported",
            "Run `cr auth logout` on this T3 server host to sign out of CodeRabbit.",
          ),
        deleteSession: () =>
          unsupportedManagement(
            "session_delete_unsupported",
            "CodeRabbit does not support deleting tasks through T3 Code.",
          ),
        listProviders: () =>
          unsupportedManagement(
            "providers_unsupported",
            "CodeRabbit manages its own runtime and model.",
          ),
        setProvider: () =>
          unsupportedManagement(
            "providers_unsupported",
            "CodeRabbit manages its own runtime and model.",
          ),
        disableProvider: () =>
          unsupportedManagement(
            "providers_unsupported",
            "CodeRabbit manages its own runtime and model.",
          ),
      },
    } satisfies ProviderInstance;
  }),
};
