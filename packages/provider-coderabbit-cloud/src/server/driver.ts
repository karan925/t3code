import { ProviderDriverKind, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
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
import { CodeRabbitCloudSettings } from "../settings.ts";
import {
  CodeRabbitCloudAdapterV2Driver,
  type CodeRabbitCloudAdapterV2DriverEnv,
} from "./adapter.ts";
import {
  checkCodeRabbitCloudProviderStatus,
  makePendingCodeRabbitCloudProvider,
} from "./status.ts";

const DRIVER_KIND = ProviderDriverKind.make("coderabbit-cloud");
const decodeSettings = Schema.decodeSync(CodeRabbitCloudSettings);
const unsupportedTextGeneration = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail:
        "CodeRabbit Cloud does not support text generation helpers. Choose another provider for this operation.",
    }),
  );

const codeRabbitCloudTextGeneration: ProviderTextGeneration = {
  generateCommitMessage: () => unsupportedTextGeneration("generateCommitMessage"),
  generatePrContent: () => unsupportedTextGeneration("generatePrContent"),
  generateBranchName: () => unsupportedTextGeneration("generateBranchName"),
  generateThreadTitle: () => unsupportedTextGeneration("generateThreadTitle"),
  generateReviewGuide: () => unsupportedTextGeneration("generateReviewGuide"),
};

export type CodeRabbitCloudDriverEnv =
  | CodeRabbitCloudAdapterV2DriverEnv
  | ProviderHost.ProviderHost;

export const CodeRabbitCloudDriver: ProviderDriver<
  CodeRabbitCloudSettings,
  CodeRabbitCloudDriverEnv
> = {
  driverKind: DRIVER_KIND,
  // The CLI owns one host-wide login; separate instance labels would not isolate accounts.
  metadata: { displayName: "CodeRabbit Cloud", supportsMultipleInstances: false },
  configSchema: CodeRabbitCloudSettings,
  defaultConfig: () => decodeSettings({}),
  create: (input) =>
    Effect.gen(function* () {
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
      const snapshot = yield* makeManagedServerProvider<
        ProviderSnapshotSettings<CodeRabbitCloudSettings>
      >({
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
          makePendingCodeRabbitCloudProvider(current.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkCodeRabbitCloudProviderStatus(config, environment, host.paths.cwd).pipe(
          Effect.map(stampIdentity),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build CodeRabbit Cloud snapshot.",
              cause,
            }),
        ),
      );
      const orchestrationAdapter = yield* CodeRabbitCloudAdapterV2Driver.create({
        ...input,
        config,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build CodeRabbit Cloud orchestration adapter.",
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
        textGeneration: codeRabbitCloudTextGeneration,
      } satisfies ProviderInstance;
    }),
};
