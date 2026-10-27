import type { RuntimeMode, ServerProvider } from "@t3tools/contracts";
import { runtimeModeConfig, runtimeModeOptions } from "./runtimeModeConfig";

export type RuntimeModeOption = {
  mode: RuntimeMode;
  disabled?: boolean;
} & (typeof runtimeModeConfig)[RuntimeMode];

export function getComposerRuntimeModeState(
  runtimeMode: RuntimeMode,
  providers: ReadonlyArray<Pick<ServerProvider, "supportedRuntimeModes"> | undefined>,
): { options: ReadonlyArray<RuntimeModeOption>; sendDisabledReason: string | null } {
  const supportedModes = runtimeModeOptions.filter((mode) =>
    providers.every(
      (provider) =>
        !provider?.supportedRuntimeModes?.length || provider.supportedRuntimeModes.includes(mode),
    ),
  );
  const isSupported = supportedModes.includes(runtimeMode);
  return {
    options: runtimeModeOptions
      .filter((mode) => mode === runtimeMode || supportedModes.includes(mode))
      .map((mode) => ({
        mode,
        ...runtimeModeConfig[mode],
        ...(!supportedModes.includes(mode)
          ? {
              disabled: true,
              label: `${runtimeModeConfig[mode].label} (unsupported)`,
              description:
                "This access mode is not supported by every selected provider. Select a supported mode before sending.",
            }
          : {}),
      })),
    sendDisabledReason:
      supportedModes.length === 0
        ? "Selected providers have no access mode in common. Select different providers."
        : isSupported
          ? null
          : "Select a supported access mode before sending.",
  };
}
