import { useState } from "react";
import type { RuntimeMode } from "@t3tools/contracts";
import { expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { ComposerFooterModeControls } from "./ComposerFooterControls";
import { getComposerRuntimeModeState } from "./composerRuntimeMode";
import { CompactComposerControlsMenu } from "./CompactComposerControlsMenu";
import { runtimeModeConfig } from "./runtimeModeConfig";

vi.mock("./ComposerPrimaryActions", () => ({ ComposerPrimaryActions: () => null }));
vi.mock("./ProviderModelPicker", () => ({ ProviderModelPicker: () => null }));
vi.mock("./ModelPickerContent", () => ({ resolveModelPickerSelectedModel: () => undefined }));

function ModeControls({
  initialMode,
  onChange,
  compact = false,
}: {
  initialMode: RuntimeMode;
  onChange: (mode: RuntimeMode) => void;
  compact?: boolean;
}) {
  const [runtimeMode, setRuntimeMode] = useState(initialMode);
  const [codeRabbit, setCodeRabbit] = useState(false);
  const [submittedMode, setSubmittedMode] = useState<RuntimeMode | null>(null);
  const { options, sendDisabledReason } = getComposerRuntimeModeState(runtimeMode, [
    codeRabbit ? { supportedRuntimeModes: ["full-access"] } : undefined,
  ]);
  const Controls = compact ? CompactComposerControlsMenu : ComposerFooterModeControls;
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (!sendDisabledReason) setSubmittedMode(runtimeMode);
      }}
    >
      <button type="button" onClick={() => setCodeRabbit(true)}>
        Select CodeRabbit
      </button>
      <Controls
        showInteractionModeToggle={false}
        interactionMode="default"
        runtimeMode={runtimeMode}
        runtimeModeOptions={options}
        onToggleInteractionMode={() => undefined}
        onRuntimeModeChange={(mode) => {
          setRuntimeMode(mode);
          onChange(mode);
        }}
      />
      <button type="submit" disabled={sendDisabledReason !== null}>
        Send
      </button>
      <output aria-label="Submitted mode">{submittedMode ?? "Not sent"}</output>
    </form>
  );
}

it.each(["approval-required", "auto"] as const)(
  "shows the actual unsupported %s mode until Full access is explicitly selected",
  async (initialMode) => {
    const onChange = vi.fn();
    await using screen = await render(
      <ModeControls initialMode={initialMode} onChange={onChange} />,
    );
    const modePicker = screen.getByRole("combobox", { name: "Runtime mode" });
    await expect.element(modePicker).toHaveTextContent(runtimeModeConfig[initialMode].label);
    await expect.element(screen.getByRole("button", { name: "Send", exact: true })).toBeEnabled();
    await screen.getByRole("button", { name: "Select CodeRabbit" }).click();
    await expect
      .element(modePicker)
      .toHaveTextContent(`${runtimeModeConfig[initialMode].label} (unsupported)`);
    await expect.element(screen.getByRole("button", { name: "Send", exact: true })).toBeDisabled();
    await expect.element(screen.getByLabelText("Submitted mode")).toHaveTextContent("Not sent");
    expect(onChange).not.toHaveBeenCalled();

    await modePicker.click();
    await expect
      .element(page.getByRole("option", { name: /unsupported/ }))
      .toHaveAttribute("aria-disabled", "true");
    await page.getByRole("option", { name: /Full access/ }).click();
    await expect.element(modePicker).toHaveTextContent("Full access");
    expect(onChange).toHaveBeenCalledExactlyOnceWith("full-access");
    await screen.getByRole("button", { name: "Send", exact: true }).click();
    await expect.element(screen.getByLabelText("Submitted mode")).toHaveTextContent("full-access");
  },
);

it("requires an explicit compatible selection in the compact access menu", async () => {
  const onChange = vi.fn();
  await using screen = await render(
    <ModeControls initialMode="approval-required" onChange={onChange} compact />,
  );
  await screen.getByRole("button", { name: "Select CodeRabbit" }).click();
  await expect.element(screen.getByRole("button", { name: "Send", exact: true })).toBeDisabled();
  expect(onChange).not.toHaveBeenCalled();
  await screen.getByRole("button", { name: "More composer controls" }).click();
  await expect
    .element(page.getByRole("menuitemradio", { name: "Supervised (unsupported)" }))
    .toHaveAttribute("aria-disabled", "true");
  await page.getByRole("menuitemradio", { name: "Full access" }).click();
  await userEvent.keyboard("{Escape}");
  await screen.getByRole("button", { name: "Send", exact: true }).click();
  await expect.element(screen.getByLabelText("Submitted mode")).toHaveTextContent("full-access");
  expect(onChange).toHaveBeenCalledExactlyOnceWith("full-access");
});
