import {
  getPromptControlledTraits,
  getTraitSelectChange,
} from "@t3tools/client-runtime/model-traits";
import type { ProviderOptionDescriptor } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { runtimeModeStateForSupportedModes, selectableChoices } from "./thread-settings-options";

const effortDescriptor: Extract<ProviderOptionDescriptor, { type: "select" }> = {
  id: "effort",
  label: "Reasoning",
  type: "select",
  options: [
    { id: "low", label: "Low" },
    { id: "medium", label: "Medium", isDefault: true },
    { id: "high", label: "High" },
    { id: "ultrathink", label: "Ultrathink" },
    { id: "ultracode", label: "Ultracode" },
  ],
  currentValue: "high",
  promptInjectedValues: ["ultrathink"],
};

describe("selectableChoices", () => {
  // regression: #21406 bring model controls in line with web
  it("includes prompt-injected and workflow choices in declared order", () => {
    expect(selectableChoices(effortDescriptor).map((choice) => choice.id)).toEqual([
      "low",
      "medium",
      "high",
      "ultrathink",
      "ultracode",
    ]);
  });
});

describe("prompt-controlled reasoning", () => {
  it("adds Ultrathink to the prompt without persisting an injected option", () => {
    const descriptors = [effortDescriptor];
    const change = getTraitSelectChange(
      { ...getPromptControlledTraits(descriptors, "solve it"), descriptors },
      effortDescriptor,
      "solve it",
      "ultrathink",
    );
    expect(change).toEqual({ prompt: "Ultrathink:\nsolve it" });
    expect(
      getPromptControlledTraits(descriptors, change?.prompt ?? "").ultrathinkPromptControlled,
    ).toBe(true);
    expect(
      getTraitSelectChange(
        { ...getPromptControlledTraits(descriptors, ""), descriptors },
        effortDescriptor,
        "",
        "ultrathink",
      ),
    ).toEqual({ prompt: "Ultrathink:\n" });
  });

  it("removes the prefix when choosing a regular effort and saves Ultracode as an option", () => {
    const descriptors = [effortDescriptor];
    const prompt = "Ultrathink:\nsolve it";
    expect(
      getTraitSelectChange(
        { ...getPromptControlledTraits(descriptors, prompt), descriptors },
        effortDescriptor,
        prompt,
        "ultracode",
      ),
    ).toEqual({ prompt: "solve it", modelOptions: [{ id: "effort", value: "ultracode" }] });
  });

  it("keeps body-controlled Ultrathink locked and preserves slash commands", () => {
    const descriptors = [effortDescriptor];
    const prompt = "please ultrathink about this";
    const selected = { ...getPromptControlledTraits(descriptors, prompt), descriptors };
    expect(selected.ultrathinkInBodyText).toBe(true);
    expect(getTraitSelectChange(selected, effortDescriptor, prompt, "low")).toBeNull();
    expect(
      getTraitSelectChange(
        { ...getPromptControlledTraits(descriptors, "/review"), descriptors },
        effortDescriptor,
        "/review",
        "ultrathink",
      ),
    ).toEqual({ prompt: "/review" });
  });
});

describe("runtimeModeStateForSupportedModes", () => {
  it("keeps retained Supervised visible and requires an explicit Full access selection", () => {
    const retained = runtimeModeStateForSupportedModes("approval-required", ["full-access"]);
    expect(retained.label).toBe("Supervised (unsupported)");
    expect(retained.sendBlockedReason).toBe(
      "Select a supported runtime mode in thread settings before sending.",
    );
    expect(retained.choices.map((choice) => choice.mode)).toEqual(["full-access"]);
    expect(retained.choices.some((choice) => choice.mode === "approval-required")).toBe(false);

    const selected = runtimeModeStateForSupportedModes("full-access", ["full-access"]);
    expect(selected.label).toBe("Full access");
    expect(selected.sendBlockedReason).toBeNull();
  });

  it("blocks every unsupported retained mode without choosing a replacement", () => {
    for (const mode of ["approval-required", "auto-accept-edits", "auto"] as const) {
      const state = runtimeModeStateForSupportedModes(mode, ["full-access"]);
      expect(state.label).toContain("(unsupported)");
      expect(state.sendBlockedReason).not.toBeNull();
    }
  });

  it("keeps all modes available when providers do not declare a restriction", () => {
    for (const supportedModes of [undefined, []]) {
      const state = runtimeModeStateForSupportedModes("approval-required", supportedModes);
      expect(state.label).toBe("Supervised");
      expect(state.choices).toHaveLength(4);
      expect(state.sendBlockedReason).toBeNull();
    }
  });

  it("allows a supported retained mode for providers with multiple choices", () => {
    const state = runtimeModeStateForSupportedModes("auto-accept-edits", [
      "approval-required",
      "auto-accept-edits",
    ]);
    expect(state.label).toBe("Auto-accept edits");
    expect(state.sendBlockedReason).toBeNull();
    expect(state.choices.map((choice) => choice.mode)).toEqual([
      "approval-required",
      "auto-accept-edits",
    ]);
  });
});
