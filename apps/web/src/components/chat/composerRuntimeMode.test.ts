import { describe, expect, it } from "vite-plus/test";
import { getComposerRuntimeModeState } from "./composerRuntimeMode";

describe("getComposerRuntimeModeState", () => {
  it.each(["approval-required", "auto-accept-edits", "auto"] as const)(
    "blocks a retained %s draft mode after switching to CodeRabbit",
    (mode) => {
      const previous = getComposerRuntimeModeState(mode, [undefined]);
      expect(previous.sendDisabledReason).toBeNull();
      const switched = getComposerRuntimeModeState(mode, [
        { supportedRuntimeModes: ["full-access"] },
      ]);
      expect(switched.sendDisabledReason).toBe("Select a supported access mode before sending.");
      expect(switched.options.map(({ mode, disabled }) => ({ mode, disabled }))).toEqual([
        { mode, disabled: true },
        { mode: "full-access", disabled: undefined },
      ]);
      expect(switched.options[0]?.label).toContain("(unsupported)");
    },
  );

  it.each([undefined, []])("treats %j supported modes as unrestricted", (supportedModes) => {
    const state = getComposerRuntimeModeState("approval-required", [
      { supportedRuntimeModes: supportedModes },
    ]);
    expect(state.sendDisabledReason).toBeNull();
    expect(state.options.map(({ mode }) => mode)).toEqual([
      "approval-required",
      "auto-accept-edits",
      "auto",
      "full-access",
    ]);
    expect(state.options.some(({ disabled }) => disabled)).toBe(false);
  });

  it("blocks every mode when selected providers have disjoint supported modes", () => {
    for (const mode of ["approval-required", "full-access"] as const) {
      const state = getComposerRuntimeModeState(mode, [
        { supportedRuntimeModes: ["approval-required"] },
        { supportedRuntimeModes: ["full-access"] },
      ]);
      expect(state.sendDisabledReason).toBe(
        "Selected providers have no access mode in common. Select different providers.",
      );
      expect(state.options.map(({ mode, disabled }) => ({ mode, disabled }))).toEqual([
        { mode, disabled: true },
      ]);
    }
  });

  it("keeps a restricted provider's modes when another provider leaves them unspecified", () => {
    const state = getComposerRuntimeModeState("approval-required", [
      { supportedRuntimeModes: [] },
      { supportedRuntimeModes: ["full-access"] },
    ]);
    expect(state.sendDisabledReason).not.toBeNull();
    expect(state.options.filter(({ disabled }) => !disabled).map(({ mode }) => mode)).toEqual([
      "full-access",
    ]);
  });

  it("allows sending once Full access is explicitly selected", () => {
    const state = getComposerRuntimeModeState("full-access", [
      { supportedRuntimeModes: ["full-access"] },
    ]);
    expect(state.sendDisabledReason).toBeNull();
    expect(state.options.map(({ mode }) => mode)).toEqual(["full-access"]);
    expect(state.options[0]?.disabled).not.toBe(true);
  });

  it("restores supported choices when switching back without changing the draft mode", () => {
    const state = getComposerRuntimeModeState("approval-required", [
      { supportedRuntimeModes: ["approval-required", "full-access"] },
    ]);
    expect(state.sendDisabledReason).toBeNull();
    expect(state.options.map(({ label }) => label)).toEqual(["Supervised", "Full access"]);
  });
});
