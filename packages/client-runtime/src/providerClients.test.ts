import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { getProviderClient, providerClients } from "./providerClients.ts";

describe("shared provider clients", () => {
  it("makes CodeRabbit available to provider discovery and settings lookup", () => {
    const driver = ProviderDriverKind.make("coderabbit");
    const client = getProviderClient("coderabbit");

    expect(client).toBeDefined();
    expect(client?.driverKind).toBe(driver);
    expect(client?.label).toBe("CodeRabbit");
    expect(providerClients.definitions.filter((entry) => entry.driverKind === driver)).toEqual([
      client,
    ]);
    expect(Object.keys(client!.settingsSchema.fields)).toEqual([
      "enabled",
      "binaryPath",
      "autoPushBranch",
    ]);
  });
});
