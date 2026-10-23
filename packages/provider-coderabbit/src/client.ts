import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";
import { CodeRabbitSettings } from "./settings.ts";

export const codeRabbitClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("coderabbit"),
  label: "CodeRabbit",
  settingsSchema: CodeRabbitSettings,
  badgeLabel: "Beta",
});
