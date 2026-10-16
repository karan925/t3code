import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";
import { CodeRabbitCloudSettings } from "./settings.ts";

export const codeRabbitCloudClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("coderabbit-cloud"),
  label: "CodeRabbit Cloud",
  settingsSchema: CodeRabbitCloudSettings,
  badgeLabel: "Beta",
});
