import { makeBinaryPathSetting, makeProviderSettingsSchema } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const CodeRabbitCloudSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(false)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    binaryPath: makeBinaryPathSetting("cr").pipe(
      Schema.annotateKey({
        title: "Binary path",
        description:
          "Path to the CodeRabbit CLI on this T3 server host. Run cr auth login there. Cloud tasks use the pushed branch on origin; local files are not uploaded.",
        providerSettingsForm: { placeholder: "cr", clearWhenEmpty: "omit" },
      }),
    ),
  },
  { order: ["binaryPath"] },
);
export type CodeRabbitCloudSettings = typeof CodeRabbitCloudSettings.Type;
