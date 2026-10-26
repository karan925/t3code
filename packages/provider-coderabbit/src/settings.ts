import { makeBinaryPathSetting, makeProviderSettingsSchema } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const CodeRabbitSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(false)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    binaryPath: makeBinaryPathSetting("cr").pipe(
      Schema.annotateKey({
        title: "Binary path",
        description:
          "Path to the CodeRabbit CLI on this T3 server host. Run cr auth login there. Tasks use the pushed branch on origin; local files are not uploaded.",
        providerSettingsForm: { placeholder: "cr", clearWhenEmpty: "omit" },
      }),
    ),
    autoPushBranch: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(true)),
      Schema.annotateKey({
        title: "Automatically push new branches",
        description:
          "Push the current branch to origin before starting a CodeRabbit task if it is not published yet. Existing origin branches are left unchanged.",
        providerSettingsForm: { control: "switch" },
      }),
    ),
  },
  { order: ["binaryPath", "autoPushBranch"] },
);
export type CodeRabbitSettings = typeof CodeRabbitSettings.Type;
