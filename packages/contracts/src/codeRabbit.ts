import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";

/** Skills the CodeRabbit CLI installs for coding agents (`coderabbit skills install`). */
export const CODERABBIT_AUTOFIX_SKILL = "autofix";
export const CODERABBIT_REVIEW_SKILL = "code-review";

/** The CodeRabbit CLI on the environment's host, which runs the reviews and owns the sign-in. */
export const CodeRabbitStatus = Schema.Struct({
  installed: Schema.Boolean,
  authenticated: Schema.Boolean,
  account: Schema.NullOr(TrimmedNonEmptyString),
});
export type CodeRabbitStatus = typeof CodeRabbitStatus.Type;

export class CodeRabbitError extends Schema.TaggedError<CodeRabbitError>()("CodeRabbitError", {
  reason: Schema.Literals(["not_installed", "plan_failed", "install_failed"]),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    switch (this.reason) {
      case "not_installed":
        return "The CodeRabbit CLI is not installed on this machine.";
      case "plan_failed":
        return "CodeRabbit could not plan the skills install.";
      case "install_failed":
        return "CodeRabbit could not install skills.";
    }
  }
}
