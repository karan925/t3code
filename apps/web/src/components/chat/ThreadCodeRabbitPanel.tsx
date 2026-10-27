import { useAtomValue } from "@effect/atom-react";
import {
  hasProviderSkill,
  resolveProviderSkillsForCwd,
} from "@t3tools/client-runtime/providerSkills";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  AuthOrchestrationOperateScope,
  CODERABBIT_AUTOFIX_SKILL,
  CODERABBIT_REVIEW_SKILL,
  type EnvironmentId,
  type ThreadId,
} from "@t3tools/contracts";
import {
  resolveThreadCurrentPullRequestLink,
  visibleThreadPullRequests,
} from "@t3tools/client-runtime/thread-pull-requests";
import { DownloadIcon, ScanSearchIcon, WrenchIcon, type LucideIcon } from "lucide-react";
import { useState } from "react";

import { cn } from "~/lib/utils";
import { useThreadShell } from "~/state/entities";
import { useEnvironmentQuery } from "~/state/query";
import { serverEnvironment } from "~/state/server";
import { readEnvironmentScope, useEnvironmentScope } from "~/state/session";
import { threadEnvironment } from "~/state/threads";
import { useAtomCommand } from "~/state/use-atom-command";
import {
  AlertDialog,
  AlertDialogClose,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogPopup,
  AlertDialogTitle,
} from "../ui/alert-dialog";
import { Button } from "../ui/button";
import { Switch } from "../ui/switch";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { ThreadDetailsControl } from "./ThreadDetailsControl";
import { ThreadDetailsSection } from "./ThreadDetailsSection";
import {
  THREAD_DETAILS_PANEL_ICON_CLASS,
  THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS,
} from "./threadDetailsPanelStyles";

function CodeRabbitRow(props: {
  title: string;
  subtitle: string;
  icon: LucideIcon;
  checked: boolean;
  disabled: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <li
      className={cn("flex items-center rounded-lg py-1.5", THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS)}
    >
      <props.icon className={THREAD_DETAILS_PANEL_ICON_CLASS} />
      <div className="min-w-0 flex-1">
        <span className="block truncate text-sm font-medium text-foreground/80">{props.title}</span>
        <p className="truncate text-2xs text-muted-foreground">{props.subtitle}</p>
      </div>
      <Switch
        checked={props.checked}
        disabled={props.disabled}
        aria-label={props.title}
        onCheckedChange={props.onCheckedChange}
      />
    </li>
  );
}

function reportFailure(title: string, error: unknown) {
  toastManager.add(
    stackedThreadToast({
      type: "error",
      title,
      description: error instanceof Error ? error.message : String(error),
    }),
  );
}

export function ThreadCodeRabbitPanel(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly cwd: string | null;
}) {
  const thread = useThreadShell({ environmentId: props.environmentId, threadId: props.threadId });
  const providers = useAtomValue(serverEnvironment.providersValueAtom(props.environmentId));
  const status = useEnvironmentQuery(
    serverEnvironment.codeRabbitStatus({
      environmentId: props.environmentId,
      input: {},
    }),
  );
  const canOperate = useEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope);
  const canInstall = useAtomValue(
    serverEnvironment.installCodeRabbitSkills.permissionAtom(props.environmentId),
  );
  const watchPullRequest = useAtomCommand(threadEnvironment.watchPullRequest);
  const updateMetadata = useAtomCommand(threadEnvironment.updateMetadata);
  const installSkills = useAtomCommand(serverEnvironment.installCodeRabbitSkills);
  const refreshProviders = useAtomCommand(serverEnvironment.refreshProviders);
  const [busy, setBusy] = useState(false);
  const [confirmingInstall, setConfirmingInstall] = useState(false);

  const provider = providers?.find(
    (candidate) => candidate.instanceId === thread?.modelSelection.instanceId,
  );
  const skills = provider ? resolveProviderSkillsForCwd(provider, props.cwd) : [];
  const hasAutofix = hasProviderSkill(skills, CODERABBIT_AUTOFIX_SKILL);
  const hasReview = hasProviderSkill(skills, CODERABBIT_REVIEW_SKILL);
  const pullRequest = resolveThreadCurrentPullRequestLink(thread?.pullRequests ?? []);
  const autofixPullRequests = visibleThreadPullRequests(thread?.pullRequests ?? []).filter(
    (link) => link.watch?.autofix === true,
  );
  const autofixEnabled = autofixPullRequests.length > 0;
  const authenticated = status.data?.authenticated === true;

  if (
    !thread ||
    thread.lineage.relationshipToParent === "subagent" ||
    (!authenticated && !thread.codeRabbitReview && !autofixEnabled)
  )
    return null;

  const autofixWarning =
    thread.archivedAt !== null
      ? "Unarchive this thread to enable Autofix"
      : thread.settledOverride === "settled" || thread.settledAt !== null
        ? "Unsettle this thread to enable Autofix"
        : !authenticated
          ? "Sign in to the CodeRabbit CLI on this machine"
          : !hasAutofix
            ? "Install the CodeRabbit autofix skill for this agent"
            : !pullRequest
              ? "Open a pull request first"
              : null;
  const reviewWarning = !authenticated
    ? "Sign in to the CodeRabbit CLI on this machine"
    : !hasReview
      ? "Install the CodeRabbit code-review skill for this agent"
      : null;

  const toggleAutofix = async (enabled: boolean) => {
    if (
      enabled &&
      (busy ||
        autofixWarning !== null ||
        !readEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope))
    )
      return;
    setBusy(true);
    const targets = enabled && pullRequest ? [pullRequest] : autofixPullRequests;
    for (const target of targets) {
      const result = await watchPullRequest({
        environmentId: props.environmentId,
        input: {
          threadId: props.threadId,
          host: target.host,
          repository: target.repository,
          number: target.number,
          watching: true,
          autofix: enabled,
        },
      });
      if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
        reportFailure("Could not update CodeRabbit Autofix", squashAtomCommandFailure(result));
      }
    }
    setBusy(false);
  };

  const toggleReview = async (enabled: boolean) => {
    if (
      enabled &&
      (busy ||
        reviewWarning !== null ||
        !readEnvironmentScope(props.environmentId, AuthOrchestrationOperateScope))
    )
      return;
    setBusy(true);
    const result = await updateMetadata({
      environmentId: props.environmentId,
      input: { threadId: props.threadId, codeRabbitReview: enabled },
    });
    setBusy(false);
    if (result._tag === "Failure" && !isAtomCommandInterrupted(result)) {
      reportFailure("Could not update CodeRabbit review", squashAtomCommandFailure(result));
    }
  };

  const install = async () => {
    if (busy || !canInstall) return;
    setBusy(true);
    const result = await installSkills({ environmentId: props.environmentId, input: {} });
    if (result._tag === "Failure") {
      setBusy(false);
      if (!isAtomCommandInterrupted(result)) {
        reportFailure("Could not install CodeRabbit skills", squashAtomCommandFailure(result));
      }
      return;
    }
    // Refresh the working directory too: its cached skill list overrides the global list.
    const refreshed = provider
      ? await refreshProviders({
          environmentId: props.environmentId,
          input: {
            instanceId: provider.instanceId,
            ...(props.cwd ? { cwd: props.cwd } : {}),
            fresh: true,
          },
        })
      : null;
    setBusy(false);
    setConfirmingInstall(false);
    if (refreshed?._tag === "Failure" && !isAtomCommandInterrupted(refreshed)) {
      reportFailure("Could not refresh CodeRabbit skills", squashAtomCommandFailure(refreshed));
    }
  };

  return (
    <>
      <ThreadDetailsSection
        headingId="thread-details-coderabbit-heading"
        title="CodeRabbit"
        data-thread-coderabbit-panel
        actions={
          !hasAutofix || !hasReview ? (
            <ThreadDetailsControl
              variant="ghost"
              size="sm"
              part="meta"
              disabled={!canInstall || busy}
              onClick={() => setConfirmingInstall(true)}
            >
              <DownloadIcon className="size-3.5" />
              Install skills
            </ThreadDetailsControl>
          ) : null
        }
      >
        <ul className="m-0 list-none p-0">
          <CodeRabbitRow
            title="Autofix"
            subtitle={
              autofixWarning ??
              (autofixEnabled
                ? `Fixes CodeRabbit reviews on ${autofixPullRequests.map((link) => `#${link.number}`).join(", ")}`
                : `Fixes CodeRabbit reviews on #${pullRequest?.number}`)
            }
            icon={WrenchIcon}
            checked={autofixEnabled}
            disabled={!autofixEnabled && (!canOperate || busy || autofixWarning !== null)}
            onCheckedChange={(enabled) => void toggleAutofix(enabled)}
          />
          <CodeRabbitRow
            title="Run CLI review"
            subtitle={reviewWarning ?? "Reviews each turn's changes with the CodeRabbit CLI"}
            icon={ScanSearchIcon}
            checked={thread.codeRabbitReview === true}
            disabled={!thread.codeRabbitReview && (!canOperate || busy || reviewWarning !== null)}
            onCheckedChange={(enabled) => void toggleReview(enabled)}
          />
        </ul>
      </ThreadDetailsSection>
      <AlertDialog open={confirmingInstall} onOpenChange={setConfirmingInstall}>
        <AlertDialogPopup>
          <AlertDialogHeader>
            <AlertDialogTitle>Install CodeRabbit skills?</AlertDialogTitle>
            <AlertDialogDescription>
              Runs <code>coderabbit skills install</code> on this machine, adding the CodeRabbit
              autofix and code-review skills for the coding agents it detects.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogClose render={<Button variant="outline" size="sm" disabled={busy} />}>
              Cancel
            </AlertDialogClose>
            <Button size="sm" disabled={!canInstall || busy} onClick={() => void install()}>
              {busy ? "Installing…" : "Install skills"}
            </Button>
          </AlertDialogFooter>
        </AlertDialogPopup>
      </AlertDialog>
    </>
  );
}
