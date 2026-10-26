# CodeRabbit

CodeRabbit runs coding tasks in its cloud environment and reports progress and results in your T3
thread. This provider is **Beta** and disabled by default.

## Set up CodeRabbit

Install CodeRabbit CLI 0.9.0 or newer on the machine running the T3 Code server, then run
`cr auth login` there and choose the GitHub organization that owns your repository. If the CLI is
using a different organization, run `cr auth logout && cr auth login` on that machine and choose
the repository's owner. Open **Settings → Providers**, select that environment, and enable
**CodeRabbit**. If needed, set its **Binary path** to the host's `cr` executable. T3 Code uses
that host login; separate accounts need separate T3 environments.

Select CodeRabbit for a thread and choose **Full access**. The cloud chooses the model; follow-up
messages continue the same task. Delegation to CodeRabbit also requires the parent thread to use
**Full access**. T3 Code does not broaden the parent's permissions to start a cloud task.

## Work with cloud tasks

Tasks start from the current branch on `origin`. By default, T3 Code pushes the branch if it does
not exist on `origin` yet. For new worktrees, it waits up to 30 seconds for the branch name before
publishing. Turn off **Automatically push new branches** in CodeRabbit's provider settings to
publish branches manually instead.

Existing origin branches are used as they are, even when your local copy differs. Commit and push
any changes the task needs before sending it. T3 Code reports when it publishes a branch, when the
origin branch differs from your local copy, and when local uncommitted changes were not uploaded.
Dirty files, unpushed commits on existing branches, and local attachments are not uploaded.

Cloud edits stay in the remote task. Use CodeRabbit's `cr code push <task>` command or its web delivery
flow to publish them, then fetch and review the changes locally.

To continue one of your existing tasks, open the provider's **Sessions** section in Settings,
select a project, list your tasks, and import one. Import creates a T3 thread for that task;
it does not copy cloud changes into the local checkout. The first follow-up shows the latest cloud
reply. Earlier history stays in the web app.

If a new task's submission cannot be confirmed, T3 Code blocks another submission in that thread
to avoid creating a duplicate. Check your CodeRabbit task list and import the task to continue.
If no task was created, start a new T3 thread.

CodeRabbit supports **Full access** and the **Default** interaction mode only. T3 Code cannot
enforce a local sandbox or forward interactive tool approvals into its cloud environment.
Text helpers such as thread titles and commit messages require another enabled provider.
