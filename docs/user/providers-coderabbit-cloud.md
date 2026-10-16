# CodeRabbit Cloud

CodeRabbit Cloud runs coding tasks remotely and reports progress and results in your T3 thread.
Install the CodeRabbit CLI 0.9.0 or newer on the T3 server host and run `cr auth login` there.
Enable **CodeRabbit Cloud** in **Settings → Providers**. If needed, set its **Binary path** to
that host's `cr` executable.

Select CodeRabbit Cloud for a thread, or ask your agent to delegate a task to its provider instance.
The cloud chooses the model. Follow-up messages continue the same cloud task.

Cloud tasks start from the pushed branch on `origin`. Commit and push the changes the cloud needs
before sending a task. Uncommitted files, local attachments, and unpushed commits are not uploaded.
Cloud changes remain in the cloud task as a proposed patch. Use CodeRabbit’s delivery flow to
publish them before fetching and reviewing them locally.

CodeRabbit Cloud offers **Full access** and the **Default** interaction mode only. Delegation requires the parent thread to use
**Full access** too; T3 never broadens its permissions to start a cloud task. T3 cannot enforce a local sandbox or forward its
interactive tool approval prompts into the cloud runtime. T3 uses your existing host CLI login;
separate CodeRabbit accounts need separate T3 environments.

Text generation helpers, such as commit messages and thread titles, require another provider.
