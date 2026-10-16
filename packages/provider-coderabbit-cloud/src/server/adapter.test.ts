import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { writeFakeCli } from "@t3tools/provider-testing/fakeCli";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Path from "effect/Path";
import { makeCodeRabbitCloudAdapterV2 } from "./adapter.ts";
import { CommandError } from "./protocol.ts";

const decodeAdapterEvent = Schema.decodeEffect(ProviderAdapter.ProviderAdapterV2Event);
const isCommandError = Schema.is(CommandError);
const THREAD = ThreadId.make("cloud-test");
const INSTANCE = ProviderInstanceId.make("coderabbit-cloud");
const SESSION = ProviderSessionId.make("cloud-session");
const modelSelection = { instanceId: INSTANCE, model: "default" };
const layer = Layer.mergeAll(NodeServices.layer, IdAllocator.layer);
const task = {
  taskId: "task-123",
  url: "https://app.coderabbit.ai/code/tasks/task-123",
  status: "patch_ready",
  turnStatus: "completed",
  resultSummary: "Cloud result",
  errorSummary: null,
  cancellationPending: false,
  collaborationMode: "code",
};
const result = {
  phase: "code",
  type: "complete",
  status: "turn_completed",
  taskId: task.taskId,
  url: task.url,
  clientOperationId: null,
  task,
  agentMinutes: 0.1,
  turn: {
    startSeq: 1,
    endSeq: 5,
    queueItemId: null,
    outcome: "completed",
    outputKind: "patch",
    finalReply: "Done in cloud",
    errorMessage: null,
  },
};
const receipt = {
  phase: "code",
  type: "status",
  status: "task_submitted",
  message: "submitted",
  taskId: task.taskId,
  url: task.url,
  clientOperationId: null,
};
const question = {
  phase: "code",
  type: "action_required",
  action: "answer_question",
  taskId: task.taskId,
  url: task.url,
  message: "Please choose",
  command: "cr code resume task-123 --answer - --agent",
  question: {
    interactionId: "question-1",
    isBlocking: true,
    questions: [
      {
        id: "q1",
        header: "Choice",
        question: "Which option?",
        options: null,
        isOther: true,
        isSensitive: false,
      },
    ],
  },
  answerTemplate: { interactionId: "question-1", answers: { q1: "" } },
};

const fixture = Effect.fnUntraced(function* (
  source: string,
  initialTask: string | null = "task-123",
  showSource: string = "emit({phase:'code',type:'complete',status:'task_shown',task:result.task,warnings:[]});",
  emitMessageReceipt: boolean = true,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const env = yield* HostProcess.Environment;
  const dir = yield* fs.makeTempDirectoryScoped();
  const log = path.join(dir, "calls.jsonl");
  const binary = writeFakeCli({
    directory: dir,
    name: "fake-cr",
    source: `
    import {readFileSync,appendFileSync,existsSync,writeFileSync,watch} from 'node:fs';
    const args=process.argv.slice(2), input=readFileSync(0,'utf8');
    appendFileSync(${JSON.stringify(log)},JSON.stringify({args,input})+'\\n');
    const result=${JSON.stringify(result)},receipt=${JSON.stringify(receipt)},question=${JSON.stringify(question)};
    const emit=e=>process.stdout.write(JSON.stringify(e)+'\\n');
    result.turn.queueItemId='fixture-message';
    if(args.includes('--message') && ${emitMessageReceipt})emit({phase:'code',type:'status',status:'message_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'fixture-message'});
    const signal=name=>${JSON.stringify(dir)}+'/signal-'+name;
    const mark=name=>writeFileSync(signal(name),'1');
    const wait=name=>new Promise(resolve=>{if(existsSync(signal(name)))return resolve();const watcher=watch(${JSON.stringify(dir)},()=>{if(existsSync(signal(name))){watcher.close();resolve();}});if(existsSync(signal(name))){watcher.close();resolve();}});
    if(args[1]==='show'){ ${showSource} process.exit(0); }
    ${source}
  `,
  });
  writeFakeCli({
    directory: dir,
    name: "git",
    source: `
    const args=process.argv.slice(2);
    if(args[0]==='symbolic-ref') console.log('main');
    if(args[0]==='rev-parse') console.log('abcdef');
    if(args[0]==='ls-remote') console.log('abcdef\\trefs/heads/main');
  `,
  });
  const adapter = yield* makeCodeRabbitCloudAdapterV2({
    instanceId: INSTANCE,
    settings: { enabled: true, binaryPath: binary },
    environment: { ...env, PATH: `${dir}${path.sep === "\\" ? ";" : ":"}${env.PATH ?? ""}` },
  });
  const runtimePolicy = {
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd: dir,
  } as const;
  const runtime = yield* adapter.openSession({
    threadId: THREAD,
    providerSessionId: SESSION,
    modelSelection,
    runtimePolicy,
    ...(initialTask === null ? {} : { initialNativeThreadId: initialTask }),
  });
  const emitted = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const allEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
  yield* runtime.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        yield* decodeAdapterEvent(event).pipe(Effect.orDie);
        allEvents.push(event);
        yield* Queue.offer(emitted, event);
      }),
    ),
    Effect.forkScoped,
  );
  const thread = yield* runtime.ensureThread({ threadId: THREAD, modelSelection, runtimePolicy });
  const now = yield* DateTime.now;
  const appThread: OrchestrationV2AppThread = {
    id: THREAD,
    projectId: ProjectId.make("cloud-project"),
    title: "Cloud test",
    providerInstanceId: INSTANCE,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    activeProviderThreadId: thread.id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const turnInput: ProviderAdapter.ProviderAdapterV2TurnInput = {
    appThread,
    threadId: THREAD,
    runId: RunId.make("cloud-run"),
    runOrdinal: 1,
    providerTurnOrdinal: 1,
    attemptId: RunAttemptId.make("cloud-attempt"),
    rootNodeId: NodeId.make("cloud-root"),
    providerThread: thread,
    modelSelection,
    runtimePolicy,
    message: {
      messageId: MessageId.make("cloud-message"),
      text: "Do bounded work",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
  };
  const next = (test: (event: ProviderAdapter.ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (test(event)) return event;
      }
    });
  const calls = fs.readFileString(log).pipe(
    Effect.map((text) =>
      text
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { args: string[]; input: string }),
    ),
  );
  const mark = (name: string) => fs.writeFileString(path.join(dir, `signal-${name}`), "1");
  const workCalls = calls.pipe(
    Effect.map((entries) => entries.filter((entry) => entry.args[1] !== "show")),
  );
  return { runtime, turnInput, next, calls, workCalls, allEvents, mark };
});

it.layer(layer, { excludeTestServices: true })("CodeRabbit Cloud adapter", (it) => {
  it.effect("preserves the cloud task receipt before a later transport failure", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        "if(args[1]==='new'){emit(receipt);process.exitCode=1;}else emit(result);",
        null,
      );
      yield* f.runtime.startTurn(f.turnInput);
      const bound = yield* f.next(
        (e) =>
          e.type === "provider_thread.updated" &&
          e.providerThread.nativeThreadRef?.nativeId === task.taskId,
      );
      assert.equal(bound.type, "provider_thread.updated");
      const end = yield* f.next((e) => e.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      const calls = yield* f.workCalls;
      assert.equal(calls.filter((entry) => entry.args.includes("new")).length, 1);
      assert.include(calls[0]!.args, "new");
      assert.equal(calls[0]!.input, "Do bounded work");
    }).pipe(Effect.scoped),
  );

  it.effect("reconnects after exit 4 without replaying creation or a message", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
      if(args.includes('--message')) { emit({phase:'code',type:'action_required',action:'resume',taskId:result.taskId,url:result.url,message:'still running',command:'cr code resume task-123 --agent',clientOperationId:null}); process.exitCode=4; }
      else emit(result);
    `);
      yield* f.runtime.startTurn(f.turnInput);
      const end = yield* f.next((e) => e.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      const calls = yield* f.workCalls;
      assert.equal(calls.length, 2);
      assert.deepEqual(calls[1]!.args, ["code", "resume", "task-123", "--agent"]);
      assert.equal(calls[1]!.input, "");
    }).pipe(Effect.scoped),
  );

  it.effect("reattaches a recovered active cloud turn without sending its message again", () =>
    Effect.gen(function* () {
      const f = yield* fixture("result.turn.queueItemId='recovered-op';emit(result);");
      const providerThread = yield* f.runtime.resumeThread({
        threadId: THREAD,
        providerThread: {
          ...f.turnInput.providerThread,
          status: "active",
          nativeConversationHeadRef: {
            driver: ProviderDriverKind.make("coderabbit-cloud"),
            nativeId: "recovered-op",
            strength: "strong",
          },
        },
      });
      yield* f.runtime.startTurn({
        ...f.turnInput,
        providerThread,
        restartContinuationOfRunId: RunId.make("previous-run"),
      });
      const end = yield* f.next((event) => event.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      const calls = yield* f.workCalls;
      assert.equal(calls.length, 1);
      assert.deepEqual(calls[0]?.args, ["code", "resume", "task-123", "--agent"]);
      assert.equal(calls[0]?.input, "");
    }).pipe(Effect.scoped),
  );

  it.effect(
    "sends fresh work when orchestration marks an existing thread active for a new run",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(
          "if(!args.includes('--message'))throw new Error('Reattached stale turn');emit(result);",
        );
        const providerThread = yield* f.runtime.resumeThread({
          threadId: THREAD,
          providerThread: {
            ...f.turnInput.providerThread,
            status: "active",
            nativeConversationHeadRef: {
              driver: ProviderDriverKind.make("coderabbit-cloud"),
              nativeId: "stale-operation",
              strength: "strong",
            },
          },
        });
        yield* f.runtime.startTurn({ ...f.turnInput, providerThread });
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
        const calls = yield* f.workCalls;
        assert.equal(calls.length, 1);
        assert.deepEqual(calls[0]?.args, [
          "code",
          "resume",
          "task-123",
          "--message",
          "-",
          "--agent",
        ]);
        assert.equal(calls[0]?.input, "Do bounded work");
        assert.isTrue(
          f.allEvents.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.nativeConversationHeadRef?.nativeId === "fixture-message",
          ),
        );
      }).pipe(Effect.scoped),
  );

  it.effect.each([null, "pending:cloud-attempt"] as const)(
    "fails recovery with an unconfirmed message receipt (%s) instead of completing a previous turn",
    (nativeId) =>
      Effect.gen(function* () {
        const f = yield* fixture("result.turn.finalReply='Previous turn result';emit(result);");
        const providerThread = yield* f.runtime.resumeThread({
          threadId: THREAD,
          providerThread: {
            ...f.turnInput.providerThread,
            status: "active",
            nativeConversationHeadRef:
              nativeId === null
                ? null
                : {
                    driver: ProviderDriverKind.make("coderabbit-cloud"),
                    nativeId,
                    strength: "strong",
                  },
          },
        });
        const started = yield* f.runtime
          .startTurn({
            ...f.turnInput,
            providerThread,
            restartContinuationOfRunId: RunId.make("previous-run"),
          })
          .pipe(Effect.result);
        if (started._tag === "Success") {
          const end = yield* f.next((event) => event.type === "turn.terminal");
          assert.equal(end.type === "turn.terminal" && end.status, "failed");
        }
        assert.isFalse(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text.includes("Previous turn result"),
          ),
        );
        const fs = yield* FileSystem.FileSystem;
        if (yield* fs.exists(`${f.turnInput.runtimePolicy.cwd}/calls.jsonl`))
          assert.isFalse((yield* f.workCalls).some((entry) => entry.args.includes("--message")));
      }).pipe(Effect.scoped),
  );

  it.effect(
    "reattaches a confirmed initial task receipt without requiring a message operation ID",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture("result.turn.queueItemId=null;emit(result);");
        const providerThread = yield* f.runtime.resumeThread({
          threadId: THREAD,
          providerThread: {
            ...f.turnInput.providerThread,
            status: "active",
            nativeConversationHeadRef: {
              driver: ProviderDriverKind.make("coderabbit-cloud"),
              nativeId: "initial:cloud-attempt",
              strength: "strong",
            },
          },
        });
        yield* f.runtime.startTurn({
          ...f.turnInput,
          providerThread,
          restartContinuationOfRunId: RunId.make("previous-run"),
        });
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
        const calls = yield* f.workCalls;
        assert.equal(calls.length, 1);
        assert.deepEqual(calls[0]?.args, ["code", "resume", "task-123", "--agent"]);
      }).pipe(Effect.scoped),
  );

  it.effect("persists the pending attempt before following an unconfirmed message send", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        "await wait('release');emit({phase:'code',type:'error',status:'task_rejected',message:'No admission'});process.exitCode=1;",
        "task-123",
        undefined,
        false,
      );
      yield* f.runtime.startTurn(f.turnInput);
      const pending = yield* f.next(
        (event) =>
          event.type === "provider_thread.updated" &&
          event.providerThread.nativeConversationHeadRef?.nativeId === "pending:cloud-attempt",
      );
      assert.equal(pending.type, "provider_thread.updated");
      yield* f.mark("release");
      const end = yield* f.next((event) => event.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "failed");
      assert.equal((yield* f.workCalls).length, 1);
    }).pipe(Effect.scoped),
  );

  it.effect.each(["message_dropped", "turn_not_running", "agent_not_running"] as const)(
    "fails the turn without reconnecting after the CLI reports %s",
    (status) =>
      Effect.gen(function* () {
        const f = yield* fixture(`
          if(args.includes('--message')){emit({phase:'code',type:'error',status:${JSON.stringify(status)},message:'The operation was rejected.'});process.exitCode=1;}
          else emit(result);
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "failed");
        assert.equal((yield* f.workCalls).length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect("answers a blocking question before publishing terminal completion", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
      if(args.includes('--answer')) { const value=JSON.parse(input); if(value.answers.q1!=='Option A') process.exit(9); mark('answered');emit({phase:'code',type:'status',status:'answer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'answer-1'}); await wait('unused'); }
      else if(existsSync(signal('answered'))) emit(result);
      else { emit(question); process.exitCode=3; }
    `);
      yield* f.runtime.startTurn(f.turnInput);
      const pending = yield* f.next((e) => e.type === "runtime_request.updated");
      assert.equal(pending.type, "runtime_request.updated");
      if (pending.type !== "runtime_request.updated") return;
      yield* f.runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        answers: { q1: "Option A" },
      });
      const resolved = yield* f.next(
        (e) => e.type === "runtime_request.updated" && e.runtimeRequest.status === "resolved",
      );
      assert.equal(resolved.type, "runtime_request.updated");
      const end = yield* f.next((e) => e.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
    }).pipe(Effect.scoped),
  );

  it.effect("reattaches after a message receipt and transport failure without resending it", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
        if(args.includes('--message')){
          emit({phase:'code',type:'status',status:'message_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'q1'});
          emit({phase:'code',type:'trace',taskId:result.taskId,seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Message accepted before disconnect'});
          process.exitCode=1;
        }else{result.turn.queueItemId='q1';emit(result);}
      `);
      yield* f.runtime.startTurn(f.turnInput);
      yield* f.next(
        (event) =>
          event.type === "message.updated" &&
          event.message.text === "Message accepted before disconnect",
      );
      const end = yield* f.next((event) => event.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      const calls = yield* f.workCalls;
      assert.equal(calls.filter((entry) => entry.args.includes("--message")).length, 1);
      assert.equal(calls.filter((entry) => entry.args[1] === "new").length, 0);
      assert.equal(
        calls.filter((entry) => entry.args[1] === "resume" && !entry.args.includes("--message"))
          .length,
        1,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("routes sensitive questions to the cloud without accepting their answers", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        "question.question.questions[0].isSensitive=true; question.answerTemplate=null; emit(question); process.exitCode=3;",
      );
      yield* f.runtime.startTurn(f.turnInput);
      const pending = yield* f.next((e) => e.type === "runtime_request.updated");
      if (pending.type !== "runtime_request.updated") return;
      assert.equal(pending.runtimeRequest.responseCapability.type, "not_resumable");
      const rejected = yield* f.runtime
        .respondToRuntimeRequest({
          requestId: pending.runtimeRequest.id,
          answers: { q1: "secret" },
        })
        .pipe(Effect.result);
      assert.isTrue(rejected._tag === "Failure");
      assert.equal((yield* f.workCalls).length, 1);
    }).pipe(Effect.scoped),
  );

  it.effect("cancellation waits for an actual cloud result", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
      if(args[1]==='cancel') emit({phase:'code',type:'complete',status:'task_canceled',taskId:result.taskId,url:result.url,task:result.task});
      else {emit(question);process.exitCode=3;}
    `);
      yield* f.runtime.startTurn(f.turnInput);
      const started = yield* f.next((e) => e.type === "provider_turn.updated");
      if (started.type !== "provider_turn.updated") return;
      yield* f.runtime.interruptTurn({
        providerThread: f.turnInput.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      const end = yield* f.next((e) => e.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "cancelled");
    }).pipe(Effect.scoped),
  );

  it.effect("steers active work and reports the resulting cloud reply", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
      if(args.includes('--steer')) { emit({phase:'code',type:'status',status:'steer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'steer-1'}); emit({phase:'code',type:'trace',taskId:result.taskId,seq:2,createdAt:'2026-10-16',kind:'steer_result',result:'accepted',reservationId:'steer-1',reason:null});await wait('unused'); }
      else { emit({phase:'code',type:'trace',taskId:result.taskId,seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Waiting for steering'}); await wait('finish');emit(result); }
    `);
      yield* f.runtime.startTurn(f.turnInput);
      const started = yield* f.next((e) => e.type === "provider_turn.updated");
      if (started.type !== "provider_turn.updated") return;
      yield* f.next(
        (e) => e.type === "message.updated" && e.message.text === "Waiting for steering",
      );
      yield* f.runtime.steerTurn({
        threadId: THREAD,
        runId: f.turnInput.runId,
        providerThread: f.turnInput.providerThread,
        providerTurnId: started.providerTurn.id,
        message: { ...f.turnInput.message, text: "Focus on tests" },
      });
      yield* f.mark("finish");
      const end = yield* f.next((e) => e.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      const calls = yield* f.workCalls;
      assert.equal(calls.length, 2);
      assert.include(calls[1]!.args, "--steer");
      assert.equal(calls[1]!.input, "Focus on tests");
    }).pipe(Effect.scoped),
  );

  it.effect("keeps following cancel_pending until cancellation actually settles", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
      const marker=process.argv[1]+'.cancel';
      if(args[1]==='cancel') { writeFileSync(marker,'1'); emit({phase:'code',type:'complete',status:'cancel_pending',taskId:result.taskId,url:result.url,task:result.task}); }
      else if(existsSync(marker)) {result.type='error';result.status='turn_canceled';result.message='Canceled';result.turn.outcome='canceled';emit(result);}
      else {emit(question);process.exitCode=3;}
    `);
      yield* f.runtime.startTurn(f.turnInput);
      const started = yield* f.next((e) => e.type === "provider_turn.updated");
      if (started.type !== "provider_turn.updated") return;
      yield* f.next((e) => e.type === "runtime_request.updated");
      yield* f.runtime.interruptTurn({
        providerThread: f.turnInput.providerThread,
        providerTurnId: started.providerTurn.id,
      });
      const end = yield* f.next((e) => e.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "cancelled");
      assert.equal((yield* f.workCalls).length, 3);
    }).pipe(Effect.scoped),
  );

  it.effect.each(["cancel_pending", "turn_finished", "not_running"] as const)(
    "never delivers a held message after interruption when cloud cancellation reports %s",
    (cancelStatus) =>
      Effect.gen(function* () {
        const f = yield* fixture(
          `
          if(args[1]==='cancel'){
            mark('interrupted');emit({phase:'code',type:'complete',status:${JSON.stringify(cancelStatus)},taskId:result.taskId,url:result.url,task:result.task});
          }else if(existsSync(signal('interrupted'))){
            if(args.includes('--message'))throw new Error('Interrupted held message was delivered');
            result.turn.queueItemId='old-turn';result.turn.finalReply='Older remote work completed';emit(result);
          }else {emit(question);process.exitCode=3;}
        `,
          "task-123",
          undefined,
          false,
        );
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        yield* f.next((event) => event.type === "runtime_request.updated");
        yield* f.runtime.interruptTurn({
          providerThread: f.turnInput.providerThread,
          providerTurnId: started.providerTurn.id,
        });
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "cancelled");
        const calls = yield* f.workCalls;
        assert.equal(calls.filter((entry) => entry.args.includes("--message")).length, 1);
        assert.equal(calls.filter((entry) => entry.args[1] === "cancel").length, 1);
        assert.isFalse(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text.includes("Older remote work completed"),
          ),
        );
      }).pipe(Effect.scoped),
  );

  it.effect.each(["deferred", "direct"] as const)(
    "waits for a %s message admission before canceling and requires the matching cloud terminal",
    (delivery) =>
      Effect.gen(function* () {
        const f = yield* fixture(
          `
          if(args[1]==='cancel'){
            if(!existsSync(signal('receipt')))throw new Error('Canceled before message admission');
            mark('cancelled');emit({phase:'code',type:'complete',status:'cancel_pending',taskId:result.taskId,url:result.url,task:result.task});
          }else if(args.includes('--answer')){
            mark('answered');emit({phase:'code',type:'status',status:'answer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:null});
            await wait('unused');
          }else if(args.includes('--message')){
            if(${delivery === "deferred"}&&!existsSync(signal('blocked'))){mark('blocked');emit(question);process.exitCode=3;}
            else{
              emit({phase:'code',type:'trace',taskId:result.taskId,seq:10,createdAt:'2026-10-16',kind:'agent_message',text:'Message admission waiting'});
              await wait('release-receipt');mark('receipt');
              emit({phase:'code',type:'status',status:'message_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'requested-message'});
              await wait('cloud-terminal');
              if(!existsSync(signal('cancelled')))throw new Error('Cloud terminal released before cancellation');
              result.type='error';result.status='turn_canceled';result.message='Canceled';result.turn.outcome='canceled';result.turn.queueItemId='requested-message';emit(result);process.exitCode=1;
            }
          }else{result.turn.queueItemId='old-operation';emit(result);}
        `,
          "task-123",
          undefined,
          false,
        );
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        if (delivery === "deferred") {
          const pending = yield* f.next((event) => event.type === "runtime_request.updated");
          if (pending.type !== "runtime_request.updated") return;
          yield* f.runtime.respondToRuntimeRequest({
            requestId: pending.runtimeRequest.id,
            answers: { q1: "Option A" },
          });
        }
        yield* f.next(
          (event) =>
            event.type === "message.updated" && event.message.text === "Message admission waiting",
        );
        const interruption = yield* f.runtime
          .interruptTurn({
            providerThread: f.turnInput.providerThread,
            providerTurnId: started.providerTurn.id,
          })
          .pipe(Effect.forkScoped({ startImmediately: true }));
        assert.isUndefined(interruption.pollUnsafe());
        assert.isFalse(f.allEvents.some((event) => event.type === "turn.terminal"));
        assert.equal((yield* f.workCalls).filter((entry) => entry.args[1] === "cancel").length, 0);
        yield* f.mark("release-receipt");
        yield* Fiber.join(interruption);
        assert.isFalse(f.allEvents.some((event) => event.type === "turn.terminal"));
        yield* f.mark("cloud-terminal");
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "cancelled");
        const calls = yield* f.workCalls;
        assert.equal(
          calls.filter((entry) => entry.args.includes("--message")).length,
          delivery === "deferred" ? 2 : 1,
        );
        assert.equal(calls.filter((entry) => entry.args[1] === "cancel").length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect.each(["failed", "canceled", "interrupted"] as const)(
    "bounds retries when plain resume repeatedly reports an unrelated %s turn",
    (outcome) =>
      Effect.gen(function* () {
        const terminalStatus =
          outcome === "failed"
            ? "turn_failed"
            : outcome === "canceled"
              ? "turn_canceled"
              : "turn_interrupted";
        const f = yield* fixture(`
          if(args.includes('--message'))process.exitCode=1;
          else{
            result.type='error';result.status=${JSON.stringify(terminalStatus)};result.message='An earlier turn ended';
            result.turn.queueItemId='old-operation';result.turn.outcome=${JSON.stringify(outcome)};emit(result);process.exitCode=1;
          }
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "failed");
        const calls = yield* f.workCalls;
        assert.equal(calls.length, 4);
        assert.equal(calls.filter((entry) => entry.args.includes("--message")).length, 1);
      }).pipe(Effect.scoped),
    15_000,
  );

  it.effect.each(["failed", "rejected", "deferred"] as const)(
    "keeps the healthy follower alive after a %s steering control",
    (failure) =>
      Effect.gen(function* () {
        const control =
          failure === "failed"
            ? "emit({phase:'code',type:'error',status:'steer_not_delivered',message:'not delivered'});process.exitCode=1;"
            : `emit({phase:'code',type:'status',status:'steer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'s1'});emit({phase:'code',type:'trace',taskId:result.taskId,seq:9,createdAt:'2026-10-16',kind:'steer_result',result:${JSON.stringify(failure)},reservationId:'s1',reason:'not_steerable'});process.exitCode=1;`;
        const f = yield* fixture(`
          if(args.includes('--steer')) { ${control} }
          else {emit({phase:'code',type:'trace',taskId:result.taskId,seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Working'});await wait('finish');emit(result);}
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        yield* f.next(
          (event) => event.type === "message.updated" && event.message.text === "Working",
        );
        const outcome = yield* f.runtime
          .steerTurn({
            threadId: THREAD,
            runId: f.turnInput.runId,
            providerThread: f.turnInput.providerThread,
            providerTurnId: started.providerTurn.id,
            message: { ...f.turnInput.message, text: "Use tests" },
          })
          .pipe(Effect.result);
        assert.equal(outcome._tag, "Failure");
        assert.isFalse(f.allEvents.some((event) => event.type === "turn.terminal"));
        yield* f.mark("finish");
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
        const calls = yield* f.workCalls;
        assert.equal(calls.filter((entry) => entry.args.includes("--message")).length, 1);
        assert.equal(calls.filter((entry) => !entry.args.includes("--steer")).length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "publishes one question across simultaneous observers and never reopens it after an answer",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(`
          if(args.includes('--steer')){
            emit({phase:'code',type:'status',status:'steer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'s1'});
            mark('question');emit(question);process.exitCode=4;
          }else if(args.includes('--answer')){
            emit({phase:'code',type:'status',status:'answer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:null});
            mark('answered');await wait('unused');
          }else{
            emit({phase:'code',type:'trace',taskId:result.taskId,seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Working'});
            await wait('question');emit(question);await wait('answered');
            emit({phase:'code',type:'trace',taskId:result.taskId,seq:2,createdAt:'2026-10-16',kind:'agent_message',text:'Answer applied'});
            emit(question);
            emit({phase:'code',type:'trace',taskId:result.taskId,seq:3,createdAt:'2026-10-16',kind:'agent_message',text:'Stale question replay drained'});
            await wait('finish');emit(result);
          }
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        yield* f.next(
          (event) => event.type === "message.updated" && event.message.text === "Working",
        );
        const steering = yield* f.runtime
          .steerTurn({
            threadId: THREAD,
            runId: f.turnInput.runId,
            providerThread: f.turnInput.providerThread,
            providerTurnId: started.providerTurn.id,
            message: { ...f.turnInput.message, text: "Use tests" },
          })
          .pipe(Effect.result, Effect.forkScoped);
        const pending = yield* f.next((event) => event.type === "runtime_request.updated");
        if (pending.type !== "runtime_request.updated") return;
        const outcome = yield* Fiber.join(steering);
        assert.equal(outcome._tag, "Failure");
        yield* f.runtime.respondToRuntimeRequest({
          requestId: pending.runtimeRequest.id,
          answers: { q1: "Option A" },
        });
        yield* f.next(
          (event) =>
            event.type === "message.updated" &&
            event.message.text === "Stale question replay drained",
        );
        assert.equal(
          f.allEvents.filter(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          ).length,
          1,
        );
        assert.equal(
          f.allEvents.filter(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.status === "resolved",
          ).length,
          1,
        );
        yield* f.mark("finish");
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
      }).pipe(Effect.scoped),
  );

  it.effect("ignores a control observer's late question after the primary turn finishes", () =>
    Effect.gen(function* () {
      const f = yield* fixture(`
        if(args.includes('--steer')){
          emit({phase:'code',type:'status',status:'steer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'s1'});
          mark('steering');await wait('late');emit(question);process.exitCode=4;
        }else{
          emit({phase:'code',type:'trace',taskId:result.taskId,seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Working'});
          await wait('steering');emit(result);
        }
      `);
      yield* f.runtime.startTurn(f.turnInput);
      const started = yield* f.next((event) => event.type === "provider_turn.updated");
      if (started.type !== "provider_turn.updated") return;
      yield* f.next(
        (event) => event.type === "message.updated" && event.message.text === "Working",
      );
      const steering = yield* f.runtime
        .steerTurn({
          threadId: THREAD,
          runId: f.turnInput.runId,
          providerThread: f.turnInput.providerThread,
          providerTurnId: started.providerTurn.id,
          message: { ...f.turnInput.message, text: "Use tests" },
        })
        .pipe(Effect.result, Effect.forkScoped);
      const end = yield* f.next((event) => event.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      yield* f.mark("late");
      assert.equal((yield* Fiber.join(steering))._tag, "Failure");
      assert.isFalse(f.allEvents.some((event) => event.type === "runtime_request.updated"));
    }).pipe(Effect.scoped),
  );

  it.effect.each(["question", "follow_limit", "error"] as const)(
    "reports steering delivery as unconfirmed after submission followed by %s",
    (ending) =>
      Effect.gen(function* () {
        const controlEnding =
          ending === "question"
            ? "emit(question);process.exitCode=4;"
            : ending === "follow_limit"
              ? "emit({phase:'code',type:'action_required',action:'resume',taskId:result.taskId,url:result.url,message:'follow limit',command:'resume',clientOperationId:'s1'});process.exitCode=4;"
              : "emit({phase:'code',type:'error',status:'agent_not_running',message:'Connection ended'});process.exitCode=1;";
        const f = yield* fixture(`
          if(args.includes('--steer')){
            emit({phase:'code',type:'status',status:'steer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'s1'});
            ${controlEnding}
          }else{
            emit({phase:'code',type:'trace',taskId:result.taskId,seq:1,createdAt:'2026-10-16',kind:'agent_message',text:'Working'});
            await wait('finish');emit(result);
          }
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        yield* f.next(
          (event) => event.type === "message.updated" && event.message.text === "Working",
        );
        const outcome = yield* f.runtime
          .steerTurn({
            threadId: THREAD,
            runId: f.turnInput.runId,
            providerThread: f.turnInput.providerThread,
            providerTurnId: started.providerTurn.id,
            message: { ...f.turnInput.message, text: "Use tests" },
          })
          .pipe(Effect.result);
        assert.equal(outcome._tag, "Failure");
        if (outcome._tag === "Failure") {
          assert.isTrue(isCommandError(outcome.failure.cause));
          if (isCommandError(outcome.failure.cause))
            assert.equal(outcome.failure.cause.operation, "steer delivery unconfirmed");
        }
        assert.isFalse(f.allEvents.some((event) => event.type === "turn.terminal"));
        yield* f.mark("finish");
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
      }).pipe(Effect.scoped),
  );

  it.effect("rejects empty and invalid option answers before sending a control command", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        "question.question.questions[0].options=[{label:'Option A',description:null}];question.question.questions[0].isOther=false;emit(question);process.exitCode=3;",
      );
      yield* f.runtime.startTurn(f.turnInput);
      const pending = yield* f.next((event) => event.type === "runtime_request.updated");
      if (pending.type !== "runtime_request.updated") return;
      for (const answers of [
        {},
        { q1: "" },
        { q1: "   " },
        { q1: "Invalid" },
        { q1: "Option A", extra: "unexpected" },
      ]) {
        const rejected = yield* f.runtime
          .respondToRuntimeRequest({ requestId: pending.runtimeRequest.id, answers })
          .pipe(Effect.result);
        assert.equal(rejected._tag, "Failure");
      }
      assert.equal(
        (yield* f.workCalls).filter((entry) => entry.args.includes("--answer")).length,
        0,
      );
      assert.isFalse(f.allEvents.some((event) => event.type === "turn.terminal"));
    }).pipe(Effect.scoped),
  );

  it.effect(
    "waits through repeated stale answered questions without reopening the request or failing the turn",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(`
          if(args.includes('--answer')){
            mark('answered');emit({phase:'code',type:'status',status:'answer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:null});
            await wait('unused');
          }else if(!existsSync(signal('answered'))){emit(question);process.exitCode=3;}
          else{
            const count=existsSync(signal('stale-count'))?Number(readFileSync(signal('stale-count'),'utf8')):0;
            writeFileSync(signal('stale-count'),String(count+1));
            if(count<4){emit(question);process.exitCode=3;}else emit(result);
          }
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const pending = yield* f.next((event) => event.type === "runtime_request.updated");
        if (pending.type !== "runtime_request.updated") return;
        yield* f.runtime.respondToRuntimeRequest({
          requestId: pending.runtimeRequest.id,
          answers: { q1: "Option A" },
        });
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
        assert.equal(
          f.allEvents.filter(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          ).length,
          1,
        );
        assert.equal(
          f.allEvents.filter(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.status === "resolved",
          ).length,
          1,
        );
        const calls = yield* f.workCalls;
        assert.equal(calls.filter((entry) => entry.args.includes("--message")).length, 1);
        assert.equal(calls.filter((entry) => entry.args.includes("--answer")).length, 1);
        assert.equal(calls.filter((entry) => entry.args.length === 4).length, 5);
      }).pipe(Effect.scoped),
    25_000,
  );

  it.effect(
    "resolves an externally answered question even when its answer trace is missing",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(
          "if(existsSync(signal('external-answer')))emit(result);else{emit(question);process.exitCode=3;}",
        );
        yield* f.runtime.startTurn(f.turnInput);
        const pending = yield* f.next((event) => event.type === "runtime_request.updated");
        if (pending.type !== "runtime_request.updated") return;
        yield* f.mark("external-answer");
        const resolved = yield* f.next(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === pending.runtimeRequest.id &&
            event.runtimeRequest.status !== "pending",
        );
        assert.equal(
          resolved.type === "runtime_request.updated" && resolved.runtimeRequest.status,
          "resolved",
        );
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
      }).pipe(Effect.scoped),
    15_000,
  );

  it.effect(
    "sends a message exactly once after a blocking question initially prevented delivery",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(
          `
        if(args.includes('--answer')) {mark('answered');emit({phase:'code',type:'status',status:'answer_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:null});await wait('unused');}
        else if(args.includes('--message') && existsSync(signal('answered'))) {
          if(input!=='Do bounded work')throw new Error('Lost original prompt');
          mark('delivered');emit({phase:'code',type:'status',status:'message_sent',message:'sent',taskId:result.taskId,url:result.url,clientOperationId:'requested-message'});
          result.turn.queueItemId='requested-message';result.clientOperationId='requested-message';emit(result);
        }
        else if(existsSync(signal('answered'))) emit(result);
        else {emit(question);process.exitCode=3;}
      `,
          "task-123",
          undefined,
          false,
        );
        yield* f.runtime.startTurn(f.turnInput);
        const pending = yield* f.next((event) => event.type === "runtime_request.updated");
        if (pending.type !== "runtime_request.updated") return;
        yield* f.runtime.respondToRuntimeRequest({
          requestId: pending.runtimeRequest.id,
          answers: { q1: "Option A" },
        });
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
        const calls = yield* f.workCalls;
        const messages = calls.filter((entry) => entry.args.includes("--message"));
        assert.equal(messages.length, 2);
        assert.isTrue(messages.every((entry) => entry.input === "Do bounded work"));
        assert.equal(calls.filter((entry) => entry.args.includes("--answer")).length, 1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "resolves the previous request before publishing a replacement question",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(
          "if(!args.includes('--message')){question.question.interactionId='question-2';question.answerTemplate.interactionId='question-2';}emit(question);process.exitCode=3;",
        );
        yield* f.runtime.startTurn(f.turnInput);
        const first = yield* f.next((event) => event.type === "runtime_request.updated");
        if (first.type !== "runtime_request.updated") return;
        const prior = yield* f.next(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.id === first.runtimeRequest.id &&
            event.runtimeRequest.status !== "pending",
        );
        assert.equal(
          prior.type === "runtime_request.updated" && prior.runtimeRequest.status,
          "resolved",
        );
        const replacement = yield* f.next(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.nativeRequestRef?.nativeId === "question-2",
        );
        assert.equal(
          replacement.type === "runtime_request.updated" && replacement.runtimeRequest.status,
          "pending",
        );
      }).pipe(Effect.scoped),
    15_000,
  );

  it.effect(
    "never treats an unrelated queued turn's completion as the requested message result",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(`
        if(args.includes('--message')){emit({phase:'code',type:'status',status:'message_sent',message:'queued',taskId:result.taskId,url:result.url,clientOperationId:'q-new'});emit({phase:'code',type:'action_required',action:'resume',taskId:result.taskId,url:result.url,message:'running',command:'resume',clientOperationId:'q-new'});process.exitCode=4;}
        else if(!existsSync(signal('old-done'))){mark('old-done');emit({phase:'code',type:'trace',taskId:result.taskId,seq:20,createdAt:'2026-10-16',kind:'user_message',source:'steered',queueItemId:'q-new',text:'Do bounded work'});result.turn.queueItemId='q-old';result.turn.finalReply='Old message';emit(result);}
        else {emit({phase:'code',type:'trace',taskId:result.taskId,seq:21,createdAt:'2026-10-16',kind:'agent_message',text:'Following requested message'});await wait('new-done');result.turn.startSeq=20;result.turn.endSeq=25;result.turn.queueItemId='q-new';result.turn.finalReply='Requested message';emit(result);}
      `);
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.next(
          (event) =>
            event.type === "message.updated" &&
            event.message.text === "Following requested message",
        );
        assert.isFalse(f.allEvents.some((event) => event.type === "turn.terminal"));
        yield* f.mark("new-done");
        yield* f.next(
          (event) =>
            event.type === "message.updated" && event.message.text.includes("Requested message"),
        );
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
        assert.equal(
          (yield* f.workCalls).filter((entry) => entry.args.includes("--message")).length,
          1,
        );
      }).pipe(Effect.scoped),
  );

  it.effect("correlates a queued message steered into the running turn after reconnecting", () =>
    Effect.gen(function* () {
      const f = yield* fixture(
        `
        if(args.includes('--message')){
          emit({phase:'code',type:'status',status:'message_sent',message:'queued',taskId:result.taskId,url:result.url,clientOperationId:'q-new'});
          emit({phase:'code',type:'action_required',action:'resume',taskId:result.taskId,url:result.url,message:'running',command:'resume',clientOperationId:'q-new'});
          process.exitCode=4;
        }else if(!existsSync(signal('joined'))){
          mark('joined');
          emit({phase:'code',type:'trace',taskId:result.taskId,seq:2,createdAt:'2026-10-16',kind:'user_message',source:'steered',queueItemId:'q-new',text:'Do bounded work'});
          emit({phase:'code',type:'status',status:'turn_started',message:'Your message joined the running turn.',taskId:result.taskId,url:result.url,clientOperationId:null});
          result.turn.queueItemId='q-old';result.turn.finalReply='Combined turn result';emit(result);
        }else {emit({phase:'code',type:'error',status:'message_dropped',message:'Unexpected duplicate resume'});process.exitCode=1;}
      `,
        "task-123",
        undefined,
        false,
      );
      yield* f.runtime.startTurn(f.turnInput);
      const end = yield* f.next((event) => event.type === "turn.terminal");
      assert.equal(end.type === "turn.terminal" && end.status, "completed");
      assert.isTrue(
        f.allEvents.some(
          (event) =>
            event.type === "message.updated" && event.message.text.includes("Combined turn result"),
        ),
      );
      const calls = yield* f.workCalls;
      assert.equal(calls.length, 2);
      assert.deepEqual(calls[1]?.args, ["code", "resume", "task-123", "--agent"]);
    }).pipe(Effect.scoped),
  );

  it.effect.each(["not_running", "turn_finished"] as const)(
    "follows the actual turn outcome after cancel reports %s",
    (cancelStatus) =>
      Effect.gen(function* () {
        const f = yield* fixture(`
          if(args[1]==='cancel'){mark('cancelled');emit({phase:'code',type:'complete',status:${JSON.stringify(cancelStatus)},taskId:result.taskId,url:result.url,task:result.task});}
          else if(existsSync(signal('cancelled')))emit(result);
          else{emit(question);process.exitCode=3;}
        `);
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        yield* f.next((event) => event.type === "runtime_request.updated");
        yield* f.runtime.interruptTurn({
          providerThread: f.turnInput.providerThread,
          providerTurnId: started.providerTurn.id,
        });
        const end = yield* f.next((event) => event.type === "turn.terminal");
        assert.equal(end.type === "turn.terminal" && end.status, "completed");
      }).pipe(Effect.scoped),
  );

  it.effect(
    "cancels the pending question's request, node, and item when cancellation settles",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(
          "if(args[1]==='cancel')emit({phase:'code',type:'complete',status:'task_canceled',taskId:result.taskId,url:result.url,task:result.task});else{emit(question);process.exitCode=3;}",
        );
        yield* f.runtime.startTurn(f.turnInput);
        const started = yield* f.next((event) => event.type === "provider_turn.updated");
        if (started.type !== "provider_turn.updated") return;
        const pending = yield* f.next((event) => event.type === "runtime_request.updated");
        if (pending.type !== "runtime_request.updated") return;
        yield* f.runtime.interruptTurn({
          providerThread: f.turnInput.providerThread,
          providerTurnId: started.providerTurn.id,
        });
        yield* f.next((event) => event.type === "turn.terminal");
        assert.isTrue(
          f.allEvents.some(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === pending.runtimeRequest.id &&
              event.runtimeRequest.status === "cancelled",
          ),
        );
        assert.isTrue(
          f.allEvents.some(
            (event) =>
              event.type === "node.updated" &&
              event.node.id === pending.runtimeRequest.nodeId &&
              event.node.status === "cancelled",
          ),
        );
        assert.isTrue(
          f.allEvents.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "user_input_request" &&
              event.turnItem.requestId === pending.runtimeRequest.id &&
              event.turnItem.status === "cancelled",
          ),
        );
      }).pipe(Effect.scoped),
  );

  it.effect("rejects attachments and supervised remote execution before starting work", () =>
    Effect.gen(function* () {
      const f = yield* fixture("emit(result);");
      const rejected = yield* f.runtime
        .startTurn({
          ...f.turnInput,
          runtimePolicy: { ...f.turnInput.runtimePolicy, runtimeMode: "approval-required" },
        })
        .pipe(Effect.result);
      assert.equal(rejected._tag, "Failure");
      const fs = yield* FileSystem.FileSystem;
      assert.isFalse(yield* fs.exists(`${f.turnInput.runtimePolicy.cwd}/calls.jsonl`));
    }).pipe(Effect.scoped),
  );
});
