// @effect-diagnostics nodeBuiltinImport:off - Local sockets synchronize fake subprocesses without timers.
import * as NodeNet from "node:net";
import { assert, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ChatAttachmentId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
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
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import {
  ProviderContinuationRequests,
  type ProviderContinuationRequest,
} from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as Stream from "effect/Stream";
import * as Path from "effect/Path";
import { makeCodeRabbitAdapterV2 } from "./adapter.ts";

const decodeAdapterEvent = Schema.decodeEffect(ProviderAdapter.ProviderAdapterV2Event);
const THREAD = ThreadId.make("cloud-test");
const INSTANCE = ProviderInstanceId.make("coderabbit");
const SESSION = ProviderSessionId.make("cloud-session");
const modelSelection = { instanceId: INSTANCE, model: "default" };
const layer = Layer.mergeAll(NodeServices.layer, IdAllocator.layer);
const task = {
  taskId: "task-123",
  url: "https://app.coderabbit.ai/code/tasks/task-123",
  status: "patch_ready",
  workingBranchName: "coderabbit/test",
  patchStatus: "ready",
  deliveryStatus: "pending",
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

interface FixtureOptions {
  readonly initialTask?: string | null;
  readonly message?: string;
  readonly open?: string;
  readonly beforeReady?: string;
  readonly eof?: string;
  readonly cancel?: string;
  readonly answer?: string;
  readonly steer?: string;
  readonly restart?: boolean;
  readonly resumeStatus?: "running" | "waiting";
  readonly savedHead?: string;
  readonly readyTask?: object;
  readonly latest?: string;
  readonly create?: string;
  readonly unpublished?: boolean;
  readonly show?: string;
  readonly preflight?: string;
}
const fixture = Effect.fn("CodeRabbitAdapterTest.fixture")(function* (
  options: FixtureOptions = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const env = yield* HostProcess.Environment;
  const dir = yield* fs.makeTempDirectoryScoped();
  const log = path.join(dir, "calls.jsonl");
  const signals = new Set<string>();
  const waiting = new Map<string, Set<() => void>>();
  const sockets = new Set<NodeNet.Socket>();
  const notify = (name: string) => {
    signals.add(name);
    for (const resume of waiting.get(name) ?? []) resume();
    waiting.delete(name);
  };
  const subscribe = (name: string, resume: () => void) => {
    if (signals.has(name)) {
      resume();
      return () => {};
    }
    let listeners = waiting.get(name);
    if (!listeners) waiting.set(name, (listeners = new Set()));
    listeners.add(resume);
    return () => {
      listeners.delete(resume);
    };
  };
  const server = NodeNet.createServer((socket) => {
    sockets.add(socket);
    let buffered = "";
    let unsubscribe = () => {};
    socket.on("data", (data) => {
      buffered += data.toString();
      if (!buffered.includes("\n")) return;
      const message = JSON.parse(buffered) as { command: "mark" | "wait"; name: string };
      if (message.command === "mark") {
        notify(message.name);
        socket.end("ready\n");
      } else unsubscribe = subscribe(message.name, () => socket.end("ready\n"));
    });
    socket.on("error", () => {});
    socket.on("close", () => {
      unsubscribe();
      sockets.delete(socket);
    });
  });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const socket of sockets) socket.destroy();
      server.close();
    }),
  );
  const port = yield* Effect.callback<number>((resume) => {
    server.once("error", (error) => resume(Effect.die(error)));
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      resume(
        address && typeof address !== "string"
          ? Effect.succeed(address.port)
          : Effect.die(new Error("Missing fixture address")),
      );
    });
  });
  const script = (source: string) => source.replace(/\bmark\(/g, "await mark(");
  const syncSource = `
    const exchange=(command,name)=>new Promise((resolve,reject)=>{
      const socket=createConnection({host:'127.0.0.1',port:${port}});
      socket.on('error',reject);
      socket.on('connect',()=>socket.write(JSON.stringify({command,name})+'\\n'));
      socket.once('data',()=>{socket.end();resolve();});
    });
    const signal=name=>${JSON.stringify(dir)}+'/signal-'+name;
    const mark=async name=>{writeFileSync(signal(name),'1');await exchange('mark',name);};
    const wait=name=>exchange('wait',name);
  `;
  const binary = writeFakeCli({
    directory: dir,
    name: "fake-cr",
    source: `
    import {readFileSync,appendFileSync,existsSync,writeFileSync,watch} from 'node:fs';
    import {createInterface} from 'node:readline';
    import {createConnection} from 'node:net';
    const args=process.argv.slice(2);
    appendFileSync(${JSON.stringify(log)},JSON.stringify({args})+'\\n');
    const result=${JSON.stringify(result)},question=${JSON.stringify(question)};
    const readyTask=${JSON.stringify(options.readyTask ?? { ...task, turnStatus: "running" })};
    let seq=0;
    const emit=e=>process.stdout.write(JSON.stringify(e)+'\\n');
    const trace=(kind,fields={})=>emit({phase:'code',type:'trace',taskId:'task-123',seq:++seq,createdAt:'2026-10-16T00:00:00.000Z',kind,...fields});
    const end=(operation='op-1',outcome='completed')=>trace('turn_end',{turn:{...result.turn,queueItemId:operation,startSeq:1,endSeq:seq+1,outcome}});
    const ack=(req,fields={})=>emit({phase:'code',type:'response',id:req.id,ok:true,status:'message_sent',clientOperationId:'client-op-1',queueItemId:'op-1',...fields});
    ${syncSource}
    if(args[1]==='new') { const prompt=readFileSync(0,'utf8');appendFileSync(${JSON.stringify(log)},JSON.stringify({prompt})+'\\n'); ${script(options.create ?? "")} emit({phase:'code',type:'complete',status:'task_submitted',taskId:'task-123',url:result.task.url,created:true,task:{...result.task,turnStatus:'running'},warnings:[]});process.exit(0); }
    if(!args.includes('--interactive')){${script(options.latest ?? "emit(result);")}process.exit(0);}
    ${script(options.beforeReady ?? "")}
    emit({phase:'code',type:'session',status:'ready',taskId:'task-123',protocolVersion:1,commands:['message','steer','answer','cancel','show','close'],cursor:0,task:readyTask});
    ${script(options.open ?? (options.initialTask === null ? "trace('agent_message',{text:'New task is running'});end(null);" : ""))}
    const rl=createInterface({input:process.stdin});
    rl.on('line',line=>{const req=JSON.parse(line);appendFileSync(${JSON.stringify(log)},JSON.stringify({request:req})+'\\n');(async()=>{
      switch(req.command){
        case 'show': ${script(options.show ?? "ack(req,{status:'task_shown',task:result.task});")} break;
        case 'message': ${script(options.message ?? "ack(req);trace('agent_message',{text:'Working remotely'});end();")} break;
        case 'cancel': ${script(options.cancel ?? "ack(req,{status:'cancel_pending'});end('op-1','canceled');")} break;
        case 'steer': ${script(options.steer ?? "ack(req,{status:'steer_sent'});")} break;
        case 'answer': ${script(options.answer ?? "ack(req,{status:'answer_sent'});trace('answer_submitted',{interactionId:question.question.interactionId});end();")} break;
      }
    })().catch(error=>{console.error(error);process.exit(1)});});
    rl.on('close',async()=>{${script(options.eof ?? "process.exit(0);")}});
  `,
  });
  writeFakeCli({
    directory: dir,
    name: "git",
    source: `
    import {appendFileSync,writeFileSync,existsSync,watch} from "node:fs";
    import {createConnection} from "node:net";
    const args=process.argv.slice(2);
    appendFileSync(${JSON.stringify(log)},JSON.stringify({git:args})+'\\n');
    if(args[0]==='symbolic-ref') console.log('main');
    if(args[0]==='rev-parse') console.log('abcdef');
    ${syncSource}
    ${script(options.preflight ?? "")}
    if(args[0]==='ls-remote') console.log('${options.unpublished ? "fedcba" : "abcdef"}\\trefs/heads/main');
    if(args[0]==='status') console.log(' M changed.ts');
  `,
  });
  const wakes = yield* Queue.unbounded<ProviderContinuationRequest>();
  const adapter = yield* makeCodeRabbitAdapterV2({
    instanceId: INSTANCE,
    settings: { enabled: true, binaryPath: binary },
    environment: { ...env, PATH: `${dir}${path.sep === "\\" ? ";" : ":"}${env.PATH ?? ""}` },
  }).pipe(
    Effect.provideService(ProviderContinuationRequests, {
      offer: (request) => Queue.offer(wakes, request).pipe(Effect.asVoid),
      take: Queue.take(wakes),
    }),
  );
  const runtimePolicy = {
    runtimeMode: "full-access",
    interactionMode: "default",
    cwd: dir,
  } as const;
  let runtime = yield* adapter.openSession({
    threadId: THREAD,
    providerSessionId: SESSION,
    modelSelection,
    runtimePolicy,
    ...(options.initialTask === null
      ? {}
      : { initialNativeThreadId: options.initialTask ?? "task-123" }),
  });
  if (options.resumeStatus) {
    runtime = yield* adapter.openSession({
      threadId: THREAD,
      providerSessionId: SESSION,
      modelSelection,
      runtimePolicy,
      initialNativeThreadId: options.initialTask ?? "task-123",
      resumeFromSession: { ...runtime.providerSession, status: options.resumeStatus },
    });
  }
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
  let thread = yield* runtime.ensureThread({ threadId: THREAD, modelSelection, runtimePolicy });
  if (options.savedHead !== undefined) {
    thread = yield* runtime.ensureThread({
      threadId: THREAD,
      modelSelection,
      runtimePolicy,
      existingProviderThread: {
        ...thread,
        nativeConversationHeadRef: {
          driver: runtime.driver,
          nativeId: options.savedHead,
          strength: "strong",
        },
      },
    });
  }
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
    ...(options.restart ? { restartContinuationOfRunId: RunId.make("previous-run") } : {}),
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

  const next = Effect.fn("CodeRabbitAdapterTest.next")(function* <
    T extends ProviderAdapter.ProviderAdapterV2Event["type"],
  >(
    type: T,
    test: (event: Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>) => boolean = () =>
      true,
  ) {
    while (true) {
      const event = yield* Queue.take(emitted);
      if (event.type === type) {
        const matching = event as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: T }>;
        if (test(matching)) return matching;
      }
    }
  });
  const calls = fs.exists(log).pipe(
    Effect.flatMap((exists) => (exists ? fs.readFileString(log) : Effect.succeed(""))),
    Effect.map((text) =>
      text
        .trim()
        .split("\n")
        .filter(Boolean)
        .map(
          (line) =>
            JSON.parse(line) as {
              args?: string[];
              git?: string[];
              prompt?: string;
              request?: {
                id: string;
                command: string;
                text?: string;
                interactionId?: string;
                answers?: Record<string, string>;
              };
            },
        ),
    ),
  );
  const mark = (name: string) =>
    fs
      .writeFileString(path.join(dir, `signal-${name}`), "1")
      .pipe(Effect.andThen(Effect.sync(() => notify(name))));
  const wait = (name: string) =>
    Effect.callback<void>((resume) => {
      const unsubscribe = subscribe(name, () => resume(Effect.void));
      return Effect.sync(unsubscribe);
    });
  const terminal = next("turn.terminal");
  const started = next(
    "provider_turn.updated",
    (event) => event.providerTurn.status === "running",
  ).pipe(Effect.map((event) => event.providerTurn));
  const assistant = (text: string) =>
    next("message.updated", (event) => event.message.text.includes(text));
  return {
    adapter,
    runtimePolicy,
    runtime,
    turnInput,
    thread,
    next,
    calls,
    allEvents,
    mark,
    wait,
    terminal,
    started,
    assistant,
    wakes,
  };
});
const live = <E>(
  body: Effect.Effect<void, E, Layer.Success<typeof layer> | import("effect/Scope").Scope>,
) => Effect.scoped(body).pipe(Effect.provide(layer));

const nextTurn = (input: ProviderAdapter.ProviderAdapterV2TurnInput, ordinal: number) => ({
  ...input,
  runId: RunId.make(`cloud-run-${ordinal}`),
  runOrdinal: ordinal,
  providerTurnOrdinal: ordinal,
  attemptId: RunAttemptId.make(`cloud-attempt-${ordinal}`),
  rootNodeId: NodeId.make(`cloud-root-${ordinal}`),
  message: { ...input.message, messageId: MessageId.make(`cloud-message-${ordinal}`) },
});

it.live(
  "preflights a new task, warns about local changes, and attaches without sending a second prompt",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({ initialTask: null });
        yield* f.runtime.startTurn({
          ...f.turnInput,
          appContext: [{ key: "context", text: "Repository context" }],
        });
        expect((yield* f.terminal).status).toBe("completed");
        const calls = yield* f.calls;
        expect(calls.filter((call) => call.git).map((call) => call.git)).toEqual([
          ["symbolic-ref", "--quiet", "--short", "HEAD"],
          ["rev-parse", "HEAD"],
          ["ls-remote", "--heads", "origin", "refs/heads/main"],
          ["status", "--porcelain", "--untracked-files=normal"],
        ]);
        expect(calls.filter((call) => call.args).map((call) => call.args)).toEqual([
          ["code", "new", "-", "--agent"],
          ["code", "resume", "task-123", "--agent", "--interactive"],
        ]);
        expect(calls.find((call) => call.prompt)?.prompt).toBe(
          "Repository context\n\nDo bounded work",
        );
        expect(calls.filter((call) => call.request && call.request.command !== "show")).toEqual([]);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text.includes("Local uncommitted changes were not uploaded."),
          ),
        ).toBe(true);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text.includes("New task is running"),
          ),
        ).toBe(true);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.nativeThreadRef?.nativeId === "task-123",
          ),
        ).toBe(true);
        expect(f.runtime.providerSession.status).toBe("ready");
      }),
    ),
);

it.live("rejects an unpublished new task before invoking the CodeRabbit CLI", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({ initialTask: null, unpublished: true });
      yield* f.runtime.startTurn(f.turnInput);
      const terminal = yield* f.terminal;
      expect(terminal.status).toBe("failed");
      expect(terminal.failure?.message).toContain("Push `main` first");
      expect((yield* f.calls).filter((call) => call.args)).toEqual([]);
    }),
  ),
);

it.live(
  "reuses the interactive process for followups and correlates terminal traces with admission",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message: `
      const operation=existsSync(signal('first-message'))?'op-second':'op-first';mark('first-message');
      trace('agent_message',{text:'Before admission '+operation});
      end('foreign-operation');
      ack(req,{queueItemId:operation});
      trace('user_message',{text:req.text,queueItemId:operation,source:'cli'});
      trace('agent_message',{text:'Admitted '+operation});
      mark(operation);await wait('finish-'+operation);end(operation);
    `,
        });
        yield* f.runtime.startTurn(f.turnInput);
        const first = yield* f.started;
        yield* f.assistant("Admitted op-first");
        expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
        yield* f.mark("finish-op-first");
        expect(yield* f.terminal).toMatchObject({ status: "completed", providerTurnId: first.id });
        yield* f.runtime.startTurn(nextTurn(f.turnInput, 2));
        const second = yield* f.started;
        yield* f.assistant("Admitted op-second");
        expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toHaveLength(1);
        yield* f.mark("finish-op-second");
        expect(yield* f.terminal).toMatchObject({
          status: "completed",
          providerTurnId: second.id,
          runOrdinal: 2,
        });
        const calls = yield* f.calls;
        expect(calls.filter((call) => call.args?.includes("--interactive"))).toHaveLength(1);
        expect(
          calls.filter((call) => call.args && !call.args.includes("--interactive")),
        ).toHaveLength(1);
        expect(
          calls.flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message", "message"]);
        expect(
          f.allEvents.filter(
            (event) =>
              event.type === "message.updated" && event.message.text.startsWith("Before admission"),
          ),
        ).toHaveLength(2);
        expect(first.id).not.toBe(second.id);
      }),
    ),
);

it.live(
  "rejects steering before admission and sends accepted steering over the same follower",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message:
            "mark('message');await wait('admit');ack(req);trace('agent_message',{text:'Message admitted'});",
          steer:
            "ack(req,{status:'steer_sent'});trace('steer_result',{result:'accepted'});trace('agent_message',{text:'Steering applied'});end();",
        });
        yield* f.runtime.startTurn(f.turnInput);
        const turn = yield* f.started;
        yield* f.wait("message");
        const steer = {
          threadId: THREAD,
          runId: f.turnInput.runId,
          providerThread: f.thread,
          providerTurnId: turn.id,
          message: { ...f.turnInput.message, text: "Focus on validation" },
        };
        expect(yield* f.runtime.steerTurn(steer).pipe(Effect.flip)).toMatchObject({
          _tag: "ProviderAdapterProtocolError",
        });
        expect((yield* f.calls).some((call) => call.request?.command === "steer")).toBe(false);
        yield* f.mark("admit");
        yield* f.assistant("Message admitted");
        yield* f.runtime.steerTurn(steer);
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          (yield* f.calls)
            .flatMap((call) =>
              call.request && call.request.command !== "show" ? [call.request] : [],
            )
            .map(({ command, text }) => ({ command, text })),
        ).toEqual([
          { command: "message", text: "Do bounded work" },
          { command: "steer", text: "Focus on validation" },
        ]);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "Steering applied",
          ),
        ).toBe(true);
      }),
    ),
);

it.live("surfaces steering rejection while leaving the active turn usable", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message: "ack(req);trace('agent_message',{text:'Message admitted'});",
        steer:
          "emit({phase:'code',type:'response',id:req.id,ok:false,status:'steer_rejected',message:'Agent is busy',error:{status:'steer_rejected',message:'Agent is busy'}});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const turn = yield* f.started;
      yield* f.assistant("Message admitted");
      expect(
        yield* f.runtime
          .steerTurn({
            threadId: THREAD,
            runId: f.turnInput.runId,
            providerThread: f.thread,
            providerTurnId: turn.id,
            message: f.turnInput.message,
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterSteerRunError" });
      expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
      yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: turn.id });
      expect((yield* f.terminal).status).toBe("cancelled");
    }),
  ),
);

it.live("answers a question over the follower and resolves each request, node, and item once", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message: "ack(req);emit(question);trace('question',{question:question.question});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const turn = yield* f.started;
      const pending = (yield* f.next(
        "runtime_request.updated",
        (event) => event.runtimeRequest.status === "pending",
      )).runtimeRequest;
      yield* f.next(
        "provider_session.updated",
        (event) => event.providerSession.status === "waiting",
      );
      expect(pending).toMatchObject({
        kind: "user_input",
        responseCapability: { type: "live", providerSessionId: SESSION },
        nativeRequestRef: { nativeId: "question-1" },
      });
      expect(
        yield* f.runtime
          .steerTurn({
            threadId: THREAD,
            runId: f.turnInput.runId,
            providerThread: f.thread,
            providerTurnId: turn.id,
            message: f.turnInput.message,
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      expect(
        yield* f.runtime
          .respondToRuntimeRequest({ requestId: pending.id, answers: { q1: "" } })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      expect(
        yield* f.runtime
          .respondToRuntimeRequest({ requestId: pending.id, answers: { q1: "A", unknown: "B" } })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      yield* f.runtime.respondToRuntimeRequest({
        requestId: pending.id,
        answers: { q1: ["Use option A"] },
      });
      expect((yield* f.terminal).status).toBe("completed");
      expect(
        (yield* f.calls).flatMap((call) =>
          call.request?.command === "answer" ? [call.request] : [],
        ),
      ).toEqual([
        {
          id: "r2",
          command: "answer",
          interactionId: "question-1",
          answers: { q1: "Use option A" },
        },
      ]);
      expect(
        f.allEvents.filter(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        ),
      ).toHaveLength(1);
      expect(
        f.allEvents.filter(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "resolved",
        ),
      ).toHaveLength(1);
      expect(
        f.allEvents.some(
          (event) =>
            event.type === "node.updated" &&
            event.node.id === pending.nodeId &&
            event.node.status === "completed",
        ),
      ).toBe(true);
      expect(
        f.allEvents.some(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "user_input_request" &&
            event.turnItem.requestId === pending.id &&
            event.turnItem.status === "completed",
        ),
      ).toBe(true);
      expect(
        yield* f.runtime
          .respondToRuntimeRequest({ requestId: pending.id, answers: { q1: "A" } })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
    }),
  ),
);

it.live(
  "keeps sensitive questions in the cloud and cancels their pending artifacts at termination",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message: `ack(req);emit({...question,question:{...question.question,questions:question.question.questions.map(q=>({...q,isSensitive:true}))}});`,
        });
        yield* f.runtime.startTurn(f.turnInput);
        const turn = yield* f.started;
        const pending = (yield* f.next("runtime_request.updated")).runtimeRequest;
        yield* f.assistant("Answer this question directly in CodeRabbit:");
        expect(pending.responseCapability).toMatchObject({ type: "not_resumable" });
        expect(
          yield* f.runtime
            .respondToRuntimeRequest({ requestId: pending.id, answers: { q1: "private answer" } })
            .pipe(Effect.flip),
        ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
        expect((yield* f.calls).some((call) => call.request?.command === "answer")).toBe(false);
        yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: turn.id });
        expect((yield* f.terminal).status).toBe("cancelled");
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "runtime_request.updated" &&
              event.runtimeRequest.id === pending.id &&
              event.runtimeRequest.status === "cancelled",
          ),
        ).toBe(true);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "node.updated" &&
              event.node.id === pending.nodeId &&
              event.node.status === "cancelled",
          ),
        ).toBe(true);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "user_input_request" &&
              event.turnItem.requestId === pending.id &&
              event.turnItem.status === "cancelled",
          ),
        ).toBe(true);
      }),
    ),
);

it.live("does not answer a question without a provider answer template", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({ message: "ack(req);emit({...question,answerTemplate:null});" });
      yield* f.runtime.startTurn(f.turnInput);
      const turn = yield* f.started;
      const pending = (yield* f.next("runtime_request.updated")).runtimeRequest;
      expect(pending.responseCapability.type).toBe("not_resumable");
      expect(
        yield* f.runtime
          .respondToRuntimeRequest({ requestId: pending.id, answers: { q1: "A" } })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: turn.id });
      yield* f.terminal;
      expect((yield* f.calls).some((call) => call.request?.command === "answer")).toBe(false);
    }),
  ),
);

it.live("treats cancel_pending as a receipt and waits for terminal confirmation", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message: "ack(req);trace('agent_message',{text:'Message admitted'});",
        cancel:
          "ack(req,{status:'cancel_pending'});trace('agent_message',{text:'Cancellation pending'});await wait('cancel-finish');end('op-1','canceled');",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const turn = yield* f.started;
      yield* f.assistant("Message admitted");
      yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: turn.id });
      yield* f.assistant("Cancellation pending");
      expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
      expect(f.runtime.providerSession.status).toBe("running");
      yield* f.mark("cancel-finish");
      expect((yield* f.terminal).status).toBe("cancelled");
      expect(f.runtime.providerSession.status).toBe("ready");
    }),
  ),
);

it.live("defers cancellation until the message is admitted", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message: "mark('message-received');await wait('admit');ack(req);",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const turn = yield* f.started;
      yield* f.wait("message-received");
      const cancelling = yield* Deferred.make<void>();
      const cancel = yield* Effect.gen(function* () {
        yield* Deferred.succeed(cancelling, undefined);
        yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: turn.id });
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(cancelling);
      expect(
        (yield* f.calls).flatMap((call) =>
          call.request && call.request.command !== "show" ? [call.request.command] : [],
        ),
      ).toEqual(["message"]);
      expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
      yield* f.mark("admit");
      yield* Fiber.join(cancel);
      expect((yield* f.terminal).status).toBe("cancelled");
      expect(
        (yield* f.calls).flatMap((call) =>
          call.request && call.request.command !== "show" ? [call.request.command] : [],
        ),
      ).toEqual(["message", "cancel"]);
    }),
  ),
);

it.live(
  "reads the latest terminal turn when attaching after a newly submitted task already finished",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({ initialTask: null, open: "", readyTask: task });
        yield* f.runtime.startTurn(f.turnInput);
        expect((yield* f.terminal).status).toBe("completed");
        expect((yield* f.calls).filter((call) => call.args).map((call) => call.args)).toEqual([
          ["code", "new", "-", "--agent"],
          ["code", "resume", "task-123", "--agent", "--interactive"],
          ["code", "resume", "task-123", "--agent"],
        ]);
        expect(
          f.allEvents.some(
            (event) => event.type === "message.updated" && event.message.text === "Done in cloud",
          ),
        ).toBe(true);
      }),
    ),
);

it.live("reads the correlated latest turn when task_updated reports terminal status", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        latest: "emit({...result,turn:{...result.turn,queueItemId:'op-1'}});",
        message:
          "ack(req);trace('agent_message',{text:'Message admitted'});await wait('task-finished');emit({phase:'code',type:'status',status:'task_updated',task:{...result.task,updatedAt:'2099-01-01T00:00:00Z'}});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      yield* f.assistant("Message admitted");
      expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
      yield* f.mark("task-finished");
      expect((yield* f.terminal).status).toBe("completed");
      expect(
        (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
      ).toHaveLength(2);
      expect(
        f.allEvents.some(
          (event) => event.type === "message.updated" && event.message.text === "Done in cloud",
        ),
      ).toBe(true);
    }),
  ),
);

it.live(
  "buffers a foreign web message and offers a continuation that consumes it without sending a prompt",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message: `
      ack(req);end();await wait('web-message');
      trace('user_message',{text:'Please continue from the web app',source:'web',queueItemId:'web-op'});
      trace('agent_message',{text:'Working for the web user'});
      trace('turn_end',{turn:{...result.turn,queueItemId:'web-op',startSeq:3,endSeq:seq+1,finalReply:'Web continuation completed'}});
    `,
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.terminal;
        assert(f.runtime.hasPendingBackgroundWork);
        assert(f.runtime.hasPendingBackgroundWorkForThread);
        expect(yield* f.runtime.hasPendingBackgroundWork).toBe(false);
        yield* f.mark("web-message");
        const wake = yield* Queue.take(f.wakes);
        expect(wake).toMatchObject({
          threadId: THREAD,
          providerThreadId: f.thread.id,
          delivery: "adapter_buffered",
          detail: null,
        });
        expect(yield* f.runtime.hasPendingBackgroundWork).toBe(true);
        expect(yield* f.runtime.hasPendingBackgroundWorkForThread(f.thread)).toBe(true);
        expect(
          yield* f.runtime.hasPendingBackgroundWorkForThread({
            ...f.thread,
            id: "another-thread" as typeof f.thread.id,
          }),
        ).toBe(false);
        assert(wake.dispatchIfCurrent);
        assert(wake.clearIfCurrent);
        expect(yield* wake.dispatchIfCurrent(Effect.succeed("dispatched"))).toEqual(
          Option.some("dispatched"),
        );
        const continuation = nextTurn(f.turnInput, 2);
        yield* f.runtime.startTurn({
          ...continuation,
          message: { ...continuation.message, createdBy: "agent", creationSource: "provider" },
        });
        expect((yield* f.terminal).status).toBe("completed");
        expect(yield* f.runtime.hasPendingBackgroundWork).toBe(false);
        expect(yield* wake.dispatchIfCurrent(Effect.succeed("stale"))).toEqual(Option.none());
        yield* wake.clearIfCurrent();
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
        expect(
          f.allEvents.flatMap((event) =>
            event.type === "message.updated" ? [event.message.text] : [],
          ),
        ).toContain("Message sent from the CodeRabbit web app: Please continue from the web app");
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "Working for the web user",
          ),
        ).toBe(true);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text === "Web continuation completed",
          ),
        ).toBe(true);
      }),
    ),
);

it.live("ignores unknown frames while retaining later known assistant and terminal events", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message: `
      emit({phase:'code',type:'future-session-status',payload:{secret:'not a conversation message'}});
      ack(req);
      trace('future-trace',{payload:'extension'});
      emit({phase:'future',type:'trace',kind:'agent_message',text:'Do not display'});
      trace('agent_message',{text:'Known assistant reply'});end();
    `,
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect((yield* f.terminal).status).toBe("completed");
      expect(
        f.allEvents.some(
          (event) =>
            event.type === "message.updated" && event.message.text === "Known assistant reply",
        ),
      ).toBe(true);
      expect(JSON.stringify(f.allEvents)).not.toContain("Do not display");
      expect(JSON.stringify(f.allEvents)).not.toContain("not a conversation message");
      expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toHaveLength(1);
    }),
  ),
);

it.live(
  "reattaches a restarted admitted turn without creating a task or resending its message",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          restart: true,
          savedHead: "persisted-operation",
          open: "trace('agent_message',{text:'Recovered existing task'});end('unrelated');end('persisted-operation');",
        });
        yield* f.runtime.startTurn(f.turnInput);
        expect((yield* f.terminal).status).toBe("completed");
        expect((yield* f.calls).filter((call) => call.args).map((call) => call.args)).toEqual([
          ["code", "resume", "task-123", "--agent", "--interactive"],
        ]);
        expect(
          (yield* f.calls).filter((call) => call.request && call.request.command !== "show"),
        ).toEqual([]);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "Recovered existing task",
          ),
        ).toBe(true);
      }),
    ),
);

it.live("recovers an initially submitted turn from terminal attach history", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        restart: true,
        savedHead: "initial:previous-attempt",
        readyTask: task,
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect((yield* f.terminal).status).toBe("completed");
      expect((yield* f.calls).filter((call) => call.args).map((call) => call.args)).toEqual([
        ["code", "resume", "task-123", "--agent", "--interactive"],
        ["code", "resume", "task-123", "--agent"],
      ]);
      expect(
        (yield* f.calls).filter((call) => call.request && call.request.command !== "show"),
      ).toEqual([]);
    }),
  ),
);

it.live("fails an ambiguous restart without resending or cancelling possibly admitted work", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({ restart: true, savedHead: "pending:previous-attempt" });
      yield* f.runtime.startTurn(f.turnInput);
      const terminal = yield* f.terminal;
      expect(terminal.status).toBe("failed");
      expect(terminal.failure?.message).toContain(
        "message delivery was not confirmed before the restart",
      );
      expect(yield* f.calls).toEqual([]);
    }),
  ),
);

it.live("shows imported task history only before its first local message", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.runtime.startTurn(f.turnInput);
      yield* f.terminal;
      yield* f.runtime.startTurn(nextTurn(f.turnInput, 2));
      yield* f.terminal;
      const previews = f.allEvents.filter(
        (event) =>
          event.type === "message.updated" &&
          event.message.text.startsWith("Latest reply from CodeRabbit:"),
      );
      expect(previews).toHaveLength(1);
      expect(previews[0]).toMatchObject({
        message: {
          text: `Latest reply from CodeRabbit: Done in cloud\n\nEarlier history is only in the web app: ${task.url}`,
        },
      });
      const calls = yield* f.calls;
      expect(
        calls.filter((call) => call.args && !call.args.includes("--interactive")),
      ).toHaveLength(1);
      expect(calls.filter((call) => call.args?.includes("--interactive"))).toHaveLength(1);
      expect(calls.filter((call) => call.request?.command === "message")).toHaveLength(2);
    }),
  ),
);

it.live(
  "rejects unsupported attachments, runtime modes, models, and invalid prompts before invoking the CLI",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture();
        const attachment = {
          type: "file",
          id: ChatAttachmentId.make("attachment-1"),
          name: "notes.txt",
          mimeType: "text/plain",
          sizeBytes: 1,
        };
        const inputs: ProviderAdapter.ProviderAdapterV2TurnInput[] = [
          { ...f.turnInput, message: { ...f.turnInput.message, attachments: [attachment] } },
          {
            ...f.turnInput,
            runtimePolicy: { ...f.runtimePolicy, runtimeMode: "approval-required" },
          },
          { ...f.turnInput, runtimePolicy: { ...f.runtimePolicy, interactionMode: "plan" } },
          { ...f.turnInput, modelSelection: { ...modelSelection, model: "other-model" } },
          { ...f.turnInput, message: { ...f.turnInput.message, text: " \n\t" } },
          { ...f.turnInput, message: { ...f.turnInput.message, text: "a".repeat(65_537) } },
          { ...f.turnInput, appContext: [{ key: "context", text: "a".repeat(65_536) }] },
        ];
        for (const input of inputs) {
          expect(yield* f.runtime.startTurn(input).pipe(Effect.flip)).toMatchObject({
            _tag: "ProviderAdapterProtocolError",
          });
        }
        expect(
          yield* f.adapter
            .openSession({
              threadId: THREAD,
              providerSessionId: SESSION,
              modelSelection,
              runtimePolicy: { ...f.runtimePolicy, runtimeMode: "approval-required" },
            })
            .pipe(Effect.flip),
        ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
        expect(
          yield* f.adapter
            .openSession({
              threadId: THREAD,
              providerSessionId: SESSION,
              modelSelection,
              runtimePolicy: { ...f.runtimePolicy, cwd: null },
            })
            .pipe(Effect.flip),
        ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
        expect(yield* f.calls).toEqual([]);
        expect(f.allEvents.filter((event) => event.type === "provider_turn.updated")).toEqual([]);
      }),
    ),
);

it.live("classifies quota failures and emits the matching error item and terminal failure", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message:
          "ack(req);trace('error',{message:'Monthly usage limit reached; update billing',retryable:false});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const terminal = yield* f.terminal;
      expect(terminal).toMatchObject({
        status: "failed",
        failure: { class: "usage_limit", retryable: false },
        threadDisposition: "reusable",
      });
      assert(terminal.status === "failed");
      const error = f.allEvents.find(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      expect(error).toMatchObject({
        turnItem: {
          type: "error",
          status: "failed",
          ordinal: terminal.failureItemOrdinal,
          failure: terminal.failure,
        },
      });
      expect(f.runtime.providerSession.status).toBe("error");
    }),
  ),
);

it.live(
  "handles rejected message admission without interpreting the response as an unknown frame",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message:
            "emit({phase:'code',type:'response',id:req.id,ok:false,error:{status:'quota_exceeded',message:'Quota exceeded'}});",
        });
        yield* f.runtime.startTurn(f.turnInput);
        expect(yield* f.terminal).toMatchObject({
          status: "failed",
          failure: { class: "usage_limit", message: "Quota exceeded" },
        });
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
      }),
    ),
);

it.live(
  "reconnects after EOF without replaying a message and ignores duplicate trace sequence numbers",
  () =>
    live(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const reconnecting = yield* Deferred.make<void>();
        const observedClock: Clock.Clock = {
          ...clock,
          sleep: (duration) =>
            Effect.gen(function* () {
              if (Duration.toMillis(duration) === 2_000)
                yield* Deferred.succeed(reconnecting, undefined);
              yield* clock.sleep(duration);
            }),
        };
        yield* Effect.gen(function* () {
          const f = yield* fixture({
            open: "if(existsSync(signal('disconnected'))){trace('agent_message',{text:'Duplicate trace'});trace('agent_message',{text:'Recovered connection'});end();}",
            message:
              "ack(req);trace('agent_message',{text:'Original connection'});mark('disconnected');process.stdout.end(()=>process.exit(0));",
          });
          yield* f.runtime.startTurn(f.turnInput);
          yield* f.assistant("Original connection");
          yield* Deferred.await(reconnecting);
          yield* clock.adjust("2 seconds");
          expect((yield* f.terminal).status).toBe("completed");
          expect(
            (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
          ).toHaveLength(2);
          expect(
            (yield* f.calls).flatMap((call) =>
              call.request && call.request.command !== "show" ? [call.request.command] : [],
            ),
          ).toEqual(["message"]);
          expect(
            f.allEvents.some(
              (event) =>
                event.type === "message.updated" && event.message.text === "Recovered connection",
            ),
          ).toBe(true);
          expect(
            f.allEvents.some(
              (event) =>
                event.type === "message.updated" && event.message.text === "Duplicate trace",
            ),
          ).toBe(false);
        }).pipe(Effect.provideService(Clock.Clock, observedClock));
      }),
    ),
);

it.live("resumes a persisted running session without issuing a new message", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        resumeStatus: "running",
        savedHead: "saved-operation",
        open: "end('saved-operation');",
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect((yield* f.terminal).status).toBe("completed");
      expect((yield* f.calls).filter((call) => call.args).map((call) => call.args)).toEqual([
        ["code", "resume", "task-123", "--agent", "--interactive"],
      ]);
      expect(
        (yield* f.calls).filter((call) => call.request && call.request.command !== "show"),
      ).toEqual([]);
    }),
  ),
);

it.live("clears an abandoned pending web continuation and invalidates its dispatch", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message:
          "ack(req);end();trace('user_message',{text:'Continue elsewhere',queueItemId:'web-operation',source:'web'});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      yield* f.terminal;
      const wake = yield* Queue.take(f.wakes);
      assert(wake.clearIfCurrent);
      assert(wake.dispatchIfCurrent);
      assert(f.runtime.hasPendingBackgroundWork);
      yield* wake.clearIfCurrent();
      expect(yield* f.runtime.hasPendingBackgroundWork).toBe(false);
      expect(f.runtime.providerSession.status).toBe("ready");
      const dispatched = yield* Deferred.make<void>();
      expect(yield* wake.dispatchIfCurrent(Deferred.succeed(dispatched, undefined))).toEqual(
        Option.none(),
      );
      expect(yield* Deferred.isDone(dispatched)).toBe(false);
      const continuation = nextTurn(f.turnInput, 2);
      expect(
        yield* f.runtime
          .startTurn({
            ...continuation,
            message: { ...continuation.message, createdBy: "agent", creationSource: "provider" },
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      expect(
        (yield* f.calls).flatMap((call) =>
          call.request && call.request.command !== "show" ? [call.request.command] : [],
        ),
      ).toEqual(["message"]);
    }),
  ),
);

it.live(
  "retains a foreign message queued behind an active turn and offers it after that turn finishes",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          latest: "emit({...result,turn:{...result.turn,queueItemId:'operation-A'}});",
          message: `
      ack(req,{queueItemId:'operation-A'});
      trace('user_message',{text:req.text,queueItemId:'operation-A',source:'cli'});
      trace('agent_message',{text:'Turn A admitted'});
      await wait('queue-B');
      trace('user_message',{text:'Queued web request B',queueItemId:'operation-B',source:'web'});
      trace('agent_message',{text:'Turn A completed'});
      end('operation-A');
      await wait('continue-B');
      emit({phase:'code',type:'status',status:'task_updated',task:result.task});
      trace('agent_message',{text:'Turn B output'});
      await wait('finish-B');
      trace('turn_end',{turn:{...result.turn,queueItemId:'operation-B',startSeq:6,endSeq:seq+1,finalReply:'Turn B completed'}});
    `,
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.assistant("Turn A admitted");
        yield* f.mark("queue-B");
        expect((yield* f.terminal).status).toBe("completed");
        const wake = yield* Queue.take(f.wakes);
        expect(wake.delivery).toBe("adapter_buffered");
        assert(f.runtime.hasPendingBackgroundWork);
        expect(yield* f.runtime.hasPendingBackgroundWork).toBe(true);
        const continuation = nextTurn(f.turnInput, 2);
        yield* f.runtime.startTurn({
          ...continuation,
          message: { ...continuation.message, createdBy: "agent", creationSource: "provider" },
        });
        yield* f.mark("continue-B");
        yield* f.assistant("Turn B output");
        expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toHaveLength(1);
        yield* f.mark("finish-B");
        expect((yield* f.terminal).status).toBe("completed");
        const texts = f.allEvents.flatMap((event) =>
          event.type === "message.updated" ? [event.message.text] : [],
        );
        expect(texts).toContain("Message sent from the CodeRabbit web app: Queued web request B");
        expect(texts).toContain("Turn B output");
        expect(texts).toContain("Turn B completed");
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
        expect(yield* f.runtime.hasPendingBackgroundWork).toBe(false);
      }),
    ),
);

it.live(
  "does not resolve a new question when the previous question's answer receipt arrives late",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message: "ack(req);emit(question);",
          answer: `
        if(req.interactionId==='question-1'){
          trace('answer_submitted',{interactionId:'question-1'});
          emit({...question,question:{...question.question,interactionId:'question-2'},answerTemplate:{...question.answerTemplate,interactionId:'question-2'}});
          await wait('answer-A-receipt');ack(req,{status:'answer_sent'});mark('answer-A-receipt-sent');
        }else{
          ack(req,{status:'answer_sent'});trace('answer_submitted',{interactionId:'question-2'});end();
        }
      `,
        });
        yield* f.runtime.startTurn(f.turnInput);
        const questionA = (yield* f.next(
          "runtime_request.updated",
          (event) => event.runtimeRequest.status === "pending",
        )).runtimeRequest;
        const answerA = yield* f.runtime
          .respondToRuntimeRequest({ requestId: questionA.id, answers: { q1: "A" } })
          .pipe(Effect.forkScoped);
        const questionB = (yield* f.next(
          "runtime_request.updated",
          (event) =>
            event.runtimeRequest.status === "pending" &&
            event.runtimeRequest.nativeRequestRef?.nativeId === "question-2",
        )).runtimeRequest;
        yield* f.next(
          "provider_session.updated",
          (event) => event.providerSession.status === "waiting",
        );
        yield* f.mark("answer-A-receipt");
        yield* f.wait("answer-A-receipt-sent");
        yield* Fiber.join(answerA);
        expect(
          f.allEvents.flatMap((event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.id === questionB.id
              ? [event.runtimeRequest.status]
              : [],
          ),
        ).toEqual(["pending"]);
        expect(f.runtime.providerSession.status).toBe("waiting");
        yield* f.runtime.respondToRuntimeRequest({ requestId: questionB.id, answers: { q1: "B" } });
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request?.command === "answer" ? [call.request.interactionId] : [],
          ),
        ).toEqual(["question-1", "question-2"]);
      }),
    ),
);

it.live("waits for the single reconnect follower to become ready before sending Stop", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const backoff = yield* Deferred.make<void>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            if (Duration.toMillis(duration) === 2_000) yield* Deferred.succeed(backoff, undefined);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          beforeReady:
            "if(existsSync(signal('disconnected'))){mark('reconnect-spawned');await wait('reconnect-ready');seq=1;}",
          message:
            "ack(req);trace('agent_message',{text:'Message admitted before disconnect'});mark('disconnected');process.stdout.end(()=>process.exit(0));",
        });
        yield* f.runtime.startTurn(f.turnInput);
        const turn = yield* f.started;
        yield* f.assistant("Message admitted before disconnect");
        yield* Deferred.await(backoff);
        const stopping = yield* Deferred.make<void>();
        const stop = yield* Effect.gen(function* () {
          yield* Deferred.succeed(stopping, undefined);
          yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: turn.id });
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(stopping);
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(1);
        yield* clock.adjust("2 seconds");
        yield* f.wait("reconnect-spawned");
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(2);
        expect((yield* f.calls).some((call) => call.request?.command === "cancel")).toBe(false);
        expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
        yield* f.mark("reconnect-ready");
        yield* Fiber.join(stop);
        expect((yield* f.terminal).status).toBe("cancelled");
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(2);
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message", "cancel"]);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live("fails unconfirmed admission after transport loss and blocks the next prompt", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message: "process.stdout.end(()=>process.exit(1));",
        show: "ack(req,{task:readyTask});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const terminal = yield* f.terminal;
      expect(terminal.status).toBe("failed");
      expect(terminal.failure?.message).toMatch(/delivery.{0,40}(not confirmed|unconfirmed)/i);
      const error = yield* f.runtime.startTurn(nextTurn(f.turnInput, 2)).pipe(Effect.flip);
      expect(error).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      expect(
        (yield* f.calls).flatMap((call) =>
          call.request && call.request.command !== "show" ? [call.request.command] : [],
        ),
      ).toEqual(["message"]);
    }),
  ),
);

it.live("fails unconfirmed admission after the request deadline and blocks the next prompt", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          message:
            "if(!existsSync(signal('message-received'))){mark('message-received');await wait('never-admit');}else{ack(req);end();}",
          show: "ack(req,{task:readyTask});",
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.wait("message-received");
        yield* clock.adjust("60 seconds");
        const terminal = yield* f.terminal;
        expect(terminal.status).toBe("failed");
        expect(terminal.failure?.message).toMatch(/delivery.{0,40}(not confirmed|unconfirmed)/i);
        expect(
          yield* f.runtime.startTurn(nextTurn(f.turnInput, 2)).pipe(Effect.flip),
        ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
        yield* f.runtime.startTurn(nextTurn(f.turnInput, 3));
        expect((yield* f.terminal).status).toBe("completed");
        expect((yield* f.calls).filter((call) => call.request?.command === "message")).toHaveLength(
          2,
        );
      }).pipe(Effect.provideService(Clock.Clock, clock));
    }),
  ),
);

it.live.each(["proven-operation", null])(
  "recovers lost admission from the exact buffered user-message trace without resending it (%s)",
  (operationId) =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message: `
      trace('user_message',{text:req.text,queueItemId:${JSON.stringify(operationId)},source:'cli'});
      trace('agent_message',{text:'The admitted work completed'});
      end(${JSON.stringify(operationId)});process.stdout.end(()=>process.exit(0));
    `,
        });
        yield* f.runtime.startTurn(f.turnInput);
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text === "The admitted work completed",
          ),
        ).toBe(true);
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "provider_thread.updated" &&
              event.providerThread.nativeConversationHeadRef?.nativeId ===
                (operationId ?? "accepted:1"),
          ),
        ).toBe(true);
      }),
    ),
);

it.live("does not treat another user's buffered message as proof of local admission", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message:
          "trace('user_message',{text:'Someone else sent this',queueItemId:'foreign-operation',source:'web'});process.stdout.end(()=>process.exit(0));",
        show: "ack(req,{task:readyTask});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect(yield* f.terminal).toMatchObject({
        status: "failed",
        failure: { message: expect.stringMatching(/delivery.{0,40}(not confirmed|unconfirmed)/i) },
      });
      expect(yield* f.runtime.startTurn(nextTurn(f.turnInput, 2)).pipe(Effect.flip)).toMatchObject({
        _tag: "ProviderAdapterProtocolError",
      });
      expect(
        (yield* f.calls).flatMap((call) =>
          call.request && call.request.command !== "show" ? [call.request.command] : [],
        ),
      ).toEqual(["message"]);
    }),
  ),
);

it.live("preserves the unconfirmed-delivery guard when restoring a previously running thread", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({ resumeStatus: "running", show: "ack(req,{task:readyTask});" });
      const thread = yield* f.runtime.ensureThread({
        threadId: THREAD,
        modelSelection,
        runtimePolicy: f.runtimePolicy,
        existingProviderThread: {
          ...f.thread,
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          nativeConversationHeadRef: {
            driver: f.runtime.driver,
            nativeId: "pending:previous-attempt",
            strength: "strong",
          },
        },
      });
      expect(
        yield* f.runtime.startTurn({ ...f.turnInput, providerThread: thread }).pipe(Effect.flip),
      ).toMatchObject({ _tag: "ProviderAdapterProtocolError" });
      expect((yield* f.calls).filter((call) => call.request?.command === "message")).toEqual([]);
      expect((yield* f.calls).some((call) => call.request?.command === "show")).toBe(true);
      yield* f.runtime.startTurn({ ...nextTurn(f.turnInput, 2), providerThread: thread });
      expect((yield* f.terminal).status).toBe("completed");
    }),
  ),
);

it.live("exhausts exactly three reconnect retries at virtual two, four, and eight seconds", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const backoffs = yield* Queue.unbounded<number>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            const milliseconds = Duration.toMillis(duration);
            if ([2_000, 4_000, 8_000].includes(milliseconds))
              yield* Queue.offer(backoffs, milliseconds);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          initialTask: null,
          beforeReady: "process.stdout.end(()=>process.exit(1));",
        });
        yield* f.runtime.startTurn(f.turnInput);
        for (const milliseconds of [2_000, 4_000, 8_000]) {
          expect(yield* Queue.take(backoffs)).toBe(milliseconds);
          yield* clock.adjust(milliseconds);
        }
        const terminal = yield* f.terminal;
        expect(terminal.status).toBe("failed");
        expect(terminal.failure?.message).toContain(task.url);
        expect(terminal.failure?.message).toContain("connection lost");
        const calls = yield* f.calls;
        expect(calls.filter((call) => call.args?.includes("--interactive"))).toHaveLength(4);
        expect(calls.filter((call) => call.args?.[1] === "new")).toHaveLength(1);
        expect(calls.filter((call) => call.request && call.request.command !== "show")).toEqual([]);
        expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toHaveLength(1);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live("keeps the original message waiter when the first child exits before readiness", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const backoff = yield* Deferred.make<void>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            if (Duration.toMillis(duration) === 2_000) yield* Deferred.succeed(backoff, undefined);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          beforeReady:
            "if(!existsSync(signal('first-attach'))){mark('first-attach');process.exit(1);}",
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* Deferred.await(backoff);
        expect(
          (yield* f.calls).filter((call) => call.request && call.request.command !== "show"),
        ).toEqual([]);
        yield* clock.adjust("2 seconds");
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(2);
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live("dispatches two complete buffered web turns as separate continuations", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const buffered = yield* Deferred.make<void>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            if (Duration.toMillis(duration) === 2_000) yield* Deferred.succeed(buffered, undefined);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          message: `
        ack(req);end();await wait('web-batch');
        trace('user_message',{text:'Web request B',queueItemId:'operation-B',source:'web'});
        trace('agent_message',{text:'Web output B'});
        trace('turn_end',{turn:{...result.turn,queueItemId:'operation-B',startSeq:2,endSeq:seq+1,finalReply:'B completed'}});
        trace('user_message',{text:'Web request C',queueItemId:'operation-C',source:'web'});
        trace('agent_message',{text:'Web output C'});
        trace('turn_end',{turn:{...result.turn,queueItemId:'operation-C',startSeq:5,endSeq:seq+1,finalReply:'C completed'}});
        process.stdout.end(()=>process.exit(0));
      `,
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.terminal;
        yield* f.mark("web-batch");
        const wakeB = yield* Queue.take(f.wakes);
        // EOF is observed after every preceding frame has reached the adapter's buffer.
        yield* Deferred.await(buffered);
        expect(wakeB.delivery).toBe("adapter_buffered");
        const turnB = nextTurn(f.turnInput, 2);
        yield* f.runtime.startTurn({
          ...turnB,
          message: { ...turnB.message, createdBy: "agent", creationSource: "provider" },
        });
        expect(yield* f.terminal).toMatchObject({ status: "completed", runOrdinal: 2 });
        const wakeC = yield* Queue.take(f.wakes);
        expect(wakeC.delivery).toBe("adapter_buffered");
        const turnC = nextTurn(f.turnInput, 3);
        yield* f.runtime.startTurn({
          ...turnC,
          message: { ...turnC.message, createdBy: "agent", creationSource: "provider" },
        });
        expect(yield* f.terminal).toMatchObject({ status: "completed", runOrdinal: 3 });
        const messages = f.allEvents.flatMap((event) =>
          event.type === "message.updated" ? [event.message] : [],
        );
        expect(messages).toContainEqual(
          expect.objectContaining({ runId: turnB.runId, text: "Web output B" }),
        );
        expect(messages).toContainEqual(
          expect.objectContaining({ runId: turnB.runId, text: "B completed" }),
        );
        expect(messages).toContainEqual(
          expect.objectContaining({ runId: turnC.runId, text: "Web output C" }),
        );
        expect(messages).toContainEqual(
          expect.objectContaining({ runId: turnC.runId, text: "C completed" }),
        );
        expect(
          messages.some(
            (message) => message.runId === turnB.runId && message.text.includes("Web request C"),
          ),
        ).toBe(false);
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live("reconnects an idle task after EOF and observes the next web wake", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const backoff = yield* Deferred.make<void>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            if (Duration.toMillis(duration) === 2_000) yield* Deferred.succeed(backoff, undefined);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          open: `if(existsSync(signal('idle-disconnected'))){
          seq=2;trace('user_message',{text:'Continue after idle disconnect',queueItemId:'web-operation',source:'web'});
          trace('agent_message',{text:'Idle reconnect observed this work'});
          trace('turn_end',{turn:{...result.turn,queueItemId:'web-operation',startSeq:3,endSeq:seq+1}});
        }`,
          message:
            "ack(req);trace('agent_message',{text:'Local work'});end();await wait('disconnect-idle');mark('idle-disconnected');process.stdout.end(()=>process.exit(0));",
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.terminal;
        expect(f.runtime.providerSession.status).toBe("ready");
        yield* f.mark("disconnect-idle");
        yield* Deferred.await(backoff);
        yield* clock.adjust("2 seconds");
        expect((yield* Queue.take(f.wakes)).delivery).toBe("adapter_buffered");
        const continuation = nextTurn(f.turnInput, 2);
        yield* f.runtime.startTurn({
          ...continuation,
          message: { ...continuation.message, createdBy: "agent", creationSource: "provider" },
        });
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" &&
              event.message.text === "Idle reconnect observed this work",
          ),
        ).toBe(true);
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(2);
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live(
  "restores the saved conversation head after preview failure so reopening does not fence unsent work",
  () =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          savedHead: "previous-operation",
          latest:
            "emit({phase:'code',type:'error',status:'preview_unavailable',message:'Preview unavailable'});process.exit(1);",
        });
        yield* f.runtime.startTurn(f.turnInput);
        expect((yield* f.terminal).status).toBe("failed");
        expect(
          (yield* f.calls).filter((call) => call.request && call.request.command !== "show"),
        ).toEqual([]);
        const savedThread = f.allEvents
          .flatMap((event) =>
            event.type === "provider_thread.updated" ? [event.providerThread] : [],
          )
          .at(-1);
        assert(savedThread);
        expect(savedThread.nativeConversationHeadRef?.nativeId).toBe("previous-operation");
        const reopened = yield* f.adapter.openSession({
          threadId: THREAD,
          providerSessionId: ProviderSessionId.make("reopened-session"),
          modelSelection,
          runtimePolicy: f.runtimePolicy,
        });
        const terminal = yield* reopened.events.pipe(
          Stream.filter((event) => event.type === "turn.terminal"),
          Stream.runHead,
          Effect.forkScoped,
        );
        const thread = yield* reopened.ensureThread({
          threadId: THREAD,
          modelSelection,
          runtimePolicy: f.runtimePolicy,
          existingProviderThread: savedThread,
        });
        yield* reopened.startTurn({ ...nextTurn(f.turnInput, 2), providerThread: thread });
        const result = yield* Fiber.join(terminal);
        assert(Option.isSome(result));
        expect(result.value.status).toBe("completed");
        expect(
          (yield* f.calls).flatMap((call) =>
            call.request && call.request.command !== "show" ? [call.request.command] : [],
          ),
        ).toEqual(["message"]);
        expect(
          (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
        ).toHaveLength(1);
      }),
    ),
);

it.live("cleans up an EOF-stuck process before spawning the next connection attempt", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const backoff = yield* Deferred.make<void>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            if (Duration.toMillis(duration) === 2_000) yield* Deferred.succeed(backoff, undefined);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          initialTask: null,
          beforeReady: `if(existsSync(signal('stuck-pid'))){
          try{process.kill(Number(readFileSync(signal('stuck-pid'),'utf8')),0);mark('previous-process-alive');}catch{}
        }`,
          open: `if(!existsSync(signal('stuck-pid'))){
          writeFileSync(signal('stuck-pid'),String(process.pid));
          watch(${JSON.stringify(".")},()=>{});process.stdout.end();
        }else if(existsSync(signal('previous-process-alive'))){
          emit({phase:'code',type:'error',status:'process_overlap',message:'Previous process survived into the next connection attempt'});
        }else{
          trace('agent_message',{text:'Cleanup preceded retry'});end(null);
        }`,
          eof: "if(Number(readFileSync(signal('stuck-pid'),'utf8'))===process.pid){mark('stuck-eof');}else{process.exit(0);}",
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.wait("stuck-eof");
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(1);
        yield* clock.adjust("60 seconds");
        yield* Deferred.await(backoff);
        yield* clock.adjust("2 seconds");
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          f.allEvents.some(
            (event) =>
              event.type === "message.updated" && event.message.text === "Cleanup preceded retry",
          ),
        ).toBe(true);
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(2);
        expect(
          (yield* f.calls).filter((call) => call.request && call.request.command !== "show"),
        ).toEqual([]);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live("resets the retry budget after every successful ready connection", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      const backoffs = yield* Queue.unbounded<number>();
      const observedClock: Clock.Clock = {
        ...clock,
        sleep: (duration) =>
          Effect.gen(function* () {
            const ms = Duration.toMillis(duration);
            if (ms < 60_000) yield* Queue.offer(backoffs, ms);
            yield* clock.sleep(duration);
          }),
      };
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          beforeReady:
            "const generation=existsSync(signal('generation'))?Number(readFileSync(signal('generation'),'utf8'))+1:0;writeFileSync(signal('generation'),String(generation));seq=generation;",
          open: "if(generation>0){trace('agent_message',{text:'Ready generation '+generation});if(generation===5)end();else{await wait('disconnect-'+generation);process.stdout.end(()=>process.exit(0));}}",
          message:
            "ack(req);trace('agent_message',{text:'Ready generation 0'});await wait('disconnect-0');process.stdout.end(()=>process.exit(0));",
        });
        yield* f.runtime.startTurn(f.turnInput);
        for (let generation = 0; generation < 5; generation++) {
          yield* f.assistant("Ready generation " + generation);
          yield* f.mark("disconnect-" + generation);
          expect(yield* Queue.take(backoffs)).toBe(2_000);
          yield* clock.adjust("2 seconds");
        }
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          (yield* f.calls).filter((call) => call.args?.includes("--interactive")),
        ).toHaveLength(6);
      }).pipe(Effect.provideService(Clock.Clock, observedClock));
    }),
  ),
);

it.live("skips stale terminal statuses buffered before message admission", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        readyTask: task,
        message:
          "for(let i=0;i<3;i++)emit({phase:'code',type:'status',status:'task_updated',task:result.task});ack(req);trace('user_message',{text:req.text,queueItemId:'op-1',source:'cli'});trace('agent_message',{text:'Admitted after stale statuses'});await wait('finish');end();",
      });
      yield* f.runtime.startTurn(f.turnInput);
      yield* f.assistant("Admitted after stale statuses");
      expect(
        (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
      ).toHaveLength(1);
      expect(f.runtime.providerSession.status).toBe("running");
      yield* f.mark("finish");
      expect((yield* f.terminal).status).toBe("completed");
    }),
  ),
);

it.live("latest failure is deduplicated and leaves the turn and follower running", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        latest:
          "if(!existsSync(signal('previewed'))){mark('previewed');emit(result);}else{mark('latest-requested');await wait('latest-fail');process.exit(1);}",
        message:
          "ack(req);trace('agent_message',{text:'Admitted'});await wait('terminal-status');for(let i=0;i<3;i++)emit({phase:'code',type:'status',status:'task_updated',task:{...result.task,updatedAt:'2099-01-01T00:00:00Z'}});await wait('after-latest');trace('agent_message',{text:'Follower survived latest failure'});await wait('finish');end();",
      });
      yield* f.runtime.startTurn(f.turnInput);
      yield* f.assistant("Admitted");
      yield* f.mark("terminal-status");
      yield* f.wait("latest-requested");
      expect(
        (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
      ).toHaveLength(2);
      yield* f.mark("latest-fail");
      yield* f.mark("after-latest");
      yield* f.assistant("Follower survived latest failure");
      expect(f.runtime.providerSession.status).toBe("running");
      expect(f.allEvents.filter((event) => event.type === "turn.terminal")).toEqual([]);
      yield* f.mark("finish");
      expect((yield* f.terminal).status).toBe("completed");
      expect((yield* f.calls).filter((call) => call.args?.includes("--interactive"))).toHaveLength(
        1,
      );
    }),
  ),
);

it.live("correlates queue ids when the client operation id is distinct", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        message:
          "ack(req,{clientOperationId:'different-client-id',queueItemId:'queue-id'});trace('user_message',{text:req.text,queueItemId:'queue-id',source:'cli'});end('queue-id');",
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect((yield* f.terminal).status).toBe("completed");
      expect(yield* Queue.size(f.wakes)).toBe(0);
      expect(
        f.allEvents.some(
          (event) =>
            event.type === "provider_thread.updated" &&
            event.providerThread.nativeConversationHeadRef?.nativeId === "queue-id",
        ),
      ).toBe(true);
    }),
  ),
);

it.live.each(["trace", "latest", "probe"])(
  "settles a status-only wake via %s despite an earlier turn start",
  (mode) =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          message:
            "ack(req);end();await wait('wake');seq=20;emit({phase:'code',type:'status',status:'task_updated',task:{...readyTask," +
            (mode === "probe" ? "" : "updatedAt:'2099-01-01T00:00:00Z'") +
            "}});await wait('finish-web');" +
            (mode === "trace"
              ? "trace('turn_end',{turn:{...result.turn,queueItemId:'web-queue',startSeq:1,endSeq:21}});"
              : "emit({phase:'code',type:'status',status:'task_updated',task:{...result.task,updatedAt:'2099-01-02T00:00:00Z'}});"),
          latest:
            "emit({...result,turn:{...result.turn,queueItemId:'web-queue',startSeq:1,endSeq:21}});",
        });
        yield* f.runtime.startTurn(f.turnInput);
        expect((yield* f.terminal).status).toBe("completed");
        yield* f.mark("wake");
        yield* Queue.take(f.wakes);
        const next = nextTurn(f.turnInput, 2);
        yield* f.runtime.startTurn({
          ...next,
          message: { ...next.message, createdBy: "agent", creationSource: "provider" },
        });
        yield* f.mark("finish-web");
        expect((yield* f.terminal).status).toBe("completed");
      }),
    ),
);

it.live("ignores a trailing running status from the just-settled turn", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        latest: "emit({...result,turn:{...result.turn,queueItemId:'op-1',endSeq:1}});",
        message:
          "ack(req);end();emit({phase:'code',type:'status',status:'task_updated',task:readyTask});emit({phase:'code',type:'trace',kind:'agent_message',taskId:'task-123',seq:3,createdAt:'2026-10-16T00:00:00Z',text:'trailing processed'});mark('trailing');",
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect((yield* f.terminal).status).toBe("completed");
      yield* f.wait("trailing");
      // A following command response proves the stdout consumer passed the trailing status.
      yield* f.runtime.startTurn(nextTurn(f.turnInput, 2));
      expect((yield* f.terminal).status).toBe("completed");
      expect(yield* Queue.size(f.wakes)).toBe(0);
    }),
  ),
);

it.live("Stop during workspace preflight cancels without creating a cloud task", () =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        initialTask: null,
        preflight: "if(args[0]==='ls-remote'){mark('preflight');await wait('release-preflight');}",
      });
      yield* f.runtime.startTurn(f.turnInput);
      const started = yield* f.started;
      yield* f.wait("preflight");
      yield* f.runtime.interruptTurn({ providerThread: f.thread, providerTurnId: started.id });
      expect((yield* f.terminal).status).toBe("cancelled");
      yield* f.mark("release-preflight");
      expect((yield* f.calls).filter((call) => call.args)).toEqual([]);
    }),
  ),
);

it.live.each([true, false])("refreshes delivery metadata with show (success=%s)", (success) =>
  live(
    Effect.gen(function* () {
      const f = yield* fixture({
        show: success
          ? "ack(req,{task:{...result.task,workingBranchName:'fresh-branch',patchStatus:'fresh-patch',deliveryStatus:'delivered'}});"
          : "emit({phase:'code',type:'response',id:req.id,ok:false,error:{status:'unavailable',message:'Show failed'}});",
      });
      yield* f.runtime.startTurn(f.turnInput);
      expect((yield* f.terminal).status).toBe("completed");
      const delivery = f.allEvents.find(
        (event) =>
          event.type === "message.updated" && event.message.text.includes("Working branch:"),
      );
      assert(delivery?.type === "message.updated");
      expect(delivery.message.text).toContain(
        success ? "Working branch: fresh-branch" : "Working branch: coderabbit/test",
      );
      expect(delivery.message.text).toContain(
        success ? "delivery: delivered" : "delivery: pending",
      );
      expect((yield* f.calls).filter((call) => call.request?.command === "show")).toHaveLength(1);
    }),
  ),
);

it.live("resolves unconfirmed admission from an idle show and latest before the next prompt", () =>
  live(
    Effect.gen(function* () {
      const clock = yield* TestClock.make();
      yield* Effect.gen(function* () {
        const f = yield* fixture({
          message:
            "if(!existsSync(signal('message-received'))){mark('message-received');await wait('never-admit');}else{ack(req);end();}",
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.wait("message-received");
        yield* clock.adjust("60 seconds");
        expect((yield* f.terminal).status).toBe("failed");
        yield* f.runtime.startTurn(nextTurn(f.turnInput, 2));
        expect((yield* f.terminal).status).toBe("completed");
        expect((yield* f.calls).filter((call) => call.request?.command === "message")).toHaveLength(
          2,
        );
      }).pipe(Effect.provideService(Clock.Clock, clock));
    }),
  ),
);

it.live.each([true, false])(
  "recovers completion buffered before acknowledgement (timestamp=%s)",
  (timestamp) =>
    live(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.setTime(1_000);
        yield* Effect.gen(function* () {
          const f = yield* fixture({
            latest: "emit({...result,turn:{...result.turn,queueItemId:'op-1'}});",
            message:
              "trace('user_message',{text:req.text,queueItemId:'op-1',source:'cli'});emit({phase:'code',type:'status',status:'task_updated',task:readyTask});emit({phase:'code',type:'status',status:'task_updated',task:{...result.task," +
              (timestamp ? "updatedAt:'1970-01-01T00:00:01.500Z'" : "") +
              "}});mark('completion-buffered');await wait('acknowledge');ack(req);",
          });
          yield* f.runtime.startTurn(f.turnInput);
          yield* f.wait("completion-buffered");
          yield* clock.setTime(2_000);
          yield* f.mark("acknowledge");
          expect((yield* f.terminal).status).toBe("completed");
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
);

it.live.each(["failed", "unrelated"])(
  "rechecks a newer terminal update after an in-flight lookup returned %s",
  (first) =>
    live(
      Effect.gen(function* () {
        const f = yield* fixture({
          latest:
            "if(!existsSync(signal('previewed'))){mark('previewed');emit(result);}else if(!existsSync(signal('lookup-started'))){mark('lookup-started');await wait('release-lookup');" +
            (first === "failed" ? "process.exit(1);" : "emit(result);") +
            "}else{emit({...result,turn:{...result.turn,queueItemId:'op-1'}});}",
          message:
            "ack(req);trace('agent_message',{text:'Admitted'});await wait('status-one');emit({phase:'code',type:'status',status:'task_updated',task:{...result.task,updatedAt:'2099-01-01T00:00:00Z'}});await wait('status-two');emit({phase:'code',type:'status',status:'task_updated',task:{...result.task,updatedAt:'2099-01-02T00:00:00Z'}});trace('agent_message',{text:'New status processed'});",
        });
        yield* f.runtime.startTurn(f.turnInput);
        yield* f.assistant("Admitted");
        yield* f.mark("status-one");
        yield* f.wait("lookup-started");
        yield* f.mark("status-two");
        yield* f.assistant("New status processed");
        expect(
          (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
        ).toHaveLength(2);
        yield* f.mark("release-lookup");
        expect((yield* f.terminal).status).toBe("completed");
        expect(
          (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
        ).toHaveLength(3);
      }),
    ),
);

it.live.each(["failed", "unrelated"])(
  "retains a status-only wake arriving during an idle probe that returns %s",
  (first) =>
    live(
      Effect.gen(function* () {
        const barrier = yield* Deferred.make<void>();
        const logger = Logger.make(({ message }) => {
          if (JSON.stringify(message).includes("probe-barrier"))
            Deferred.doneUnsafe(barrier, Effect.void);
        });
        yield* Effect.gen(function* () {
          const f = yield* fixture({
            latest:
              "if(!existsSync(signal('previewed'))){mark('previewed');emit(result);}else if(!existsSync(signal('probe-started'))){mark('probe-started');await wait('release-probe');" +
              (first === "failed"
                ? "process.exit(1);"
                : "emit({...result,turn:{...result.turn,queueItemId:'op-1',endSeq:1}});") +
              "}else{emit({...result,turn:{...result.turn,queueItemId:'web-queue',startSeq:1,endSeq:21}});}",
            message:
              "ack(req);end();await wait('stale-status');emit({phase:'code',type:'status',status:'task_updated',task:readyTask});await wait('new-status');emit({phase:'code',type:'status',status:'task_updated',task:readyTask});emit({phase:'code',type:'status',status:'task_updated',task:result.task});emit({phase:'code',type:'probe-barrier'});",
          });
          yield* f.runtime.startTurn(f.turnInput);
          expect((yield* f.terminal).status).toBe("completed");
          yield* f.mark("stale-status");
          yield* f.wait("probe-started");
          yield* f.mark("new-status");
          yield* Deferred.await(barrier);
          yield* f.mark("release-probe");
          yield* Queue.take(f.wakes);
          const next = nextTurn(f.turnInput, 2);
          yield* f.runtime.startTurn({
            ...next,
            message: { ...next.message, createdBy: "agent", creationSource: "provider" },
          });
          expect((yield* f.terminal).status).toBe("completed");
          expect(
            (yield* f.calls).filter((call) => call.args && !call.args.includes("--interactive")),
          ).toHaveLength(3);
        }).pipe(
          Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
          Effect.provideService(References.MinimumLogLevel, "Debug"),
        );
      }),
    ),
);
