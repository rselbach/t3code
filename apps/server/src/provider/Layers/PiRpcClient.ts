/** Typed JSONL transport + pure parsing helpers for `pi --mode rpc`. */
import type { ModelCapabilities, ModelSelection, ServerProviderModel } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities, getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { buildSelectOptionDescriptor } from "../providerSnapshot.ts";

export type PiThinkingLevel = "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";

export interface ModelInfo {
  readonly provider: string;
  readonly id: string;
  readonly name?: string;
  readonly reasoning?: boolean;
  readonly thinkingLevelMap?: Partial<Record<PiThinkingLevel, string | null>>;
}

export interface AgentSessionEvent {
  readonly type: string;
  readonly assistantMessageEvent?: {
    readonly type?: string;
    readonly delta?: string;
  };
  readonly toolCallId?: string;
  readonly toolName?: string;
  readonly args?: unknown;
  readonly partialResult?: unknown;
  readonly result?: unknown;
  readonly isError?: boolean;
  readonly willRetry?: boolean;
}

export interface PiImageContent {
  readonly type: "image";
  readonly data: string;
  readonly mimeType: string;
}

export type RpcCommand =
  | {
      readonly id?: string;
      readonly type: "prompt";
      readonly message: string;
      readonly images?: PiImageContent[];
    }
  | {
      readonly id?: string;
      readonly type: "steer";
      readonly message: string;
      readonly images?: PiImageContent[];
    }
  | { readonly id?: string; readonly type: "abort" }
  | { readonly id?: string; readonly type: "new_session" }
  | { readonly id?: string; readonly type: "get_state" }
  | {
      readonly id?: string;
      readonly type: "set_model";
      readonly provider: string;
      readonly modelId: string;
    }
  | { readonly id?: string; readonly type: "get_available_models" }
  | { readonly id?: string; readonly type: "set_thinking_level"; readonly level: PiThinkingLevel }
  | { readonly id?: string; readonly type: "get_available_thinking_levels" }
  | { readonly id?: string; readonly type: "get_session_stats" }
  | { readonly id?: string; readonly type: "fork"; readonly entryId: string }
  | { readonly id?: string; readonly type: "get_fork_messages" }
  | { readonly id?: string; readonly type: "get_last_assistant_text" }
  | { readonly id?: string; readonly type: "get_messages" }
  | { readonly id?: string; readonly type: "get_commands" };

export interface RpcResponse {
  readonly id?: string;
  readonly type: "response";
  readonly command: string;
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string;
}

export type RpcExtensionUIRequest =
  | {
      readonly type: "extension_ui_request";
      readonly id: string;
      readonly method: "select";
      readonly title: string;
      readonly options: string[];
    }
  | {
      readonly type: "extension_ui_request";
      readonly id: string;
      readonly method: "confirm";
      readonly title: string;
      readonly message: string;
    }
  | {
      readonly type: "extension_ui_request";
      readonly id: string;
      readonly method: "input";
      readonly title: string;
      readonly placeholder?: string;
    }
  | {
      readonly type: "extension_ui_request";
      readonly id: string;
      readonly method: "editor";
      readonly title: string;
      readonly prefill?: string;
    }
  | {
      readonly type: "extension_ui_request";
      readonly id: string;
      readonly method: "notify" | "setStatus" | "setWidget" | "setTitle" | "set_editor_text";
      readonly title?: string;
      readonly message?: string;
    };

export type RpcExtensionUIResponse =
  | { readonly type: "extension_ui_response"; readonly id: string; readonly value: string }
  | { readonly type: "extension_ui_response"; readonly id: string; readonly confirmed: boolean }
  | { readonly type: "extension_ui_response"; readonly id: string; readonly cancelled: true };

export type PiStdoutMessage =
  | { readonly _tag: "response"; readonly id: string | undefined; readonly response: RpcResponse }
  | { readonly _tag: "extension-ui"; readonly request: RpcExtensionUIRequest }
  | { readonly _tag: "event"; readonly event: AgentSessionEvent };

const decodeUnknownJsonStringExit = Schema.decodeUnknownExit(Schema.UnknownFromJsonString);
const encodeUnknownJsonString = Schema.encodeSync(Schema.UnknownFromJsonString);

export function tryParsePiJsonObject(text: string): Record<string, unknown> | null {
  const trimmed = text.trim();
  if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) {
    return null;
  }
  const result = decodeUnknownJsonStringExit(trimmed);
  if (!Exit.isSuccess(result)) return null;
  const value = result.value;
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function isPiExtensionUIRequest(msg: Record<string, unknown>): msg is RpcExtensionUIRequest {
  if (typeof msg["id"] !== "string" || typeof msg["method"] !== "string") return false;
  switch (msg["method"]) {
    case "select":
      return (
        typeof msg["title"] === "string" &&
        Array.isArray(msg["options"]) &&
        msg["options"].every((option) => typeof option === "string")
      );
    case "confirm":
      return typeof msg["title"] === "string" && typeof msg["message"] === "string";
    case "input":
    case "editor":
      return typeof msg["title"] === "string";
    case "notify":
    case "setStatus":
    case "setWidget":
    case "setTitle":
    case "set_editor_text":
      return true;
    default:
      return false;
  }
}

export function classifyPiStdoutMessage(msg: Record<string, unknown>): PiStdoutMessage | null {
  const type = msg["type"];
  if (type === "response") {
    if (typeof msg["command"] !== "string" || typeof msg["success"] !== "boolean") return null;
    return {
      _tag: "response",
      id: typeof msg["id"] === "string" ? msg["id"] : undefined,
      response: msg as unknown as RpcResponse,
    };
  }
  if (type === "extension_ui_request") {
    return isPiExtensionUIRequest(msg) ? { _tag: "extension-ui", request: msg } : null;
  }
  if (typeof type === "string" && type.length > 0) {
    return { _tag: "event", event: msg as unknown as AgentSessionEvent };
  }
  return null;
}

export function parsePiStdoutLine(line: string): PiStdoutMessage | null {
  const msg = tryParsePiJsonObject(line);
  return msg ? classifyPiStdoutMessage(msg) : null;
}

// only text_delta is user-visible; thinking/toolcall deltas leak raw json
export function extractAssistantTextDelta(event: AgentSessionEvent): string | null {
  if (event.type !== "message_update") return null;
  const assistantEvent = event.assistantMessageEvent;
  if (!assistantEvent || assistantEvent.type !== "text_delta") return null;
  return typeof assistantEvent.delta === "string" ? assistantEvent.delta : null;
}

export function extractReasoningTextDelta(event: AgentSessionEvent): string | null {
  if (event.type !== "message_update") return null;
  const assistantEvent = event.assistantMessageEvent;
  if (!assistantEvent || assistantEvent.type !== "thinking_delta") return null;
  const delta = (assistantEvent as { delta?: unknown }).delta;
  return typeof delta === "string" ? delta : null;
}

// Pi tool results are usually `{ content: [{ type: "text", text }] }`, not bare strings.
// `tool_execution_update.partialResult` is cumulative; callers must diff against prior text.
export function extractPiToolResultText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (Array.isArray(value)) {
    const parts = value
      .map((entry) => extractPiToolResultText(entry))
      .filter((entry): entry is string => entry !== null && entry.length > 0);
    return parts.length > 0 ? parts.join("") : null;
  }
  if (typeof value !== "object") return null;

  const record = value as Record<string, unknown>;
  if (typeof record.text === "string") return record.text;
  if (typeof record.stdout === "string" || typeof record.stderr === "string") {
    const stdout = typeof record.stdout === "string" ? record.stdout : "";
    const stderr = typeof record.stderr === "string" ? record.stderr : "";
    const combined = `${stdout}${stdout && stderr ? "\n" : ""}${stderr}`;
    return combined.length > 0 ? combined : null;
  }
  if ("content" in record) return extractPiToolResultText(record.content);
  if ("output" in record) return extractPiToolResultText(record.output);
  if ("result" in record) return extractPiToolResultText(record.result);
  return null;
}

// slugs are provider/id; keep any extra "/" in the id
export function splitPiModelSlug(slug: string): { provider: string; id: string } | null {
  const trimmed = slug.trim();
  const idx = trimmed.indexOf("/");
  if (idx <= 0 || idx >= trimmed.length - 1) return null;
  return { provider: trimmed.slice(0, idx), id: trimmed.slice(idx + 1) };
}

export function piModelSlug(model: Pick<ModelInfo, "provider" | "id">): string {
  return `${model.provider}/${model.id}`;
}

export type PiTurnCommand = Extract<RpcCommand, { type: "prompt" } | { type: "steer" }>;

// raw base64, not a data URL
export function piImageContentFromBytes(input: {
  readonly mimeType: string;
  readonly bytes: Uint8Array;
}): PiImageContent {
  return {
    type: "image",
    data: Buffer.from(input.bytes).toString("base64"),
    mimeType: input.mimeType,
  };
}

// mid-turn must "steer": a bare prompt is rejected while streaming and, being
// fire-and-forget, would be silently dropped
export function buildPiTurnCommand(args: {
  readonly isMidTurn: boolean;
  readonly message: string;
  readonly images?: ReadonlyArray<PiImageContent>;
}): PiTurnCommand {
  const hasImages = args.images !== undefined && args.images.length > 0;
  const images = hasImages ? [...(args.images as ReadonlyArray<PiImageContent>)] : undefined;
  return args.isMidTurn
    ? { type: "steer", message: args.message, ...(images ? { images } : {}) }
    : { type: "prompt", message: args.message, ...(images ? { images } : {}) };
}

const PI_THINKING_LEVELS = [
  { value: "off", label: "Off" },
  { value: "minimal", label: "Minimal" },
  { value: "low", label: "Low" },
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "xhigh", label: "Extra High" },
  { value: "max", label: "Max" },
] as const;

export const PI_THINKING_OPTION_ID = "thinking";

export const PI_THINKING_LEVEL_VALUES = PI_THINKING_LEVELS.map(
  (level) => level.value,
) as ReadonlyArray<PiThinkingLevel>;

const PI_THINKING_LEVEL_SET: ReadonlySet<string> = new Set(PI_THINKING_LEVEL_VALUES);

export function asPiThinkingLevel(value: string | undefined): PiThinkingLevel | undefined {
  return value !== undefined && PI_THINKING_LEVEL_SET.has(value)
    ? (value as PiThinkingLevel)
    : undefined;
}

export function resolvePiThinkingLevel(
  modelSelection: ModelSelection | null | undefined,
): PiThinkingLevel | undefined {
  return asPiThinkingLevel(
    getModelSelectionStringOptionValue(modelSelection, PI_THINKING_OPTION_ID),
  );
}

export type PiModelSwitchPlan =
  | { readonly kind: "noop" }
  | { readonly kind: "invalid"; readonly slug: string }
  | {
      readonly kind: "switch";
      readonly provider: string;
      readonly modelId: string;
      readonly slug: string;
    };

export function planPiModelSwitch(
  currentModel: string | undefined,
  requestedModel: string | undefined,
): PiModelSwitchPlan {
  if (requestedModel === undefined || requestedModel === currentModel) return { kind: "noop" };
  const parts = splitPiModelSlug(requestedModel);
  if (!parts) return { kind: "invalid", slug: requestedModel };
  return { kind: "switch", provider: parts.provider, modelId: parts.id, slug: requestedModel };
}

export function supportedPiThinkingLevels(model: ModelInfo): ReadonlyArray<PiThinkingLevel> {
  if (!model.reasoning) return ["off"];
  return PI_THINKING_LEVEL_VALUES.filter((level) => {
    const mapped = model.thinkingLevelMap?.[level];
    if (mapped === null) return false;
    if (level === "xhigh" || level === "max") return mapped !== undefined;
    return true;
  });
}

export function piModelCapabilities(model: ModelInfo): ModelCapabilities {
  if (!model.reasoning) {
    return createModelCapabilities({ optionDescriptors: [] });
  }
  const supported = new Set(supportedPiThinkingLevels(model));
  const defaultLevel = (["medium", "high", "xhigh", "max", "low", "minimal", "off"] as const).find(
    (level) => supported.has(level),
  );
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: PI_THINKING_OPTION_ID,
        label: "Thinking",
        options: PI_THINKING_LEVELS.filter((level) => supported.has(level.value)).map((level) => ({
          value: level.value,
          label: level.label,
          ...(level.value === defaultLevel ? { isDefault: true } : {}),
        })),
      }),
    ],
  });
}

export function piModelInfoToServerModel(model: ModelInfo): ServerProviderModel {
  const slug = piModelSlug(model);
  const name =
    typeof model.name === "string" && model.name.trim().length > 0 ? model.name.trim() : model.id;
  return {
    slug,
    name,
    subProvider: model.provider,
    isCustom: false,
    capabilities: piModelCapabilities(model),
  };
}

export function piResponseData(response: RpcResponse | undefined): Record<string, unknown> | null {
  if (!response || response.type !== "response" || response.success !== true) return null;
  const data = (response as { data?: unknown }).data;
  return data !== null && typeof data === "object" ? (data as Record<string, unknown>) : null;
}

export interface PiSessionStateSelection {
  readonly sessionFile?: string;
  readonly model?: string;
  readonly thinkingLevel?: PiThinkingLevel;
  readonly isStreaming?: boolean;
}

export function extractPiSessionState(
  response: RpcResponse | undefined,
): PiSessionStateSelection | undefined {
  if (!piResponseSucceeded(response, "get_state")) return undefined;
  const data = piResponseData(response);
  if (!data) return {};
  const sessionFile =
    typeof data["sessionFile"] === "string" && data["sessionFile"].trim().length > 0
      ? data["sessionFile"].trim()
      : undefined;
  const rawModel = data["model"];
  const model =
    typeof rawModel === "object" &&
    rawModel !== null &&
    typeof (rawModel as Record<string, unknown>)["provider"] === "string" &&
    typeof (rawModel as Record<string, unknown>)["id"] === "string"
      ? `${(rawModel as Record<string, unknown>)["provider"]}/${(rawModel as Record<string, unknown>)["id"]}`
      : undefined;
  const thinkingLevel =
    typeof data["thinkingLevel"] === "string"
      ? asPiThinkingLevel(data["thinkingLevel"])
      : undefined;
  const isStreaming = typeof data["isStreaming"] === "boolean" ? data["isStreaming"] : undefined;
  return {
    ...(sessionFile ? { sessionFile } : {}),
    ...(model ? { model } : {}),
    ...(thinkingLevel ? { thinkingLevel } : {}),
    ...(isStreaming !== undefined ? { isStreaming } : {}),
  };
}

// surface Pi's error string (or timeout/exit) instead of a generic ack failure
export function describePiCommandFailure(
  response: RpcResponse | undefined,
  command: string,
): string {
  if (response === undefined) {
    return `Pi did not acknowledge '${command}' (timed out or process exited).`;
  }
  if (response.success !== true) {
    const error =
      typeof response.error === "string" && response.error.trim().length > 0
        ? response.error.trim()
        : "unknown error";
    return `Pi rejected '${command}': ${error}`;
  }
  if (response.command !== command) {
    return `Pi responded to '${response.command}' while awaiting '${command}'.`;
  }
  return `Pi failed to acknowledge '${command}'.`;
}

export function extractSessionFile(response: RpcResponse | undefined): string | undefined {
  return extractPiSessionState(response)?.sessionFile;
}

export function extractAvailableModels(
  response: RpcResponse | undefined,
): ReadonlyArray<ModelInfo> {
  const models = piResponseData(response)?.["models"];
  if (!Array.isArray(models)) return [];
  return models.flatMap((entry) => {
    if (entry === null || typeof entry !== "object") return [];
    const model = entry as Record<string, unknown>;
    if (typeof model["provider"] !== "string" || typeof model["id"] !== "string") return [];
    return [entry as ModelInfo];
  });
}

// approval-gate handshake: the sentinel command must come from our extension file
export function piResponseHasCommand(
  response: RpcResponse | undefined,
  commandName: string,
  expectedSourcePath?: string,
): boolean {
  const commands = piResponseData(response)?.["commands"];
  if (!Array.isArray(commands)) return false;
  return commands.some((entry) => {
    if (typeof entry !== "object" || entry === null) return false;
    const command = entry as Record<string, unknown>;
    if (command["name"] !== commandName) return false;
    if (expectedSourcePath === undefined) return true;
    const sourceInfo = command["sourceInfo"];
    return (
      typeof sourceInfo === "object" &&
      sourceInfo !== null &&
      (sourceInfo as Record<string, unknown>)["path"] === expectedSourcePath
    );
  });
}

export function extractLastAssistantText(response: RpcResponse | undefined): string | null {
  const text = piResponseData(response)?.["text"];
  return typeof text === "string" ? text : null;
}

// fail-closed: a timeout (undefined) or mismatched command counts as failure
export function piResponseSucceeded(response: RpcResponse | undefined, command: string): boolean {
  return (
    response !== undefined &&
    response.type === "response" &&
    response.success === true &&
    (response as { command?: unknown }).command === command
  );
}

// branch-scoped user messages (each with entryId) — the only valid fork targets
export function extractForkMessages(
  response: RpcResponse | undefined,
): ReadonlyArray<{ readonly entryId: string; readonly text: string }> | undefined {
  if (!piResponseSucceeded(response, "get_fork_messages")) return undefined;
  const messages = piResponseData(response)?.["messages"];
  if (!Array.isArray(messages)) return undefined;
  return messages.flatMap((entry) => {
    if (!entry || typeof entry !== "object") return [];
    const record = entry as Record<string, unknown>;
    const entryId = record["entryId"];
    if (typeof entryId !== "string" || entryId.length === 0) return [];
    const text = typeof record["text"] === "string" ? (record["text"] as string) : "";
    return [{ entryId, text }];
  });
}

// fail-closed; both fork/new_session return { cancelled } on success
export function piForkSucceeded(response: RpcResponse | undefined): boolean {
  if (
    !response ||
    response.type !== "response" ||
    response.success !== true ||
    (response.command !== "fork" && response.command !== "new_session")
  ) {
    return false;
  }
  return piResponseData(response)?.["cancelled"] !== true;
}

// linear 1-user-message-per-turn mapping; mid-turn steers can under-drop (deferred)
export function resolveForkTargetEntryId(
  userMessages: ReadonlyArray<{ readonly entryId: string }>,
  numTurns: number,
): { readonly kind: "fork"; readonly entryId: string } | { readonly kind: "reset" } | null {
  if (numTurns <= 0 || userMessages.length === 0) return null;
  const targetIndex = userMessages.length - numTurns;
  if (targetIndex <= 0) return { kind: "reset" };
  return { kind: "fork", entryId: userMessages[targetIndex]!.entryId };
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

export interface PiRpcTransport {
  readonly writeExtensionResponse: (response: RpcExtensionUIResponse) => Effect.Effect<void>;
  // sends a command and awaits its correlated response; times out to `undefined`
  readonly request: (
    command: RpcCommand,
    id: string,
    timeoutMs: number,
  ) => Effect.Effect<RpcResponse | undefined>;
  readonly messages: Queue.Dequeue<PiStdoutMessage, Cause.Done<void>>;
  readonly isClosed: Effect.Effect<boolean>;
  readonly kill: Effect.Effect<void>;
}

export interface MakePiRpcTransportOptions {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly onExit: Effect.Effect<void>;
}

export const makePiRpcTransport = (options: MakePiRpcTransportOptions) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const spawnCommand = yield* resolveSpawnCommand(options.binaryPath || "pi", options.args, {
      env: options.env,
    });
    const child = yield* spawner.spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        cwd: options.cwd,
        env: options.env,
        shell: spawnCommand.shell,
        forceKillAfter: 5000,
      }),
    );

    const outgoing = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
    const messages = yield* Queue.unbounded<PiStdoutMessage, Cause.Done<void>>();
    const pendingRequests = new Map<string, Deferred.Deferred<RpcResponse>>();
    // resolved on process exit to unblock in-flight requests (fail fast, not full timeout)
    const closed = yield* Deferred.make<void>();

    const offerLine = (obj: RpcCommand | RpcExtensionUIResponse): Effect.Effect<boolean> =>
      Queue.offer(outgoing, Buffer.from(`${encodeUnknownJsonString(obj)}\n`));

    const writeLine = (obj: RpcCommand | RpcExtensionUIResponse): Effect.Effect<void> =>
      offerLine(obj).pipe(
        Effect.flatMap((offered) =>
          offered
            ? Effect.void
            : Effect.die(new Error("Pi RPC process exited before the command could be delivered.")),
        ),
      );

    const handleLine = (line: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const message = parsePiStdoutLine(line);
        if (!message) return;
        if (message._tag === "response") {
          if (message.id !== undefined) {
            const deferred = pendingRequests.get(message.id);
            if (deferred) {
              pendingRequests.delete(message.id);
              yield* Deferred.succeed(deferred, message.response);
            }
          }
          return;
        }
        yield* Queue.offer(messages, message);
      });

    const onProcessExit = Effect.gen(function* () {
      yield* Deferred.succeed(closed, undefined);
      yield* Queue.end(outgoing);
      yield* Queue.end(messages);
      pendingRequests.clear();
      yield* options.onExit;
    });

    yield* Stream.fromQueue(outgoing).pipe(
      Stream.run(child.stdin),
      Effect.ignore,
      Effect.forkScoped,
    );

    // stderr drain (prevents the pipe from blocking)
    yield* child.stderr.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);

    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.splitLines,
      Stream.runForEach(handleLine),
      Effect.ignore,
      Effect.ensuring(onProcessExit),
      Effect.forkScoped,
    );

    const request = (
      command: RpcCommand,
      id: string,
      timeoutMs: number,
    ): Effect.Effect<RpcResponse | undefined> =>
      Effect.gen(function* () {
        const deferred = yield* Deferred.make<RpcResponse>();
        pendingRequests.set(id, deferred);
        const offered = yield* offerLine({ ...command, id });
        if (!offered) {
          pendingRequests.delete(id);
          return undefined;
        }
        // resolve on response, process exit, or timeout — whichever comes first
        const outcome = yield* Deferred.await(deferred).pipe(
          Effect.map((response) => Option.some(response)),
          Effect.race(Deferred.await(closed).pipe(Effect.as(Option.none<RpcResponse>()))),
          Effect.timeoutOption(timeoutMs),
        );
        pendingRequests.delete(id);
        return outcome._tag === "None" ? undefined : Option.getOrUndefined(outcome.value);
      });

    const kill = child.kill().pipe(Effect.ignore);

    return {
      writeExtensionResponse: (response) => writeLine(response),
      request,
      messages,
      isClosed: Deferred.isDone(closed),
      kill,
    } satisfies PiRpcTransport;
  });
