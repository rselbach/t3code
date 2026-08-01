// Default-deny tool-approval gate, loaded into `pi --mode rpc` via `--extension`.
// Runs in the user's Pi runtime without importing Pi into the T3 server.
interface PiExtensionAPI {
  readonly registerCommand: (
    name: string,
    command: { readonly description: string; readonly handler: () => Promise<void> },
  ) => void;
  readonly getAllTools: () => ReadonlyArray<{
    readonly name: string;
    readonly sourceInfo: { readonly source: string; readonly path: string };
  }>;
  readonly on: (
    event: "tool_call",
    handler: (
      toolCall: { readonly toolName: string; readonly input?: Record<string, unknown> },
      context: {
        readonly hasUI: boolean;
        readonly ui: {
          readonly confirm: (title: string, message: string) => Promise<boolean>;
        };
      },
    ) => Promise<{ readonly block: true; readonly reason: string } | undefined>,
  ) => void;
}

const READ_ONLY_TOOLS = new Set(["read", "grep", "find", "ls"]);
const EDIT_TOOLS = ["write", "edit"];

function autoApprovedTools(approvalMode: string | undefined): ReadonlySet<string> {
  const allowed = new Set(READ_ONLY_TOOLS);
  if (approvalMode === "auto-accept-edits") {
    for (const tool of EDIT_TOOLS) allowed.add(tool);
  }
  return allowed;
}

function gateDecision(opts: {
  readonly hasUI: boolean;
  readonly confirmed: boolean;
}): { readonly block: true; readonly reason: string } | undefined {
  if (!opts.hasUI || !opts.confirmed) return { block: true, reason: DENIED_REASON };
  return undefined;
}

function isTrustedBuiltinTool(pi: PiExtensionAPI, toolName: string): boolean {
  const tool = pi.getAllTools().find((candidate) => candidate.name === toolName);
  return tool?.sourceInfo.source === "builtin" && tool.sourceInfo.path === `<builtin:${toolName}>`;
}

// keep in sync with PI_APPROVAL_SENTINEL_COMMAND in PiAdapter.ts
const SENTINEL_COMMAND = "t3-approval-gate";

const DENIED_REASON = "Denied in T3 Code";

function describeToolCall(toolName: string, input: Record<string, unknown> | undefined): string {
  if (!input) return toolName;
  const command = input["command"] ?? input["cmd"];
  if (typeof command === "string" && command.trim().length > 0) {
    return command.trim().slice(0, 500);
  }
  const filePath = input["file_path"] ?? input["path"] ?? input["filePath"];
  if (typeof filePath === "string" && filePath.trim().length > 0) {
    return filePath.trim().slice(0, 500);
  }
  try {
    return JSON.stringify(input).slice(0, 500);
  } catch {
    return toolName;
  }
}

export default function (pi: PiExtensionAPI): void {
  pi.registerCommand(SENTINEL_COMMAND, {
    description: "T3 Code approval gate (active)",
    handler: async () => {},
  });

  const allowed = autoApprovedTools(process.env["T3_PI_APPROVAL_MODE"]);

  pi.on("tool_call", async (event, ctx) => {
    if (allowed.has(event.toolName) && isTrustedBuiltinTool(pi, event.toolName)) {
      return undefined;
    }

    const input = event.input;
    const detail = describeToolCall(event.toolName, input);
    const confirmed = ctx.hasUI ? await ctx.ui.confirm(`Run ${event.toolName}?`, detail) : false;

    return gateDecision({ hasUI: ctx.hasUI, confirmed });
  });
}

export { autoApprovedTools, describeToolCall, gateDecision, isTrustedBuiltinTool };
export type { PiExtensionAPI };
