import { NodeServices } from "@effect/platform-node";
import { assert, describe, expect, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ServerProviderSkill,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { AcpRegistrySettings } from "../settings.ts";
import * as AcpRegistrySupport from "./AcpRegistrySupport.ts";
import { isCopilotAcpSettings } from "./copilotAcp.ts";
import {
  applyCopilotDiscoveredSkills,
  CopilotSkillsProbeError,
  decodeCopilotSkillList,
  discoverCopilotSkills,
  filterCopilotSlashCommands,
  isCopilotAdvertisedSkill,
  leadingCopilotSkillMention,
} from "./copilotSkills.ts";

const decodeSettings = Schema.decodeSync(AcpRegistrySettings);

interface RecordedSpawn {
  readonly command: string;
  readonly args: ChildProcess.StandardCommand["args"];
  readonly options: ChildProcess.CommandOptions | undefined;
}

const makeCatalog = (
  resolve: AcpRegistrySupport.AcpRegistryCatalog["Service"]["resolve"],
): AcpRegistrySupport.AcpRegistryCatalog["Service"] => ({
  search: () => Effect.die("unused search"),
  prepare: () => Effect.die("unused prepare"),
  inspect: () => Effect.die("unused inspect"),
  resolve,
  uninstallManagedBinary: () => Effect.die("unused uninstall"),
});

const makeResolvedCatalog = (input: {
  readonly command?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly calls?: Array<{
    readonly settings: AcpRegistrySettings;
    readonly cwd: string;
    readonly environment: NodeJS.ProcessEnv | undefined;
  }>;
}): AcpRegistrySupport.AcpRegistryCatalog["Service"] =>
  makeCatalog((settings, cwd, environment) => {
    input.calls?.push({ settings, cwd, environment });
    const env = input.env ?? environment;
    return Effect.succeed({
      distribution: "local",
      spawn: {
        command: input.command ?? "/usr/local/bin/copilot",
        args: ["--acp"],
        cwd,
        ...(env ? { env } : {}),
      },
    });
  });

const makeSpawner = (options: {
  readonly stdout?: string;
  readonly exitCode?: number;
  readonly hang?: boolean;
  readonly failSpawn?: boolean;
  readonly spawns?: Array<RecordedSpawn>;
}) =>
  ChildProcessSpawner.make((command) => {
    if (command._tag === "StandardCommand") {
      options.spawns?.push({
        command: command.command,
        args: command.args,
        options: command.options,
      });
    }
    if (options.failSpawn) {
      return Effect.fail(
        PlatformError.systemError({
          _tag: "NotFound",
          module: "ChildProcess",
          method: "spawn",
          description: "copilot binary not found",
        }),
      );
    }
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: options.hang
          ? Effect.never
          : Effect.succeed(ChildProcessSpawner.ExitCode(options.exitCode ?? 0)),
        isRunning: Effect.succeed(Boolean(options.hang)),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: options.hang
          ? Stream.never
          : Stream.encodeText(Stream.make(options.stdout ?? "[]")),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });

function makeProviderSnapshot(input: {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
}): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make("acpRegistry_copilot"),
    driver: ProviderDriverKind.make("acpRegistry"),
    displayName: "GitHub Copilot",
    continuation: { groupKey: "acpRegistry:instance:acpRegistry_copilot" },
    supportsTextGeneration: false,
    enabled: true,
    installed: true,
    version: "1.0.105",
    status: "ready",
    auth: { status: "authenticated", canLogout: false },
    checkedAt: "2026-11-23T12:00:00.000Z",
    setup: { canInstall: false, canAuthenticate: true },
    models: [],
    slashCommands: input.slashCommands,
    skills: [],
  };
}

describe("copilotSkills", () => {
  const copilotSettings = decodeSettings({
    source: "registry",
    agentId: "github-copilot-cli",
  });

  it("identifies Copilot ACP settings and leading $skill mentions", () => {
    expect(
      isCopilotAcpSettings({ source: "registry", agentId: "github-copilot-cli", commandPath: "" }),
    ).toBe(true);
    expect(
      isCopilotAcpSettings({
        source: "local",
        agentId: "",
        commandPath: "C:\\Program Files\\Copilot\\COPILOT.CMD",
      }),
    ).toBe(true);
    expect(
      isCopilotAcpSettings({
        source: "local",
        agentId: "github-copilot-cli",
        commandPath: "/usr/local/bin/copilot-helper",
      }),
    ).toBe(false);
    expect(
      isCopilotAcpSettings({ source: "registry", agentId: "devin", commandPath: "copilot" }),
    ).toBe(false);

    assert.deepEqual(leadingCopilotSkillMention("$deploy ship it"), {
      name: "deploy",
      rest: " ship it",
    });
    assert.deepEqual(leadingCopilotSkillMention("  $deploy\nship it"), {
      name: "deploy",
      rest: "\nship it",
    });
    assert.equal(leadingCopilotSkillMention("please run $deploy"), undefined);
    assert.equal(leadingCopilotSkillMention("$25 off"), undefined);
  });

  it("decodes copilot skill list JSON with scope mapping, precedence deduplication, and malformed input handling", () => {
    const stdout = JSON.stringify([
      null,
      { name: "   ", path: "/tmp/a" },
      { name: "missing-path" },
      {
        name: "  writing-docs ",
        description: "  Write Greendale docs. ",
        source: "personal-agents",
        path: " /home/troy/.agents/skills/writing-docs ",
        enabled: true,
      },
      {
        name: "deploy",
        description: "Project deploy.",
        source: "project-agents",
        path: "/workspaces/greendale/.github/skills/deploy",
        enabled: true,
      },
      {
        name: "deploy",
        description: "Personal deploy shadow.",
        source: "personal",
        path: "/home/troy/.copilot/skills/deploy",
        enabled: false,
      },
      {
        name: "core-helper",
        description: "Built-in Copilot helper.",
        source: "builtin",
        path: "/opt/copilot/skills/core-helper",
        enabled: false,
      },
      {
        name: "acme-plugin",
        description: "  ",
        source: "  plugin  ",
        path: "/home/troy/.copilot/plugins/acme",
      },
    ]);

    expect(decodeCopilotSkillList(stdout)).toEqual([
      {
        name: "acme-plugin",
        path: "/home/troy/.copilot/plugins/acme",
        scope: "plugin",
        enabled: true,
      },
      {
        name: "core-helper",
        description: "Built-in Copilot helper.",
        path: "/opt/copilot/skills/core-helper",
        scope: "system",
        enabled: false,
      },
      {
        name: "deploy",
        description: "Project deploy.",
        path: "/workspaces/greendale/.github/skills/deploy",
        scope: "project",
        enabled: true,
      },
      {
        name: "writing-docs",
        description: "Write Greendale docs.",
        path: "/home/troy/.agents/skills/writing-docs",
        scope: "personal",
        enabled: true,
      },
    ]);

    for (const invalid of ["not json", "null", "{}", "42"]) {
      expect(decodeCopilotSkillList(invalid)).toBeUndefined();
    }
  });

  it("filters skill-hinted commands, checks ACP skill advertisements, and marks built-in collisions non-user-invocable", () => {
    const slashCommands: ReadonlyArray<ServerProviderSlashCommand> = [
      { name: "compact", description: "Compact context" },
      {
        name: "Deploy",
        description: "Deploy skill echoed as slash command",
        input: { hint: "instructions for the skill" },
      },
      { name: "plan", description: "Create a plan", input: { hint: "prompt" } },
      { name: "env", description: "Show environment" },
    ];
    const skills: ReadonlyArray<ServerProviderSkill> = [
      {
        name: "deploy",
        path: "/workspaces/greendale/.github/skills/deploy",
        scope: "project",
        enabled: true,
      },
      {
        name: "plan",
        path: "/workspaces/greendale/.github/skills/plan",
        scope: "project",
        enabled: true,
      },
      {
        name: "env",
        path: "/workspaces/greendale/.github/skills/env",
        scope: "project",
        enabled: true,
      },
    ];

    expect(filterCopilotSlashCommands(slashCommands)).toEqual([
      { name: "compact", description: "Compact context" },
      { name: "plan", description: "Create a plan", input: { hint: "prompt" } },
      { name: "env", description: "Show environment" },
    ]);
    expect(isCopilotAdvertisedSkill("deploy", slashCommands)).toBe(true);
    expect(isCopilotAdvertisedSkill("plan", slashCommands)).toBe(false);
    expect(isCopilotAdvertisedSkill("env", slashCommands)).toBe(false);

    const updated = applyCopilotDiscoveredSkills(makeProviderSnapshot({ slashCommands }), skills);
    expect(updated.skills).toEqual([
      skills[0],
      { ...skills[1]!, userInvocable: false },
      { ...skills[2]!, userInvocable: false },
    ]);
  });

  it.effect(
    "discovers skills via copilot CLI and parses top-level YAML user-invocable: false frontmatter",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-copilot-skills-" });
        const resolveCalls: Array<{
          readonly settings: AcpRegistrySettings;
          readonly cwd: string;
          readonly environment: NodeJS.ProcessEnv | undefined;
        }> = [];
        const spawns: Array<RecordedSpawn> = [];
        const resolvedEnv = {
          COPILOT_HOME: "/home/troy/.copilot",
          MISE_OFFLINE: "1",
          COPILOT_EXTRA: "from-distribution",
        };
        const fixtures = [
          {
            name: "hidden-skill",
            dir: "hidden-skill",
            source: "builtin",
            content: "---\nname: hidden-skill\nuser-invocable: false\n---\nHidden.\n",
          },
          {
            name: "bom-quoted-hidden",
            dir: "bom-quoted-hidden.md",
            source: "project",
            content: '\uFEFF---\nname: bom-quoted-hidden\n"user-invocable": false\n---\nHidden.\n',
          },
          {
            name: "nested-visible",
            dir: "nested-visible",
            source: "project",
            content:
              "---\nname: nested-visible\ndescription: |\n  user-invocable: false\nmetadata:\n  user-invocable: false\n---\nVisible.\n",
          },
          {
            name: "visible-skill",
            dir: "visible-skill",
            source: "project",
            content: "---\nname: visible-skill\nuser-invocable: true\n---\nVisible.\n",
          },
        ] as const;

        const payload = [];
        for (const fixture of fixtures) {
          const skillDir = path.join(tempDir, fixture.dir);
          yield* fs.makeDirectory(skillDir, { recursive: true });
          yield* fs.writeFileString(path.join(skillDir, "SKILL.md"), fixture.content);
          payload.push({
            name: fixture.name,
            description: fixture.name,
            source: fixture.source,
            path: skillDir,
            enabled: true,
          });
        }

        const skills = yield* discoverCopilotSkills({
          settings: copilotSettings,
          environment: { COPILOT_HOME: "/home/troy/.copilot" },
          cwd: tempDir,
        }).pipe(
          Effect.provideService(
            AcpRegistrySupport.AcpRegistryCatalog,
            makeResolvedCatalog({
              command: "/opt/copilot/bin/copilot",
              env: resolvedEnv,
              calls: resolveCalls,
            }),
          ),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            makeSpawner({ stdout: JSON.stringify(payload), spawns }),
          ),
        );

        expect(resolveCalls).toEqual([
          {
            settings: copilotSettings,
            cwd: tempDir,
            environment: { COPILOT_HOME: "/home/troy/.copilot", MISE_OFFLINE: "1" },
          },
        ]);
        expect(spawns[0]).toMatchObject({
          command: "/opt/copilot/bin/copilot",
          args: ["skill", "list", "--json"],
          options: { cwd: tempDir, env: resolvedEnv },
        });
        expect(
          skills.map(({ name, scope, userInvocable }) => ({ name, scope, userInvocable })),
        ).toEqual([
          { name: "bom-quoted-hidden", scope: "project", userInvocable: false },
          { name: "hidden-skill", scope: "system", userInvocable: false },
          { name: "nested-visible", scope: "project", userInvocable: undefined },
          { name: "visible-skill", scope: "project", userInvocable: undefined },
        ]);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "maps resolve, spawn, exit, decode, and timeout failures to CopilotSkillsProbeError",
    () =>
      Effect.gen(function* () {
        const resolveError = new AcpRegistrySupport.AcpRegistryError({
          reason: "runner_unavailable",
          detail: "secret-path-not-in-message",
        });
        const resolveFailure = yield* discoverCopilotSkills({
          settings: copilotSettings,
          cwd: "/workspaces/greendale",
        }).pipe(
          Effect.flip,
          Effect.provideService(
            AcpRegistrySupport.AcpRegistryCatalog,
            makeCatalog(() => Effect.fail(resolveError)),
          ),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, makeSpawner({})),
        );
        expect(resolveFailure).toBeInstanceOf(CopilotSkillsProbeError);
        expect(resolveFailure).toMatchObject({
          stage: "resolve",
          cwd: "/workspaces/greendale",
          cause: resolveError,
        });
        expect(resolveFailure.message).not.toContain("secret-path-not-in-message");

        const spawnFailure = yield* discoverCopilotSkills({
          settings: copilotSettings,
          cwd: "/workspaces/greendale",
        }).pipe(
          Effect.flip,
          Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, makeResolvedCatalog({})),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            makeSpawner({ failSpawn: true }),
          ),
        );
        expect(spawnFailure).toMatchObject({ stage: "spawn", cwd: "/workspaces/greendale" });

        const exitFailure = yield* discoverCopilotSkills({
          settings: copilotSettings,
          cwd: "/workspaces/greendale",
        }).pipe(
          Effect.flip,
          Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, makeResolvedCatalog({})),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            makeSpawner({ stdout: "[]", exitCode: 2 }),
          ),
        );
        expect(exitFailure).toMatchObject({
          stage: "exit",
          cwd: "/workspaces/greendale",
          exitCode: 2,
        });

        const decodeFailure = yield* discoverCopilotSkills({
          settings: copilotSettings,
          cwd: "/workspaces/greendale",
        }).pipe(
          Effect.flip,
          Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, makeResolvedCatalog({})),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            makeSpawner({ stdout: "{not valid json" }),
          ),
        );
        expect(decodeFailure).toMatchObject({ stage: "decode", cwd: "/workspaces/greendale" });
        expect(decodeFailure.message).not.toContain("{not valid json");

        const hungProbe = yield* discoverCopilotSkills({
          settings: copilotSettings,
          cwd: "/workspaces/greendale",
        }).pipe(
          Effect.flip,
          Effect.provideService(AcpRegistrySupport.AcpRegistryCatalog, makeResolvedCatalog({})),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            makeSpawner({ hang: true }),
          ),
          Effect.forkChild({ startImmediately: true }),
        );
        yield* TestClock.adjust("4 seconds");
        expect(yield* Fiber.join(hungProbe)).toMatchObject({
          stage: "timeout",
          cwd: "/workspaces/greendale",
        });
      }),
  );
});
