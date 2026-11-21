import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { parse as parseYamlDocument } from "yaml";

/** Match Python os.path.expanduser: USERPROFILE, then HOMEDRIVE+HOMEPATH on Windows; HOME elsewhere. */
export function resolveAntigravityUserHome(
  platform: NodeJS.Platform,
  environment: NodeJS.ProcessEnv,
  home: string,
): string {
  if (platform === "win32") {
    if (environment.USERPROFILE) return environment.USERPROFILE;
    if (environment.HOMEDRIVE && environment.HOMEPATH) {
      return `${environment.HOMEDRIVE}${environment.HOMEPATH}`;
    }
    return home;
  }
  return environment.HOME || home;
}

/** Link private GEMINI_HOME skills to ~/.gemini; ~/.agents/skills is ignored because .agents/skills is project-only. */
export function antigravityUserSkillDirectories(
  path: Path.Path,
  geminiHome: string,
): readonly [configSkills: string, cliSkills: string] {
  return [
    path.join(geminiHome, "config", "skills"),
    path.join(geminiHome, "antigravity-cli", "skills"),
  ];
}

const MAX_SKILL_BYTES = 1_000_000;
const MAX_SCAN_BYTES = 8_000_000;
const MAX_SCAN_ENTRIES = 10_000;
const MAX_INHERIT_DEPTH = 10;
// Typical frontmatter fits in one chunk, so most skills cost about this much of the scan budget.
const SKILL_READ_CHUNK_BYTES = 1024;

const SkillFrontmatter = Schema.Struct({
  name: Schema.optional(Schema.NullOr(Schema.String)),
  description: Schema.optional(Schema.NullOr(Schema.String)),
});
const decodeSkillFrontmatter = Schema.decodeUnknownSync(SkillFrontmatter);
const SkillConfigEntry = Schema.Struct({
  path: Schema.String,
  include_only: Schema.optional(Schema.NullOr(Schema.Array(Schema.String))),
  exclude: Schema.optional(Schema.NullOr(Schema.Array(Schema.String))),
});
const decodeSkillsConfig = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      inherits: Schema.optional(Schema.NullOr(Schema.Array(SkillConfigEntry))),
      entries: Schema.optional(Schema.NullOr(Schema.Array(SkillConfigEntry))),
    }),
  ),
);
const decodePluginPreferenceEntry = Schema.decodeUnknownOption(
  Schema.Struct({ enabled: Schema.optional(Schema.Boolean) }),
);
const decodePluginPreferences = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      plugins: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  ),
);
const decodePluginManifest = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      disabled: Schema.optional(Schema.Boolean),
    }),
  ),
);

export class AntigravitySkillsProbeError extends Schema.TaggedError<AntigravitySkillsProbeError>()(
  "AntigravitySkillsProbeError",
  {
    reason: Schema.Literals(["scan-budget-exhausted", "filesystem-error"]),
    path: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.reason === "scan-budget-exhausted"
      ? `Antigravity skill discovery exceeded its scan limit at '${this.path}'.`
      : `Antigravity could not read skills at '${this.path}'.`;
  }
}

interface ScanBudget {
  remainingBytes: number;
  remainingEntries: number;
}

const readIfPresent = <A, R>(
  effect: Effect.Effect<A, PlatformError.PlatformError, R>,
  path: string,
) =>
  effect.pipe(
    Effect.catchTags({
      PlatformError: (cause) =>
        cause.reason._tag === "NotFound"
          ? Effect.undefined
          : Effect.fail(
              new AntigravitySkillsProbeError({ reason: "filesystem-error", path, cause }),
            ),
    }),
  );

const readAntigravityPluginPreferences = Effect.fn("readAntigravityPluginPreferences")(function* (
  configPath: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const info = yield* readIfPresent(fileSystem.stat(configPath), configPath);
  if (info?.type !== "File") return new Map<string, boolean>();
  if (info.size > BigInt(MAX_SKILL_BYTES)) {
    return yield* new AntigravitySkillsProbeError({
      reason: "scan-budget-exhausted",
      path: configPath,
    });
  }
  const config = yield* readIfPresent(fileSystem.readFileString(configPath), configPath);
  const preferences = config === undefined ? undefined : decodePluginPreferences(config);
  return new Map(
    Object.entries(
      (preferences?._tag === "Some" ? preferences.value.plugins : undefined) ?? {},
    ).flatMap(([name, entry]) => {
      const parsed = decodePluginPreferenceEntry(entry);
      return parsed._tag === "Some" && typeof parsed.value.enabled === "boolean"
        ? [[name, parsed.value.enabled] as const]
        : [];
    }),
  );
});

/** agy skips a plugin folder without one of these manifests. */
const ANTIGRAVITY_PLUGIN_MANIFESTS = [
  "plugin.json",
  ".antigravity-plugin/plugin.json",
  ".gemini-plugin/plugin.json",
] as const;

const readAntigravityPluginManifest = Effect.fn("readAntigravityPluginManifest")(function* (
  pluginDirectory: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const manifest of ANTIGRAVITY_PLUGIN_MANIFESTS) {
    const manifestPath = path.join(pluginDirectory, manifest);
    const info = yield* readIfPresent(fileSystem.stat(manifestPath), manifestPath);
    if (info?.type !== "File") continue;
    if (info.size > BigInt(MAX_SKILL_BYTES)) {
      return yield* new AntigravitySkillsProbeError({
        reason: "scan-budget-exhausted",
        path: manifestPath,
      });
    }
    const raw = yield* readIfPresent(fileSystem.readFileString(manifestPath), manifestPath);
    const parsed = raw === undefined ? undefined : decodePluginManifest(raw);
    return { disabledByDefault: parsed?._tag === "Some" && parsed.value.disabled === true };
  }
  return undefined;
});

export const resolveManagedAntigravityUserPluginLinkTarget = Effect.fn(
  "resolveManagedAntigravityUserPluginLinkTarget",
)(function* (linkPath: string, userPluginsDirectory: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const target = yield* fileSystem.readLink(linkPath).pipe(
    Effect.map((value) => path.resolve(path.dirname(linkPath), value)),
    Effect.orElseSucceed(() => undefined),
  );
  return target !== undefined && path.dirname(target) === path.resolve(userPluginsDirectory)
    ? target
    : undefined;
});

export const readEnabledAntigravityUserPlugins = Effect.fn("readEnabledAntigravityUserPlugins")(
  function* (userHome: string) {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const pluginsDirectory = path.resolve(userHome, ".gemini", "config", "plugins");
    const plugins = new Map<string, string>();
    const names = yield* readIfPresent(
      fileSystem.readDirectory(pluginsDirectory),
      pluginsDirectory,
    );
    if (names === undefined || names.length === 0) return plugins;
    const preferences = yield* readAntigravityPluginPreferences(
      path.resolve(userHome, ".gemini", "config", "config.json"),
    );
    for (const name of names.toSorted()) {
      if (preferences.get(name) === false) continue;
      const pluginDirectory = path.join(pluginsDirectory, name);
      const info = yield* readIfPresent(fileSystem.stat(pluginDirectory), pluginDirectory);
      if (info?.type !== "Directory") continue;
      const manifest = yield* readAntigravityPluginManifest(pluginDirectory);
      if (manifest === undefined || manifest.disabledByDefault) continue;
      plugins.set(name, pluginDirectory);
    }
    return plugins;
  },
);

export const resolveEffectiveAntigravityPluginDirectories = Effect.fn(
  "resolveEffectiveAntigravityPluginDirectories",
)(function* (input: { readonly profileDirectory: string; readonly userHome: string }) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const pluginsDirectory = path.resolve(input.profileDirectory, "config", "plugins");
  const userPluginsDirectory = path.resolve(input.userHome, ".gemini", "config", "plugins");
  const plugins = yield* readEnabledAntigravityUserPlugins(input.userHome);
  const names = yield* readIfPresent(fileSystem.readDirectory(pluginsDirectory), pluginsDirectory);
  for (const name of names ?? []) {
    const pluginDirectory = path.join(pluginsDirectory, name);
    const managedTarget = yield* resolveManagedAntigravityUserPluginLinkTarget(
      pluginDirectory,
      userPluginsDirectory,
    );
    if (managedTarget !== undefined) continue;
    const info = yield* readIfPresent(fileSystem.stat(pluginDirectory), pluginDirectory);
    if (info?.type === "Directory") {
      plugins.set(name, pluginDirectory);
    } else {
      plugins.delete(name);
    }
  }
  if (plugins.size === 0) return [];
  const preferences = yield* readAntigravityPluginPreferences(
    path.resolve(input.profileDirectory, "config", "config.json"),
  );
  const enabled: Array<string> = [];
  for (const [name, pluginDirectory] of [...plugins].sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  )) {
    const override = preferences.get(name);
    if (override === false) continue;
    if (override !== true) {
      const manifest = yield* readAntigravityPluginManifest(pluginDirectory);
      if (manifest?.disabledByDefault) continue;
    }
    enabled.push(pluginDirectory);
  }
  return enabled;
});

function parseSkillFrontmatter(contents: string, fileName: string) {
  const start = contents.indexOf("---");
  if (start === -1) return undefined;
  const end = contents.indexOf("---", start + 3);
  if (end === -1) return undefined;
  try {
    const frontmatter = decodeSkillFrontmatter(
      parseYamlDocument(contents.slice(start + 3, end).trim()) ?? {},
    );
    const name = frontmatter.name || fileName.slice(0, -3);
    const description = frontmatter.description?.trim();
    // Native names are not trimmed. Do not rename one to fit the picker contract.
    if (!name || name !== name.trim()) return undefined;
    return { name, ...(description ? { description } : {}) };
  } catch {
    return undefined;
  }
}

/** The native loader orders child paths with Go's URL.EscapedPath encoding. */
function skillPathSortKey(entry: string) {
  return encodeURI(entry).replace(
    /[!'()*?#]/g,
    (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

const DASH = 0x2d;

/** Finds the byte offset after the second `---`, as parseSkillFrontmatter's indexOf calls do. */
function makeFrontmatterEndFinder() {
  let bytesSeen = 0;
  let dashes = 0;
  let markers = 0;
  return (chunk: Uint8Array) => {
    for (const byte of chunk) {
      bytesSeen += 1;
      dashes = byte === DASH ? dashes + 1 : 0;
      if (dashes === 3) {
        dashes = 0;
        markers += 1;
        if (markers === 2) return bytesSeen;
      }
    }
    return undefined;
  };
}

/** Reads through the frontmatter only, so skill bodies do not spend the scan budget. */
const readSkillFrontmatter = Effect.fn("readAntigravitySkill")(function* (
  skillPath: string,
  budget: ScanBudget,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const info = yield* readIfPresent(fileSystem.stat(skillPath), skillPath);
  if (info?.type !== "File") return undefined;

  const byteLimit = Math.min(MAX_SKILL_BYTES, budget.remainingBytes);
  const findFrontmatterEnd = makeFrontmatterEndFinder();
  let frontmatterEnd: number | undefined;
  const chunks = yield* readIfPresent(
    fileSystem
      .stream(skillPath, { bytesToRead: byteLimit + 1, chunkSize: SKILL_READ_CHUNK_BYTES })
      .pipe(
        Stream.takeUntil((chunk) => (frontmatterEnd = findFrontmatterEnd(chunk)) !== undefined),
        Stream.runCollect,
      ),
    skillPath,
  );
  if (chunks === undefined) return undefined;
  const bytes = Buffer.concat(chunks);
  budget.remainingBytes -= bytes.byteLength;
  if ((frontmatterEnd ?? bytes.byteLength) > byteLimit) {
    return yield* new AntigravitySkillsProbeError({
      reason: "scan-budget-exhausted",
      path: skillPath,
    });
  }
  return bytes.toString("utf8", 0, frontmatterEnd);
});

interface SkillEntryFilter {
  readonly includeGroups: ReadonlyArray<ReadonlyArray<string>>;
  readonly excludeNames: ReadonlyArray<string>;
}

const EMPTY_SKILL_FILTER: SkillEntryFilter = {
  includeGroups: [],
  excludeNames: [],
};

function matchesSkillFilter(entry: string, filter: SkillEntryFilter): boolean {
  for (const group of filter.includeGroups) {
    if (!group.includes(entry)) return false;
  }
  return !filter.excludeNames.includes(entry);
}

function narrowSkillFilter(
  parent: SkillEntryFilter,
  entry: typeof SkillConfigEntry.Type,
): SkillEntryFilter {
  const includeGroups =
    entry.include_only && entry.include_only.length > 0
      ? [...parent.includeGroups, entry.include_only]
      : parent.includeGroups;
  const excludeNames =
    entry.exclude && entry.exclude.length > 0
      ? [...parent.excludeNames, ...entry.exclude]
      : parent.excludeNames;
  return { includeGroups, excludeNames };
}

// agy does not expand ~, so a ~/ path stays relative to the config file.
function resolveSkillsConfigPath(path: Path.Path, rawPath: string, baseDir: string): string {
  return path.isAbsolute(rawPath) ? path.resolve(rawPath) : path.resolve(baseDir, rawPath);
}

/** Keep read errors typed so workspace snapshots cannot cache partial results. */
export const discoverAntigravitySkills = Effect.fn("discoverAntigravitySkills")(function* (input: {
  readonly cwd: string;
  readonly userHome: string;
  /** The instance's GEMINI_HOME, where agy loads plugins from. */
  readonly profileDirectory: string;
}): Effect.fn.Return<
  ReadonlyArray<ServerProviderSkill>,
  AntigravitySkillsProbeError,
  FileSystem.FileSystem | Path.Path
> {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const [configSkills, cliSkills] = antigravityUserSkillDirectories(
    path,
    path.join(input.userHome, ".gemini"),
  );
  const roots = [
    { directory: configSkills, scope: "user" },
    { directory: path.resolve(input.cwd, ".gemini", "skills"), scope: "project" },
    { directory: cliSkills, scope: "user" },
    { directory: path.resolve(input.cwd, ".agents", "skills"), scope: "project" },
    { directory: path.resolve(input.cwd, ".agent", "skills"), scope: "project" },
  ];
  const budget: ScanBudget = {
    remainingBytes: MAX_SCAN_BYTES,
    remainingEntries: MAX_SCAN_ENTRIES,
  };
  const skillsByName = new Map<string, ServerProviderSkill>();

  const addSkillFile = Effect.fn("addAntigravitySkillFile")(function* (
    skillPath: string,
    scope: string,
  ) {
    const fileName = path.basename(skillPath);
    if (fileName.toLowerCase() !== "skill.md" || !fileName.endsWith(".md")) return;
    const contents = yield* readSkillFrontmatter(skillPath, budget);
    if (contents === undefined) return;
    const skill = parseSkillFrontmatter(contents, fileName);
    if (!skill || skillsByName.has(skill.name)) return;
    skillsByName.set(skill.name, {
      ...skill,
      path: skillPath,
      scope,
      enabled: true,
    });
  });

  const scanDirectory = Effect.fn("scanAntigravitySkillDirectory")(function* (
    directory: string,
    scope: string,
    scanChildren: boolean,
    filter: SkillEntryFilter = EMPTY_SKILL_FILTER,
    acceptSkillFile = false,
  ): Effect.fn.Return<void, AntigravitySkillsProbeError, FileSystem.FileSystem> {
    const info = yield* readIfPresent(fileSystem.stat(directory), directory);
    if (info?.type === "File" && acceptSkillFile) {
      if (budget.remainingEntries === 0) {
        return yield* new AntigravitySkillsProbeError({
          reason: "scan-budget-exhausted",
          path: directory,
        });
      }
      budget.remainingEntries -= 1;
      if (matchesSkillFilter(path.basename(path.dirname(directory)), filter)) {
        yield* addSkillFile(directory, scope);
      }
      return;
    }
    if (info?.type !== "Directory") return;
    const entries = yield* readIfPresent(fileSystem.readDirectory(directory), directory);
    if (entries === undefined) return;
    if (entries.length > budget.remainingEntries) {
      return yield* new AntigravitySkillsProbeError({
        reason: "scan-budget-exhausted",
        path: directory,
      });
    }
    budget.remainingEntries -= entries.length;

    const sortedEntries = entries.toSorted();
    const skillFileName = sortedEntries.find((entry) => entry.toLowerCase() === "skill.md");
    if (skillFileName !== undefined) {
      yield* addSkillFile(path.join(directory, skillFileName), scope);
      return;
    }
    if (scanChildren) {
      const children = sortedEntries
        .filter((entry) => matchesSkillFilter(entry, filter))
        .map((entry) => ({ entry, sortKey: skillPathSortKey(entry) }))
        .sort((left, right) =>
          left.sortKey < right.sortKey ? -1 : left.sortKey > right.sortKey ? 1 : 0,
        );
      for (const { entry } of children) {
        yield* scanDirectory(path.join(directory, entry), scope, false);
      }
    }
  });

  const scanSkillsConfig = Effect.fn("scanAntigravitySkillsConfig")(function* (
    configPath: string,
    baseDir: string,
    scope: string,
    filter: SkillEntryFilter,
    visitedConfigs: ReadonlySet<string>,
    depth: number,
  ): Effect.fn.Return<void, AntigravitySkillsProbeError, FileSystem.FileSystem> {
    if (depth > MAX_INHERIT_DEPTH) return;
    const resolvedConfigPath = path.resolve(configPath);
    if (visitedConfigs.has(resolvedConfigPath)) return;
    const configInfo = yield* readIfPresent(
      fileSystem.stat(resolvedConfigPath),
      resolvedConfigPath,
    );
    if (configInfo?.type !== "File") return;
    const byteLimit = Math.min(MAX_SKILL_BYTES, budget.remainingBytes);
    if (configInfo.size > BigInt(byteLimit)) {
      return yield* new AntigravitySkillsProbeError({
        reason: "scan-budget-exhausted",
        path: resolvedConfigPath,
      });
    }
    const raw = yield* readIfPresent(
      fileSystem.readFileString(resolvedConfigPath),
      resolvedConfigPath,
    );
    if (raw === undefined) return;
    budget.remainingBytes -= Buffer.byteLength(raw, "utf8");
    const parsed = decodeSkillsConfig(raw);
    if (parsed._tag === "None") return;

    const nextVisited = new Set(visitedConfigs);
    nextVisited.add(resolvedConfigPath);
    for (const inherited of parsed.value.inherits ?? []) {
      const inheritedPath = resolveSkillsConfigPath(path, inherited.path, baseDir);
      yield* scanSkillsConfig(
        inheritedPath,
        path.dirname(inheritedPath),
        scope,
        narrowSkillFilter(filter, inherited),
        nextVisited,
        depth + 1,
      );
    }
    for (const entry of parsed.value.entries ?? []) {
      const entryDirectory = resolveSkillsConfigPath(path, entry.path, baseDir);
      yield* scanDirectory(entryDirectory, scope, true, narrowSkillFilter(filter, entry), true);
    }
  });

  for (const root of roots) {
    yield* scanDirectory(root.directory, root.scope, true);
  }

  for (const config of [
    {
      configPath: path.resolve(input.cwd, ".agents", "skills.json"),
      baseDir: input.cwd,
      scope: "project",
    },
    {
      configPath: path.resolve(input.cwd, ".agent", "skills.json"),
      baseDir: input.cwd,
      scope: "project",
    },
    {
      configPath: path.join(input.userHome, ".gemini", "config", "skills.json"),
      baseDir: path.join(input.userHome, ".gemini", "config"),
      scope: "user",
    },
  ]) {
    yield* scanSkillsConfig(
      config.configPath,
      config.baseDir,
      config.scope,
      EMPTY_SKILL_FILTER,
      new Set(),
      0,
    );
  }

  for (const pluginDirectory of yield* resolveEffectiveAntigravityPluginDirectories(input)) {
    yield* scanDirectory(path.join(pluginDirectory, "skills"), "user", true);
  }
  return [...skillsByName.values()].sort((left, right) => left.name.localeCompare(right.name));
});
