# GJC Compatibility Gap Matrix

**Document Status**: Historical Planning Reference (updated 2026-10-13)  
**Scope**: SDK wire protocol, session lifecycle, tools, skills, settings, hooks, commands, and AGENTS.md rules  
**Target**: Rust fork-in-spirit of gajae-code (GJC) SDK-compatible and capability-compatible  
**Note**: This document captures the initial compatibility analysis and proposed roadmap. Implementation details are now tracked in focused slice-specific documents (e.g., `tool-semantics-phase1.md`). Status updates reflect work merged as of 2026-10-13 (PRs #3330, #3332, #3334, #3336, #3339).

---

## 1. EXECUTIVE SUMMARY

This matrix maps GJC capabilities (from gajae-code) against claw-code's current Rust implementation, identifying gaps and proposing crate/module ownership for each slice.

### Key Findings

| Area | Status | Priority | Notes |
|------|--------|----------|-------|
| **SDK Wire Protocol** | ✅ Implemented | P0 (Foundation) | PR #3330: WebSocket transport, frame, error types |
| **Session Lifecycle** | 🟡 In Progress | P0 (Foundation) | SDK transport merged; session reconciliation in progress |
| **Tool Catalog (55 tools)** | 🟡 Partial (35/55+) | P1 (Core) | Read selectors (PR #3332/#3334); /tools, /info (PR #3339) |
| **Skill Discovery** | 🟡 Partial (basic) | P1 (Core) | No changes in recent slices |
| **Settings & Config** | 🟡 Partial | P2 (Enhancing) | Not yet addressed in landed PRs |
| **Hooks & Plugins** | ✅ Existing | P3 (Polish) | No changes needed |
| **AGENTS.md Rules** | ✅ Existing | P3 (Polish) | No changes needed |
| **Slash Commands (120+)** | 🟡 Partial (42/120+) | P2 (Core) | /tools, /info added (PR #3339) |
| **Subagent Delegation** | ✅ Existing | P2 (Core) | No changes needed |

---

## 2. SDK WIRE PROTOCOL

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/sdk/client/client.ts`, `sdk/host/session-runtime.ts`

GJC uses a **WebSocket-based JSON-RPC-like protocol** with:
- **Transport**: WebSocket over TCP
- **Frame Format**: JSON object with typed fields
- **Correlation**: Request/response matched by `frame.id` (UUID)
- **Operations**: 65+ operation types (Q1-Q32, C01-C06, reverse ops)
- **Error Handling**: Typed `SdkErrorCode` with `uncertain_after_send` recovery
- **Reconnect**: Exponential backoff with deadline management
- **Request Timeout**: 10s default (`DEFAULT_SDK_REQUEST_TIMEOUT_MS`), per-request override
- **Handshake**: `client.hello` with capabilities advertised; `server.hello` response

**Operations** (sample):
- **Control**: `turn.create`, `turn.prompt`, `turn.steer`, `turn.abort_and_prompt`
- **Queries**: `turn.list`, `turn.get_one`, `turn.message_list`, `skill.source`, `artifact.content`
- **Reverse**: `ask.answer`, `tool.dispatch`, `tool.fetch_capability`

**Event Schema**:
```json
{
  "id": "uuid-string",
  "operation": "turn.create",
  "input": { ... },
  "connectionId": "optional",
  "settings": { ... }
}
```

### claw-code Status

**Current** (as of PR #3330): Implemented. WebSocket-based JSON-RPC transport in `rust/crates/sdk/` with:
- `transport.rs`: WebSocket client/server, frame send/recv
- `frame.rs`: Frame struct, JSON serialization
- `error.rs`: SDK error types with `uncertain_after_send` recovery semantics

Remaining: Operation type definitions (Q1-Q32, C01-C06), reverse operation handlers, session reconciliation.

**Files**: None  
**Lines**: 0

**Reality**: 
- claw-code operates as a local CLI only (stdin/stdout/stderr).
- Sessions are file-based (`.claw/sessions/`).
- No remote access or external SDK clients.
- No WebSocket transport.
- No frame correlation or reconnect logic.

### Gap Assessment

| Component | GJC | claw-code | Gap | Complexity |
|-----------|-----|-----------|-----|------------|
| WebSocket Transport | 1500 LOC | — | 100% | High |
| Frame Correlation | 800 LOC | — | 100% | Medium |
| Operation Registry | 400 LOC | 0 | 100% | Medium |
| Error Handling & Recovery | 600 LOC | — | 100% | High |
| Handshake & Negotiation | 200 LOC | — | 100% | Low |
| **Total** | **3,500 LOC** | **0** | **3,500 LOC** | — |

### Proposed Implementation

**Owner Crate**: `sdk` (new)

**Module Structure**:
```
rust/crates/sdk/
├── Cargo.toml
└── src/
    ├── lib.rs              (exports + top-level API)
    ├── client/
    │   ├── mod.rs          (SdkClient, WebSocket transport)
    │   ├── correlation.rs  (frame ID tracking, reconnect)
    │   └── errors.rs       (SdkErrorCode, recovery)
    ├── protocol/
    │   ├── mod.rs          (Operation registry)
    │   ├── operations.rs   (operation definitions)
    │   └── schema.rs       (JSON schemas, frame validation)
    ├── session/
    │   ├── mod.rs          (session lifecycle over SDK)
    │   ├── router.rs       (session routing, durable state)
    │   └── reconciliation.rs (uncertain outcome recovery)
    └── tests/
        ├── mod.rs
        └── fixtures/       (mock server scenarios)
```

**Size Estimate**: 4,500 LOC (Rust + test fixtures)

**Dependencies**:
- `tokio` (async I/O, WebSocket)
- `tungstenite` (WebSocket protocol)
- `serde_json` (frame serialization)
- `uuid` (correlation IDs)
- `runtime` crate (session persistence bridge)

---

## 3. SESSION LIFECYCLE

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/sdk/host/session-runtime.ts`, `sdk/client/`, `sdk/lifecycle/`

GJC sessions have **three entry points**:
1. **Native SDK** (via WebSocket): typed `SdkClient` with `session.create`, `session.prompt`, etc.
2. **Browser ACP** (via iframe): same protocol, browser sandbox isolate
3. **CLI Daemon** (stdio): JSON-RPC over stdio for local CLI access

**Session Operations**:
- **create**: `turn.create(model, input)` → returns `sessionId`, persistent state written to broker DB
- **prompt**: `turn.prompt(request, tools, settings)` → streams events: `agent_begin`, `tool_use`, `tool_result`, `agent_end`
- **steer**: `turn.steer(overrides)` → mid-flight direction change (model, settings, system prompt)
- **abort**: `turn.abort_and_prompt()` → terminal failure + new prompt in one operation
- **list**: `turn.list(filters)` → paginated session history
- **get_one**: `turn.get_one(sessionId, turnId)` → single turn with full details

**State Persistence**:
- Invocation record (prompt request, outcome, correlation)
- Message buffer (system, assistant, tool results, agent events)
- Reconciliation store (uncertain → durable mapping)
- Terminal scope (last session per correlation key)

**Lifecycle Events**:
1. `turn_accepted` (immediate, transient)
2. `agent_begin` (before first model call)
3. `tool_use` + `tool_result` (interleaved)
4. `agent_end` (final model output)
5. `turn_ended` (durable, terminal outcome recorded)

### claw-code Status

**Current**: The SDK now exposes `sdk::session::SessionRouter` with durable `turn.create`, `turn.prompt`, `turn.steer`, `turn.abort_and_prompt`, and paginated `turn.list` handling. It bridges `runtime::Session`/`ConversationRuntime`, streams ordered lifecycle events, persists invocation outcomes for reconciliation, and stores terminal session scope by correlation key. The WebSocket transport remains a client transport; a remotely hosted broker and automatic recovery of an unknown post-crash outcome remain outside this slice.

**Files**:
- `rust/crates/runtime/src/session.rs` (1,961 LOC)
- `rust/crates/runtime/src/conversation.rs` (conversation loop and lifecycle event bridge)
- `rust/crates/runtime/src/session_control.rs` (file ops, migration)
- `rust/crates/runtime/src/lib.rs` (re-exports `Session`, `SessionStore`, `ConversationRuntime`)
- `rust/crates/sdk/src/session/` (router, event stream, reconciliation, terminal scope)

**Lines**: ~3,900 LOC (session + conversation)

**Reality**:
- `Session` holds persisted conversation state and `SessionStore` resolves session IDs in the workspace namespace.
- `ConversationRuntime` drives the local agent loop and emits lifecycle events to an optional callback.
- `sdk::session::SessionRouter` dispatches the five turn operations and exposes a per-invocation event stream.
- Invocation terminal outcomes and correlation-key terminal scopes are durable; startup recovery marks unfinished invocations uncertain for later reconciliation.
- A remote WebSocket server/broker and inference of an outcome that was never durably recorded are still absent.

### Gap Assessment

| Component | GJC | claw-code | Gap | Complexity |
|-----------|-----|-----------|-----|------------|
| Remote Session Router | 2,500 LOC | Local router/dispatch | Partial | High |
| Invocation Reconciliation | 1,200 LOC | Durable invocation records | Partial | High |
| Terminal Scope Tracking | 600 LOC | Implemented | — | Medium |
| Async Event Streaming | 800 LOC | Local per-invocation stream | Partial | Medium |
| Uncertain Outcome Recovery | 1,000 LOC | Unfinished records recovered as uncertain | Partial | High |
| **Total** | **6,100 LOC** | **Session lifecycle slice implemented** | **Remote broker remains** | — |

### Proposed Implementation

**Owner Crate**: `sdk` (new, or extend `runtime`)

**Module Structure** (`sdk/session/`):
```
sdk/session/
├── mod.rs                (session routing and operation dispatch)
├── reconciliation.rs    (uncertain → terminal recovery)
├── terminal_scope.rs    (per-correlation state retention)
└── events.rs            (ordered lifecycle event types)
```

**Integration Points**:
- Bridge `runtime::Session` and `ConversationRuntime` into SDK lifecycle operations
- Persist reconciliation and terminal-scope data alongside the session namespace
- Emit lifecycle callbacks from `ConversationRuntime`

**Size Estimate**: 2,500 LOC (Rust, new + modifications)

---

## 4. TOOL CATALOG (55 Tools)

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/tools/` (55 tool files + shared harness)

GJC implements **55 distinct tools** across multiple domains:
- **File I/O**: `read`, `write`, `edit`, `search`, `find`
- **Shell**: `bash`, `sleep`
- **Task Delegation**: `task` (subagent spawning)
- **Workflow**: `skill`, `skill_discovery`, `goal`, `ask`
- **Search**: `search_tool_bm25` (semantic search discovery)
- **Image**: `image_gen` (DALL-E), `image_tag_scan`
- **Integration**: `web_fetch`, `web_search`, `custom_tool_fetch`
- **Developer**: `notebook_edit`, `command_repl`, `shell_repl`, `powershell`
- **Config**: `config_read`, `config_write` (settings, modes)
- **Structured**: `model_registry`, `cost_calculator`, `settings_apply`
- **Workflow Gates**: `workflow_gate_answer`, `workflow_plan_approve`
- **Move**: `move_cwd`, `move_session`

**Tool Execution Model**:
1. Input deserialized from JSON schema
2. Permission check (read-only vs. destructive)
3. Tool invocation with typed inputs
4. Structured output or error
5. Result published to agent loop

**Permission Gating**: Every tool has a `required_permission` (read-only, approve, unsafe).

### claw-code Status

**Current** (as of PR #3332/#3334/#3339): Partial. 35+ tools implemented.
- PR #3332/#3334: Read tool enhanced with GJC-compatible line range selectors (`:50`, `:50-200`, `:50+150`, `:raw`, `:conflicts`, etc.)
- PR #3339: `/tools` and `/info` commands implemented
- Still missing: image_gen (DALL-E), ask (interactive), workflow_gate_*, command_repl, shell_repl, powershell, cost_calculator

**Files**: `rust/crates/tools/src/lib.rs` (10,892 LOC, single file with all 55 spec + implementations)

**Implemented Tools** (~35):
- `read`, `write`, `edit`, `search`, `find`
- `bash`, `sleep`
- `task` (subagent spawning)
- `skill`, `skill_discovery` (basic)
- `bash_list_*, bash_check_*` (validation helpers)
- `web_fetch`, `search_bm25` (basic)
- `config_*` (read/write settings)
- `notebooks` (edit, execute)
- `goal` (basic ledger)
- `move_*` (cwd, session)
- Various validation and utility tools

**Missing / Incomplete Tools** (~20):
- `image_gen` (DALL-E)
- `image_tag_scan`
- `custom_tool_fetch`
- `command_repl`, `shell_repl`, `powershell`
- `model_registry`, `cost_calculator`
- `workflow_gate_answer`, `workflow_plan_approve`
- `ask` (interactive prompt)
- `irc` (live coordination)
- Search/discovery integration tools
- Advanced structured output handling

### Gap Assessment

| Component | GJC | claw-code | Gap | Complexity |
|-----------|-----|-----------|-----|------------|
| Tool Registry | 400 LOC | 400 LOC | 0% | — |
| Spec Table (55 entries) | 1,800 LOC | 1,800 LOC | 0% | — |
| image_gen | 600 LOC | — | 100% | High |
| command_repl, shell_repl | 900 LOC | — | 100% | Medium |
| workflow_gate_* | 800 LOC | — | 100% | Medium |
| custom_tool_fetch | 400 LOC | — | 100% | Low |
| ask (interactive) | 1,200 LOC | Stub | 80% | High |
| cost_calculator | 300 LOC | — | 100% | Low |
| **Total** | **11,500 LOC** | **8,000** | **3,500 LOC** | — |

### Proposed Implementation

**Owner Crate**: `tools` (expand existing)

**New Modules**:
```
rust/crates/tools/src/
├── tools/
│   ├── image_gen.rs         (DALL-E integration)
│   ├── ask_interactive.rs   (blocking user prompts)
│   ├── workflow_gates.rs    (plan approval, gate answer)
│   ├── repl/*.rs            (command_repl, shell_repl, powershell)
│   └── cost_calc.rs         (pricing lookup)
└── vendors/
    ├── dalle.rs             (image generation API)
    └── integrations.rs      (custom tool fetch)
```

**Size Estimate**: 3,500 LOC (new tools + vendor integration)

**Dependencies**:
- `reqwest` (image generation API calls)
- `api` crate (provider client reuse)
- Existing: `runtime`, `commands`, `serde_json`

---

## 5. SKILL DISCOVERY & INVOCATION

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/tools/skill-discovery.ts`, `packages/coding-agent/src/tools/skill.ts`, `packages/coding-agent/src/sdk-skills/`

GJC supports **dynamic skill loading** with:
- **Discovery**: `skill_discovery` tool scans `.gjc/skills/`, project `.gjc/skills/`, bundled skills
- **Frontmatter Parsing**: YAML headers in `SKILL.md` define metadata (name, description, use conditions)
- **Invocation**: `skill` tool loads and executes a named skill (skill name + args)
- **Skill Execution**: Skill = subprocess call to `gjc /skill:<name>` with args
- **Bundled Skills**: `autoresearch`, `deep-interview`, `ralplan`, `ultragoal`
- **Project Skills**: Discoverable from `.gjc/skills/*/SKILL.md`
- **User Skills**: Discoverable from `~/.gjc/skills/*/SKILL.md`

**Skill Metadata**:
```yaml
# /skill:my-skill
# description: What this skill does
# scope: project | user | bundled
# requires: List of capabilities
```

**Skill Discovery Result**:
```json
{
  "name": "skill-name",
  "description": "...",
  "path": "...",
  "bundled": true | false,
  "scope": "project" | "user" | "bundled"
}
```

### claw-code Status

**Current**: Basic. Skill discovery exists but limited to filesystem scanning.

**Files**:
- `rust/crates/tools/src/lib.rs` (skill_discovery + skill dispatch, ~200 LOC)
- `rust/crates/runtime/src/prompt.rs` (skill loading in prompts, ~100 LOC)

**Implementation**:
- Scans `.claw/skills/` directory for `SKILL.md` files
- Parses frontmatter (basic YAML)
- Returns discovery results as JSON
- `skill` tool invokes skill as subprocess

**Missing**:
- Bundled skill registry (autoresearch, deep-interview, ralplan, ultragoal)
- Project + user skill discovery hierarchy
- Frontmatter validation and standardization
- Skill chaining (`skill()` tool to invoke another skill from within a skill)
- Skill state persistence (workflow handoff)

### Gap Assessment

| Component | GJC | claw-code | Gap | Complexity |
|-----------|-----|-----------|-----|------------|
| Bundled Skill Registry | 200 LOC | — | 100% | Low |
| Frontmatter Parser | 150 LOC | 100 LOC | 30% | Low |
| Project + User Discovery | 300 LOC | Basic | 70% | Low |
| Skill Chaining | 250 LOC | — | 100% | Medium |
| Workflow State Handoff | 400 LOC | — | 100% | Medium |
| Skill Execution Harness | 200 LOC | Partial | 40% | Low |
| **Total** | **1,500 LOC** | **100** | **1,400 LOC** | — |

### Proposed Implementation

**Owner Crate**: `tools` (expand existing) or `skills` (new, lightweight)

**Module Structure**:
```
rust/crates/tools/src/
├── skill_discovery.rs       (enhanced parsing, project/user/bundled hierarchy)
├── skill_execution.rs       (subprocess dispatch, state handoff)
├── bundled_skills.rs        (registry of autoresearch, deep-interview, ralplan, ultragoal)
└── skill_state.rs           (workflow handoff serialization)
```

**Size Estimate**: 1,400 LOC (Rust)

**Integration**:
- Extend `skill_discovery` tool to search three scopes (bundled, project, user)
- Add `skill` tool support for chaining (returning another skill invocation)
- Store workflow state in durable files for handoff

---

## 6. SETTINGS & CONFIGURATION

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/config.ts`, `packages/coding-agent/src/sdk/host/session-runtime.ts`

GJC supports **settings override** at multiple levels:
1. **Global Settings**: `~/.gjc/config.json` (user defaults)
2. **Session Settings**: per-session override via `turn.create` or `turn.prompt` input
3. **Runtime Override**: `settings` field in SDK request frames
4. **Setting Categories**: 
   - **Model**: `modelName`, `modelTemperature`, `modelMaxTokens`
   - **Behavior**: `allowance`, `permissions.*`, `features.*`
   - **UX**: `theme`, `shell`, `notification*`
   - **Advanced**: `memory.context`, `memory.compression`, `cost.*`

**Config Schema**: Typed with JSON schema validation per setting

**Permission Modes**: 
- `read_only` (no execution)
- `approve` (interactive approval)
- `unsafe` (no gates)

### claw-code Status

**Current**: Partial. Config exists but no session-level override or setting propagation.

**Files**:
- `rust/crates/runtime/src/config.rs` (3,894 LOC, largest file in runtime)
- `rust/crates/runtime/src/config_validate.rs` (validation helpers)

**Implementation**:
- Reads `.claw/config.json` at startup
- Stores global settings (model, permissions, plugins, features)
- No session-level override
- No setting propagation through tool execution
- No typed setting validation against schema

**Missing**:
- Per-session setting override (session-specific model, permissions)
- Setting propagation to all tool invocations
- Dynamic setting application (e.g., changing model mid-session)
- Setting schema validation
- Config hotreload (changes without restart)

### Gap Assessment

| Component | GJC | claw-code | Gap | Complexity |
|-----------|-----|-----------|-----|------------|
| Setting Schema Definition | 300 LOC | — | 100% | Low |
| Session-Level Override | 400 LOC | — | 100% | Medium |
| Setting Propagation | 300 LOC | — | 100% | Low |
| Dynamic Application | 200 LOC | — | 100% | Medium |
| Config Hotreload | 150 LOC | — | 100% | Low |
| **Total** | **1,350 LOC** | **3,894** | **-2,544 LOC** | — |

### Proposed Implementation

**Owner Crate**: `runtime` (enhance existing `config.rs`)

**Additions to config.rs**:
- Setting schema registry
- Session setting override struct
- Setting applicator (merge global + session + override)
- Hotreload support (watch config file, signal on change)

**Size Estimate**: 500 LOC (new code, mostly schema + applicators)

---

## 7. HOOKS & PLUGINS

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/hooks.ts`, `packages/coding-agent/src/plugin_lifecycle.ts`

GJC provides **5 hook points**:
- `on_session_created`: After session starts
- `on_prompt_received`: Before agent loop begins
- `on_tool_use`: Before tool invocation
- `on_tool_result`: After tool completes
- `on_session_ended`: Before session cleanup

**Plugin System**:
- Plugins are shell scripts or binaries in `.claw/plugins/`
- Each plugin implements hook handlers (exit code + JSON output)
- Plugin registry stored in config

### claw-code Status

**Current**: Existing and comprehensive.

**Files**:
- `rust/crates/runtime/src/hooks.rs` (L155)
- `rust/crates/runtime/src/plugin_lifecycle.rs`
- `rust/crates/plugins/src/` (plugin manager)

**Implementation**: Hooks fully implemented; plugin system working.

### Gap Assessment

| Component | GJC | claw-code | Gap |
|-----------|-----|-----------|-----|
| Hook Points | 5 | 5 | 0% |
| Plugin Registry | ✓ | ✓ | 0% |
| Plugin Execution | ✓ | ✓ | 0% |

**Status**: **No gaps**. Hooks and plugins are fully compatible.

---

## 8. AGENTS.MD RULES & CONTEXT

### GJC Behavior (gajae-code Reference)

**Source**: gajae-code root + `docs/` (AGENTS.md files at multiple levels)

GJC loads **directory-specific rules** from `AGENTS.md` files:
- **Root AGENTS.md**: Project-level knowledge base
- **Directory AGENTS.md**: Directory-specific rules override parent
- **Rules Loaded**: Automatically prepended to system prompt during agent reasoning
- **Hierarchy**: Deepest (most specific) rules override higher level

**Rule Content**:
- Code organization and conventions
- Tool restrictions (never use X in Y context)
- Testing requirements
- Deployment/safety gates
- Architecture decisions

### claw-code Status

**Current**: Existing and matching GJC.

**Files**:
- Root: `AGENTS.md`
- Crate-level: `rust/crates/*/AGENTS.md`
- Implementation: `rust/crates/runtime/src/` (loads rules)

**Implementation**: Rules loaded and applied to prompts.

### Gap Assessment

| Component | GJC | claw-code | Gap |
|-----------|-----|-----------|-----|
| Rule Loading | ✓ | ✓ | 0% |
| Hierarchy | ✓ | ✓ | 0% |
| Prompt Integration | ✓ | ✓ | 0% |

**Status**: **No gaps**. AGENTS.md system is compatible.

---

## 9. SLASH COMMANDS (120+ Commands)

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/commands/lib.ts` (7,200 LOC with 120+ command specs)

GJC implements **120+ slash commands** for REPL interaction:
- **Session**: `/session list`, `/session create`, `/session restore`, `/session delete`
- **Model**: `/model set`, `/model list`, `/model info`
- **Permissions**: `/permissions set`, `/permissions show`
- **Config**: `/config get`, `/config set`, `/config reload`
- **Skills**: `/skills list`, `/skills install`, `/skills create`
- **Plugins**: `/plugins list`, `/plugins install`
- **Workflow**: `/plan`, `/review`, `/approve`, `/ultragoal`
- **Debugging**: `/debug`, `/dump`, `/trace`
- **Utilities**: `/help`, `/clear`, `/quit`, `/export`

**Command Format**: `/command [arg1] [arg2] ...` with arg parsing per command

### claw-code Status

**Current** (as of PR #3339): Partial. ~42 commands implemented out of 120+.
- PR #3339: Added `/tools` and `/info` commands
- Remaining: /model set/list, /theme, /cost, /dump, /trace, /tasks list/cancel, workflow commands (/plan, /review, /ultragoal), and others

**Files**:
- `rust/crates/commands/src/lib.rs` (7,183 LOC)
- Command specs in `SLASH_COMMAND_SPECS` static table

**Implemented Commands** (~40):
- Session mgmt (list, create, etc.)
- Help, status, version
- Config (get, set, reload)
- Skills (list, install)
- Plugins (list, install)
- Workspace/debug (clear, compact, etc.)

**Missing Commands** (~80):
- Advanced workflow (`/plan`, `/review`, `/ultragoal`)
- Model selection and info
- Cost calculator (`/cost`)
- Advanced debugging (`/dump`, `/trace`)
- Theme management
- Advanced permission controls
- Task management (`/tasks list`, `/tasks cancel`)
- IRC/coordination (`/irc`)
- Many others

### Gap Assessment

| Component | GJC | claw-code | Gap |
|-----------|-----|-----------|-----|
| Command Parser | 800 LOC | 800 LOC | 0% |
| Spec Table (120+ entries) | 2,000 LOC | 2,000 LOC | 0% |
| Handlers (80 missing) | 3,200 LOC | 1,800 LOC | 56% |
| Help/suggestion | 600 LOC | 400 LOC | 30% |
| **Total** | **7,200 LOC** | **4,000** | **3,200 LOC** | — |

### Proposed Implementation

**Owner Crate**: `commands` (expand existing)

**New Modules**:
```
rust/crates/commands/src/
├── handlers/
│   ├── workflow.rs          (/plan, /review, /approve, /ultragoal)
│   ├── model.rs             (/model set, /model list, /model info)
│   ├── cost.rs              (/cost)
│   ├── advanced_debug.rs    (/dump, /trace, /debug advanced)
│   ├── theme.rs             (/theme set, /theme list)
│   ├── task.rs              (/tasks list, /tasks cancel)
│   └── advanced_perms.rs    (advanced permission commands)
```

**Size Estimate**: 3,200 LOC (new command handlers)

**Dependencies**: `runtime`, `tools`, `plugins`

---

## 10. SUBAGENT DELEGATION & TASK EXECUTION

### GJC Behavior (gajae-code Reference)

**Source**: `packages/coding-agent/src/tools/task.ts`, `packages/coding-agent/src/task/`

GJC's **subagent system** enables parallel task execution:
- **task Tool**: Spawn parallel subtasks with independent context
- **Task States**: spawning, running, succeeded, failed
- **Task Isolation**: Each task has isolated session, permissions, context
- **Task Coordination**: `irc` tool for live inter-task communication
- **Task Monitoring**: `task.inspect`, `task.list`, `task.cancel`
- **Artifact Sharing**: Tasks share artifact URLs but not direct state

**Task Spawning**:
```json
{
  "agent": "executor|architect|planner",
  "tasks": [
    {
      "id": "task-1",
      "description": "...",
      "assignment": "..."
    }
  ]
}
```

### claw-code Status

**Current**: Existing and working.

**Files**:
- `rust/crates/tools/src/lib.rs` (task tool, ~300 LOC)
- `rust/crates/runtime/src/task_*.rs` (task execution, state mgmt)

**Implementation**: Task tool works; spawning, monitoring, and coordination functional.

### Gap Assessment

| Component | GJC | claw-code | Gap |
|-----------|-----|-----------|-----|
| Task Spawning | ✓ | ✓ | 0% |
| Task Isolation | ✓ | ✓ | 0% |
| Task Monitoring | ✓ | ✓ | 0% |
| IRC Coordination | ✓ | ✓ | 0% |

**Status**: **No significant gaps**. Task execution is compatible.

---

## 11. IMPLEMENTATION ROADMAP

**Status Summary** (as of 2026-10-13):
- Phase 1.1 (SDK transport): ✅ Merged via PR #3330
- Phase 1.2 (Operation registry): 🟡 In progress
- Phase 1.3 (Session integration): 🟡 In progress
- Phase 2.1 (Tool enhancement): 🟡 Partial (read selectors via PR #3332/#3334)
- Phase 3.2 (Slash commands): 🟡 Partial (/tools, /info via PR #3339)

### Phase 1: Foundation (SDK Wire Protocol)

**Slice 1.1: WebSocket Transport & Frame Correlation** ✅ (PR #3330)
- Create `sdk` crate structure
- Implement `SdkClient` with WebSocket connection
- Add frame ID correlation and response matching
- Add reconnect logic with exponential backoff
- Tests: transport connectivity, frame correlation, reconnect behavior

**Slice 1.2: Operation Registry & Schema**
- Define Operation enum (65+ operation types)
- JSON schema validation for requests
- Error code definitions and handlers
- Uncertain outcome recovery protocol
- Tests: operation parsing, schema validation, error scenarios

**Slice 1.3: Session SDK Integration**
- Bridge `runtime::Session` to SDK session model
- Implement session router (durable lookup)
- Add invocation reconciliation store
- Terminal scope tracking per correlation
- Tests: session creation, routing, reconciliation

### Phase 2: Core Capabilities (Tools & Skills)

**Slice 2.1: Missing Tool Implementation**
- `image_gen` (DALL-E integration)
- `ask` (interactive blocking prompts)
- `workflow_gate_*` (plan approval gates)
- `command_repl`, `shell_repl`, `powershell`
- `cost_calculator`

**Slice 2.2: Skill Enhancement**
- Bundled skill registry (autoresearch, deep-interview, ralplan, ultragoal)
- Enhanced skill discovery (project, user, bundled hierarchy)
- Skill chaining support (skill calling skill)
- Workflow state handoff

### Phase 3: Polish (Settings, Commands, Integration)

**Slice 3.1: Settings & Configuration**
- Session-level setting override
- Setting schema validation
- Setting propagation to tools
- Config hotreload

**Slice 3.2: Slash Commands**
- Workflow commands (/plan, /review, /ultragoal)
- Model selection (/model set, /model list)
- Cost calculator (/cost)
- Advanced debugging (/dump, /trace)
- Task management (/tasks list, /tasks cancel)
- Theme management (/theme)

---

## 12. DEPENDENCY & CRATE RELATIONSHIPS

```
┌────────────────────────────────────────────────────────┐
│ cli (rusty-claude-cli: main entry, REPL, TUI)          │
└────────────┬───────────────────────────────────────────┘
             │
┌────────────▼──────────────────────────────────────────┐
│ commands (slash command specs & handlers)              │
└────────┬────────────────────────────────────────┬──────┘
         │                                        │
         │                                    ┌───▼────────────────────┐
         │                                    │ tools (55 tool impl)    │
         │                                    └──────┬─────────────────┘
         │                                           │
┌────────▼─────────────────────────────────────────▼──────────────────┐
│ runtime (session, conversation, config, mcp, hooks, permissions)   │
└────────┬──────────────────────────────┬────────────────────────────┘
         │                              │
    ┌────▼──────────────┐       ┌──────▼─────────────────┐
    │ api (providers)   │       │ plugins (plugin mgmt) │
    └───────────────────┘       └───────────────────────┘
    
    NEW: sdk (WebSocket, session routing, reconciliation)
         │
         └─→ depends on: runtime, serde_json, tokio, tungstenite
```

---

## 13. RISK & MITIGATION

| Risk | Probability | Severity | Mitigation |
|------|-------------|----------|-----------|
| SDK protocol complexity | High | High | Start with mock transport tests, iterate on real scenarios |
| Session state inconsistency | Medium | High | Comprehensive reconciliation tests, durable state verification |
| Tool compatibility gaps | Medium | Medium | Tool-by-tool comparison with GJC, shadow testing |
| Breaking changes to runtime | Medium | High | Version gates, feature flags, phased rollout |
| Performance regression | Medium | Medium | Benchmark key paths (session create, tool dispatch), profile |

---

## 14. VERIFICATION & SIGN-OFF

- [ ] SDK transport connects and communicates with mock server
- [ ] Session lifecycle operations (create, prompt, steer, abort) work end-to-end
- [ ] Tool execution with SDK matches GJC behavior
- [ ] Skill discovery finds all three scopes (bundled, project, user)
- [ ] Settings override works per-session
- [ ] Slash commands parse and dispatch correctly
- [ ] All AGENTS.md tests pass
- [ ] Hooks fire at correct lifecycle points
- [ ] Subagent tasks spawn and communicate via IRC
- [ ] Public documentation has no internal paths, hosts, or private infra

---

**Document Version**: 1.0  
**Last Updated**: 2026-10-13  
**Next Review**: After Slice 1.1 implementation complete
