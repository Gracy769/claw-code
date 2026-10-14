# Bundled Skills Registry and Chaining

## Overview

The bundled skills registry enables claw-code to include built-in skills with each release, similar to how bundled plugins work. This feature supports skill chaining, allowing skills to invoke and compose with other skills.

## Implementation

### Architecture

**Bundled Skills Root Discovery**: Skills are discovered from the following locations (in order of precedence):

1. `<exe_dir>/../share/claw/skills/bundled` — standard FHS install layout
2. `<exe_dir>/bundled-skills` — simple relocated layout  
3. `CARGO_MANIFEST_DIR/bundled` — dev/source-tree fallback
4. Default fallback if above paths don't exist

The directory structure mirrors the plugin system:
```
rust/crates/commands/bundled/
├── example-skill/
│   └── SKILL.md
└── chain-example/
    └── SKILL.md
```

### SkillOrigin Enum

A new `Bundled` variant was added to the `SkillOrigin` enum:

```rust
enum SkillOrigin {
    SkillsDir,           // User/project skills directories
    LegacyCommandsDir,   // Legacy /commands directories
    Bundled,             // Built-in bundled skills [NEW]
}
```

### Skill Discovery

The `discover_skill_roots()` function was updated to include bundled skills:
- Bundled skills are added to the discovery roots with `SkillOrigin::Bundled`
- They're loaded using the same mechanism as regular skills
- Bundled skills have origin metadata ("bundled") in JSON output

### Skill Chaining

Skills can invoke other skills through standard mechanisms:

1. **Slash Command Invocation**: A skill can document that it chains to another skill
   ```
   /skill chain-example
   ```
   
2. **Tool-based Invocation**: Via the Skill tool or similar mechanisms

Skills marked as "bundled" indicate they're managed by the claw-code release and persist across upgrades.

## Example Bundled Skills

### `example-skill`
A simple demonstration skill showing the bundled skills feature.

### `chain-example`  
An example showing how one skill can reference and chain to other skills (e.g., `example-skill`).

## Testing

Three new tests were added:

1. **`discovers_bundled_skills_from_binary_layout`**
   - Verifies that `discover_skill_roots()` includes bundled skills
   - Tests directory discovery logic

2. **`bundled_skills_have_correct_origin_metadata`**
   - Confirms `SkillOrigin::Bundled` generates correct metadata
   - Tests JSON output format

3. **`bundled_skills_are_included_in_json_output`**
   - Verifies loaded bundled skills appear in JSON output
   - Tests end-to-end loading flow

All tests pass with:
```bash
cargo test -p commands --lib
```

## Backward Compatibility

- Existing skill discovery paths unchanged
- `SkillsDir` and `LegacyCommandsDir` origins fully preserved
- No changes to skill file format or content
- Bundled skills are additive; they don't replace user/project skills

## Future Enhancement Points

1. **Bundled Skills Registry Sync**: Auto-sync bundled skills into a system registry (similar to plugins)
2. **Skill Dependency Resolution**: Automatic dependency discovery and loading
3. **Skill Versioning**: Track versions of bundled skills
4. **Skill Auto-Update**: Sync bundled skills on claw-code updates

## Related

- **Issue**: #3324 [Slice 2.2] Skills bundled registry & chaining
- **Plugin System**: `rust/crates/plugins/src/lib.rs` (bundled plugins reference implementation)
- **Commands Crate**: `rust/crates/commands/src/lib.rs` (implementation location)
