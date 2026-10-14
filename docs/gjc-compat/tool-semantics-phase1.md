# GJC Tool Semantics Alignment: Phase 1 (read/edit/search/find)

**Status**: Implementation plan  
**Issue**: #3323  
**Scope**: Slice 2.1 - Tool catalog semantic parity for read/edit/search/find  
**Target**: Ship in batches with focused PRs

---

## 1. EXECUTIVE SUMMARY

This document details the semantic gaps between claw-code's current tool implementations and GJC's (gajae-code) reference implementations for the highest-value tools: **read**, **edit**, **search**, and **find**.

### Key Gaps

| Tool | Current (claw-code) | Target (GJC) | Gap | Complexity |
|------|-----|--------|-----|------------|
| **read** | offset/limit pagination | Line selectors (`:L-L+N`), raw mode, truncation footers | High | Medium |
| **write** | Simple file replacement | Full replacement semantics (matching GJC) | Low | Low |
| **edit** | String replacement (old→new) | String replacement with rich patch output | Low | Low |
| **find** (glob) | Pattern expansion only | Recursive glob with filtering | Low | Low |
| **search** (grep) | Regex search with context | Regex with context + advanced filtering | Low | Low |

---

## 2. GJC TOOL SEMANTICS REFERENCE

### 2.1 READ Tool

**Source**: `gajae-code/packages/coding-agent/src/tools/read.ts` (3,500+ LOC)

#### Input Schema
```typescript
{
  path: string;           // File path (relative or absolute)
  offset?: number;        // Line offset (for pagination)
  limit?: number;         // Lines to read (for pagination)
  // GJC extensions (missing in claw-code):
  selector?: string;      // Line range syntax:
                          // ":50" = line 50 onward
                          // ":50-200" = lines 50-200 inclusive
                          // ":50+150" = 150 lines starting at line 50
                          // ":raw" = full file verbatim
                          // ":conflicts" = git merge conflicts only
  truncationDirection?: "head" | "tail" | "both";  // Which end to keep
  maxSummaryBytes?: number;  // Summary budget (default 50KB)
  maxSummaryLines?: number;  // Summary budget (default 20K lines)
}
```

#### Output Structure
```json
{
  "file": {
    "path": "src/main.rs",
    "content": "...",
    "totalLines": 500,
    "totalBytes": 12345,
    "truncatedBy": "lines" | "bytes" | "middle" | null,
    "displayMode": "summarized" | "raw" | "receipt"
  },
  "truncationFooter": "[Showing lines 1-50 of 500; re-read ...]"
}
```

#### Key Behaviors

1. **Line Range Selectors** (`:selector` syntax)
   - `:50` = start at line 50
   - `:50-200` = lines 50-200 inclusive
   - `:50+150` = 150 lines starting at 50
   - `:50+1` = exactly one line
   - `:5-16,960-973` = multiple ranges
   - `:raw` = full verbatim
   - `:conflicts` = git merge conflicts

2. **Truncation Modes**
   - Default: 50KB bytes limit, 20K lines limit
   - Direction: `head` (keep start), `tail` (keep end), `both` (keep both ends)
   - Footer includes recovery hint (e.g., "`re-read path:1-9999` for verbatim")

3. **Structural Summarization**
   - Code: collapse function bodies to `..` or `…` (merged brace pairs)
   - Prose: preserve full lines, truncate at word boundaries
   - Special files: documents (PDF, DOCX, XLSX) converted via markit

4. **Archive Support**
   - `.tar`, `.tar.gz`, `.tgz`, `.zip`: member read with `archive.ext:path/inside`
   - SQLite: `.sqlite:table` or `.sqlite:table:key` queries
   - Line ranges apply to archive members

5. **Output Formatting**
   - Anchored output: Line prefixes with source line numbers and hash anchors
   - Optional hash lines: Marks structural boundaries (`# pragma`, function start, etc.)
   - Prose column cap: Long lines wrapped/truncated at column limit

### 2.2 EDIT Tool

**Source**: `gajae-code/packages/coding-agent/src/tools/edit.ts`

#### Input Schema
```typescript
{
  path: string;           // File to edit
  old_text: string;       // Text to replace (required for edit)
  new_text: string;       // Replacement text
  all?: boolean;          // Replace all occurrences (default: first only)
}
```

#### Output Structure
```json
{
  "file": {
    "path": "src/main.rs",
    "editCount": 1,
    "beforeSize": 12345,
    "afterSize": 12400
  },
  "patch": [
    {
      "type": "replace",
      "lineStart": 50,
      "lineEnd": 55,
      "before": "old content",
      "after": "new content",
      "context": {"before": 3, "after": 3}
    }
  ]
}
```

#### Key Behaviors

1. **Exact Match Required**: `old_text` must match exactly (no fuzzy matching)
2. **Smallest Replacement**: Use smallest unique old_text that identifies the change
3. **All-or-Nothing**: Either all occurrences replaced or none (no partial)
4. **Patch Metadata**: Line numbers, context, before/after preserved

### 2.3 SEARCH Tool

**Source**: `gajae-code/packages/coding-agent/src/tools/search.ts`

#### Input Schema
```typescript
{
  pattern: string;        // Regex pattern
  paths?: string[];       // Files/dirs to search
  context?: number;       // Context lines before/after (default: 2)
  i?: boolean;            // Case-insensitive
  glob_exclude?: string[];  // Patterns to skip
  file_type?: string;     // Filter by extension
  limit?: number;         // Max results
  truncation?: "head" | "tail" | "both";
}
```

#### Output Structure
```json
{
  "matches": [
    {
      "file": "src/main.rs",
      "lineNumber": 42,
      "line": "fn process() {",
      "before": ["// Process data", ""],
      "after": ["  // Implementation", ""]
    }
  ],
  "summary": "Found 5 matches in 3 files"
}
```

### 2.4 FIND Tool

**Source**: `gajae-code/packages/coding-agent/src/tools/find.ts`

#### Input Schema
```typescript
{
  paths: string[];        // Glob patterns
  timeout?: number;       // Timeout in seconds (default: 5)
  gitignore?: boolean;    // Respect .gitignore (default: true)
  hidden?: boolean;       // Include hidden files (default: true)
  limit?: number;         // Max results (default: 1000)
}
```

#### Output Structure
```json
{
  "files": [
    {
      "path": "src/main.rs",
      "modified": "2026-10-13T12:34:56Z",
      "size": 12345
    }
  ],
  "summary": "Found 42 files matching pattern",
  "truncated": false
}
```

---

## 3. CURRENT CLAW-CODE IMPLEMENTATION GAPS

### 3.1 READ Tool Gaps

**File**: `rust/crates/runtime/src/file_ops.rs:713`

**Current**:
```rust
pub fn read_file_in_workspace(
    path: &str,
    offset: Option<usize>,     // Line offset only
    limit: Option<usize>,      // Lines only
    workspace_root: &Path,
) -> io::Result<ReadFileOutput>
```

**Missing**:
- ✗ Line range selectors (`:L-L+N` syntax)
- ✗ Raw mode (`:raw`)
- ✓ Git merge conflict extraction (`:conflicts`)
- ✗ Truncation footers with recovery hints
- ✗ Structural summarization (collapse braces)
- ✗ Archive/SQLite support
- ✗ Multi-range selectors (`:5-16,960-973`)
- ✗ Prose column cap
- ✗ Hash line anchors

**Estimated LOC to Add**: 1,200-1,500 (including tests)

### 3.2 EDIT Tool Gaps

**File**: `rust/crates/runtime/src/file_ops.rs:760`

**Current**: Basic string replacement works  
**Missing**:
- ✗ Rich patch metadata (line numbers, context lines)

**Estimated LOC to Add**: 200-300

### 3.3 SEARCH Tool Gaps

**File**: `rust/crates/tools/src/lib.rs` (grep search)

**Current**: Basic regex with context  
**Missing**:
- ✗ Advanced filtering (file type, glob exclude)
- ✗ Structured output format

**Estimated LOC to Add**: 300-400

### 3.4 FIND Tool Gaps

**File**: `rust/crates/tools/src/lib.rs` (glob search)

**Current**: Basic glob expansion  
**Missing**:
- ✗ Recursive globs
- ✗ Gitignore filtering
- ✗ Hidden file control
- ✗ File metadata

**Estimated LOC to Add**: 200-300

---

## 4. IMPLEMENTATION PLAN (BATCHED)

### Batch 1: READ Tool (Priority P0)
- Add line range selector parsing
- Implement `:raw` mode
- ✓ Implement `:conflicts` mode
- Add truncation footers with recovery hints
- Implement structural summarization
- Add tests for all selector combinations

**Files to Modify**:
- `rust/crates/runtime/src/file_ops.rs` (read_file_in_workspace)
- `rust/crates/tools/src/lib.rs` (ReadFileInput struct)
- Tests in both

**Size Estimate**: 1,200 LOC  
**Complexity**: Medium (string parsing, output formatting)

### Batch 2: EDIT Tool (Priority P1)
- Add patch metadata to output
- Ensure exact match semantics
- Add comprehensive tests

**Files to Modify**:
- `rust/crates/runtime/src/file_ops.rs` (edit_file_in_workspace)
- Tests

**Size Estimate**: 200 LOC  
**Complexity**: Low

### Batch 3: SEARCH/FIND Tools (Priority P1)
- Enhance grep/find with GJC-compatible filtering
- Add structured output format
- Add metadata (file type, modified time)

**Files to Modify**:
- `rust/crates/tools/src/lib.rs` (glob_search, grep_search)
- `rust/crates/runtime/src/file_ops.rs`

**Size Estimate**: 500 LOC  
**Complexity**: Low-Medium

---

## 5. VERIFICATION STRATEGY

Each batch will include:
1. **Semantic Tests**: Verify tool inputs/outputs match GJC reference
2. **Edge Case Tests**: Line ranges, truncation, archives, etc.
3. **Integration Tests**: End-to-end with claw CLI
4. **Parity Shadow Test**: Run same inputs against both GJC and claw-code, compare outputs

---

## 6. ROLLOUT SCHEDULE

- **Batch 1 (READ)**: Week 1 - Enable most impactful tool first
- **Batch 2 (EDIT)**: Week 1-2 - Quick follow-up
- **Batch 3 (SEARCH/FIND)**: Week 2 - Lower priority but valuable for discoverability

---

## 7. REFERENCES

- GJC Read Tool: `/home/bellman/Workspace/gajae-code/packages/coding-agent/src/tools/read.ts`
- claw-code file_ops: `rust/crates/runtime/src/file_ops.rs`
- claw-code tools: `rust/crates/tools/src/lib.rs`
- Gap Matrix: `docs/gjc-compat/gap-matrix.md`
