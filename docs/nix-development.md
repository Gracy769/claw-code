# Nix flake development environment

This repository includes a **`flake.nix`** for reproducible development environments on NixOS and systems with Nix installed.

## Prerequisites

- **Nix** (version 2.4 or later with flakes support enabled)
- **direnv** (optional, for automatic environment loading via `.envrc`)

To verify Nix is installed and flakes are enabled:

```bash
nix --version
nix flake --help
```

If flakes are not enabled, add this to your `~/.config/nix/nix.conf`:

```
experimental-features = nix-command flakes
```

## Quick start

### Enter the development environment

**Option 1: Using `nix flake`** (without direnv)

```bash
nix flake update              # Update inputs (optional, pins nixpkgs)
nix develop                    # Enter default dev shell
nix develop .#full             # Enter full dev shell with extra tools
exit                           # Leave the shell
```

**Option 2: Using direnv** (automatic on directory entry)

```bash
direnv allow                   # Trust the .envrc file (one-time setup)
cd .                           # Direnv reloads the flake automatically
```

When you leave the repository directory, the environment is automatically unloaded.

## Development shells

The flake provides two development shells:

### `default` (minimal)

```bash
nix develop
```

Includes:
- Rust (stable, latest)
- Rust source code (`rust-src`)
- `cargo-edit` (dependency management helpers)
- `cargo-expand` (macro expansion tool)
- `cargo-watch` (file watcher for incremental builds)
- Build tools: `pkg-config`, `openssl`, `git`, `ca-certificates`

### `full` (with extra tooling)

```bash
nix develop .#full
```

Includes everything from `default` plus:
- `clippy` (linter)
- `rustfmt` (code formatter)
- `mold` (fast linker)
- `lldb` (debugger)

## Building the claw binary

### With the dev environment

```bash
nix develop
cd rust
cargo build -p rusty-claude-cli
```

### Directly with `nix build`

```bash
nix build .#claw          # Build the claw binary
nix build                 # Same as .#claw (default package)
```

The compiled binary is symlinked to `./result/bin/claw`.

## Running common tasks

Once in the dev shell:

```bash
# Format code
cd rust
cargo fmt

# Run clippy linter
cargo clippy --workspace --all-targets -- -D warnings

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p rusty-claude-cli

# Build with optimizations
cargo build --release -p rusty-claude-cli
```

## Environment variables

The flake automatically sets:

- `RUSTUP_TOOLCHAIN=stable` — uses stable Rust from nixpkgs
- Standard Rust build flags are inherited from the environment

For development, the minimal dev shell includes a helpful message on entry:

```
Claw Code development environment
Run 'cargo build -p rusty-claude-cli' to build the claw binary
Run 'cargo test --workspace' to run tests
Run 'scripts/fmt.sh' to format code
```

## Troubleshooting

### `command not found: nix`

Nix is not installed. Install it from [nixos.org/download.html](https://nixos.org/download.html).

### `error: experimental Nix feature 'flakes' is disabled`

Enable flakes in your Nix configuration (`~/.config/nix/nix.conf` or `/etc/nix/nix.conf`):

```
experimental-features = nix-command flakes
```

### `error: ... does not provide attribute 'claw'`

The flake structure may have changed. Try updating:

```bash
nix flake update
nix flake check
```

### Rebuilds are slow after updating dependencies

Cargo will recompile dependencies if `Cargo.lock` changes. This is expected:

```bash
nix build --rebuild
```

### `cache lock file needs to be updated`

The Cargo.lock in `rust/` needs to be committed. Run:

```bash
cd rust
cargo generate-lockfile
git add Cargo.lock
git commit -m "update: Cargo.lock"
```

## Integration with other tools

### IDE integration (VS Code, IntelliJ, etc.)

Many IDEs can detect and use Nix environments. Direnv integration is recommended:

1. Install the direnv extension
2. Enable `direnv` shell integration in your IDE
3. Run `direnv allow` in the repository

Your IDE will automatically use the correct Rust toolchain and dependencies from the flake.

### CI/CD

For GitHub Actions, consider using [cachix/install-nix-action](https://github.com/cachix/install-nix-action) to set up Nix on the runner:

```yaml
- uses: cachix/install-nix-action@v18
  with:
    nix_path: nixpkgs=channel:nixos-unstable
    extra_nix_config: "experimental-features = nix-command flakes"

- run: nix develop -c cargo test --workspace
```

## More information

- [NixOS flakes documentation](https://nixos.wiki/wiki/Flakes)
- [Direnv documentation](https://direnv.net/)
- [Rust on Nix](https://github.com/oxalica/rust-overlay)
