{
  description = "claw-code: Claude Code CLI agent harness in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Build inputs needed for Rust and the project
        buildInputs = with pkgs; [
          pkg-config
          openssl
          ca-certificates
          git
        ];

        # Runtime inputs for the dev environment
        nativeBuildInputs = with pkgs; [
          (rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" ];
            targets = [ ];
          })
          cargo-edit
          cargo-expand
          cargo-watch
        ];

        # Derivation to build the claw binary
        clawBinary = pkgs.rustPlatform.buildRustPackage {
          pname = "claw";
          version = (builtins.fromTOML (builtins.readFile ./rust/Cargo.toml)).workspace.package.version;

          src = ./rust;

          # Hash of all the dependencies in Cargo.lock
          # This needs to be updated when dependencies change
          # Run: nix flake update
          cargoLock = {
            lockFile = ./rust/Cargo.lock;
          };

          cargoBuildFlags = [ "-p" "rusty-claude-cli" ];

          buildInputs = buildInputs ++ (with pkgs; [
            openssl
          ]);

          nativeBuildInputs = with pkgs; [
            pkg-config
          ];

          meta = with pkgs.lib; {
            description = "Claude Code CLI: agent harness with Rust implementation";
            homepage = "https://github.com/ultraworkers/claw-code";
            license = licenses.mit;
            platforms = platforms.unix;
          };
        };

      in
      {
        # Provide the claw binary as a package
        packages.claw = clawBinary;
        packages.default = clawBinary;

        # Development shell for contributors
        devShells.default = pkgs.mkShell {
          name = "claw-dev";
          inherit buildInputs nativeBuildInputs;

          shellHook = ''
            echo "Claw Code development environment"
            echo "Run 'cargo build -p rusty-claude-cli' to build the claw binary"
            echo "Run 'cargo test --workspace' to run tests"
            echo "Run 'scripts/fmt.sh' to format code"
          '';

          # Make sure we have the right Rust toolchain
          RUSTUP_TOOLCHAIN = "stable";
        };

        # Alternative shell with additional development tools
        devShells.full = pkgs.mkShell {
          name = "claw-dev-full";
          inherit buildInputs nativeBuildInputs;

          nativeBuildInputs = nativeBuildInputs ++ (with pkgs; [
            clippy
            rustfmt
            mold
            lldb
          ]);

          shellHook = ''
            echo "Claw Code full development environment (with extra tools)"
            echo "Available tools: clippy, rustfmt, mold (linker), lldb (debugger)"
          '';

          RUSTUP_TOOLCHAIN = "stable";
        };
      }
    );
}
