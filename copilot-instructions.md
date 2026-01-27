# Copilot Instructions for This Repository

This repository enforces linting and formatting gates. When proposing or generating code, ensure it passes these checks locally before opening a PR.

## Required Tooling
- Go: 1.25.x
- golangci-lint: uses the repo’s `.golangci.yaml`
- gci (Go import formatter)

## Import Formatting (gci)
Imports must follow the sections defined in `.golangci.yaml`:

1. standard
2. default
3. Prefix(github.com/openkcm/crypto-edge-operator)
4. blank
5. dot
6. alias
7. localmodule

Recommended commands:

Check which files need formatting:

```bash
gci list -s standard -s default -s 'Prefix(github.com/openkcm/crypto-edge-operator)' -s blank -s dot -s alias -s localmodule --skip-generated .
```

Apply formatting in-place:

```bash
gci write -s standard -s default -s 'Prefix(github.com/openkcm/crypto-edge-operator)' -s blank -s dot -s alias -s localmodule --skip-generated .
```

Notes:
- Quote the `Prefix(...)` section to avoid zsh globbing.
- Keep `_ "embed"` for files using `//go:embed` to avoid unused import errors.

## Linting (golangci-lint)
All code must pass:

```bash
golangci-lint run ./...
```

The configuration is in `.golangci.yaml`. Do not add `//nolint` unless strictly necessary; remove unused `//nolint` directives flagged by `nolintlint`.

## Style Notes
- Prefer Go 1.22+ integer range loops when appropriate (e.g., `for range 2 {}`)
- Avoid unnecessary trailing blank lines between top-level declarations.
- Keep changes minimal and focused; match existing patterns and structure.

## Pre-PR Checklist
- Imports formatted with gci (sections above).
- `golangci-lint run ./...` returns zero issues.
- Build passes: `go build ./...`.

If a `Taskfile` target is available (e.g., `task validate`), you may use it, but the authoritative checks are the commands above.
