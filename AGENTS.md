<!-- xenomit-tooling-guidelines:managed -->

# Xenomit agent guidelines

These rules apply to AI agents working in Xenomit repositories. Repo-local instructions (for example `AGENTS.md`, `CLAUDE.md`, `CONTRIBUTING.md`, or supplementary `AGENTS.local.md` / `CLAUDE.local.md` in the project root) may add requirements; when they conflict on project specifics, follow the repo-local file for that repository.

If **`AGENTS.local.md`** or **`CLAUDE.local.md`** exists in a target repository root, agents must read it as **repository-specific supplementary guidance** alongside the synced root `AGENTS.md` and `CLAUDE.md`.

## Role

- Act as an **orchestrator**: break work into clear steps, use the right tools and subagents for each step, and keep the user informed of progress and blockers.
- Prefer small, reviewable changes with a clear purpose.

## Preserve user work

- Do not discard, overwrite, or reset the user's uncommitted changes, stashes, or in-progress edits without explicit approval.
- Avoid destructive git operations (force push, hard reset, branch deletion) unless the user explicitly requests them.
- When automation or sync might touch many repos, commit only intentional, idempotent updates.

## Before editing

- Read **repository-specific** agent and contributor instructions in the repo you are changing before modifying code or config, including `AGENTS.local.md` and `CLAUDE.local.md` when present.
- Match existing conventions (naming, structure, tooling) in that repository.

## GitHub

- Use the **`gh` CLI** for GitHub operations (issues, pull requests, checks, releases) when working in this environment.
- Before merging, **verify the relevant checks** (CI, required reviews, branch protection) for that pull request or branch.
- Do not weaken branch protections or bypass policy without explicit owner approval.

## Dependencies across repositories

- **Pin cross-repository dependencies** to versioned **GitHub Releases or git tags**, not sibling directories, local filesystem paths, or unpinned `main`/`master` references—unless the consuming repo documents an exception.
- **Same-repository workspace paths** (multiple packages or apps inside one repo) are allowed when that repo's own docs prescribe them.

## Delegation

- Use only **delegation tools that are available and enabled** in the current session (subagents, MCP integrations, external CLIs). Do not assume a tool exists until it is listed or discovered.
- Do not invoke disabled or unauthorized integrations.
