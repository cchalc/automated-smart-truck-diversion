# dig-this-shovel-talk — Claude Operating Notes

Inherits from `../CLAUDE.md` (uv env + databricks CLI profile conventions). Read that first.

## What this project is

Critical analysis and synthetic-data simulation of **ShovelSense-style XRF ore-sorting and truck-diversion systems** for mining operations. Ships three things:

1. Academic critique of the ShovelSense white paper against 9 peer-reviewed papers (`docs/analysis/`).
2. Realistic synthetic mining-ops data — XRF bucket measurements, truck loads, shift summaries — emitted as parquet, landed in a Unity Catalog volume.
3. A Databricks Bronze → Silver → Gold pipeline (Spark Declarative Pipelines / DLT) deployed as a DAB.

Primary doc to skim before coding: `docs/data-architecture.md`, `docs/pipeline-documentation.md`, `tasks/project-management.md`.

## Environment

| Setting | Value |
| --- | --- |
| Python | `>=3.12` (`.python-version` pins it) |
| Venv path | `~/.virtualenvs/dig-this-shovel-talk` (via `UV_PROJECT_ENVIRONMENT` in `.envrc`) |
| Databricks profile | `fevm-cjc` |
| Workspace | `https://fevm-cjc-aws-workspace.cloud.databricks.com` |
| Catalog / Schema | `cjc_aws_workspace_catalog.shovelsense` |
| Warehouse | `751fe324525584e5` |
| Shell | fish (justfile uses `set shell := ["fish", "-c"]`) |

If `databricks auth describe --profile fevm-cjc` fails, run `databricks auth login --host https://fevm-cjc-aws-workspace.cloud.databricks.com --profile fevm-cjc` and retry.

## Repo layout

| Path | Purpose |
| --- | --- |
| `bundles/` | Databricks Asset Bundle: `databricks.yml` + `src/` DLT pipeline + data-quality notebooks. Dev/prod targets. |
| `scripts/` | Local Python — synthetic data gen, PDF gen, validation, post-deploy metadata apply. |
| `dialectics/` | Electric-Monk dialectical reasoning trees (`round_N_*` files). Pure analysis, not code. See `dialectics/*/SKILL.md`. |
| `notebooks/` | Analysis notebooks uploaded to the workspace via `just upload-notebooks`. |
| `tasks/` | Fizzy board + GitHub Issues mirror. Features tagged A1/C2/D1 per phase. `todo.md`, `lessons.md`, `project-management.md`. |
| `tests/` | pytest: schema, referential integrity, business rules, value ranges. Run via `just test`. |
| `docs/` | Architecture, paper analysis, critical assessments (01-09), academic references. |
| `integrations/` | External integration glue (Fizzy webhook, etc.). |
| `bundles/` resources | Pipeline name: `shovelsense_pipeline`. Run via `just run-pipeline`. |

## Workflow — `just` is the entrypoint

The `justfile` orchestrates everything. Don't shell out to `databricks` / `pytest` / `python` directly when a recipe exists — use the recipe so env vars and the profile flag stay consistent.

| Recipe | When to use |
| --- | --- |
| `just setup` | Validate `databricks auth describe` for the pinned profile. Run after fresh `databricks auth login`. |
| `just create-infra` | Idempotent CREATE for catalog/schema/volumes. Safe to re-run. |
| `just generate-data` | Local synthetic data gen → `data/generated/*.parquet`. |
| `just upload-data` | Pushes parquet to the UC volume. |
| `just validate-data` / `just test` / `just validate-all` | Schema + business-rule checks; exit 2 = warnings only. |
| `just deploy` / `just deploy-all` | `databricks bundle deploy` from `bundles/`. `deploy-all` also uploads notebooks. |
| `just run-pipeline` | Trigger `shovelsense_pipeline` DLT run. |
| `just apply-metadata` | Apply table/column descriptions post-deploy. Required after schema changes. |
| `just all` | Full end-to-end (setup → infra → gen-data → gen-pdfs → deploy). |
| `just drop-data` | **Destructive**: `DROP SCHEMA … CASCADE`. Confirms before running. Don't auto-run. |

All recipes invoke Python via `$UV_PROJECT_ENVIRONMENT/bin/python` so they pick up the project venv automatically.

## Tooling conventions

- **Lint**: `ruff` with `line-length = 100`, `target-version = "py312"`, rules `E, F, I, W`. Run `uv run ruff check .` before committing.
- **Tests**: pytest discovers `tests/test_*.py`. Add a test alongside any change to schema or business rules.
- **Notebooks** live under `notebooks/<topic>/` and ship to `/Workspace/Users/christopher.chalcraft@databricks.com/shovelsense/<topic>/`.
- **DLT pipeline** uses Spark Declarative Pipelines (materialized views, not Delta tables). Don't add Delta-only operations to silver/gold.

## Tasks, dialectics, bundles — what they are

- **Tasks (`tasks/`)** — Fizzy board synced with GitHub Issues. Issues are tagged with phase (`phase-1/2/3`) and category (`dispatch-simulation`, `economic-modeling`, `xrf-physics`, `classification-analysis`, `vrp-rl`). Feature IDs like `A1`, `C2`, `D1` link Fizzy ↔ GitHub. When asked to "implement D1" or similar, find the linked issue first.
- **Dialectics (`dialectics/<topic>/`)** — Multi-round Electric-Monk reasoning artifacts. Files named `round_N_candidate_<X>_<aspect>.md`. Read existing rounds before writing a new one. They are not code; they're stress-tested arguments.
- **Bundles (`bundles/`)** — Standard Databricks Asset Bundle. `databricks.yml` declares `dev` (user dir) and `prod` (Shared) targets. Pipeline source in `bundles/src/`.

## Version control — git + jujutsu (jj)

The repo is git-backed but use **jj** as the day-to-day VCS (colocated, so git tools still work):

```bash
jj st          # status — replaces git status
jj diff        # diff working copy
jj describe -m "msg"   # set commit message
jj new         # start a new change on top
jj log         # history
jj git push    # push current change to origin
```

**Commit early and often.** jj's working-copy-is-a-commit model rewards small, frequent `jj describe` + `jj new` cycles. Don't let the working copy accumulate more than ~1 logical change.

## Worktrees — worktrunk (wt)

For parallel exploration (a refactor, a spike, an alternate dialectic round) prefer **`wt`** worktrees over branch-juggling:

```bash
wt list                  # show worktrees
wt switch <name>         # cd to / create worktree
wt switch -c <name>      # create new worktree
```

Hooks in `~/.config/worktrunk/config.toml` (or a project `.config/wt.toml`) can sync `.envrc`, the venv, and `~/.databrickscfg` into the new worktree so it's instantly usable. Add project-specific hooks under `.config/wt.toml` when something needs to land in every worktree.

## Subagent strategy — use it

Default to dispatching subagents for anything that fits one of these shapes:

- **Exploration / read-only research** (codebase survey, doc digest, "where is X defined") → `Agent(subagent_type="Explore", …)`.
- **Independent parallel work** (multiple unrelated files, multiple unrelated questions) → multiple `Agent(...)` calls in one message.
- **Heavy output that would pollute main context** (large file dumps, transcripts, log analysis) → delegate, ask for a ≤400-word report.
- **Planning a multi-step change** → `Agent(subagent_type="Plan", …)` before touching code on anything non-trivial.

Don't delegate the *thinking* — write a self-contained prompt with paths and acceptance criteria. The subagent doesn't see the conversation.

## Things to avoid

- Don't run `pip install` — always `uv add` / `uv sync`.
- Don't hardcode the venv path; use `$UV_PROJECT_ENVIRONMENT`.
- Don't run `databricks` commands without `--profile fevm-cjc`.
- Don't add Delta-table-only features to DLT silver/gold layers.
- Don't run `just drop-data` without explicit user confirmation in this conversation.
- Don't amend a jj change someone else may have pulled — start a new change.
