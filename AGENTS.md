# Repository Guidelines

## Core Rules

- Work from the current checkout root reported by `git rev-parse --show-toplevel`.
- Do not checkout a different branch inside an existing worktree. Use the worktree that already owns the target branch, or create a new worktree for that branch.
- Keep changes scoped to the requested behavior. Do not refactor unrelated operators, configs, Docker files, demos, or docs while fixing a narrow issue.
- Preserve local-only files. Do not commit generated files such as `yaml.base64`, `outputs/`, caches, virtual environments, or temporary artifacts.
- Main package code lives under `data_juicer/`; CLI entry points and utility scripts live under `tools/`; tests live under `tests/`; runnable examples live under `demos/`.
- If new metrics are added, update `/Users/bytedance/repo/data-juicer-task-b/docs/grafana/data_juicer_metrics_dashboard.json` in the same change.

## Task Routing

- For testing policy, focused commands, fallback runners, and coverage expectations, read [AgentTesting.md](docs/AgentTesting.md).
- For non-trivial development work, behavior changes, review loops, subagent delegation boundaries, and final handoff gates, read [AgentDevelopmentWorkflow.md](docs/AgentDevelopmentWorkflow.md) and prefer the repo-local `data-juicer-development-workflow` skill under `.agents/skills/data-juicer-development-workflow/`.
- For local Ray E2E, Mac HDFS E2E, online Ray submission/debugging, and HDFS read-only inspection, read [AgentRunbooks.md](docs/AgentRunbooks.md).
- For third-party SDK, Ray/byted-ray, PyArrow, PyIceberg, Magnus, Lance, datasink, and runtime-env boundary failures, read [AgentSdkBoundary.md](docs/AgentSdkBoundary.md).
- For online Ray E2E jobs launched through `ad.ai.data_forge`, prefer the repo-local `online-ray-e2e` skill under `.agents/skills/online-ray-e2e/`.

## Operator Pipeline Design

- Compose existing operators in YAML when practical. If an existing operator is close but insufficient, first consider a narrow reusable enhancement.
- Do not create a large operator that bundles independent operations. Split multi-step logic into small focused operators and compose them in YAML.
- New operators should be generic and business-agnostic unless the generic form cannot express the required behavior cleanly.
- Euler RPC operators should default their caller/source PSM identity to `ad.ai.data_forge_merlin` unless a task-specific service identity is explicitly required.

## Testing Baseline

- Every behavior change must add or update a focused test before or alongside the fix. This applies to bug fixes, demos, config parsing, runtime-env behavior, dependency resolution, IO adapters, and operator loading.
- For clear bug fixes or behavior changes, follow a TDD gate: first add or update one focused test that captures the target behavior, run it or explain why the local environment cannot run it, then make the production change, then rerun the focused test.
- If you skip test-first, state the reason in the handoff. Do not silently replace TDD with only `py_compile` or mock-only checks.
- A syntax check such as `python -m py_compile ...` is not enough. It only proves files compile; it does not prove object state, branch behavior, operator loading, or Ray/Data-Juicer runtime paths work.
- Test the real control path. For configs, `init_configs(..., load_configs_only=True)` is not equivalent to `load_ops(cfg.process, op_env_manager)` or `executor.run()`.
- For Ray Data and PyArrow paths, test block-level schema stability, not just Python dict or single-row behavior.
- If full integration is blocked by internal services, credentials, cluster limits, network restrictions, or heavyweight dependency installation, state the exact blocker and the narrower verification that did run.

## Avoiding Shallow Fixes

- After fixing one failure, continue to the next meaningful workflow stage before stopping. For Ray jobs, think in stages: `config parse -> operator load -> runtime_env generation -> Ray working_dir distribution -> Ray task startup -> operator execution -> export`.
- Do not declare a Ray/Data-Juicer issue fixed only because the previous stack trace disappeared. Verify the next stage starts successfully or document the new blocker.
- For dependency/runtime-env changes, separate pure logic tests from network or package-install tests. Pure tests should not require downloading `opencc`, `torch`, model weights, or internal wheels.
- For dynamic Python attributes, constructors must establish object invariants on every branch. Empty/default objects must still have all attributes that public methods access.

## Commit And Handoff

- Before committing, run `git status --short` and review the staged diff.
- Do not include unrelated local files in commits.
- When a request results in code or repository file changes, commit the scoped changes and push the branch after verification.
- In the final handoff, list the exact tests or smoke checks run. If any expected test could not run, include the reason and the substitute verification.
