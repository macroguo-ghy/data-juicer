# Agent Development Workflow

Use this guide for non-trivial Data-Juicer development work, behavior changes,
review loops, subagent delegation boundaries, and final handoff gates. Keep
`AGENTS.md` short; put workflow detail here.

## Default Workflow

Follow this loop for feature work, bug fixes, risky config changes, Ray/SDK/IO
changes, metrics changes, and any task where correctness depends on more than a
single local edit:

```text
clarify or plan -> TDD -> implement -> self-review -> spec review ->
code review -> fix review findings -> re-review -> verify -> commit/push
```

Small docs-only or pure investigation tasks may skip TDD and review, but the
final handoff must state the exception.

## Planning Gate

- If requirements, scope, output contract, or rollout expectations are unclear,
  discuss the design before editing files.
- For multi-step work, write or update a plan under `docs/plans/` with exact
  file targets, focused tests, online E2E needs, and review checkpoints.
- Do not turn a narrow bug fix into broad refactoring. Prefer the smallest
  owning layer: YAML, operator, executor, export sink, SDK boundary, or runbook.

## TDD Gate

Use [AgentTesting.md](AgentTesting.md) for behavior changes. The minimum bar is:

1. Name the observable behavior that changes.
2. Add or update the smallest focused test first.
3. Run it and record the expected RED result, unless an explicit exception
   applies.
4. Make the minimal production change.
5. Rerun the focused GREEN test and broaden only when risk warrants it.

Docs-only changes, pure investigation, and blocked internal-service integration
are valid exceptions, but they must be named in the handoff.

## Self-Review

Self-review is done by the implementer before requesting review. It is not a
substitute for review. Check:

- The diff is scoped and contains no generated files, debug prints, secrets, or
  unrelated formatting churn.
- Tests and verification actually cover the changed behavior or the exception is
  explicit.
- Config paths exercise real operator loading or executor behavior when that is
  the contract under test.
- Ray Data/PyArrow changes preserve block-level schema stability.
- Ray execution-path changes compare old and new data representations and
  wrappers: Arrow batch vs row callback, `to_pydict()` conversion,
  null/scalar values, resource args, `runtime_env`, fault-tolerance behavior,
  and stats/meta column setup.
- SDK-boundary changes model the real third-party response shape instead of
  mocking away the contract.
- Metrics changes update the matching dashboard JSON when required by
  `AGENTS.md`.

## Review Gates

Use two review passes for non-trivial or high-risk work:

- Spec review: verify the implementation does exactly what was requested, no
  less and no unrelated extras.
- Code review: verify maintainability, test quality, error handling, Ray/SDK/IO
  safety, performance risks, and compatibility with existing patterns.

Important or critical review findings must be fixed before commit/push. After a
fix, rerun the relevant tests and repeat the review for the changed area. If a
review suggestion is technically wrong or conflicts with prior user decisions,
push back with code and test evidence instead of implementing blindly.

## Subagent Use

Use subagents only when the user explicitly asks for subagents or an approved
plan explicitly requires delegated work.

Good subagent tasks:

- Independent implementation slices with disjoint write scopes.
- Spec review or code review after the main diff exists.
- Parallel read-only investigation of separate code paths.

Keep local ownership of urgent or tightly coupled root-cause analysis, especially
live Ray job debugging. The parent agent owns final integration: inspect the
subagent diff, rerun verification, resolve conflicts, and decide whether the
work is ready.

When delegating, give the subagent:

- The exact task text and acceptance criteria.
- The intended write scope.
- Relevant paths: `AGENTS.md`, this workflow, `AgentTesting.md`, and any Ray or
  SDK runbook needed for the task.
- A reminder not to revert unrelated user or agent changes.

## Review Templates

Implementer report:

```text
Implemented:
- <short summary>

Verification:
- RED: <command/result or explicit exception>
- GREEN: <command/result>
- Broader checks: <command/result or not run with reason>

Changed files:
- <paths>

Self-review:
- <issues found and fixed, or none>

Risks:
- <remaining blocker or residual risk, or none>
```

Spec review prompt:

```text
Review whether this implementation matches the requested behavior.
Check the actual diff, not only the implementer report.
Report missing requirements, extra scope, or misunderstood behavior with
file/line references.
```

Code review prompt:

```text
Review for bugs, regressions, missing tests, maintainability, error handling,
Ray/Data-Juicer runtime risks, SDK-boundary contract drift, IO/schema stability,
and metrics/dashboard consistency. Lead with findings by severity.
```

## Verification And Handoff

Before commit/push:

- Run `git status --short` and review the diff.
- Run focused tests first; broaden for shared behavior or high-risk runtime
  paths.
- For Ray changes, verify the next meaningful stage, not only that the previous
  stack trace disappeared.
- For online E2E jobs launched during validation, stop any one-off Federal Ray
  job before handoff.

The final handoff must include exact tests or smoke checks run, skipped checks
with reasons, the commit/push result when files changed, and any remaining risk.
