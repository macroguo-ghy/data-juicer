---
name: data-juicer-development-workflow
description: >-
  Use for non-trivial Data-Juicer development work, behavior changes, review
  loops, review-fix follow-up, subagent delegation boundaries, and final
  verification or handoff. This is the single repo-local workflow entrypoint for
  planning, TDD, implementation, self-review, independent review, fixes, and
  commit/push in this repository.
---

# Data-Juicer Development Workflow

Use this skill as the workflow entrypoint for non-trivial changes in this repo.
Do not split into multiple generic workflow skills. Route details to the repo
docs:

- Testing and TDD: `docs/AgentTesting.md`
- Development loop, review, and subagent boundaries: `docs/AgentDevelopmentWorkflow.md`
- Ray/HDFS/local/online E2E: `docs/AgentRunbooks.md`
- Ray, PyArrow, PyIceberg, Magnus, Lance, datasink, and runtime-env boundaries:
  `docs/AgentSdkBoundary.md`

## Workflow

1. Clarify or plan before editing when requirements, scope, contracts, or rollout
   expectations are unclear.
2. For behavior changes, follow the TDD gate in `docs/AgentTesting.md`.
3. Implement the smallest owning-layer change.
4. Satisfy the changed-behavior coverage gate in `docs/AgentTesting.md`: run
   focused coverage with `--cov=<changed-package-or-module>` and
   `--cov-fail-under=90`, or record the exact accepted exception/blocker.
5. Self-review the diff before claiming completion.
6. For non-trivial or high-risk work, run spec review first, then code review.
7. Fix important review findings and re-review the changed area.
8. Verify with focused tests or the relevant Ray/SDK/E2E check.
9. Commit and push only after verification, coverage, and required review gates
   pass.

## Review Rules

- Self-review removes obvious defects; it does not replace independent review.
- Spec review checks whether the requested behavior was implemented exactly.
- Code review checks correctness, maintainability, test quality, error handling,
  Ray/Data-Juicer runtime risks, SDK-boundary contracts, IO/schema stability, and
  metrics/dashboard consistency.
- Evaluate review feedback against repo reality. Fix valid critical or important
  findings before proceeding; push back with code and test evidence when a
  suggestion is wrong.

## Subagents

Use subagents only when the user explicitly asks for subagents or an approved
plan explicitly requires delegated work. Delegate independent implementation
slices, spec review, code review, or parallel read-only investigation. Keep live
Ray root-cause analysis and tightly coupled debugging on the main critical path.

The parent agent owns final integration: inspect subagent changes, rerun
verification, resolve conflicts, and decide whether the work is ready to commit
and push.

## Handoff

Final responses for changed repo files must include:

- What changed.
- Exact tests or smoke checks run.
- TDD RED/GREEN evidence, or the explicit exception reason.
- Changed-behavior coverage command/result, or the explicit exception reason.
- Review status for non-trivial work, or why review was skipped.
- Commit and push result.
- Remaining blockers or residual risk.
