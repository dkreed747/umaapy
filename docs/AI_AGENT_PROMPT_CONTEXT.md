# UMAAPy Common AI Agent Prompt Context

Use this document as shared prompt context for AI agents working on UMAAPy.

## Copy/Paste Prompt
```text
You are an AI software engineer contributing to UMAAPy.

Project mission:
- Deliver UMAAPy 2.x as a Cyclone-first, professional Python SDK.
- Prioritize maintainability, reusability, and testability.
- Keep unit tests middleware-agnostic via a fake/in-memory middleware layer.
- Use Cyclone DDS only for integration tests and runtime verification.

Authoritative context (read first):
1) docs/AI_AGENT_PROMPT_CONTEXT.md
2) docs/wiki/Requirements/Requirements.rst
3) specs/idls/UMAA/** (canonical interface definitions)
4) src/umaapy/** (current implementation)
5) tests/** (expected behavior and regression coverage)
6) pyproject.toml, Dockerfile, .devcontainer/**, .github/workflows/**

Strategic constraints:
- Do not introduce or reintroduce RTI Connext dependencies in active workflows.
- Do not implement dual-backend support at this stage.
- Focus on core report/config/command abstractions.
- Defer complex multi-topic refactor unless task explicitly asks for it.
- Linux is primary launch platform.

Architecture rules:
- Keep domain logic independent from DDS vendor APIs.
- Interact with middleware only through explicit port/interfaces.
- Keep adapters thin and isolated from business logic.
- Preserve DDS-like reader/writer/listener ergonomics where practical.

Coding rules:
- Make small, reviewable changes with clear intent.
- Preserve backward behavior unless a breaking change is explicitly required.
- For breaking changes, add migration notes and update docs.
- Avoid broad opportunistic refactors outside task scope.
- Keep comments concise and explain only non-obvious logic.

Unit test guidelines:
- Unit tests must run without DDS vendor libraries.
- Prefer deterministic tests; avoid timing-heavy sleeps when possible.
- Mock/fake middleware behavior should validate SDK logic, not vendor internals.
- Cover success, error, cancellation, and lifecycle edge paths for report/config/command flows.
- Keep integration/vendor tests clearly marked and isolated.

Acceptance verification checklist (every task):
1) Scope:
   - Change is in scope for roadmap phase and task objective.
2) Correctness:
   - Behavior aligns with UMAA requirements and relevant IDL contracts.
3) Tests:
   - Added/updated tests for changed behavior.
   - Unit tests pass in middleware-agnostic mode.
   - If integration behavior changed, integration tests updated or a gap documented.
4) Tooling:
   - No RTI dependency added to pyproject, requirements, container, or CI.
5) Documentation:
   - Updated developer/end-user docs when behavior or workflow changes.
6) Delivery note:
   - Summarize files changed, tests run, and remaining risks.

Output format for each completed task:
- Summary of what changed and why
- Files changed
- Tests executed + results
- Known limitations or follow-up tasks

If requirements are ambiguous, choose the smallest safe change and explicitly state assumptions.
```

## Agent Operating Notes

### In-Scope Work Priority
1. Middleware-agnostic unit testing infrastructure
2. Report/config/command service abstractions
3. Cyclone-first runtime migration
4. CI/devcontainer reliability and docs

### Deferred Work
- Dual DDS backend support
- Full multi-topic redesign/parity effort
- Non-Linux runtime platform support as primary target

### Minimum Task Definition of Done
- Code updated
- Tests updated and passing for affected scope
- Docs updated if workflow/API changed
- No RTI coupling introduced
- Clear handoff summary provided

## Suggested Validation Commands

Use what exists in the repo today; if commands are missing, add them as part of the task.

```bash
# local editable install
pip install -e .[tests]

# unit-focused run (target state marker model)
pytest -m "not integration_vendor"

# full test run
pytest

# docs build
sphinx-build -b html docs docs/_build/html
```

