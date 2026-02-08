# UMAAPy AI Agent Task Template

Use this template for every implementation task. Keep tasks small (target <= 1 focused day).

Related context:
- `docs/AI_AGENT_PROMPT_CONTEXT.md`
- `docs/wiki/Requirements/Requirements.rst`
- `specs/idls/UMAA/**`

---

## 1) Task Metadata

- Task ID:
- Title:
- Roadmap Phase: `P0` | `P1` | `P2` | `P3`
- Owner: `Human` | `AI Agent` | `Human + AI Agent`
- Priority: `High` | `Medium` | `Low`
- Estimated Effort:
- Dependencies:

## 2) Objective

Describe the concrete outcome in 1-3 sentences.

## 3) In Scope

- 
- 

## 4) Out of Scope

- 
- 

## 5) Preconditions

- [ ] Required design decisions/ADRs are approved
- [ ] Required environment/tooling is available
- [ ] Blocking upstream tasks are complete
- [ ] Relevant docs/specs reviewed

Notes:

## 6) Implementation Plan

1. 
2. 
3. 

## 7) Files/Modules Expected to Change

- ``
- ``

## 8) Test Plan

### Unit Tests (middleware-agnostic required)
- [ ] Add/update unit tests for changed behavior
- [ ] Validate deterministic behavior (avoid unnecessary sleeps/flaky timing)
- [ ] Run: `pytest -m "not integration_vendor"`

### Integration Tests (Cyclone lane if applicable)
- [ ] Add/update integration tests if runtime behavior changed
- [ ] Run: `pytest -m integration_cyclone`

### Additional Validation
- [ ] Lint/format checks
- [ ] Docs build if docs changed: `sphinx-build -b html docs docs/_build/html`

## 9) Acceptance Criteria (Task-Level)

- [ ] Change satisfies objective and scope
- [ ] No RTI dependency added or reintroduced
- [ ] Middleware boundary respected (domain logic does not import vendor SDK directly)
- [ ] Tests added/updated and passing for affected scope
- [ ] Docs updated for behavior/workflow/API changes
- [ ] Risks/limitations documented

## 10) Deliverables

- Code changes:
- Tests added/updated:
- Docs added/updated:

## 11) Risks and Mitigations

- Risk:
  - Mitigation:
- Risk:
  - Mitigation:

## 12) Rollback/Recovery Plan

Describe how to revert safely if this task introduces regressions.

## 13) Completion Report (Fill After Implementation)

### Summary

What changed and why:

### Files Changed

- ``
- ``

### Commands Run

```bash
# paste commands actually run
```

### Test Results

- Unit:
- Integration:
- Notes:

### Known Limitations / Follow-up Tasks

1. 
2. 

### Final Status

- [ ] Complete
- [ ] Partial (follow-up required)
- [ ] Blocked

