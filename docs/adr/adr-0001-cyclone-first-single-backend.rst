ADR 0001: Cyclone-First Single-Backend Strategy
================================================

Status
------

Approved

Date
----

2026-02-09

Context
-------

- UMAAPy 2.x planning is Cyclone-first for runtime verification and integration workflows.
- Active docs and workflows still contain RTI-specific assumptions that conflict with a single-backend migration direction.
- Introducing dual-backend support at this phase would expand CI, test matrix, and maintenance overhead without roadmap priority.

Decision
--------

1. Active development and integration verification target a single DDS backend: Cyclone DDS.
2. Active workflow documentation must avoid RTI-specific assumptions.
3. Unit tests remain middleware-agnostic via fake/in-memory middleware abstractions.
4. No new dual-backend architecture layer is introduced during this phase.
5. Existing RTI references may remain temporarily as tracked migration debt until follow-up issues close them.

Consequences
------------

Positive
~~~~~~~~

- Reduced architecture and CI complexity.
- Clear backend target for contributor onboarding and debugging.
- Stronger separation between domain logic and middleware implementation details.

Negative
~~~~~~~~

- Users relying on RTI-specific runtime behavior will need migration guidance.
- Temporary mismatch may remain while historical documents and legacy code are retired.

Non-Goals
---------

- Implementing dual-backend support in the current phase.
- Removing every historical RTI mention from archived wiki/planning material in this issue.
- Completing all runtime dependency migration in one change set.

Follow-Up
---------

1. Rename legacy pytest marker ``integration_vendor`` to ``integration_cyclone``.
2. Remove RTI package/runtime coupling from packaging and integration code paths.
3. Update legacy requirements/wiki docs to align with the accepted architecture.
