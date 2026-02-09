RTI Reference Audit Notes
=========================

Last updated: 2026-02-09

Scope
-----

- Command used: ``rg -n "RTI|Connext|rtiddsgen" docs README.md``
- Focus: documentation and README references only

Changes made in this issue
--------------------------

- Removed RTI-specific wording from active entry-point docs:
  ``README.md``, ``docs/index.rst``, ``docs/usage/development.rst``, ``docs/usage/getting_started.rst``.
- Updated workflow comment language in ``.github/workflows/ci.yml`` to backend-agnostic wording.
- Added ADR draft for Cyclone-first single-backend direction.

Remaining RTI references (docs + README)
----------------------------------------

Active docs intentionally mentioning RTI as a guardrail
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``docs/AI_AGENT_PROMPT_CONTEXT.md``: policy language prohibiting RTI reintroduction.
- ``docs/AI_AGENT_TASK_TEMPLATE.md``: acceptance checklist item preventing RTI dependency reintroduction.

Historical/archival docs
~~~~~~~~~~~~~~~~~~~~~~~~

- ``docs/wiki/Requirements/Requirements.rst``
- ``docs/wiki/Requirements/Architectural.rst``
- ``docs/wiki/Project-Planning/Sprint-1/*.rst``
- ``docs/wiki/Project-Planning/Sprint-2/Week-8-Report.rst``

These are legacy planning/requirements artifacts and were left unchanged in this issue.

Out-of-scope non-doc references
-------------------------------

- ``pyproject.toml`` dependency and marker descriptions
- ``src/umaapy/**`` RTI-oriented implementation/docstrings
- ``tests/**`` RTI-oriented integration marker usage and runtime checks

Recommended follow-up
---------------------

1. Replace RTI-specific language in legacy requirements docs with Cyclone-first or vendor-agnostic wording.
2. Rename marker ``integration_vendor`` to ``integration_cyclone`` across tooling/tests.
3. Remove RTI dependency from packaging/runtime once Cyclone adapter path is complete.
