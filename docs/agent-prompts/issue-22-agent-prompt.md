# Agent Prompt — Complete Issue #22: `generate-types` Automation

## Your Task

Implement everything needed to close GitHub issue #22 in the `dkreed747/umaapy` repository:
**"[P1] Implement `generate-types` automation and CI validation"**

Read CLAUDE.md in the repo root before doing anything else — it is the authoritative guide for conventions, tooling, and the strategic direction of this project.

---

## Branch

Create and work on branch: `claude/issue-22-generate-types-automation`

```bash
git checkout master
git pull origin master
git checkout -b claude/issue-22-generate-types-automation
```

Push early and often in logical chunks. Do not squash everything into one commit.

---

## What the Issue Requires

Acceptance criteria from issue #22:

1. New developers can generate types end-to-end inside the devcontainer
2. CI validates generated artifacts and fails with clear, actionable error messages
3. Documentation covers exact commands, expected outputs, and troubleshooting
4. No RTI toolchain dependencies in the generation workflow

---

## Current State Audit — Read These Files First

Before writing any code, read and understand what is already in place:

| File | What it does |
|---|---|
| `scripts/generate_types.py` | Complete generation script — discovers IDLs, runs `idlc -l py`, patches the UMAA alias, validates output |
| `Makefile` | Already has `generate-types` and `generate-types-clean` targets |
| `.github/workflows/ci.yml` | Already has a `type-gen-check` job on windows-latest with the pinned cyclonedds version |
| `docs/usage/development.rst` | Already documents type generation commands and troubleshooting |
| `tests/test_cyclone_types.py` | Smoke-tests that generated UMAA types import correctly |
| `.devcontainer/devcontainer.json` | Runs `pip install -e .[tests,cyclone]` on create — this installs the `idlc` compiler via the `cyclone` extra |

Many pieces are already in place. Your job is to find the **gaps** and fill them.

---

## Gap Analysis — What Is Likely Missing

Work through these checks in order. Read each file carefully before deciding what to add.

### 1. Tests for `scripts/generate_types.py`

The script itself has no unit or component tests. The internal functions contain real logic that can be tested without running `idlc`:

- `resolve_idlc()` — should raise `FileNotFoundError` when no `idlc` is found and `IDLC_PATH` is not set
- `resolve_idlc()` — should respect the `IDLC_PATH` environment variable
- `validate_generated_tree()` — should raise `RuntimeError` when the `UMAA/` directory is missing
- `validate_generated_tree()` — should raise `RuntimeError` when fewer Python files than the minimum threshold are present
- `patch_generated_root_alias()` — should be idempotent (calling it twice must not insert the alias twice)
- `patch_generated_root_alias()` — should raise `RuntimeError` when `UMAA/__init__.py` does not exist
- `discover_idls()` — should raise `RuntimeError` when no IDL files are found

Write these tests in `tests/util/test_generate_types.py`. Use `pytest.mark.unit`. Use `tmp_path` for filesystem fixtures. Import the functions directly from `scripts.generate_types` — you may need to add `scripts/` to `sys.path` at the top of the test file or use `importlib` since `scripts/` is not a package.

Mark the test module with `pytestmark = pytest.mark.unit`.

### 2. Devcontainer — verify `idlc` is available after `postCreateCommand`

Read `devcontainer.json` and `docker-compose.yml` (and the `Dockerfile` it references).

- The `postCreateCommand` installs `.[tests,cyclone]` which includes `cyclonedds-nightly==2025.11.25`
- That package bundles `idlc` at `<package>/.libs/idlc`
- `generate_types.py` already handles this discovery path via `cyclonedds.__file__`
- Verify no explicit `IDLC_PATH` override is needed in the devcontainer for the script to find `idlc`
- If the devcontainer is missing a `postStartCommand` or feature that would make `generate-types` runnable from the terminal without extra steps, add it

### 3. CI `type-gen-check` job — verify error messages link to docs

Read the `type-gen-check` job in `.github/workflows/ci.yml`. The issue requires CI failures to emit **clear, actionable error messages**. Check:

- Does the drift-detected failure message tell the developer exactly what command to run locally to fix it?
- Does the untracked-files failure message do the same?
- Do both messages link to the contributor docs (the `docs/` URL)?

The test-non-vendor job already has a "Test failure guidance" step with a doc link. Apply the same pattern to `type-gen-check` if it is missing.

### 4. `development.rst` — verify it documents expected output

Read `docs/usage/development.rst`. It should show what a **successful** generation run looks like (the printed summary line). Add a note showing what a successful run prints:

```
Generated Cyclone Python types successfully: N IDL files processed, M Python files written, B bytes in src/umaapy/UMAA/.
```

If this is already present, skip. If not, add it.

---

## Commit Strategy

Commit in these logical chunks — each should be a standalone green state:

1. **`tests: add unit tests for generate_types.py script logic`**
   — `tests/util/test_generate_types.py` only

2. **`ci: add actionable failure guidance to type-gen-check job`**
   — `.github/workflows/ci.yml` only (if the guidance step is missing)

3. **`docs: document expected output for generate-types command`**
   — `docs/usage/development.rst` only (if the expected output is missing)

4. **`devcontainer: verify idlc path for generate-types workflow`**
   — `.devcontainer/devcontainer.json` or `docker-compose.yml` only (if a change is needed)

If a section requires no change because it's already correct, skip that commit. Do not create empty or no-op commits.

---

## Before Each Commit

Run the non-vendor test suite to make sure nothing is broken:

```bash
UMAAPY_AUTO_INIT=0 pytest -m "not integration_vendor"
```

Run black to keep formatting clean:

```bash
black ./src ./tests
```

---

## Pull Request

When all changes are committed and pushed, open a pull request:

- **Base**: `master`
- **Head**: `claude/issue-22-generate-types-automation`
- **Title**: `[P1] Complete generate-types automation and CI validation (#22)`
- **Body**: Use the template below

```markdown
## Summary

- Adds unit tests for `scripts/generate_types.py` internal functions (`resolve_idlc`, `validate_generated_tree`, `patch_generated_root_alias`, `discover_idls`)
- [Add any other changes made]

Closes #22

## Test plan

- [ ] `pytest -m "not integration_vendor"` passes locally
- [ ] All new tests in `tests/util/test_generate_types.py` pass
- [ ] `black --check ./src ./tests` passes
- [ ] CI lint + test-non-vendor lanes green
```

---

## Constraints

- **Do not touch** `src/umaapy/UMAA/` — it is generated code
- **Do not add** any `rti.connextdds` imports — this project is migrating to CycloneDDS
- **Do not** upgrade the `cyclonedds-nightly==2025.11.25` pin — the CI drift check depends on this exact version
- **Do not** modify `src/umaapy/umaa_types.py` — it is a manually curated file excluded from Black
- All new test files must have a `pytestmark = pytest.mark.unit` or `.component` marker at module level
