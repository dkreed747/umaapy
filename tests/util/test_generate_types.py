"""Unit tests for scripts/generate_types.py internal functions.

The scripts/ directory is not a package, so we add it to sys.path before
importing generate_types directly.
"""

import sys
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Bootstrap: inject scripts/ into sys.path so generate_types is importable.
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import generate_types  # noqa: E402

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# resolve_idlc
# ---------------------------------------------------------------------------


def test_resolve_idlc_raises_when_not_found(monkeypatch, tmp_path):
    """FileNotFoundError raised when IDLC_PATH is unset, idlc absent from PATH, and no bundled idlc."""
    monkeypatch.delenv("IDLC_PATH", raising=False)
    monkeypatch.setattr(generate_types, "which", lambda _name: None)
    # Replace cyclonedds in sys.modules with a fake whose __file__ points to a tmp
    # directory that has no .libs/idlc, so all discovery paths fail.
    fake_cyclonedds = types.ModuleType("cyclonedds")
    fake_cyclonedds.__file__ = str(tmp_path / "cyclonedds" / "__init__.py")
    monkeypatch.setitem(sys.modules, "cyclonedds", fake_cyclonedds)

    with pytest.raises(FileNotFoundError, match="idlc"):
        generate_types.resolve_idlc()


def test_resolve_idlc_respects_idlc_path_env_var(monkeypatch, tmp_path):
    """Returns the resolved path when IDLC_PATH points to an existing file."""
    fake_idlc = tmp_path / "idlc"
    fake_idlc.touch()
    monkeypatch.setenv("IDLC_PATH", str(fake_idlc))

    result = generate_types.resolve_idlc()

    assert result == fake_idlc.resolve()


def test_resolve_idlc_raises_when_idlc_path_points_to_missing_file(monkeypatch, tmp_path):
    """FileNotFoundError raised when IDLC_PATH is set but the path does not exist."""
    monkeypatch.setenv("IDLC_PATH", str(tmp_path / "nonexistent_idlc"))
    monkeypatch.setattr(generate_types, "which", lambda _name: None)

    with pytest.raises(FileNotFoundError, match="IDLC_PATH"):
        generate_types.resolve_idlc()


# ---------------------------------------------------------------------------
# validate_generated_tree
# ---------------------------------------------------------------------------


def test_validate_generated_tree_raises_when_directory_missing(monkeypatch, tmp_path):
    """RuntimeError raised when the UMAA generated directory does not exist."""
    monkeypatch.setattr(generate_types, "GENERATED_ROOT", tmp_path / "UMAA")

    with pytest.raises(RuntimeError, match="did not create"):
        generate_types.validate_generated_tree(expected_idl_count=10)


def test_validate_generated_tree_raises_when_too_few_python_files(monkeypatch, tmp_path):
    """RuntimeError raised when generated output has fewer Python files than the minimum threshold."""
    umaa_root = tmp_path / "UMAA"
    umaa_root.mkdir()
    # Create 2 Python files; minimum for expected_idl_count=20 is max(8, 10)=10.
    (umaa_root / "a.py").write_text("# placeholder\n")
    (umaa_root / "b.py").write_text("# placeholder\n")
    monkeypatch.setattr(generate_types, "GENERATED_ROOT", umaa_root)

    with pytest.raises(RuntimeError, match="incomplete"):
        generate_types.validate_generated_tree(expected_idl_count=20)


# ---------------------------------------------------------------------------
# patch_generated_root_alias
# ---------------------------------------------------------------------------


def test_patch_generated_root_alias_raises_when_init_missing(monkeypatch, tmp_path):
    """RuntimeError raised when UMAA/__init__.py does not exist."""
    umaa_root = tmp_path / "UMAA"
    umaa_root.mkdir()
    monkeypatch.setattr(generate_types, "GENERATED_ROOT", umaa_root)

    with pytest.raises(RuntimeError, match="missing expected file"):
        generate_types.patch_generated_root_alias(verbose=False)


def test_patch_generated_root_alias_is_idempotent(monkeypatch, tmp_path):
    """Calling patch_generated_root_alias twice does not insert the alias a second time."""
    umaa_root = tmp_path / "UMAA"
    umaa_root.mkdir()
    (umaa_root / "__init__.py").write_text("from . import SomeSubpackage\n")
    monkeypatch.setattr(generate_types, "GENERATED_ROOT", umaa_root)

    generate_types.patch_generated_root_alias(verbose=False)
    generate_types.patch_generated_root_alias(verbose=False)

    content = (umaa_root / "__init__.py").read_text()
    marker = '_sys.modules.setdefault("UMAA", _sys.modules[__name__])'
    assert content.count(marker) == 1


# ---------------------------------------------------------------------------
# discover_idls
# ---------------------------------------------------------------------------


def test_discover_idls_raises_when_no_idl_files(monkeypatch, tmp_path):
    """RuntimeError raised when no IDL files exist under IDL_ROOT."""
    empty_idl_root = tmp_path / "idls"
    empty_idl_root.mkdir()
    monkeypatch.setattr(generate_types, "IDL_ROOT", empty_idl_root)

    with pytest.raises(RuntimeError, match="No IDL files"):
        generate_types.discover_idls()
