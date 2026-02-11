import os
import sys
import types
import pathlib
import pytest

os.environ.setdefault("UMAAPY_AUTO_INIT", "0")

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
LOCAL_PACKAGE_ROOT = REPO_ROOT / "src" / "umaapy"


def _license_present() -> bool:
    """Detect whether an RTI license file is available.

    We avoid importing rti.connextdds here to prevent crashes when a license is invalid.
    """
    license_path = os.environ.get("RTI_LICENSE_FILE")
    if license_path and pathlib.Path(license_path).is_file():
        return True

    ndds_home = os.environ.get("NDDSHOME")
    if ndds_home:
        candidate = pathlib.Path(ndds_home) / "rti_license.dat"
        if candidate.is_file():
            return True
    return False


LICENSE_OK = _license_present()


def _connext_importable() -> bool:
    try:
        import rti.connextdds  # noqa: F401
        return True
    except Exception:
        return False


CONNEXT_AVAILABLE = _connext_importable()


def _install_stub_modules() -> None:
    """Provide minimal stubs so imports succeed when RTI is unavailable.

    This prevents ImportError during test collection so we can cleanly skip tests.
    """
    # Stub only rti.connextdds and preserve a real `rti` package when present.
    if "rti.connextdds" in sys.modules:
        return

    try:
        import rti as rti_pkg  # type: ignore[import-not-found]
    except Exception:
        rti_pkg = types.ModuleType("rti")
        sys.modules["rti"] = rti_pkg

    connextdds_mod = types.ModuleType("connextdds")
    rti_pkg.connextdds = connextdds_mod
    sys.modules["rti.connextdds"] = connextdds_mod


if not CONNEXT_AVAILABLE:
    _install_stub_modules()


def _local_import_hygiene_error() -> str | None:
    try:
        import umaapy  # noqa: F401
    except Exception as exc:
        return (
            f"Could not import umaapy ({exc!r}). "
            "Install the workspace package in editable mode with: pip install -e .[tests]"
        )

    import_path = pathlib.Path(umaapy.__file__).resolve()
    try:
        import_path.relative_to(LOCAL_PACKAGE_ROOT)
    except ValueError:
        return (
            f"import umaapy resolved to '{import_path}', not this workspace source tree '{LOCAL_PACKAGE_ROOT}'. "
            "Reinstall with: pip install -e .[tests]"
        )
    return None


def pytest_sessionstart(session: pytest.Session) -> None:
    error = _local_import_hygiene_error()
    if error:
        raise pytest.UsageError(error)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not LICENSE_OK:
        skip = pytest.mark.skip(
            reason="RTI Connext DDS license missing/expired; integration_vendor tests skipped."
        )
        for item in items:
            if item.get_closest_marker("integration_vendor"):
                item.add_marker(skip)
