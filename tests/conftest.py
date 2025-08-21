import os
import sys
import types
import pathlib
import pytest


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


def _install_stub_modules() -> None:
    """Provide minimal stubs so imports succeed when RTI is unavailable.

    This prevents ImportError during test collection so we can cleanly skip tests.
    """
    # Stub rti.connextdds
    if "rti.connextdds" not in sys.modules:
        rti_pkg = types.ModuleType("rti")
        connextdds_mod = types.ModuleType("connextdds")
        rti_pkg.connextdds = connextdds_mod
        sys.modules["rti"] = rti_pkg
        sys.modules["rti.connextdds"] = connextdds_mod


if not LICENSE_OK:
    _install_stub_modules()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not LICENSE_OK:
        skip = pytest.mark.skip(reason="RTI Connext DDS license missing/expired; tests skipped.")
        for item in items:
            item.add_marker(skip)
        return
