import importlib

import pytest


pytestmark = pytest.mark.unit

TOP_LEVEL_UMAA_MODULES = ("CO", "Common", "EO", "MM", "MO", "SA", "SEM", "SO")


def test_umaa_package_imports() -> None:
    package = importlib.import_module("umaapy.UMAA")
    assert package is not None


@pytest.mark.parametrize("module_name", TOP_LEVEL_UMAA_MODULES)
def test_top_level_umaa_modules_import(module_name: str) -> None:
    module = importlib.import_module(f"umaapy.UMAA.{module_name}")
    assert module is not None


def test_representative_type_is_accessible() -> None:
    from umaapy.UMAA.SA.GlobalPoseStatus import GlobalPoseReportType

    assert GlobalPoseReportType is not None
