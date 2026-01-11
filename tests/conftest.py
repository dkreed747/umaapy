import pytest


pytest.importorskip("cyclonedds", reason="Cyclone DDS Python not installed")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    return
