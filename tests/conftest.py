import os
import sys
import types
import pathlib
import dataclasses
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
    """Provide minimal RTI stubs so non-vendor tests can import the package."""
    if all(name in sys.modules for name in ("rti.connextdds", "rti.idl", "rti.rpc")):
        return

    try:
        import rti as rti_pkg  # type: ignore[import-not-found]
    except Exception:
        rti_pkg = types.ModuleType("rti")
        sys.modules["rti"] = rti_pkg

    class _DDSStub:
        ALL = 0
        NONE = 0
        ANY = 0

        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, *args, **kwargs):
            return _DDSStub()

        def __iter__(self):
            return iter(())

        def __getattr__(self, name):
            return _DDSStub()

        @classmethod
        def find(cls, *args, **kwargs):
            return None

        def read(self, *args, **kwargs):
            return []

        def take(self, *args, **kwargs):
            return []

        def set_listener(self, *args, **kwargs):
            return None

    class _QosProvider(_DDSStub):
        def participant_qos_from_profile(self, *args, **kwargs):
            return _DDSStub()

        def datawriter_qos_from_profile(self, *args, **kwargs):
            return _DDSStub()

        def datareader_qos_from_profile(self, *args, **kwargs):
            return _DDSStub()

    connextdds_mod = types.ModuleType("rti.connextdds")
    connextdds_mod.DataReader = _DDSStub
    connextdds_mod.DataWriter = _DDSStub
    connextdds_mod.DataReaderListener = _DDSStub
    connextdds_mod.DataWriterListener = _DDSStub
    connextdds_mod.NoOpDataReaderListener = _DDSStub
    connextdds_mod.NoOpDataWriterListener = _DDSStub
    connextdds_mod.QosProvider = _QosProvider
    connextdds_mod.DomainParticipant = _DDSStub
    connextdds_mod.Publisher = _DDSStub
    connextdds_mod.Subscriber = _DDSStub
    connextdds_mod.Topic = _DDSStub
    connextdds_mod.ContentFilteredTopic = _DDSStub
    connextdds_mod.Filter = _DDSStub
    connextdds_mod.DataWriterQos = _DDSStub
    connextdds_mod.DataReaderQos = _DDSStub
    connextdds_mod.InstanceHandle = _DDSStub
    connextdds_mod.StatusMask = _DDSStub
    connextdds_mod.InstanceState = _DDSStub
    connextdds_mod.Uint8Seq = lambda values=(): list(values)
    connextdds_mod.__getattr__ = lambda _name: _DDSStub

    class _ModuleNamespace:
        def __init__(self, name: str):
            self.__name__ = name

        def __repr__(self):
            return f"<RTIStubModule {self.__name__}>"

    idl_mod = types.ModuleType("rti.idl")
    _idl_modules: dict[str, _ModuleNamespace] = {}

    def _idl_get_module(name: str):
        module = _idl_modules.get(name)
        if module is None:
            module = _ModuleNamespace(name)
            _idl_modules[name] = module
        return module

    def _idl_dataclass_decorator(*args, **kwargs):
        def decorator(cls):
            return dataclasses.dataclass(cls)

        return decorator

    idl_mod.get_module = _idl_get_module
    idl_mod.alias = _idl_dataclass_decorator
    idl_mod.struct = _idl_dataclass_decorator
    idl_mod.union = _idl_dataclass_decorator
    idl_mod.enum = lambda cls: cls
    idl_mod.array = lambda dims: tuple(dims)

    def _idl_array_factory(typ, dims=None):
        if dims is None:
            return lambda: []
        length = int(dims[0]) if isinstance(dims, (list, tuple)) else int(dims)
        return lambda: [typ() if callable(typ) else 0 for _ in range(length)]

    idl_mod.array_factory = _idl_array_factory
    idl_mod.bound = lambda *args, **kwargs: ("bound", args, kwargs)
    idl_mod.case = lambda *args, **kwargs: ("case", args, kwargs)
    idl_mod.default = lambda *args, **kwargs: ("default", args, kwargs)
    idl_mod.type_name = lambda *args, **kwargs: ("type_name", args, kwargs)
    idl_mod.key = lambda *args, **kwargs: ("key", args, kwargs)
    idl_mod.uint8 = int
    idl_mod.uint64 = int
    idl_mod.int32 = int
    idl_mod.char = str

    rpc_mod = types.ModuleType("rti.rpc")

    rti_pkg.connextdds = connextdds_mod
    rti_pkg.idl = idl_mod
    rti_pkg.rpc = rpc_mod

    sys.modules["rti.connextdds"] = connextdds_mod
    sys.modules["rti.idl"] = idl_mod
    sys.modules["rti.rpc"] = rpc_mod


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
        skip = pytest.mark.skip(reason="RTI Connext DDS license missing/expired; integration_vendor tests skipped.")
        for item in items:
            if item.get_closest_marker("integration_vendor"):
                item.add_marker(skip)
