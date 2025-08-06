from typing import Type, Dict, override, Any, List
import importlib
import inspect
import re
from threading import Condition

from umaapy.util.umaa_utils import UMAAConcept, validate_umaa_obj, HashableNumericGUID

import rti.connextdds as dds


def get_specializations_from_generalization(
    generalization: Type, module_name: str = "umaapy.umaa_types"
) -> Dict[str, Type]:
    """
    Scans `module_name` for all classes matching *generalization's* base-type
    (using your regex), then returns a dict mapping the short name (after the
    last underscore) to the actual class object.
    """
    if not validate_umaa_obj(generalization(), UMAAConcept.GENERALIZATION):
        raise RuntimeError(f"Invalid generalization type '{generalization.__name__}'")

    mod = importlib.import_module(module_name)
    base = generalization.__name__.split("_")[-1]
    regex = re.compile(rf"^UMAA_.+(?<!_){re.escape(base)}$")

    out: Dict[str, Type] = {}
    for name, cls in inspect.getmembers(mod, inspect.isclass):
        if cls.__module__ != module_name or not regex.match(name):
            continue

        if not validate_umaa_obj(cls(), UMAAConcept.SPECIALIZATION):
            raise RuntimeError(f"Invalid specialization type '{cls.__name__}'")

        short = name.split("_")[-1]
        out[short] = cls

    return out


class SpecializationReaderDecorator(dds.DataReaderListener):
    def __init__(self, generalization_reader: dds.DataReader, specialization_readers: Dict[Type, dds.DataReader]):
        self._generalization_reader: dds.DataReader = generalization_reader
        self._specialization_readers: Dict[Type, dds.DataReader] = specialization_readers

        # topic_name -> { specID -> sample}
        self._buffers: Dict[str, Dict[HashableNumericGUID, Any]] = {
            self._generalization_reader.topic_name: {},
            **{sr.topic_name: {} for sr in self._specialization_readers.values()},
        }

        self._lock = Condition()

        self._generalization_reader.set_listener(self, dds.StatusMask.DATA_AVAILABLE)
        for spec_reader in self._specialization_readers.values():
            spec_reader.set_listener(self, dds.StatusMask.DATA_AVAILABLE)

    @override
    def on_data_available(self, reader: dds.DataReader):
        with self._lock:
            for data, info in reader.take():
                if info.valid:
                    if type(data).__name__ == self._generalization_reader.type_name:
                        # received generalization
                        # self._buffers[reader.topic_name][]
                        # If specialization in buffer queue that matches this call parent

                        pass
                    else:
                        # received specialization
                        # if generalization in buffer that matches specialization call parent
                        pass
                else:
                    pass
