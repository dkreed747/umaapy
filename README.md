# UMAAPy

[![CI](https://github.com/dkreed747/umaapy/actions/workflows/ci.yml/badge.svg)](https://github.com/dkreed747/umaapy/actions/workflows/ci.yml)
[![Docs](https://github.com/dkreed747/umaapy/actions/workflows/docs.yml/badge.svg)](https://github.com/dkreed747/umaapy/actions/workflows/docs.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/umaapy.svg)](https://pypi.org/project/umaapy/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

UMAAPy is a Python SDK for building UMAA-compliant maritime autonomy applications on top of DDS middleware, with a Cyclone-first architecture direction for runtime and integration workflows. It provides:

- High-level reader/writer adapters for UMAA multi-topic graphs (generalization/specialization, Large Sets, Large Lists)
- Convenient editors for composing complex nested messages for publishing
- Simple helpers for building report providers/consumers and command providers/consumers

Quick start
-----------

Install:

```bash
pip install umaapy
```

Development setup:

```bash
pip install -e .[tests]
UMAAPY_AUTO_INIT=0 python -c "import umaapy,inspect; print(umaapy.__file__)"
pytest -m "not integration_vendor"
```

Minimal reader/writer:

```python
from umaapy import get_configurator, reset_dds_participant
from umaapy.umaa_types import UMAA_SA_GlobalPoseStatus_GlobalPoseReportType as GlobalPoseReport

reset_dds_participant()
cfg = get_configurator()
reader = cfg.get_reader(GlobalPoseReport)
writer = cfg.get_writer(GlobalPoseReport)

writer.write(GlobalPoseReport())
print(len(list(reader.read_data())))
```

Docs & links
------------

- **Docs**: https://dkreed747.github.io/umaapy/
- **Source**: https://github.com/dkreed747/umaapy
- **Issues**: https://github.com/dkreed747/umaapy/issues
- Build locally: `sphinx-build -b html docs docs/_build/html`

License
-------

MIT
