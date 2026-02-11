Developing UMAAPy
=================

Contributor Environment
-----------------------

Use an editable install so tests and local commands resolve the workspace ``src/`` tree:

.. code-block:: bash

   python -m pip install --upgrade pip
   pip install -e .[tests]

Verify import resolution points to this checkout (not a stale site-packages install):

.. code-block:: bash

   UMAAPY_AUTO_INIT=0 python -c "import umaapy,inspect; print(umaapy.__file__)"

Expected output ends with ``src/umaapy/__init__.py`` from your local repository path.

Testing Workflow
----------------

UMAAPy tests are classified with explicit pytest markers:

- ``unit``: Fast isolated tests with no live DDS middleware requirements.
- ``component``: Multi-module tests without live vendor middleware requirements.
- ``integration_vendor``: Tests that require a live DDS runtime (Cyclone-first lane; marker name retained for compatibility).

Default test runs use the fast path and exclude vendor integration tests:

.. code-block:: bash

   pytest

This is equivalent to:

.. code-block:: bash

   pytest -m "not integration_vendor"

Run only middleware integration tests:

.. code-block:: bash

   pytest -m integration_vendor

Useful collection checks:

.. code-block:: bash

   pytest --collect-only
   pytest -m "not integration_vendor" --collect-only
   pytest -m integration_vendor --collect-only
