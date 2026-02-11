Developing UMAAPy
=================

Testing Workflow
----------------

UMAAPy tests are classified with explicit pytest markers:

- ``unit``: Fast isolated tests with no live DDS middleware requirements.
- ``component``: Multi-module tests without live vendor middleware requirements.
- ``integration_vendor``: Tests that require RTI Connext DDS runtime and/or license.

Default test runs use the fast path and exclude vendor integration tests:

.. code-block:: bash

   pytest

This is equivalent to:

.. code-block:: bash

   pytest -m "not integration_vendor"

Run only vendor integration tests (for licensed RTI environments):

.. code-block:: bash

   pytest -m integration_vendor

Useful collection checks:

.. code-block:: bash

   pytest --collect-only
   pytest -m "not integration_vendor" --collect-only
   pytest -m integration_vendor --collect-only
