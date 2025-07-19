UMAAPy Software Requirements
----------------------------

SR-01 - IDL Type Generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall support automatic generation of Python classes from the
   UMAA IDL files, either by invoking ``rtiddsgen`` at install time or
   consuming pre-generated modules and expose a CLI or API hook to
   trigger regeneration.

**Scenario: Automatic generation at install time**

.. code:: gherkin

   Given the UMAA IDL files are present in the project directory  
   When the user installs the SDK with type-generation enabled  
   Then the installer invokes `rtiddsgen`  
   And Python classes are generated under the SDK package  
   And a success message is shown to the user  

**Scenario: Regeneration via CLI using pre-generated modules**

.. code:: gherkin

   Given pre-generated Python modules for UMAA types exist in the project  
   When the user runs `umaa-py regenerate-types`  
   Then the SDK skips calling `rtiddsgen`  
   And verifies the pre-generated modules are up to date  
   And outputs “Regeneration complete”  

--------------

SR-02 - Runtime Type Introspection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall provide runtime APIs to list available DDS topics and
   their associated UMAA types, and to query type metadata (fields,
   specializations) for debugging and dynamic workflows.

**Scenario: Listing available topics and types**

.. code:: gherkin

   Given a DDS domain with UMAA topics “Telemetry” and “CommandStatus” published  
   When the user calls `sdk.list_topics()`  
   Then the SDK returns a list containing “Telemetry: TelemetryType” and “CommandStatus: StatusType”  

**Scenario: Querying type metadata**

.. code:: gherkin

   Given topic “Telemetry” is available at runtime  
   When the user calls `sdk.get_type_metadata("Telemetry")`  
   Then the SDK returns the list of fields and their types for TelemetryType  
   And indicates any specializations or inheritance hierarchy  

--------------

SR-03 - UMAA Generalization/Specialization Abstraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall allow users to publish and subscribe to UMAA defined
   specialization types while transparently handling the underlying two
   topic interface design - merging and down-casting messages so that
   the user simply works with the highest common ancestor or concrete
   subclass.

**Scenario: Publishing a specialized type as its base**

.. code:: gherkin

   Given a `SensorReading` specialization `TemperatureReading`  
   When the user publishes a `TemperatureReading` via `sdk.publish("SensorReading", msg)`  
   Then the SDK merges the two underlying topics  
   And the message appears on both the `SensorReading` and `TemperatureReading` topics  

**Scenario: Subscribing to a base type and receiving a specialized
message**

.. code:: gherkin

   Given a subscriber on “SensorReading”  
   And a `PressureReading` specialization is published  
   When the SDK delivers the message to the subscriber  
   Then the subscriber receives a `PressureReading` object  
   And the SDK down-casts it to the base `SensorReading` interface  

--------------

SR-04 - UMAA Report Provider/Consumer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall provide ``ReportProvider[T]`` and ``ReportConsumer[T]``
   classes for UMAA report types, hiding low-level DDS details as well
   as UMAA-specific boilerplate logic. Users shall be able to configure
   the QoS for reports at runtime without impacting other UMAA
   paradigms.

**Scenario: Creating a ReportProvider with custom QoS**

.. code:: gherkin

   Given the user configures `qos_profile` with reliability=RELIABLE  
   When they instantiate `ReportProvider[HealthStatus](qos_profile=qos_profile)`  
   Then the provider uses the specified QoS for publishing  
   And other SDK components retain their default QoS  

**Scenario: Consuming reports with default settings**

.. code:: gherkin

   Given a `ReportPublisher` is publishing `HealthStatus` reports  
   When the user creates `ReportConsumer[HealthStatus]()`  
   Then the consumer subscribes with SDK’s default QoS  
   And invokes the user callback on each report message  

--------------

SR-05 - UMAA Large Collections API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall expose pythonic ``LargeSet[T]`` and ``LargeList[T]``
   classes that mirror the built-in ``set`` and ``list`` interfaces.
   They should support add/remove/iteration/length and shall
   automatically maintain the latest valid view, with methods to check
   validity or await completeness.

**Scenario: Adding and iterating a LargeSet**

.. code:: gherkin

   Given an empty `LargeSet[Task]` subscribed to DDS  
   When the user calls `collection.add(task1)` and `collection.add(task2)`  
   Then `len(collection)` returns 2  
   And iterating `for t in collection` yields `task1` and `task2`  

**Scenario: Awaiting completeness on a LargeList**

.. code:: gherkin

   Given a `LargeList[Waypoint]` is being populated over DDS  
   When the user calls `collection.await_complete(timeout=5s)`  
   Then the call blocks until all initial elements arrive or timeout elapses  
   And returns True if complete, False if timeout occurred  

--------------

SR-06 - Request/Reply and Pub/Sub Services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall enable request/reply and pub/sub command flows using
   DDS listeners and a thread-pool executor to align with DDS best
   practices, as well as enabling a high-throughput, low-latency API.
   Specifically:

- Consumers may send a command and block for a reply or register
  callbacks for each UMAA command state.
- Providers shall register handler callbacks, execute them on the thread
  pool, and return status updates.
- Built-in support for timeouts, retries, and graceful error
  propagation.

**Scenario: Blocking request/reply command**

.. code:: gherkin

   Given a provider registered for the “ComputePath” command  
   When the consumer calls `reply = sdk.send_request("ComputePath", cmd, timeout=2s)`  
   Then the call blocks until a reply or timeout  
   And returns the command result on success  

**Scenario: Callback-based pub/sub command flow**

.. code:: gherkin

   Given a consumer has registered callbacks for “StartMission” states  
   When the provider invokes state updates for the mission command  
   Then the SDK dispatches each state change to the consumer’s callbacks on the thread pool  

--------------

SR-07 - UMAA Commanded Services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall include two first-class abstractions,
   ``CommandConsumer`` and ``CommandProvider``, that implement UMAA’s
   specific RPC semantics, hiding DDS details and UMAA business logic to
   expose a simple API for sending commands, handling states, and
   processing errors per the UMAA ICDs.

**Scenario: Sending a command via CommandConsumer**

.. code:: gherkin

   Given a `CommandProvider` is available for “DeploySensor”  
   When the user calls `consumer.send("DeploySensor", params)`  
   Then the SDK returns a `CommandHandle` for tracking  
   And the user can await or register callbacks on it  

**Scenario: Handling a command in CommandProvider**

.. code:: gherkin

   Given a user-supplied handler for “DeploySensor”  
   When a command arrives  
   Then the SDK schedules the handler on its thread pool  
   And publishes state updates and final status messages  

--------------

SR-08 - Behavior Skeleton (Objective Executor)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall allow components to register as “Objective Executors”,
   accepting objective commands, and let users supply:

- Execution logic, success/failure criterion, and progress reporting.
- Lifecycle hooks for each UMAA objective state.
- Inter-executor delegation so objectives can call others for subtasks.

**Scenario: Registering an Objective Executor**

.. code:: gherkin

   Given a user-defined class `InspectAreaExecutor`  
   When the user calls `sdk.register_executor(InspectAreaExecutor)`  
   Then the SDK advertises the executor in the capability map  
   And invokes its lifecycle hooks on objective commands  

**Scenario: Delegating to another executor**

.. code:: gherkin

   Given `ScanPerimeterExecutor` and `AnalyzeDataExecutor` are registered  
   When `ScanPerimeterExecutor` calls `delegate("AnalyzeData", data)`  
   Then the SDK routes the command to `AnalyzeDataExecutor`  
   And returns the subtask result to the caller  

--------------

SR-09 - Mission Orchestrator (Mission Management)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall detect registered objective executors, build a dynamic
   capability map, and expose an API for external C2 tools to:

- Query available objectives and their parameters.
- Submit high-level mission goals.
- Monitor status via UMAA report topics.

**Scenario: Querying available objectives**

.. code:: gherkin

   Given executors “Navigate” and “CollectSample” are registered  
   When an external tool calls `sdk.get_capabilities()`  
   Then the SDK returns a list of objectives with their parameters and descriptions  

**Scenario: Submitting and monitoring a mission**

.. code:: gherkin

   Given the mission orchestrator is initialized  
   When the user calls `sdk.submit_mission([Navigate, CollectSample])`  
   Then the SDK publishes a mission goal on the report topic  
   And the user can subscribe to mission status updates via `sdk.on_mission_update()`  

--------------

SR-10 - QoS Configuration and Tuning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall provide a global configuration object for each UMAA
   paradigm (reports, commands, collections, logs, etc.), allowing
   use-case-specific QoS overrides at runtime.

**Scenario: Overriding global QoS for commands**

.. code:: gherkin

   Given default command QoS is “Reliable”  
   When the user sets `sdk.config.commands.qos = BestEffort`  
   Then new CommandProviders use BestEffort QoS  
   And other paradigms retain their default settings  

**Scenario: Applying runtime QoS change to collections**

.. code:: gherkin

   Given a `LargeList` subscription exists  
   When the user updates `sdk.config.collections.history_depth = 1000`  
   Then the SDK reconfigures the subscription depth without restarting  

--------------

SR-11 - Logging
~~~~~~~~~~~~~~~

   The SDK shall wrap Python’s standard ``logging`` module and, on each
   ``info``, ``warn``, or ``error`` call, forward a UMAA log-report
   message over DDS in parallel with local logging as specified in the
   UMAA ICD and respecting user configuration.

**Scenario: Forwarding log messages over DDS**

.. code:: gherkin

   Given user logging level is INFO  
   When the user calls `logger.info("System initialized")`  
   Then the message is logged locally  
   And a UMAA log-report with level INFO is published over DDS  

**Scenario: Suppressing DDS logs below warning**

.. code:: gherkin

   Given user config sets `sdk.config.logging.dds_min_level = WARN`  
   When the user calls `logger.info("Debug detail")`  
   Then the local log is recorded  
   And no DDS log-report is sent  

--------------

SR-12 - Thread Safety and Concurrency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   All SDK components shall be safe for use from multiple threads,
   leveraging an internal thread pool for service execution and ensuring
   low-latency, high-throughput operation on both x86 and ARM
   architectures.

**Scenario: Concurrent publishing from multiple threads**

.. code:: gherkin

   Given two threads each call `sdk.publish("Telemetry", msg)` simultaneously  
   When the SDK handles both calls  
   Then no race conditions occur  
   And both messages are delivered to DDS listeners  

**Scenario: Handling service execution on thread pool**

.. code:: gherkin

   Given a `CommandProvider` handler that sleeps for 1s  
   When 10 commands arrive in quick succession  
   Then the SDK schedules each handler on the thread pool  
   And processes them concurrently within pool limits  

--------------

SR-13 - Platform and Packaging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall support Python 3.11+ on Windows and Linux, be
   installable via pip, and provide Docker-friendly images for rapid
   development/deployment.

**Scenario: Installing via pip on Linux**

.. code:: gherkin

   Given Python 3.11 is available on Ubuntu 22.04  
   When the user runs `pip install umaapy-sdk`  
   Then the SDK installs without errors  
   And the `umaa-py` CLI is available in PATH  

**Scenario: Running in Docker container**

.. code:: gherkin

   Given the official UMAAPy Docker image  
   When the user runs `docker run umaapy/sdk:latest python -c "import umaapy"`  
   Then no import errors occur  
   And the SDK version matches the image tag  

--------------

SR-14 - Testing and Mocks
~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall include Pytest-based unit tests and provide mock
   implementations of DDS publishers, subscribers, and services so that
   users can write unit tests for their components without a live DDS
   bus.

**Scenario: Writing a unit test with mocks**

.. code:: gherkin

   Given a component depends on `DDSPublisher`  
   When the user injects `MockPublisher` into the component  
   And runs `pytest`  
   Then the component’s publish calls are recorded by the mock  
   And tests pass without a real DDS bus  

**Scenario: Running the SDK’s test suite**

.. code:: gherkin

   Given the SDK repository is cloned  
   When the user runs `pytest tests/`  
   Then all Pytest tests execute  
   And the test report shows 0 failures  

--------------

SR-15 - CI/CD
~~~~~~~~~~~~~

   The SDK repository shall include configuration examples for GitLab
   CI/CD pipelines, with stages for linting, type-checking, code
   generation, running the full test suite, as well as deploying the
   packages and Docker images as artifacts.

**Scenario: Linting stage in CI**

.. code:: gherkin

   Given the GitLab CI pipeline is triggered on push  
   When the lint stage runs `flake8`  
   Then any style violations are reported  
   And the pipeline fails if errors exist  

**Scenario: Deployment stage produces artifacts**

.. code:: gherkin

   Given tests and type-checking have passed  
   When the deploy stage runs  
   Then the built Python wheel and Docker image are published as artifacts  
   And tagged with the commit SHA  

--------------

SR-16 - Automated API documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The system shall integrate the Sphinx documentation generator into
   its CI/CD pipeline such that, upon every successful build and test
   stage, up-to-date HTML (and optionally PDF) API documentation is
   produced automatically for the system’s API.

**Scenario: Invoke Sphinx to build HTML documentation after tests**

.. code:: gherkin

   Given the CI/CD pipeline has completed the build and test stages successfully
   When the pipeline runs the documentation step
   Then `sphinx-build` is executed against the `docs/` directory
   And HTML documentation is generated in `build/docs/html/`

**Scenario: (Optional) Generate PDF documentation when enabled**

.. code:: gherkin

   Given PDF output is enabled in the Sphinx configuration
   When the pipeline runs `sphinx-build` with the PDF builder target
   Then PDF documentation is generated in `build/docs/pdf/`
