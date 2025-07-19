UMAAPy Architectural Requirements
---------------------------------

AR-01 Layered Modular Design
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall be organized into clear layers and independent modules

- ``umaapy.types``
- ``umaapy.report``
- ``umaapy.command``
- ``umaapy.collections``
- ``umaapy.behaviors``
- ``umaapy.orchestrator``
- ``umaapy.config``
- ``umaapy.logging``
- ``umaapy.util``

AR-02 Event-Driven Concurrency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   All incoming data shall be handled by listener callbacks that enqueue
   work on a shared thread pool: no blocking or syncrhonous DDS calls
   are allowed on the main thread.

AR-03 High Cohesion and Low Coupling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   Modules shall communicate through well-defined interfaces and data
   contracts; internal data structures and implementation details should
   not leak across modules

AR-04 Extensible Abstraction Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

   The SDK shall provide abstract base classes or interfaces for
   ``ReportProvider``, ``ReportConsumer``, ``CommandProvider``,
   ``CommandConsumer``, ``LargeList``, ``LargeSet``, and
   ``ObjectiveExecutor`` to allow users or downstream projects to
   subclass and extend behavior.

AR-05 Containers
~~~~~~~~~~~~~~~~

   The SDK shall use Docker for configuration management and deployment
   ensuring consistent behavior and portability

UMAAPy Architectural Design
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. mermaid::

   graph LR
     %% External Systems
     subgraph External
       UMAA_IDL["UMAA IDL Files"]
       RTI["RTI Connext DDS"]
       C2["External C2 Tool"]
     end

     %% SDK Core Components
     subgraph UMAAPy
       TypeGen["IDL Type Generation"]
       TypeIntrospect["Runtime Type Introspection"]
       GlobalConfig["GlobalConfig"]
       LoggingWrapper["Logging Wrapper"]
       ThreadPool["Thread Pool"]
       ReportProvider["ReportProvider<T>"]
       ReportConsumer["ReportConsumer<T>"]
       LargeList["LargeList<T>"]
       LargeSet["LargeSet<T>"]
       CommandProvider["CommandProvider"]
       CommandConsumer["CommandConsumer"]
       ObjectiveExecutor["ObjectiveExecutor"]
       MissionOrchestrator["Mission Orchestrator"]
     end

     %% IDL & Type Flow
     UMAA_IDL --> TypeGen
     TypeGen --> TypeIntrospect

     %% Configuration & Logging
     GlobalConfig --> ReportProvider
     GlobalConfig --> ReportConsumer
     GlobalConfig --> CommandProvider
     GlobalConfig --> CommandConsumer
     GlobalConfig --> LargeList
     GlobalConfig --> LargeSet
     GlobalConfig --> LoggingWrapper

     %% Concurrency
     ThreadPool --> CommandProvider
     ThreadPool --> CommandConsumer
     ThreadPool --> ObjectiveExecutor
     ThreadPool --> ReportProvider
     ThreadPool --> ReportConsumer

     %% DDS Interactions
     ReportProvider -- publishes --> RTI
     ReportConsumer -- subscribes --> RTI
     LargeList    -- pub/sub--> RTI
     LargeSet     -- pub/sub --> RTI
     CommandProvider -- listens/reply  --> RTI
     CommandConsumer -- requests/listens--> RTI

     %% External C2 Integration
     MissionOrchestrator -- exposes capability map --> C2

Type generation and Introspection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- UMAA IDL Files are passed to rsiddsgen to create the full Python data
  types.
- TypeIntrospect exposes runtime APIs to list active topics and inspect
  their data.

Global Configuration and Logging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- GlobalConfig is a central object that holds QoS settings for each UMAA
  paradigm. Users can adjust these settings at runtime.
- LoggingWrapper wraps Python's logging module in a smart way to
  implement the UMAA log report type

Core pub/Sub services
^^^^^^^^^^^^^^^^^^^^^

- Report Provider/Consumer are high-level classes for
  publishing/subscribing UMAA report types
- LargeList/LargeSet are pythonic abstractions of the complex UMAA Large
  Collection paradigm

Commanded Services
^^^^^^^^^^^^^^^^^^

- CommandConsumer/CommandProvider implement UMAA's special form of RPC
  using DDS listeners, driven by events and executed via the internal
  thread poool

Objectives and Mission Orchestration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- These two are behavior level APIs that expose methods for building
  behaviors and planing missions with them.
- The ObjectiveExecutor is a base skeleton that components register
  against.
- The MissionOrchestrator listens for objective registrations and
  maintains a live capability map

Concurrency and DDS integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Thread pool ensures all tasks from the core UMAA components are ran
  safely and efficiently.
- RTI Connect DDS is the fabric for all data exchange. Every provider,
  consumer, list/set, command, and log interface interacts with DDS
  topics to publish and subscribe data reliably according to configured
  QoS.

Motivation
^^^^^^^^^^

Together, these components form a modular, highly configurable SDK that
shields users from DDS and UMAA boilerplate while providing full access
to UMAA's powerful autonomy paradigms.
