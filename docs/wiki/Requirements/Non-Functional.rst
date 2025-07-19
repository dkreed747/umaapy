UMAAPy Non-Funtional Requirements
---------------------------------

NFR-01 - Performance
~~~~~~~~~~~~~~~~~~~~

   Low Latency: End-to-end pub/sub loops shall complete within
   configurable bounds.

..

   High throughput: The SDK shall sustaint at least 100 messages/sec per
   topic on modern hardware.

NFR-02 - Scalability
~~~~~~~~~~~~~~~~~~~~

   The SDK shall support hundreds of concurrent publishers/subscirbers
   and UMAA objects without degradation.

..

   Thread pool sizing and DDS resource limits shall be tunable.

NFR-03 - Reliability
~~~~~~~~~~~~~~~~~~~~

   All core components must handle transient failures gracefully and
   detect/recover from faults where possible.

NFR-04 - Availability
~~~~~~~~~~~~~~~~~~~~~

   All core services must be stateless and support clean shutdown and
   startup for picking up where they left off based on the data on the
   DDS bus.

NFR-05 - Usability
~~~~~~~~~~~~~~~~~~

   APIs should follow common pythonic conventions creating a clear and
   easy to use SDK with clear documentation and turtorials.

NFR-06 - Extensibility
~~~~~~~~~~~~~~~~~~~~~~

   The SDK architecture should be open to additional expansion with
   minimal changes to core code.
