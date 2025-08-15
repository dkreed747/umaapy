Welcome to UMAAPy's documentation!
==================================

.. image:: images/umaapy-logo.png
   :scale: 50

The UMAAPy SDK provides a comprehensive software framework for autonomous maritime operations,
leveraging the RTI Connext DDS Python API for robust, scalable, and
real-time data communication. UMAAPy aims to facilitate the rapid development and
deployment of maritime autonomy systems by abstracting core UMAA components and
integrating them seamlessly through event-driven mechanisms.

Features
""""""""

#. ReportProvider: DDS writer abstraction to publish UMAA reports to the DDS bus.
#. ReportConsumer: DDS reader abstraction to consume incoming UMAA reports.
#. CommandProvider: DDS reader and writer remote procedure call like abstraction for defining business logic for incoming commands.
#. CommandConsumer: DDS reader and writer abstraction for commanding remote UMAA services.
#. LargeSet: DDS reader and writer abstraction for the UMAA large set data structure topics.
#. LargeList: DDS reader and writer abstraction for the UMAA large list data structure topics.
#. Specializations: DDS reader and writer abstraction for UMAA generalization/specialization topic relationships.


.. toctree::
   :maxdepth: 2
   :caption: UMAAPy Documentation

   usage/installation
   usage/getting_started
   umaapy/multi_topic_umaa

.. toctree::
   :maxdepth: 2
   :caption: API Documentation

   umaapy/index

.. toctree::
   :maxdepth: 2
   :caption: Internal

   wiki/Design/index
   wiki/Project-Planning/index
   wiki/Requirements/index
