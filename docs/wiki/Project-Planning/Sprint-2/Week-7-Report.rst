Weekly Progress Report
======================

**Project Name:** UMAAPy

**Sprint #:** 2

**Reporting Period:** 06/23/25–06/29/25

**Date of Report:** 6/25/25

**Prepared By:** Devon Reed

1. High-Level Summary
---------------------

This week, a very large portion of software development activity has
taken place on the essential elements of the SDK that will be required
for the MVP demonstration at the end of Sprint 2. Additionally, sequence
diagrams for the command flow and event management for the command
provider have been created this week to show progress for UI design.
Lastly, based on feedback from the first week’s assignment, a
traceability matrix for requirements completed and prioritized is added
in addition to the acceptance criteria for each requirement.

2. Sprint Planning (Review & Adjustments)
-----------------------------------------

2.1. Sprint Goal & Scope
~~~~~~~~~~~~~~~~~~~~~~~~

- **Original Sprint Goal:** Software development of core SDK components
  and MVP development
- **Scope for this Week:** Develop report provider and command provider
  interface

2.2. Definition of Done (DoD) Refinement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Issue #45 was added and had the definition of done defined. The
definition of done reflects the importance of providing a visualization
of how the vehicle can be controlled through the SDK interfaces, as well
as producing artifacts that can be used for the MVP demo. See the issue
description for full details:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/45

2.3. New/Changed Sprint Backlog Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------+----------------+---------------------------+-------------+-------------+
| GitLab      | Description    | Type                      | Rationale   | Impact      |
| Issue       |                | (Add/Remove/Reprioritize) |             |             |
+=============+================+===========================+=============+=============+
| #5          | Report         | Remove                    | Dropped to  | Low         |
|             | Consumer API   |                           | better      |             |
|             | development    |                           | support MVP |             |
|             |                |                           | product     |             |
+-------------+----------------+---------------------------+-------------+-------------+
| #4          | Report         | Reprioritize              | Moved to    | Medium      |
|             | Provider API   |                           | higher      |             |
|             | development    |                           | priority    |             |
|             |                |                           | for MVP     |             |
+-------------+----------------+---------------------------+-------------+-------------+
| #11         | Command        | Remove                    | Dropped to  | Low         |
|             | Consumer API   |                           | better      |             |
|             | development    |                           | support MVP |             |
|             |                |                           | product     |             |
+-------------+----------------+---------------------------+-------------+-------------+
| #8          | Synchronous    | Remove                    | Dropped to  | Low         |
|             | command API    |                           | better      |             |
|             | support        |                           | support MVP |             |
|             |                |                           | product     |             |
+-------------+----------------+---------------------------+-------------+-------------+
| #9          | Asynchronous   | Remove                    | Dropped to  | Low         |
|             | command API    |                           | better      |             |
|             | support        |                           | support MVP |             |
|             |                |                           | product     |             |
+-------------+----------------+---------------------------+-------------+-------------+
| #12         | Command        | Add                       | Added to    | Medium      |
|             | Provider       |                           | better      |             |
|             | abstraction    |                           | support MVP |             |
|             |                |                           | product     |             |
+-------------+----------------+---------------------------+-------------+-------------+
| #45         | Simulation     | Add                       | Added to    | High        |
|             | backend demo   |                           | capture MVP |             |
|             | implementation |                           | demo work   |             |
+-------------+----------------+---------------------------+-------------+-------------+

3. Source Code Development
--------------------------

3.1. GitLab Repository Links
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Repo:** https://git.psu.edu/psu-sweng/sweng-894
- **Branches of Interest:** 4 and 12. Since these branches have already
  been merged, see repository graph for timeline and commit progress:
  https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/network/main?ref_type=heads

3.2. Summary of Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Devon:**

- Primarily source code development
- Closed 3 merge requests
- Created acceptance criteria for entire requirements list
- Created documentation and diagrams for core SDK components
- Created weekly report

3.3. Important Merge Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------+---------------+-------------+--------------+-------------+
| Merge       | Description   | Issue Ref   | Associated   | Notes       |
| Request     |               |             | Requirements |             |
+=============+===============+=============+==============+=============+
| !7          | Create DDS    | #15         | **SR-10,     | Merged      |
|             | configuration |             | AR-01,       |             |
|             | abstraction   |             | NFR-05**     |             |
+-------------+---------------+-------------+--------------+-------------+
| !8          | Create UMAA   | #4          | **SR-04,     | Merged      |
|             | report        |             | SR-06,       |             |
|             | provider      |             | AR-04**      |             |
|             | abstraction   |             |              |             |
+-------------+---------------+-------------+--------------+-------------+
| !9          | Create UMAA   | #12         | **SR-06,     | Merged      |
|             | command       |             | SR-07,       |             |
|             | provider      |             | AR-02**      |             |
|             | abstraction   |             |              |             |
+-------------+---------------+-------------+--------------+-------------+

3.4. Burndown Chart
~~~~~~~~~~~~~~~~~~~

This week’s burndown was fairly significant due to the scope of the
sprint being well-defined after the latest email correspondence with
professor on the final MVP product. The burn up is associated with one
new issue being added to support the extra work required to get the MVP
stood up as well as capture the work required to produce the MVP demo.

.. figure::
   ../../uploads/d8b4dc5738c5d858fded5594cd6a4d24/Sprint2-Week2-Burndown.png
   :alt: Sprint2-Week2-Burndown
   :width: 1037px
   :height: 571px

   Sprint2-Week2-Burndown

4. Software Testing
-------------------

4.1. Acceptance Criteria Defined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

From the feedback on the last report, acceptance criteria for each
functional requirement has been defined and added to the `requirements’
wiki
page <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/wikis/home/Requirements>`__.
The acceptance criteria then drive test case development for the user
stories that satisfy each requirement, as reported in the test
traceability matrix. Additionally, a comprehensive status update on the
progress of the system requirements can be found below and will be
included/updated on all reports going forward.

+-------------------------------+-------------------+-----------+---------------------------+
| Requirement ID                | Status            | Reference | Notes                     |
|                               |                   | Issues    |                           |
+===============================+===================+===========+===========================+
| SR-01 – IDL Type Generation   | SATISFIED         | #1        | Completed and automated   |
|                               |                   |           | in pipeline               |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-02 – Runtime Type          | UNSATISFIED       | #2        | BACKLOG                   |
| Introspection                 |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-03 – UMAA                  | UNSATISFIED       | #3        | BACKLOG                   |
| Generalization/Specialization |                   |           |                           |
| Abstraction                   |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-04 – UMAA Report           | PARTIALY          | #4, #5    | Report Provider complete  |
| Provider/Consumer             | SATISIFED         |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-05 – UMAA Large            | UNSATISFIED       | #6, #7    | BACKLOG                   |
| Collections API               |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-06 – Request/Reply and     | PARTIALY          | #8, #9,   | Report pub and command    |
| Pub/Sub Services              | SATISIFED         | #10       | request/reply implemented |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-07 – UMAA Commanded        | PARTIALLY         | #11, #12  | Command provider          |
| Services                      | SATISFIED         |           | completed                 |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-08 – Behavior Skeleton     | UNSATISFIED       | #13       | BACKLOG                   |
| (Objective Executor)          |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-09 – Mission Orchestrator  | UNSATISFIED       | #14       | BACKLOG                   |
| (Mission Management)          |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-10 – QoS Configuration and | SATISFIED         | #15       | Complete                  |
| Tuning                        |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-11 – Logging               | PARTIALLY         | #16       | Python logging utility    |
|                               | SATISFIED         |           | setup                     |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-12 – Thread Safety and     | SATISFIED         | #17       | Complete                  |
| Concurrency                   |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-13 – Platform and          | SATISFIED         | #18       | Complete                  |
| Packaging                     |                   |           |                           |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-14 – Testing and Mocks     | PARTIALLY         | #19       | Pytest and Pymock         |
|                               | SATISFIED         |           | initialized and automated |
|                               |                   |           | in pipeline               |
+-------------------------------+-------------------+-----------+---------------------------+
| SR-15 – CI/CD                 | SATISFIED         | #20, #21, | Complete                  |
|                               |                   | #22       |                           |
+-------------------------------+-------------------+-----------+---------------------------+

4.2. Test Case Specification (Incremental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

6 new test cases were defined this reporting period for two use cases:
#4 and #6.

Test cases for #4:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/4

- https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/46
- https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/47
- https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/48

Test cases for #12:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/12

- https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/49
- https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/50
- https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/51

4.3. Traceability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~

+---------+-------------+---------+---------+---------+---------+-------------+
| Use     | Functional  | Arch.   | MR      | Test ID | Test    | Comments    |
| Case    | Requirement | Element |         |         | Status  |             |
+=========+=============+=========+=========+=========+=========+=============+
| #15     | SR-10       | Class   | !7      | #42     | PASS    | QoS profile |
|         |             |         |         |         |         | loading     |
+---------+-------------+---------+---------+---------+---------+-------------+
| #15     | SR-10       | Class   | !7      | #43     | PASS    | Specific    |
|         |             |         |         |         |         | profile     |
|         |             |         |         |         |         | selection   |
+---------+-------------+---------+---------+---------+---------+-------------+
| #15     | SR-10       | Class   | !7      | #44     | PASS    | UMAA Topic  |
|         |             |         |         |         |         | loading     |
+---------+-------------+---------+---------+---------+---------+-------------+
| #4      | SR-04,      | Class   | !8      | #46     | PASS    | Proper UMAA |
|         | SR-06       |         |         |         |         | metadata    |
|         |             |         |         |         |         | setting     |
+---------+-------------+---------+---------+---------+---------+-------------+
| #4      | SR-04,      | Class   | !8      | #47     | PASS    | Test report |
|         | SR-06       |         |         |         |         | publication |
+---------+-------------+---------+---------+---------+---------+-------------+
| #4      | SR-04,      | Class   | !8      | #48     | PASS    | Test writer |
|         | SR-06       |         |         |         |         | callbacks   |
+---------+-------------+---------+---------+---------+---------+-------------+
| #12     | SR-06,      | Class   | !9      | #49     | PASS    | Test UMAA   |
|         | SR-07       |         |         |         |         | command     |
|         |             |         |         |         |         | flow        |
+---------+-------------+---------+---------+---------+---------+-------------+
| #12     | SR-06,      | Class   | !9      | #50     | PASS    | Test        |
|         | SR-07       |         |         |         |         | destination |
|         |             |         |         |         |         | content     |
|         |             |         |         |         |         | filtering   |
+---------+-------------+---------+---------+---------+---------+-------------+
| #12     | SR-06,      | Class   | !9      | #51     | PASS    | Test new    |
|         | SR-07       |         |         |         |         | commands    |
|         |             |         |         |         |         | executed in |
|         |             |         |         |         |         | thread pool |
+---------+-------------+---------+---------+---------+---------+-------------+

5. Backlog Grooming
-------------------

5.1. Changes to Product/Sprint Backlog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One ticket has been added to the product backlog, then pulled into
Sprint 2 to address the work required to integrate the SDK with the Holo
Ocean sim environment to produce.

5.2. Rationale & Impact
~~~~~~~~~~~~~~~~~~~~~~~

The integration, sim, and demo ticket will have no impact on the
timeline of the Sprint since other low priority tickets were pushed to
make room. This ticket will provide the time needed to use the first
parts of the SDK to create an interface that any UMAA enabled autonomy
framework can talk to simulate a real vehicle.

6. Issues, Risks & Mitigations
------------------------------

6.1. New Issues / Blockers
~~~~~~~~~~~~~~~~~~~~~~~~~~

No new issues or blockers this reporting period.

6.2. Potential Risks
~~~~~~~~~~~~~~~~~~~~

- Field travel for work.

  - *Likelihood:* High
  - *Impact:* Less time available for project
  - *Mitigation:* None

7. Metrics & Charts
-------------------

7.1 PyTest results from latest merge pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/pipelines/414456/test_report?job_name=test

7.2 Latest PyTest coverage report
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/jobs/1384343/artifacts/file/coverage.xml

8. UI/API Design
----------------

UMAA Command Provider
~~~~~~~~~~~~~~~~~~~~~

A command provider in terms of UMAA is any service that responds to the
standard command flow laid out in the ICD. Command providers offer up
some form of service that others can consume. The end result is very
much like a remote procedure call design. See below for an example
command flow for a service that has a determinate completion condition.

.. mermaid::

   sequenceDiagram
   actor c as Command Consumer
   participant p as Command Provider

   Note left of p: Simplified UMAA command flow

   c->>p: Consumer sends CommandType<br>to provider
   p->>c: Provider must respond<br>with and AcknowledgementType

   p->>c: Provider sends CommandStatusTypes<br>To inform the consumer of progress<br>

   opt Optionally send CommandExecutionStatus
     p->>c: Provider can send detailed execution<br>status messages if defined
   end

   p->>c: Provider sends COMPLETED status<br>when done

Some command providers may also offer services that don’t have a
determinate end and instead represent long-running actions such as
moving or starting behaviors. These services must be cancelled by the
consumer to reach the end of the command session.

.. mermaid::

   sequenceDiagram
   actor c as Command Consumer
   participant p as Command Provider

   c->>p: Consumer sends CommandType<br>to provider
   p->>c: Provider must respond<br>with and AcknowledgementType

   p->>c: Provider sends CommandStatusTypes<br>To inform the consumer of progress<br>

   opt Optionally send CommandExecutionStatus
     p->>c: Provider can send detailed execution<br>status messages if defined
   end

   Note left of c: Consumer must cancel the command when it is finished

   c->>p: Consumer disposes CommandType instance

   p->>c: Consumer sends canceled status<br>and starts cleanup

   p->>c: Provider sends COMPLETED status<br>when done

The last use case of a command provider is to be updated by it consumer
after execution has begun. This is very common in services where new
data becomes available and is passed to the provider while executing
rather than canceling the command and sending a new one, which is
wasteful. Below is the expected sequence for a command update to the
provider.

.. mermaid::

   sequenceDiagram
   actor c as Command Consumer
   participant p as Command Provider

   c->>p: Consumer sends CommandType<br>to provider
   p->>c: Provider must respond<br>with and AcknowledgementType

   p->>c: Provider sends CommandStatusTypes<br>To inform the consumer of progress<br>

   opt Optionally send CommandExecutionStatus
     p->>c: Provider can send detailed execution<br>status messages if defined
   end

   Note left of c: Consumer triggers update
   c->>p: Consumer sends CommandType with same SessionID

   p->>c: Provider resets command status back to ISSUED with reason UPDATED and restarts the command session

   p->>c: Provider sends CommandStatusTypes<br>To inform the consumer of progress<br>

   opt Optionally send CommandExecutionStatus
     p->>c: Provider can send detailed execution<br>status messages if defined
   end
