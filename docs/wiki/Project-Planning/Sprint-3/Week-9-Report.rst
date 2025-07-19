Weekly Progress Report
======================

**Project Name:** UMAAPy

**Sprint #:** 3

**Reporting Period:** 07/07/25–07/13/25

**Date of Report:** 07/13/25

**Prepared By:** Devon Reed

1. High-Level Summary
---------------------

This week, I was out on vacation and had little bandwidth for project
development. Due to the entire team being unavailable, the main focus
was on completing the algorithmic component document as well as defining
the sprint backlog while also looking at possible requirements to drop
as we enter the last half of the project. For sprint 3, the objective is
to complete all non-negotiable software elements, thereby creating a
complete SDK. That will leave Sprint 4 for creating in-depth API
documentation, as well as system testing and hitting stretch goals.

2. Sprint Planning (Review & Adjustments)
-----------------------------------------

2.1. Sprint Goal & Scope
~~~~~~~~~~~~~~~~~~~~~~~~

- **Original Sprint Goal:** Complete remaining core SDK features
- **Scope for this Week:** Complete algorithm design document and sprint
  planning

2.2. Definition of Done (DoD) Refinement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No definition of done refinement was made to any of the user stories
added to the sprint 3 backlog.

2.3. New/Changed Sprint Backlog Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+
| GitLab Issue                                                         | Description    | Type                      | Rationale     | Impact      |
|                                                                      |                | (Add/Remove/Reprioritize) |               |             |
+======================================================================+================+===========================+===============+=============+
| `#5 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/5>`__   | UMAA Report    | Add                       | Core SDK      | Medium      |
|                                                                      | Consumer       |                           | requirement   |             |
|                                                                      | abstraction    |                           | essential for |             |
|                                                                      | for reading    |                           | UMAA          |             |
|                                                                      | subscribing to |                           | communication |             |
|                                                                      | report topics  |                           |               |             |
+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+
| `#11 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/11>`__ | UMAA Command   | Add                       | Core SDK      | Medium      |
|                                                                      | Consumer       |                           | requirement   |             |
|                                                                      | abstraction    |                           | essential for |             |
|                                                                      | for requesting |                           | UMAA          |             |
|                                                                      | services from  |                           | communication |             |
|                                                                      | a command      |                           |               |             |
|                                                                      | provider       |                           |               |             |
+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+
| `#8 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/8>`__   | API            | Add                       | Required for  | Low         |
|                                                                      | non-functional |                           | Command       |             |
|                                                                      | requirement    |                           | Consumer      |             |
|                                                                      | for usability  |                           |               |             |
+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+
| `#9 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/9>`__   | API            | Add                       | Required for  | Low         |
|                                                                      | non-functional |                           | Command       |             |
|                                                                      | requirement    |                           | Consumer      |             |
|                                                                      | for usability  |                           |               |             |
+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+
| `#10 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/10>`__ | API            | Add                       | Required for  | Low         |
|                                                                      | non-functional |                           | Command       |             |
|                                                                      | requirement    |                           | Consumer      |             |
|                                                                      | for usability  |                           |               |             |
+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+
| `#7 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/7>`__   | UMAA LargeSet  | Add                       | Core SDK      | Medium      |
|                                                                      | API for        |                           | requirement   |             |
|                                                                      | abstracting    |                           | essential for |             |
|                                                                      | UMAA set data  |                           | several UMAA  |             |
|                                                                      | structures     |                           | components    |             |
|                                                                      | across         |                           |               |             |
|                                                                      | multiple DDS   |                           |               |             |
|                                                                      | topics         |                           |               |             |
+----------------------------------------------------------------------+----------------+---------------------------+---------------+-------------+

3. Source Code Development
--------------------------

Source code development for this reporting period was stifled due to the
project team being on vacation. No source code development to report.

3.1. GitLab Repository Links
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Repo:** https://git.psu.edu/psu-sweng/sweng-894/umaapy
- **Branches of Interest:** N/A

3.2. Summary of Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Devon:**

- Completed Algorithmic Component Algorithm Design Document
- Planned sprint backlog and performed tasking estimation on each user
  story
- Reviewed project requirements to determine what the final set of
  deliverable features will be for the UMAAPy SDK (No changes to product
  requirement set yet, only investigation)
- Completed Weekly Report 8

3.3. Important Merge Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There were no merge requests for this reporting period

3.4. Burndown Chart
~~~~~~~~~~~~~~~~~~~

.. figure:: ../../uploads/1f24a997425cc5cd4387775b2e7274d6/Sprint3-Week1.png
   :alt: Sprint3-Week1
   :width: 1037px
   :height: 571px

   Sprint3-Week1

4. Software Testing
-------------------

4.1. Acceptance Criteria Defined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No new acceptance criteria were defined for the requirements and use
cases for this reporting period.

4.2 Requirement Fulfillment Status
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

4.3. Test Case Specification (Incremental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Following the test-driven design approach, since no source code was
written this reporting period, there were no new test cases defined.

4.4. Traceability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~

No test cases to report in the traceability matrix for this reporting
period.

5. Backlog Grooming
-------------------

5.1. Changes to Product/Sprint Backlog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The product backlog was unaltered for this reporting period.

5.2. Rationale & Impact
~~~~~~~~~~~~~~~~~~~~~~~

No backlog grooming changes for this reporting period.

6. Metrics & Charts
-------------------

Metrics and charts unchanged since Week 8. See `Week 8
Report <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/wikis/home/Project-Planning/Sprint-2/Week-8-Report>`__.

8. Next Steps
-------------

For the upcoming week, the plan will be to define the test cases for
each sprint backlog item to begin developing source code against them in
a test-driven development approach. Lastly, as we approach the end of
the product development period, the main documentation, which serves as
the main developer guide, will need to be scoped, and I am thinking it
will likely replace one of the stretch requirements of the project.
