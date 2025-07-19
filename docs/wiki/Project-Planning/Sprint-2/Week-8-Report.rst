Weekly Progress Report
======================

**Project Name:** UMAAPy

**Sprint #:** 2

**Reporting Period:** 06.30.25–07.06.25

**Date of Report:** 07.06.25

**Prepared By:** Devon Reed

1. High-Level Summary
---------------------

In the final week of sprint 2, the main goal was to use the MVP release
of the SDK completed in sprint 2 iteration 2 to rapidly build a
UMAA-wrapped
`HoloOcean <https://frostlab.byu.edu/holoocean-underwater-simulator>`__
simulation environment. Work was tiresome due to also being on field
travel for work with 10-12 work days on a boat, which meant development
took place late at night in the hotel room. Despite this blocker, a
functioning simulation prototype built using the SDK has been delivered,
which showcases the user stories that have been completed thus far.
Lastly, some challenges were discovered in situ with containerizing
HoloOcean, which caused integration issues, a completely external
autonomy, like I had hoped to demonstrate. Instead, local manual
commands and listeners were used to interact with the SIM. See section
6.

2. Sprint Planning (Review & Adjustments)
-----------------------------------------

2.1. Sprint Goal & Scope
~~~~~~~~~~~~~~~~~~~~~~~~

- **Original Sprint Goal:** Source code development to SDK MVP and demo.
- **Scope for this Week:** Demo SDK implementation of a UMAA-wrapped
  maritime SIM.

2.2. Definition of Done (DoD) Refinement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No changes were made to the definition of done as of 06/30/25.

2.3. New/Changed Sprint Backlog Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No changes to Sprint Backlog as of 06/30/25.

3. Source Code Development
--------------------------

3.1. GitLab Repository Links
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Repos:**

  - `UMAAPy <https://git.psu.edu/psu-sweng/sweng-894/umaapy>`__
  - `UMAAPySIM <https://git.psu.edu/psu-sweng/sweng-894/umaapysim>`__
  - `HoloOcean
    Mirror <https://git.psu.edu/psu-sweng/sweng-894/holoocean>`__

  *Note the HoloOcean mirror repository is private to respect the Unreal
  Engine EULA*

- **Branches of Interest:**

  Since all branches have been closed this sprint, please see the
  repository graphs for each repository for branch status.

  - `UMAAPy Repository
    Graph <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/network/main?ref_type=heads>`__
  - `UMAAPySIM Repository
    Graph <https://git.psu.edu/psu-sweng/sweng-894/umaapysim/-/network/main?ref_type=heads>`__

3.2. Summary of Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Devon:**

- Massive source code development
- Completed MVP SDK
- Completed MVP SDK demo
- Completed Weekly 8 Report, Sprint 2 Retrospective, and product MVP
  demonstration.

3.3. Important Merge Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------------------------------------------------------------------+----------------------+-----------------------------------------------------------------------+-----------------+
| Merge Request                                                                 | Description          | Issue Ref                                                             | Notes           |
+===============================================================================+======================+=======================================================================+=================+
| `!10 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/merge_requests/10>`__  | Final touches to     | `#45 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/45>`__  | Integration     |
|                                                                               | UMAAPy SDK found     |                                                                       | related bug     |
|                                                                               | while                |                                                                       | fixes and       |
|                                                                               | integrating/building |                                                                       | improvements    |
|                                                                               | demo app.            |                                                                       |                 |
+-------------------------------------------------------------------------------+----------------------+-----------------------------------------------------------------------+-----------------+
| `!1 <https://git.psu.edu/psu-sweng/sweng-894/umaapysim/-/merge_requests/1>`__ | MVP Demo Development | `#1 <https://git.psu.edu/psu-sweng/sweng-894/umaapysim/-/issues/1>`__ | Complete UMAA   |
|                                                                               |                      |                                                                       | wrapped         |
|                                                                               |                      |                                                                       | simulation      |
|                                                                               |                      |                                                                       | environment for |
|                                                                               |                      |                                                                       | underwater      |
|                                                                               |                      |                                                                       | robots          |
+-------------------------------------------------------------------------------+----------------------+-----------------------------------------------------------------------+-----------------+

3.4. Burndown Chart
~~~~~~~~~~~~~~~~~~~

Sprint 2 burndown remained stable over the last week as the final
tasking of setting up the MVP demo was being completed.

.. figure:: ../../uploads/728f0066916552bd09e82ebdecf84b7b/Sprint2-Week3.png
   :alt: Sprint2-Week3
   :width: 1037px
   :height: 571px

   Sprint2-Week3

4. Software Testing
-------------------

4.1. Acceptance Criteria Defined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No new acceptance criteria were defined as of 06/30/25.

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

Three new test cases were created to measure the quality and enforce the
completion criteria for the SDK demo. The associated use case is
`#45 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/issues/45>`__.

- `Test 52 UMAAPySIM accepts
  commands <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/52>`__
- `Test 53 UMAAPySIM communicates over
  DDS <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/53>`__
- `Test 54 umaapy sim publishes reports
  externally <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/work_items/54>`__

4.4. Traceability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~

+---------+-------------+-------------------+---------+---------+---------+-------------+
| Use     | Functional  | Arch. Element     | MR      | Test ID | Test    | Comments    |
| Case    | Requirement |                   |         |         | Status  |             |
+=========+=============+===================+=========+=========+=========+=============+
| #15     | SR-10       | Class             | !7      | #42     | PASS    | QoS profile |
|         |             |                   |         |         |         | loading     |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #15     | SR-10       | Class             | !7      | #43     | PASS    | Specific    |
|         |             |                   |         |         |         | profile     |
|         |             |                   |         |         |         | selection   |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #15     | SR-10       | Class             | !7      | #44     | PASS    | UMAA Topic  |
|         |             |                   |         |         |         | loading     |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #4      | SR-04,      | Class             | !8      | #46     | PASS    | Proper UMAA |
|         | SR-06       |                   |         |         |         | metadata    |
|         |             |                   |         |         |         | setting     |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #4      | SR-04,      | Class             | !8      | #47     | PASS    | Test report |
|         | SR-06       |                   |         |         |         | publication |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #4      | SR-04,      | Class             | !8      | #48     | PASS    | Test writer |
|         | SR-06       |                   |         |         |         | callbacks   |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #12     | SR-06,      | Class             | !9      | #49     | PASS    | Test UMAA   |
|         | SR-07       |                   |         |         |         | command     |
|         |             |                   |         |         |         | flow        |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #12     | SR-06,      | Class             | !9      | #50     | PASS    | Test        |
|         | SR-07       |                   |         |         |         | destination |
|         |             |                   |         |         |         | content     |
|         |             |                   |         |         |         | filtering   |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #12     | SR-06,      | Class             | !9      | #51     | PASS    | Test new    |
|         | SR-07       |                   |         |         |         | commands    |
|         |             |                   |         |         |         | executed in |
|         |             |                   |         |         |         | thread pool |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #45     | N/A         | Repository/Python | !10, !1 | #52     | PARTIAL | Test        |
|         |             | Project           |         |         | PASS    | completed   |
|         |             |                   |         |         |         | locally but |
|         |             |                   |         |         |         | due to      |
|         |             |                   |         |         |         | docker      |
|         |             |                   |         |         |         | issues with |
|         |             |                   |         |         |         | HoloOcean   |
|         |             |                   |         |         |         | was unable  |
|         |             |                   |         |         |         | to test on  |
|         |             |                   |         |         |         | a different |
|         |             |                   |         |         |         | system      |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #45     | N/A         | Repository/Python | !10, !1 | #53     | PASS    | UAT         |
|         |             | Project           |         |         |         | completed   |
|         |             |                   |         |         |         | using RTI   |
|         |             |                   |         |         |         | Admin       |
|         |             |                   |         |         |         | Console     |
+---------+-------------+-------------------+---------+---------+---------+-------------+
| #45     | N/A         | Repository/Python | !10, !1 | #54     | PASS    | UAT         |
|         |             | Project           |         |         |         | completed   |
|         |             |                   |         |         |         | using RTI   |
|         |             |                   |         |         |         | Admin       |
|         |             |                   |         |         |         | Console     |
+---------+-------------+-------------------+---------+---------+---------+-------------+

5. Backlog Grooming
-------------------

5.1. Changes to Product/Sprint Backlog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No backlog grooming changes for this reporting period.

5.2. Rationale & Impact
~~~~~~~~~~~~~~~~~~~~~~~

No backlog grooming changes for this reporting period.

6. Issues, Risks & Mitigations
------------------------------

6.1. New Issues / Blockers
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------+-------------+-----------+---------------+-----------+------------+
| Issue     | Description | Severity  | Status        | Owner     | Mitigation |
|           |             |           |               |           | Plan       |
+===========+=============+===========+===============+===========+============+
| Vulkan x  | My          | High      | Triaging and  | @clr5436  | Test with  |
| laptop    | development |           | Investigating |           | bare metal |
| drivers   | laptop has  |           | Solutions     |           | packages   |
|           | an Nvidia   |           |               |           | until      |
|           | RTX 5080 in |           |               |           | docker     |
|           | it which    |           |               |           | image is   |
|           | uses the    |           |               |           | created    |
|           | Vulkan API  |           |               |           | that works |
|           | version     |           |               |           | regardless |
|           | 1.4.x       |           |               |           | of driver  |
|           | however     |           |               |           | and        |
|           | there is no |           |               |           | hardware   |
|           | linux       |           |               |           |            |
|           | Nvidia      |           |               |           |            |
|           | driver      |           |               |           |            |
|           | ready, yet  |           |               |           |            |
|           | that has    |           |               |           |            |
|           | the same    |           |               |           |            |
|           | version.    |           |               |           |            |
+-----------+-------------+-----------+---------------+-----------+------------+

6.2. Potential Risks
~~~~~~~~~~~~~~~~~~~~

- Targeting development dependencies causes instability (Related to
  Vulkan driver issue).

  - *Likelihood:* Medium
  - *Impact:* High
  - *Mitigation:* Use frozen or mirrored branches/tags to prevent
    dependencies from disappearing if they get updated or moved.

7. Metrics & Charts
-------------------

7.1 Release `v1.0.0 <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/releases/v1.0.0>`__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. figure:: ../../uploads/959af6a930f772365311995ce3644f8e/image.png
   :alt: image
   :width: 1656px
   :height: 881px

   image

At the end of sprint 2 the first official release of UMAAPy is ready and
is utilized by the UMAAPySIM project.

See the release link for the collected OQE, source code, and output
docker images and python packages.

7.2 Pytest
~~~~~~~~~~

.. figure:: ../../uploads/999e698bb68608dd01a2723a4c662e42/image.png
   :alt: image
   :width: 2116px
   :height: 691px

   image

At the end of sprint 2 there are 27 unit tests executing and passing in
the CI/CD pipeline. See the `test results
page <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/pipelines/415176/test_report?job_name=test>`__
for the release pipeline for version v1.0.0

7.3 Code Coverage
~~~~~~~~~~~~~~~~~

The code coverage as of the release of v1.0.0 is 97% on UMAAPy source
code only. The source code in the UMAAPySIM repository is not covered
here. See the table below for a more detailed snapshot.

======================= ======
Metric                  Value
======================= ======
**Total lines**         8,485
**Covered lines**       8,248
**Line coverage (%)**   97.21%
**Total branches**      0
**Covered branches**    0
**Branch coverage (%)** 0.00%
**Complexity**          0
======================= ======

*The data in this table is sourced from the*
```coverage.xml`` <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/jobs/1388427/artifacts/file/coverage.xml>`__
*file from the latest release pipeline*

8. Next Steps
-------------

1. Continue investigating the containerization issue discovered this
   week.
2. Add additional functionality to the Python SDK in line with the SDK
   requirements and backlog.

9. Attachments & Links
----------------------

9.1 Sprint 2 Retrospective
~~~~~~~~~~~~~~~~~~~~~~~~~~

https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/wikis/home/Project-Planning/Sprint-2/Retrospective

9.2 MVP Demo Video
~~~~~~~~~~~~~~~~~~

TODO
