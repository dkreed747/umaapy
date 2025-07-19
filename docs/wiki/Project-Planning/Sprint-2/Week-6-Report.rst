Weekly Progress Report 6
========================

**Project Name:** UMAAPy

**Sprint #:** 2

**Reporting Period:** 06.16.25–06.22.25

**Date of Report:** 06.22.25

**Prepared By:** Devon Reed

1. High-Level Summary
---------------------

This week,aa the focus has been to plan the sprint backlog and queue up
the appropriate user stories that on completion will put the project in
a good MVP state to demo. Planning has been done on what a demo would
look like for the UMAAPy SDK and initial investigation into the
HoloOcean simulation environment has been completed to see if it could
be used as the first floor of a UMAA autonomy stack. Additionally, the
sprint backlog has been fully estimated and organized by order of how
they should be completed. Test cases for the first ticket to be
completed have also been defined. I have an active email out to the
professor regarding questions on the demo and Sprint 2 in general that
will also be used to guide the direction of this sprint and create a
clearer requirement for the demo.

2. Sprint Planning (Review & Adjustments)
-----------------------------------------

2.1. Sprint Goal & Scope
~~~~~~~~~~~~~~~~~~~~~~~~

- **Original Sprint Goal:** Heavy software development to MVP product.
- **Scope for this Week:** Software development and spike investigation
  for demo.

2.2. Definition of Done (DoD) Refinement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No changes made to the user stories added to the sprint backlog.

2.3. New/Changed Sprint Backlog Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------+---------------+---------------------------+----------------+-------------+
| GitLab      | Description   | Type                      | Rationale      | Impact      |
| Issue       |               | (Add/Remove/Reprioritize) |                |             |
+=============+===============+===========================+================+=============+
| #15         | QoS           | Add                       | Core component | High        |
|             | configuration |                           | for entire SDK |             |
|             | API           |                           |                |             |
|             | development   |                           |                |             |
+-------------+---------------+---------------------------+----------------+-------------+
| #5          | Report        | Add                       | Common element | Medium      |
|             | Consumer API  |                           | in UMAA used   |             |
|             | development   |                           | frequently     |             |
+-------------+---------------+---------------------------+----------------+-------------+
| #4          | Report        | Add                       | Common element | Medium      |
|             | Provider API  |                           | in UMAA used   |             |
|             | development   |                           | frequently     |             |
+-------------+---------------+---------------------------+----------------+-------------+
| #11         | Command       | Add                       | Important      | Medium      |
|             | Consumer API  |                           | element for    |             |
|             | development   |                           | MVP product    |             |
+-------------+---------------+---------------------------+----------------+-------------+
| #8          | Synchronous   | Add                       | Interface      | Low         |
|             | command API   |                           | simplification |             |
|             | support       |                           | increasing     |             |
|             |               |                           | usability      |             |
+-------------+---------------+---------------------------+----------------+-------------+
| #9          | Asynchronous  | Add                       | Interface      | Low         |
|             | command API   |                           | simplification |             |
|             | support       |                           |                |             |
+-------------+---------------+---------------------------+----------------+-------------+

3. Source Code Development
--------------------------

3.1. GitLab Repository Links
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Repo:** https://git.psu.edu/psu-sweng/sweng-894
- **Branches of Interest:**
  https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/tree/main?ref_type=heads

3.2. Summary of Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Devon:**

- Spike investigation of possible MVP/Demo options
- Sprint backlog creation
- Sprint estimation/time boxing
- Test case development
- Weekly report creation

3.3. Important Merge Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No merge requests created this reporting period.

3.4. Burndown Chart
~~~~~~~~~~~~~~~~~~~

Initial burn down chart.

.. figure:: ../../uploads/ed69d2c8265efbad7599ad04c78f878e/s2-bd.png
   :alt: s2-bd
   :width: 720px
   :height: 360px

   s2-bd

4. Software Testing
-------------------

4.1. Acceptance Criteria Defined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No new acceptance criteria defined for this reporting period.

4.2. Test Case Specification (Incremental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

3 new test cases were developed for the highest priority ticket of the
sprint. These tests will be unit/system tests and are defined as GitLab
subtasks.

4.3. Traceability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~

+---------+-------------+---------+---------+---------+---------+-----------+
| Use     | Functional  | Arch.   | MR      | Test ID | Test    | Comments  |
| Case    | Requirement | Element |         |         | Status  |           |
+=========+=============+=========+=========+=========+=========+===========+
| #15     | SR-10       | AR-01   | N/A     | #42     | NOT     | QoS       |
|         |             |         |         |         | TESTED  | profile   |
|         |             |         |         |         |         | loading   |
+---------+-------------+---------+---------+---------+---------+-----------+
| #15     | SR-10       | AR-01   | N/A     | #43     | NOT     | Specific  |
|         |             |         |         |         | TESTED  | profile   |
|         |             |         |         |         |         | selection |
+---------+-------------+---------+---------+---------+---------+-----------+
| #15     | SR-10       | AR-01   | N/A     | #44     | NOT     | Metadata  |
|         |             |         |         |         | TESTED  | retrieval |
+---------+-------------+---------+---------+---------+---------+-----------+

5. Backlog Grooming
-------------------

5.1. Changes to Product/Sprint Backlog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No new user stories have been added this reporting period. Depending on
feedback from current email out to professor. May need to add new
tickets for MVP.

5.2. Rationale & Impact
~~~~~~~~~~~~~~~~~~~~~~~

No backlog grooming changes for this reporting period.

6. Issues, Risks & Mitigations
------------------------------

6.1. New Issues / Blockers
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------+------------------+-----------+-----------+-----------+------------+
| Issue     | Description      | Severity  | Status    | Owner     | Mitigation |
|           |                  |           |           |           | Plan       |
+===========+==================+===========+===========+===========+============+
| Field     | Time blocker as  | Medium    | In        | @clr5436  | Work       |
| travel    | I am once again  |           | progress  |           | harder     |
|           | out in Keyport   |           |           |           |            |
|           | WA               |           |           |           |            |
|           | (6/22/25–7/3/25) |           |           |           |            |
+-----------+------------------+-----------+-----------+-----------+------------+

6.2. Potential Risks
~~~~~~~~~~~~~~~~~~~~

- Limited time available due to field travel for work

  - *Likelihood:* High
  - *Impact:* Less time for software development
  - *Mitigation:* Email professor early to inform them of the situation.

7. Metrics & Charts
-------------------

No updates to coverage or testing status since last reporting period.

8. Next Steps
-------------

Begin software development and testing on the highest priority tasks
this sprint. Wait for feedback from professor on software MVP and plan
backlog accordingly

9. Attachments & Links
----------------------

HoloOcean simulation backend for potential demo:
https://byu-holoocean.github.io/holoocean-docs/v2.0.0/index.html
