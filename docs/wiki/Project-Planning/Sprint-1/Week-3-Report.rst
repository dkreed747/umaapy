Weekly Progress Report 3
========================

**Project Name:** UMAAPy

**Sprint #:** 1

**Reporting Period:** 05.26.25–06.01.25

**Date of Report:** 06.01.25

**Prepared By:** Devon Reed

1. High-Level Summary
---------------------

This week the focus for the project has been to set up most of the
project infrastructure such as a dedicated GitLab runner for CI/CD, a
working development environment, and retrieving educational licenses for
project dependencies. Progress has been steady and at the end of week 1
the GitLab runner is up and the bones of the development container
including the RTI dependencies have been built and verified with hello
world manual testing. Next steps will be onto CI/CD and project
structuring for linting (Dockerfile and Python), testing (Pytest),
building (pypi and docker), and deployment (Docker hub and GitLab
Package registry). With that, we will be in a great place to start work
on burning down the first foundational tickets for the SDK and cross off
use cases/ requirements.

2. Sprint Planning (Review & Adjustments)
-----------------------------------------

2.1. Sprint Goal & Scope
~~~~~~~~~~~~~~~~~~~~~~~~

- **Original Sprint Goal:** Project setup, Infrastructure procurement,
  development environment construction, CI/CD, early code development
- **Scope for this Week:** Set up the GitLab runner and build a
  development container with dependencies included.

2.2. Definition of Done (DoD) Refinement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No changes were made to the definition of done sections this week for
the active sprint.

2.3. New/Changed Sprint Backlog Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------+-------------+---------------------------+--------------+-------------+
| GitLab      | Description | Type                      | Rationale    | Impact      |
| Issue       |             | (Add/Remove/Reprioritize) |              |             |
+=============+=============+===========================+==============+=============+
| #22         | Setup       | Added                     | Decomposed   | Low         |
|             | docker      |                           | into more    |             |
|             | development |                           | well-defined |             |
|             | environment |                           | issue        |             |
|             |             |                           | discovered   |             |
|             |             |                           | while        |             |
|             |             |                           | grooming     |             |
|             |             |                           | backlog      |             |
+-------------+-------------+---------------------------+--------------+-------------+
| #21         | Initialize  | Added                     | Decomposed   | Low         |
|             | self-hosted |                           | into more    |             |
|             | GitLab      |                           | well-defined |             |
|             | runner on   |                           | issue        |             |
|             | home server |                           | discovered   |             |
|             |             |                           | when         |             |
|             |             |                           | grooming     |             |
|             |             |                           | backlog      |             |
+-------------+-------------+---------------------------+--------------+-------------+

3. Source Code Development
--------------------------

No source code development has taken place this week for the SDK.
Infrastructure as Code (IaC) is the main focus of this sprint.

3.1. GitLab Repository Links
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Repo:** https://git.psu.edu/psu-sweng/sweng-894/umaapy

3.2. Summary of Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Devon Reed:** - Groomed sprint backlog decomposing issues from high
level use cases where deemed necessary - Setup dedicated docker executor
within GitLab runner on home server and linked to university GitLab -
Acquired educational licenses for RTI Connext dependency - Staged
project files defining development environment and verified RTI Connext
and packages installed correctly - Created weekly report template based
on instructors’ guidance - Prepared weekly report

3.3. Important Merge Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------+----------------+-------------+----------------+---------------+
| Merge       | Description    | Issue Ref   | Associated     | Notes         |
| Request     |                |             | requirement(s) |               |
+=============+================+=============+================+===============+
| !1          | Create dev     | #22         | SR-15, AR-05,  | Development   |
|             | container      |             |                | environment   |
|             | infrastructure |             |                | foundation    |
|             |                |             |                | completed,    |
|             |                |             |                | tested by     |
|             |                |             |                | running some  |
|             |                |             |                | of RTI’s      |
|             |                |             |                | sample        |
|             |                |             |                | applications  |
|             |                |             |                | from within   |
|             |                |             |                | the           |
|             |                |             |                | containerized |
|             |                |             |                | environment   |
+-------------+----------------+-------------+----------------+---------------+

3.4. Burndown Chart
~~~~~~~~~~~~~~~~~~~

Without the enterprise version of GitLab, there is no automatic burndown
chart generation like I am used to or formal sprint definition. Instead,
I worked around this by defining each sprint as a milestone. As of
today, sprint 1 is 33% complete by issues completed. See milestone page
for details on estimation and actual time spent:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/milestones/1#tab-issues

**Issue Board after week 3:**

.. figure:: ../../uploads/f2efc0abf0c973e5cca60cdb27ac9c34/image.png
   :alt: image
   :width: 940px
   :height: 265px

   image

4. Software Testing
-------------------

4.1. Acceptance Criteria Defined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No user stories had their acceptance criteria updated/changed this week.

4.2. Test Case Specification (Incremental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Unit Tests:**

  - No Change

- **System Tests:**

  - No Change

- **User Acceptance Tests:**

  - **UAT-01:** GitLab runner online and responds to pings from the
    university GitLab and accepts jobs under the runner tag “dkr-run”.
  - **UAT-02:** RTI Connext DDS containerized with Python API was able
    to execute hello world examples and run the code generator.

4.3. Traceability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~

+---------+-------------+--------------------+----------------+------------+-----------+-------------+
| Use     | Functional  | Arch. Element      | MR             | Test ID    | Test      | Comments    |
| Case    | Requirement |                    |                |            | Status    |             |
+=========+=============+====================+================+============+===========+=============+
| #22     | **SR-15**   | Repository/IaC     | !1             | **UAT-01** | PARITAL   | Manually    |
|         |             |                    |                |            | PASS      | confirmed   |
|         |             |                    |                |            |           | to be       |
|         |             |                    |                |            |           | automated   |
|         |             |                    |                |            |           | in #20      |
+---------+-------------+--------------------+----------------+------------+-----------+-------------+
| #21     | **SR-15**   | Infrastructure/IaC | N/A            | **UAT-02** | PASS      | Direct      |
|         |             |                    |                |            |           | requirement |
|         |             |                    |                |            |           | of #20      |
+---------+-------------+--------------------+----------------+------------+-----------+-------------+
| #20     | **SR-15**   | IaC                | .gitlab-ci.yml | In         | Undefined | NOT TESTED  |
|         |             |                    |                | progress   |           |             |
+---------+-------------+--------------------+----------------+------------+-----------+-------------+

5. Backlog Grooming
-------------------

5.1. Changes to Product Backlog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No new issues have been added to the product backlog, only the active
sprint after holding a sprint grooming session.

5.2. Rationale & Impact
~~~~~~~~~~~~~~~~~~~~~~~

No backlog grooming changes for this reporting period.

6. Issues, Risks & Mitigations
------------------------------

6.1. New Issues / Blockers
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------+-------------+-------------+-------------+-------------+
| Description | Severity    | Status      | Owner       | Mitigation  |
|             |             |             |             | Plan        |
+=============+=============+=============+=============+=============+
| Educational | Low         | Unresolved  | Devon Reed  | Connext     |
| Connext     |             |             |             | 7.5.0 is    |
| license     |             |             |             | the latest, |
| only works  |             |             |             | and it      |
| for the     |             |             |             | looks like  |
| latest and  |             |             |             | it should   |
| not LTS     |             |             |             | work just   |
| version     |             |             |             | fine for    |
| 7.3.0       |             |             |             | this        |
|             |             |             |             | project,    |
|             |             |             |             | and no      |
|             |             |             |             | action      |
|             |             |             |             | should be   |
|             |             |             |             | needed.     |
+-------------+-------------+-------------+-------------+-------------+

6.2. Potential Risks
~~~~~~~~~~~~~~~~~~~~

- Using the cutting-edge development branch of RTI Connext could cause
  unexpected behavior.

  - *Likelihood:* Low
  - *Impact:* Could slow down SDK development
  - *Mitigation:* Consult RTI upgrade guide to identify any
    compatibility issues early

7. Metrics & Charts
-------------------

No code coverage reports, test results, or key deployments to show yet

8. Next Steps
-------------

1. Complete repository setup and begin to pull in CI/CD elements for
   testing and construct CI/CD pipeline
2. Begin source code development and track test cases using GitLabs
   internal value stream analytics

9. Attachments & Links
----------------------

- `Gitlab Report
  Location <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/wikis/home/Project-Planning/Sprint-1>`__

**Notes:**

Weekly reports will be constructed and tracked in GitLab then exported
to PDF for assignment submission
