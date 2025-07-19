Weekly Progress Report 5
========================

**Project Name:** UMAAPy

**Sprint #:** 1

**Reporting Period:** 06.09.25–06.15.25

**Date of Report:** 06.15.25

**Prepared By:** Devon Reed

1. High-Level Summary
---------------------

In the last iteration of sprint 1, the focus has been to get the final
touches on the CICD pipeline fully completed, which included python
package publishing and docker hub registry usage. Both of these tasks
have been completed as planned for sprint 1. Lastly, source code
development has finally begun with the implementation of the first big
algorithmic piece of the SDK, the ``EventProcessor``. This thread pool
implementation will serve as the beating heart of the SDK, ensuring
maximum throughput and low latency.

2. Sprint Planning (Review & Adjustments)
-----------------------------------------

2.1. Sprint Goal & Scope
~~~~~~~~~~~~~~~~~~~~~~~~

- **Original Sprint Goal:** Infrastructure setup and source code
  development.
- **Scope for this Week:** Finalize infrastructure and begin source code
  development.

2.2. Definition of Done (DoD) Refinement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No issues had their definition of done refined this week.

2.3. New/Changed Sprint Backlog Items
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No changes to Sprint Backlog 06/09/25.

3. Source Code Development
--------------------------

3.1. GitLab Repository Links
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Repo:** https://git.psu.edu/psu-sweng/sweng-894/umaapy
- **Branches of Interest:** All development branches merged for end of
  sprint.

End of sprint product release:

https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/releases/v0.1.0

Sprint 1 Milestone:

https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/milestones/1#tab-issues

3.2. Summary of Contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Devon:**

- Completed the infrastructure setup.
- Completed core source code/algorithm development on key SDK component.
- Prepared project report.
- Prepared project retrospective.
- Closed all active MRs for this sprint.

3.3. Important Merge Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------------+-----------------+-----------------+-----------------+
| Merge Request   | Description     | Issue Ref       | Notes           |
+=================+=================+=================+=================+
| !1              | Development     | #22             | Completed early |
|                 | Environment     |                 | in sprint       |
|                 | Setup           |                 |                 |
+-----------------+-----------------+-----------------+-----------------+
| !2              | Integrate UMAA  | #18             | Merged from     |
|                 | IDL type        |                 | last week along |
|                 | autogeneration  |                 | with pipeline   |
|                 | into the        |                 | setup           |
|                 | pipeline        |                 |                 |
+-----------------+-----------------+-----------------+-----------------+
| !3              | Testing         | #18             | Merged as part  |
|                 | packaging       |                 | of test case    |
|                 | pipeline per    |                 | for #18         |
|                 | test case #36   |                 |                 |
+-----------------+-----------------+-----------------+-----------------+
| !4              | Implement       | #17             | Completed       |
|                 | EventProcessor  |                 | source code and |
|                 | algorithm and   |                 | unit testing    |
|                 | class           |                 | for class       |
+-----------------+-----------------+-----------------+-----------------+

3.4. Burndown Chart
~~~~~~~~~~~~~~~~~~~

All open issues were resolved by the end of the sprint, with the
majority of tasks sitting in merge request just needing to be tied in
together with the main pipeline.

.. figure:: ../../uploads/68dc4247494fa423f4fc0709400805ed/Sprint1Burndown.png
   :alt: Sprint1Burndown
   :width: 720px
   :height: 360px

   Sprint1Burndown

Completed milestone:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/milestones/1#tab-issues
Completed issue board:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/boards?milestone_title=Sprint%201

4. Software Testing
-------------------

4.1. Acceptance Criteria Defined
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No new acceptance criteria was added this sprint iteration.

4.2. Test Case Specification (Incremental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

6 test cases were added this week to supplement the final 2 issues that
were still open on this sprint. See the full list below:

Test cases for #17:

- #38
- #39
- #40

Test cases for #18:

- #35
- #36
- #37

4.3. Traceability Matrix
~~~~~~~~~~~~~~~~~~~~~~~~

Below is the final traceability matrix for the entire sprint. All test
cases were successfully executed and tracked as Gitlab tasks. The
majority of the early sprint test cases were user acceptance tests or
system tests, since the main priority was focused on setting up project
infrastructure. In the last iteration of the sprint with automated unit
testing and code coverage online, the first key component of the SDK was
built and tested using pytest.

+---------+-------------+---------+---------+---------+---------+----------------+
| Use     | Functional  | Arch.   | MR      | Test ID | Test    | Comments       |
| Case    | Requirement | Element |         |         | Status  |                |
+=========+=============+=========+=========+=========+=========+================+
| #1      | SR-01       | AR-01   | !2      | #32     | PASS    | RTI/UMAA types |
|         |             |         |         |         |         | generated      |
|         |             |         |         |         |         | succesfully    |
|         |             |         |         |         |         | from clean     |
|         |             |         |         |         |         | installation.  |
+---------+-------------+---------+---------+---------+---------+----------------+
| #1      | SR-01       | AR-01   | !2      | #33     | PASS    | Python package |
|         |             |         |         |         |         | deployed       |
|         |             |         |         |         |         | automatically. |
+---------+-------------+---------+---------+---------+---------+----------------+
| #1      | SR-01       | AR-01   | !2      | #34     | PASS    | Docker image   |
|         |             |         |         |         |         | deployed to    |
|         |             |         |         |         |         | docker hub     |
+---------+-------------+---------+---------+---------+---------+----------------+
| #17     | SR-12       | AR-02   | !4      | #38     | PASS    | Unit test pass |
+---------+-------------+---------+---------+---------+---------+----------------+
| #17     | SR-12       | AR-02   | !4      | #39     | PASS    | Unit test pass |
+---------+-------------+---------+---------+---------+---------+----------------+
| #17     | SR-12       | AR-02   | !4      | #40     | PASS    | Unit test pass |
+---------+-------------+---------+---------+---------+---------+----------------+
| #18     | SR-15       | AR-05   | !2      | #35     | PASS    | Local pip      |
|         |             |         |         |         |         | install UAT    |
|         |             |         |         |         |         | complete       |
+---------+-------------+---------+---------+---------+---------+----------------+
| #18     | SR-15       | AR-05   | !2      | #36     | PASS    | Confirmed      |
|         |             |         |         |         |         | python package |
|         |             |         |         |         |         | builds in      |
|         |             |         |         |         |         | pipeline       |
+---------+-------------+---------+---------+---------+---------+----------------+
| #18     | SR-15       | AR-05   | !2      | #37     | PASS    | Images         |
|         |             |         |         |         |         | confirmed in   |
|         |             |         |         |         |         | deployment to  |
|         |             |         |         |         |         | container      |
|         |             |         |         |         |         | registry       |
+---------+-------------+---------+---------+---------+---------+----------------+
| #20     | SR-13,      | AR-05   | !2      | #29     | PASS    | Final pipeline |
|         | SR-14,      |         |         |         |         | implementation |
|         | SR-15       |         |         |         |         | complete       |
+---------+-------------+---------+---------+---------+---------+----------------+
| #20     | SR-13,      | AR-05   | !2      | #30     | PASS    | Gitlab         |
|         | SR-14,      |         |         |         |         | secretes       |
|         | SR-15       |         |         |         |         | masking        |
|         |             |         |         |         |         | correctly for  |
|         |             |         |         |         |         | sensitive      |
|         |             |         |         |         |         | information.   |
+---------+-------------+---------+---------+---------+---------+----------------+
| #20     | SR-13,      | AR-05   | !2      | #31     | PASS    | Tag pipeline   |
|         | SR-14,      |         |         |         |         | setup for      |
|         | SR-15       |         |         |         |         | automatically  |
|         |             |         |         |         |         | handling code  |
|         |             |         |         |         |         | releases.      |
+---------+-------------+---------+---------+---------+---------+----------------+
| #21     | SR-15       | AR-05   | !1      | #23     | PASS    | UAT            |
|         |             |         |         |         |         | verificaiton   |
|         |             |         |         |         |         | of             |
|         |             |         |         |         |         | devcontainer   |
|         |             |         |         |         |         | files          |
+---------+-------------+---------+---------+---------+---------+----------------+
| #21     | SR-15       | AR-05   | !1      | #24     | PASS    | UAT            |
|         |             |         |         |         |         | verification   |
|         |             |         |         |         |         | that VSCode    |
|         |             |         |         |         |         | works with     |
|         |             |         |         |         |         | building the   |
|         |             |         |         |         |         | container      |
+---------+-------------+---------+---------+---------+---------+----------------+
| #21     | SR-15       | AR-05   | !1      | #25     | PASS    | UAT manual     |
|         |             |         |         |         |         | verification   |
|         |             |         |         |         |         | that all       |
|         |             |         |         |         |         | extensions and |
|         |             |         |         |         |         | packages are   |
|         |             |         |         |         |         | installed      |
|         |             |         |         |         |         | correctly in   |
|         |             |         |         |         |         | the resulting  |
|         |             |         |         |         |         | development    |
|         |             |         |         |         |         | container      |
+---------+-------------+---------+---------+---------+---------+----------------+
| #22     | SR-15       | N/A     | !1      | #26     | PASS    | UAT manual     |
|         |             |         |         |         |         | verification   |
|         |             |         |         |         |         | that gitlab    |
|         |             |         |         |         |         | runner docker  |
|         |             |         |         |         |         | service is     |
|         |             |         |         |         |         | running on     |
|         |             |         |         |         |         | self-hosted    |
|         |             |         |         |         |         | server         |
+---------+-------------+---------+---------+---------+---------+----------------+
| #22     | SR-15       | N/A     | !1      | #27     | PASS    | UAT manually   |
|         |             |         |         |         |         | check          |
|         |             |         |         |         |         | connection     |
|         |             |         |         |         |         | between server |
|         |             |         |         |         |         | and university |
|         |             |         |         |         |         | Gitlab         |
+---------+-------------+---------+---------+---------+---------+----------------+
| #22     | SR-15       | N/A     | !1      | #28     | PASS    | Successful     |
|         |             |         |         |         |         | pipeline run   |
|         |             |         |         |         |         | with dummy     |
|         |             |         |         |         |         | pipeline       |
+---------+-------------+---------+---------+---------+---------+----------------+

5. Backlog Grooming
-------------------

5.1. Changes to Product/Sprint Backlog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No changes to sprint backlog this iteration.

5.2. Rationale & Impact
~~~~~~~~~~~~~~~~~~~~~~~

No backlog grooming changes for this reporting period.

6. Issues, Risks & Mitigations
------------------------------

6.1. New Issues / Blockers
~~~~~~~~~~~~~~~~~~~~~~~~~~

+-----------+-------------+-----------+-----------+-----------+------------+
| Issue     | Description | Severity  | Status    | Owner     | Mitigation |
|           |             |           |           |           | Plan       |
+===========+=============+===========+===========+===========+============+
| #41       | Pipeline    | Medium    | Resolved  | @clr5436  | Resolved   |
|           | hotfix      |           |           |           | in branch  |
|           |             |           |           |           | and merged |
|           |             |           |           |           | into main  |
|           |             |           |           |           | - see !5   |
+-----------+-------------+-----------+-----------+-----------+------------+
| #41       | Add code    | Low       | Resolved  | @clr5436  | Resolved   |
|           | coverage to |           |           |           | in branch  |
|           | pipeline    |           |           |           | and merged |
|           |             |           |           |           | into main  |
|           |             |           |           |           | - see !6   |
+-----------+-------------+-----------+-----------+-----------+------------+

6.2. Potential Risks
~~~~~~~~~~~~~~~~~~~~

No new risks identified this iteration.

7. Metrics & Charts
-------------------

All testing and metric gathering has been automated and compiled in the
GitLab project CI/CD pipeline. See the latest tagged pipeline for the
end of sprint
```v0.1.0`` <https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/tags/v0.1.0>`__
release for the full execution results and artifacts. Several key
artifacts are provided below for convenience.

7.1 Pytest results
~~~~~~~~~~~~~~~~~~

In gitlab:
https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/pipelines/412794/test_report?job_name=test

`XML Report
Summary <uploads/930a86cf73beacad6c6fc3bf9190e6b2/report.xml>`__

7.2 Code Coverage results
~~~~~~~~~~~~~~~~~~~~~~~~~

`XML Coverage
Summary <uploads/241a9e5160c6436a722637cc97690e75/coverage.xml>`__

8. Next Steps
-------------

The next steps for the project will be to lay out the next sprint and
make a plan to complete the MVP requirements by the end of sprint 2. The
large focus of sprint 2 will be to take full advantage of the
infrastructure built in sprint 1 to make rapid progress prototyping out
the full SDK.

9. Sprint Retrospective
-----------------------

https://git.psu.edu/psu-sweng/sweng-894/umaapy/-/wikis/home/Project-Planning/Sprint-1/Retrospective
