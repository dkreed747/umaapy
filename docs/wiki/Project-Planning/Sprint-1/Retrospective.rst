Sprint Retrospective: Sprint 1
==============================

**Date:** 06/15/25

**Facilitator:** Devon Reed

**Participants:** Devon Reed

**Timebox:** 15m

1. Sprint Overview
------------------

**Sprint Goal:**

- Setup project repo and lay foundation
- Setup GitLab CI/CD runner
- Acquire dependencies such as educational licenses
- Setup project development environment
- Setup initial CI/CD pipeline
- Begin work on core components of SDK

**Scope / Deliverables:**

- Complete CI/CD pipeline
- All planned issues completed by end of sprint
- All test cases completed by end of sprint

2. Technical Difficulties & Challenges
--------------------------------------

**Technical Issues Encountered**

- University GitLab instance is the free version only and is lacking
  some of the nice features for sprint/agile planning which would have
  made several aspects of completing the course requirements less
  time-consuming.
- RTI trial license is only good for the latest bleeding-edge release of
  Connext DDS. However, I spoke with some people from RTI and was able
  to obtain a full license for the year under academic usage.

**Process / Collaboration Blockers**

- Work related field travel at the beginning of the course made project
  planning and collaboration nearly impossible.
- Meeting assignment expectations and requirements turned out to be
  harder than actually coding. The slow feedback loop with each
  assignment meant that it took a few weeks to really understand project
  expectations and get into a good place reporting wise.

3. What Went Well ✅
--------------------

- Complete CI/CD automation achieved in first sprint! Code linting,
  building, testing, packaging, and docker image creation all automated
  and reproducible.
- Working with RTI for a license was very easy, and was even able to
  network a bit with some of their developers.
- Containerizing the development environment made early development and
  testing easier.
- GitLab is very nice - only wish we had the ultimate tier. As a solo
  project, allowing the tools to automatically handle agile overhead is
  very important to maintaining adequate progress.
- Working with the professor, making templates for weekly reports,
  retrospectives, test cases, and issues meant producing high quality
  work became way easier using a feature built right into GitLab.

4. What Didn’t Go So Well ❌
----------------------------

- Project conception and first few weeks of sprint were rough due to a
  majority of time being out of state on work travel.
- Meeting course assignment expectations was hard, and most of my early
  time was focused on getting better grades on the course assignments
  than actually working on moving the project forward.

5. What Can Be Done Better 💡
-----------------------------

- More frequent communication with the professor when any part of the
  assignment is unclear (Get clarification fast and early)

6. Improvements & Action Items for Next Sprint
----------------------------------------------

+--------------+--------------+--------------+------------------------+
| Action Item  | Owner        | Target       | Status                 |
|              |              | Sprint/Date  |                        |
+==============+==============+==============+========================+
| Review all   | @clr5436     | Sprint 2     | TODO                   |
| GitLab       |              |              |                        |
| templates    |              |              |                        |
| for use      |              |              |                        |
| cases, test  |              |              |                        |
| cases,       |              |              |                        |
| weekly       |              |              |                        |
| reports with |              |              |                        |
| professor    |              |              |                        |
| for quality  |              |              |                        |
| going        |              |              |                        |
| forward.     |              |              |                        |
+--------------+--------------+--------------+------------------------+

7. Additional Notes / Parking Lot
---------------------------------

- Spin-up on the project was a bit painful, but I am optimistic with a
  lot of the spin up confusion and pain out of the way, the next sprints
  will be way more productive and stress-free.
- In the long term, the early investment into processes like CI/CD and
  reporting templates for the course assignments will mean way more time
  spent coding and less time messing around with agile overhead. This
  will be a major key to success for a solo developer on this project.
