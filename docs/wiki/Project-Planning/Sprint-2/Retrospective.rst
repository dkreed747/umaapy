Sprint Retrospective: Sprint 2
==============================

| **Date:** 7/5/25 **Facilitator:** Devon Reed
| **Participants:** Devon Reed **Timebox:** 30m

1. Sprint Overview
------------------

- **Sprint Goal:**

The main goal of sprint 2 was to capitalize on the CI/CD and
foundational work done in sprint 1 to make very rapid software
development progress towards an MVP version of the UMAAPy SDK.

- **Scope / Deliverables:**

.. figure:: ../../uploads/761cb1f01edacf99b49de0c5516237bd/image.png
   :alt: image
   :width: 2090px
   :height: 996px

   image

- 3 Use Cases were planned and completed on time.
- 1 Integration effort task for both testing the SDK and building a
  viable demo completed on time.
- 10/15 functional requirements, either satisfied or partially
  satisfied.

2. Technical Difficulties & Challenges
--------------------------------------

- **Technical Issues Encountered**

  - Newer development laptop hardware made it impossible to containerize
    UMAAPySIM by the end of the sprint. Nvidia driver for RTX 5080 uses
    Vulkan 1.4.\* but all current available runtime images use 1.3.\*
    meaning the simulation engine can’t start due to the
    incompatibility.

- **Process / Collaboration Blockers**

  - Field travel for 80% of the sprint duration meant that time spent on
    the project was low, and my feelings of burnout were very high due
    to consistent early mornings and late nights to meet work and school
    responsibilities.

3. What Went Well ✅
--------------------

This sprint was a standout success on several fronts, starting with my
disciplined delivery: I completed every item in the sprint backlog on
time, with zero carry-over into next week’s planning. Early in the
project, I invested heavily in designing and implementing a robust CI/CD
pipeline and supporting infrastructure. That investment paid dividends
this sprint—I could simply push code, open a merge request, and watch
automated tests run in parallel, immediately surfacing any regressions.
Upon merge, my builds were automatically packaged and containerized, so
spinning up fresh test environments became a one-click affair. This
streamlined flow not only eliminated much of the manual toil but also
freed me to focus on core UMAA Python SDK features instead of wrestling
with environment setup or deployment headaches.

Mid-sprint, I demonstrated true agility by pivoting in real time to
accommodate updated requirements from my professor for the end-of-sprint
MVP demo. Rather than treating scope changes as a blocker, I used daily
progress checkpoints and rapid backlog refinements to reshuffle
priorities, swap in the new demo functionality, and de-prioritize less
critical tasks—all without jeopardizing any of my existing commitments.
This quick reprioritization showcased my ability to adapt on the fly and
reinforced my confidence in my process.

Finally, my containerization strategy continued to pay off. Throughout
the sprint, spinning up development or test environments via Docker was
smooth and predictable—no more “it works on my machine” surprises. The
only hiccup arose when I attempted to bundle Unreal Engine into a
container and forward its GUI to an external host, which proved more
complex than anticipated. Although that was the lone snag, it delivered
valuable lessons for future modeling of heavy-GUI applications. Overall,
the stability and consistency afforded by containers made both
source-code development and integration testing noticeably easier than
in previous sprints.

Overall, a great sprint for the UMAAPy team (me) with a functional
release on the shelf and an example implementation already provided for
a multipurpose maritime robotic simulation environment. The next 2
sprints are sure to see an explosive growth in functionality and
capability.

4. What Didn’t Go So Well ❌
----------------------------

I started this sprint halfway across the country in Washington state,
working long days supporting a field test for my day job. By the time I
got to my development tasks each evening, I was often running on
fumes—physically exhausted and mentally drained from coordinating
logistics, troubleshooting field equipment, and staying on call for
real-time data issues. That fatigue showed in my early sprint reports: I
rushed through analyses, missed edge cases in my unit tests, and
submitted draft documentation that needed substantial revisions. The
grades I received on those initial deliverables reflected those
shortcomings, and it was frustrating to see my usual attention to detail
slip away simply because I didn’t have the bandwidth to catch my own
mistakes. Even though I managed to rally later in the sprint, the uneven
quality of my work early on meant I had to scramble to catch up, which
added stress and ate into time that could have been spent refining core
SDK features.

On the technical side, I ran headlong into Docker’s limitations on
Windows when trying to demo UMAAPySIM’s live autonomy capabilities. DDS
multicast and unicast traffic simply wouldn’t cross the host/container
boundary because Docker Desktop’s host-network mode on Windows is poorly
supported. I spent hours experimenting with workarounds—tweaking network
configs, trying alternate container runtimes, even exploring lightweight
VMs—but ultimately couldn’t get a real off-the-shelf autonomy stack to
connect to the simulated environment. That was a big blow for the MVP
demo vision. In the end, I fell back on a “dumb” sea-floor survey agent
running locally to highlight the core UMAA SDK APIs. While this
simplified agent still validated how quickly I could spin up autopilot
and navigation components—completing in days what larger teams often
take weeks to code—it wasn’t as flashy or compelling as a full autonomy
demo. The combination of time pressure and these unforeseen container
networking issues was a hard lesson that I’ll need to iron out before
the next major presentation.

5. What Can Be Done Better 💡
-----------------------------

Going forward, I need to carve out dedicated, protected time for sprint
work—especially when I’m on location for field tests—to prevent burnout
from creeping into my development tasks. I’ll set firm boundaries
between my day-job responsibilities and my SDK sprint commitments, and
build in buffer days for recovery, so I’m not scrambling to catch up
after long field hours. Crucially, I’ll reach out to my professor at the
first sign of any confusion around deliverable requirements, ensuring I
have clear guidance up front and avoid last-minute pivots that derail my
schedule.

On the technical side, I’ll validate my container networking
configuration within the first couple of days of the sprint so I can
spot host-container communication hurdles early. For Windows
environments, I’ll experiment immediately with WSL2 or remote Linux
hosts, or centralize DDS testing on a Linux-based CI runner, to sidestep
Docker Desktop’s host-network limitations. I’ll also draft demo fallback
strategies up front, so if integrating a full autonomy stack still
proves unreliable, I can deploy a more compelling secondary agent
without losing valuable development time.

6. Improvements & Action Items for Next Sprint
----------------------------------------------

+-----------------------------------+-----------+---------------+----------+
| Action Item                       | Owner     | Target        | Status   |
|                                   |           | Sprint/Date   |          |
+===================================+===========+===============+==========+
| Containerize UMAAPySIM            | @clr5436  | 2             | In       |
|                                   |           |               | progress |
+-----------------------------------+-----------+---------------+----------+
