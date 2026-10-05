---
status: accepted
date: 2026-10-05
---

# 0010: Frontend scope and time box

## Context and Problem Statement

The plan time-boxed the dashboard to one week (week 9) and listed "frontend consumes the schedule" as a high-likelihood risk. The owner now requires a substantially more ambitious operator console:

- a studio-level design system with light and dark themes;
- 2D and 3D map modes with live tracking;
- refined micro-interactions;
- full mobile parity;
- a public case-study page.

That is roughly four to five weeks of work. Where does that time come from?

## Considered Options

1. **Parallel UI track.** Build the UI about a day a week from week 2, then full-time in weeks 9 and 10, and move OpenTelemetry tracing and the SLO dashboard to Tier 2. Keeps the 12-week end date.
2. **Extend the programme to 14 weeks.** Keep every backend item and add two dedicated UI weeks after week 9.
3. **Keep 12 weeks and cut dbt.** Weakens the data-engineering story and Gate 4.

## Decision Outcome

Chosen option: **2, extend to 14 weeks** (owner's decision, 5 Oct 2026). The programme now ends on Sunday 10 Jan 2027.

- The operator console joins Tier 0, with full mobile parity (phones and tablets run the complete workflow, not a read-only view).
- A new **Phase 5: Experience** (weeks 10 and 11) has its own gate. Evidence becomes Phase 6 (weeks 12 to 14), and its gate becomes Gate 7.
- Success criterion 9 makes the console checkable: the Playwright incident workflow on desktop and phone, measured frame-rate and interaction budgets, and axe plus keyboard-only runs.
- Scroll-driven storytelling lives on a separate public case-study page (week 14). The console uses restrained, state-bearing motion.
- The UI is developed against recorded simulator streams, so it never waits on backend progress.

### Consequences

- Good: the console gets real time and a gate, instead of an unrealistic one-week box.
- Good: every backend Tier 0 and Tier 1 item survives.
- Bad: two more weeks of a solo build, which raises the momentum risk in plan section 20.
- Bad: full mobile parity on a map-plus-panels console is the most expensive layout choice. Phone layouts must be designed from the start, not adapted at the end.

### Confirmation

Gate 6 (end of week 11) and success criterion 9.

## More Information

Amends ADR-0001's timeline (12 weeks, ending 27 Dec 2026). Plan sections 1, 2, 13, 18, 20 and 22 were updated the same day.
