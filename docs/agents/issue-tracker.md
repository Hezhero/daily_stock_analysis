# Issue tracker: Local Markdown

Issues and specs for this repo live as markdown files under `docs/specs/`, tracked in git. There is no external tracker and no `gh`/GitHub Issues dependency.

## Conventions

- One feature per directory: `docs/specs/<feature-slug>/`
- The spec is `docs/specs/<feature-slug>/spec.md`
- Implementation issues are one file per ticket at `docs/specs/<feature-slug>/issues/<NN>-<slug>.md`, numbered from `01`, never a single combined tickets file
- Triage state is recorded as a `Status:` line near the top of each issue file (see `triage-labels.md` for the role strings)
- Comments and conversation history append to the bottom of the file under a `## Comments` heading
- Everything under `docs/specs/` is version-controlled with git

## When a skill says "publish to the issue tracker"

Create a new file under `docs/specs/<feature-slug>/` (creating the directory if needed).

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or the issue number directly.

## Ticket header fields

Every ticket file must declare, immediately after `**Blocked by:**`:

- `**Gate:**` — which flip gate may adjudicate it: `none` (implementation or
  infrastructure work), `v1-signal`, `v2-portfolio`, or `wfo`.
- `**Signal set:**` — `none`, `changed`, or `unchanged`.
- `**Gate rationale:**` — required when `Gate: v1-signal` and `Signal set: changed`;
  cite the ADR or spec section that authorizes the comparison.

`python scripts/check_spec_tickets.py` enforces the applicability boundary:
`v2-portfolio` requires an unchanged signal set (flip-gate spec §9), a `none`
gate requires `Signal set: none`, and a signal-changing v1 comparison needs a
rationale. CI runs this in the `ai-governance` job.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a file with one **child** file per ticket.

- **Map**: `docs/specs/<effort>/map.md` (the Notes / Decisions-so-far / Fog body).
- **Child ticket**: `docs/specs/<effort>/issues/NN-<slug>.md`, numbered from `01`, with the question in the body. A `Type:` line records the ticket type (`research`/`prototype`/`grilling`/`task`); a `Status:` line records `claimed`/`resolved`.
- **Blocking**: a `Blocked by: NN, NN` line near the top. A ticket is unblocked when every file it lists is `resolved`.
- **Frontier**: scan `docs/specs/<effort>/issues/` for files that are open, unblocked, and unclaimed; first by number wins.
- **Claim**: set `Status: claimed` and save before any work.
- **Resolve**: append the answer under an `## Answer` heading, set `Status: resolved`, then append a context pointer (gist + link) to the map's Decisions-so-far in `map.md`.
