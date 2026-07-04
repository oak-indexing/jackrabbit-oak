# Design: `mode=resume` index opt-in + segregated `ResumableAsyncIndexUpdate` lane

**Status:** Approved (brainstorming)
**Branch:** `resumeIndexingV3`
**Date:** 2026-07-04

## Problem

Resumable/chunked async indexing is functional but currently lives inline inside
`AsyncIndexUpdate.updateIndex()`, gated by process-global system properties
(`oak.async.resume`, `oak.async.chunkSize`, ...). This is too risky for production:
enabling resume is all-or-nothing for the whole process, the resume/chunk code sits
directly in the critical async-indexing path, and reindex is not resumable at all
(the highest-value case).

We want to integrate resumable indexing into production **incrementally and per index**,
with a small, reviewable blast radius, so it can be enabled one index at a time after
testing, and reverted cleanly.

## Goals

- Opt in to resumable indexing **per index** via a `mode=resume` property on the index
  definition — no reindex required to switch modes, just a definition refresh.
- Run the resumable implementation in a **segregated process** (`ResumableAsyncIndexUpdate`)
  on a **`resume_`-prefixed lane**, so the stock `AsyncIndexUpdate` critical path reverts
  to trunk behavior.
- Make **reindex itself resumable** (Approach A), with a feature-toggle fallback to Oak's
  native non-resumable reindex (Approach C) if the resumable-reindex path proves risky.
- Reverting `mode=resume → mode=null` cleans up all resume state and rebuilds the index
  cleanly, with no stale-gap risk. The resume lane owns its own cleanup (self-healing).

## Non-goals

- Resumable reindex for **sync** indexes promoted via `reindex-async` (the
  `async-reindex`/`switchOnSync` promotion path). `mode=resume` targets async indexes.
- Per-index chunk-size tuning. Chunk config stays lane-level (system properties) for now.

## Background: existing Oak lane machinery (as of this branch)

- An index def routes to a lane via its `async` property (e.g. `async=["async"]`), matched
  by `IndexUpdate.isIncluded(rootState.async, def)`.
- All async lane state is keyed by lane name under `:async`:
  - `:async/<lane>` — checkpoint (e.g. `:async/async`)
  - `:async/<lane>-resume` — serialized PathTree / resume cursor (this branch)
  - lease + temp checkpoint via `leasify(name)` / `getTempCpName(name)`
- `AsyncIndexUpdate.run()` reads its start checkpoint as
  `root.getChildNode(":async").getString(name)`; a fresh lane name with no checkpoint →
  `before = MISSING_NODE` → full repo traversal (a de-facto reindex).
- `checkValidName` accepts any name ending in `async`; `isAsyncLaneName` returns true for
  such names — so `resume_async` / `resume_fulltext-async` are already valid lane names.
- Reindex of an **async** index happens inline on its own lane (`shouldReindex` →
  `removeIndexState`, rebuild from `MISSING_NODE`, set `reindex=false`).
- Sync→async reindex **promotion** (`reindex-async=true`) temporarily sets
  `async=async-reindex`, runs on the dedicated `async-reindex` lane with
  `switchOnSync=true`, then removes the `async` property to switch the index back to sync.
  (Out of scope here.)

## Approach

**Approach A** (chosen): one `ResumableAsyncIndexUpdate` per base lane, on a
`resume_<base>` lane, handling **both** incremental and reindex resumably.
**Approach C** (fallback, behind a feature toggle): resume covers incremental only;
reindex uses Oak's native path.

## Section 1 — Lane topology, routing & mode lifecycle

### Lane topology

For each base async lane (`async`, `fulltext-async`), a sibling
`ResumableAsyncIndexUpdate` runs on `resume_<base>` (`resume_async`,
`resume_fulltext-async`). All of its persisted state is keyed by its own lane name and
therefore never collides with the base lane:

- `:async/resume_async` — checkpoint
- `:async/resume_async-resume` — serialized PathTree / resume cursor
- lease + temp checkpoint via the existing `leasify` / `getTempCpName` helpers

No changes to `checkValidName` / `isAsyncLaneName` are needed.

### Routing — mode-based filtering (never rewrite `async`)

New index-def property **`mode`** (string). `mode=resume` opts in; absent/null = normal.
`IndexUpdate.isIncluded(...)` gains a mode filter driven by an accepted-mode carried on
`IndexUpdateRootState`:

- Normal `AsyncIndexUpdate("async")`: match as today **AND** `mode != resume` → skips
  resume indexes.
- `ResumableAsyncIndexUpdate("resume_async")`: strip the `resume_` prefix → base lane
  `"async"`, match the def's `async` against that **AND** `mode == resume` → picks only
  resume indexes.

The def's `async` property is **never rewritten**, so index content is preserved across a
mode flip.

### Mode lifecycle

**Enable (`null → resume`), no reindex:**
1. Operator sets `mode=resume` and saves (the "refresh" — a property write; each process
   re-reads it on its next cycle).
2. Normal lane stops touching the index on its next run (mode filter).
3. Resume lane's first run finds no `:async/resume_async` checkpoint → **seeds it from the
   base lane's current `:async/async` checkpoint** → continues incrementally from exactly
   where the normal lane left off. No reindex.

**Steady state:** resume lane processes deltas in chunks, advancing `:async/resume_async`
independently of the base lane.

**Reindex while `mode=resume`:** `reindex=true` → resume lane rebuilds from `MISSING_NODE`
in resumable chunks (Section 3). Governed by the C-fallback toggle.

**Revert (`resume → null`) — resume lane self-heals:**
1. Operator sets `mode=null` and saves.
2. On its next run, the resume lane detects one of its indexes no longer has
   `mode=resume`, and performs cleanup: **deletes `:async/resume_async-resume` (PathTree)
   and clears/releases its checkpoint state**, and sets **`reindex=true`** on the def.
3. Normal lane picks the index back up and rebuilds cleanly — no stale-gap risk.

## Section 2 — Components & code changes

### New file: `ResumableAsyncIndexUpdate` (factoring 2a: subclass + hooks)

`oak-core/.../plugins/index/ResumableAsyncIndexUpdate.java`, a subclass of
`AsyncIndexUpdate`. `AsyncIndexUpdate` gains a few **protected seams** whose base
implementations are no-ops = exact trunk behavior; the subclass overrides them:

- `resolveBeforeCheckpoint(...)` — seed-from-base on first run; otherwise normal.
- `createResumeContext(...)` — PathTree load/create + chunk limits.
- `onChunkComplete(...)` — chunk-commit + persist resume cursor.
- `afterRunCleanup(...)` — orphan/revert cleanup (delete PathTree, set `reindex=true`).
- `acceptedMode()` / base-lane derivation (`stripResumePrefix(name)`).

This reverts almost all of the current inline diff in `AsyncIndexUpdate` back to trunk,
keeping the critical path low-risk, without duplicating the ~1000-line run loop.

### Shared changes (small)

- `IndexUpdate.isIncluded` — add the mode filter; `IndexUpdateRootState` carries the
  accepted mode (normal = reject `resume`; resume = require `resume`).
- `IndexConstants` — add `MODE_PROPERTY_NAME = "mode"` and `MODE_RESUME = "resume"`.
- `resume/` package (PathTree, ResumeContext, ...) — unchanged; called from the new class
  instead of inline.

### What leaves `AsyncIndexUpdate`

The `PROP_RESUME_ENABLED` / `chunkSize` / etc. system-property reads and the inline
chunk/resume blocks move into `ResumableAsyncIndexUpdate`. `AsyncIndexUpdate` keeps only
the protected no-op hooks.

## Section 3 — Resumable reindex + the C-fallback toggle

### Resumable reindex (Approach A)

Drop the `!isInitialIndex` term from `chunkedMode` **inside `ResumableAsyncIndexUpdate`
only**, so a `mode=resume` index with `reindex=true` (or a first-ever build) rebuilds from
`MISSING_NODE` in resumable chunks. The PathTree under `:async/resume_async-resume`
persists the cursor across process restarts, so an interrupted reindex resumes rather than
restarts.

`reindex=false` + `incrementReIndexCount` are applied **only when the full build
completes** (the run that reaches end-of-traversal with no remaining chunk), never on a
partial chunk — otherwise a crash mid-reindex would leave `reindex=false` on a half-built
index.

### C-fallback toggle: `FT_RESUMABLE_INDEXING_OAK-<issue>` (default off)

Scopes the **reindex path only**:

- **ON:** reindex of a `mode=resume` index runs resumably on `resume_async` (full A).
- **OFF (fallback C):** the index needing reindex is rebuilt in **normal (non-resumable)
  mode on a separate lane** (Oak's native path). The **other** `mode=resume` indexes keep
  running on the resume lane. Reset-after-reindex flow:
  1. While the reindex imports + catches up, the **resume lane is paused** (does not
     advance) to establish a clean cut point at checkpoint `C_pause`.
  2. When the reindex catch-up completes, the process **deletes the resume lane's resume
     info** (PathTree / cursor).
  3. On the next resume-lane run, **all resume-mode indexes restart from `C_pause`** (the
     paused resume-lane checkpoint), with resume logic re-engaging:
     - Non-reindexed indexes (Y, Z): no gap — they resume exactly from `C_pause`.
     - Reindexed index (X, now at head `C_head >= C_pause`): idempotently re-processes the
       small `[C_pause .. C_head]` delta. No data loss.

Flipping the toggle off in prod loses only resumable-*reindex*; steady-state resumable
incremental indexing keeps working. Incremental resume itself stays behind the per-index
`mode=resume` opt-in — the outer safety net.

## Section 4 — Config, scheduling & testing

### Configuration

- `mode` — per-index, on the index definition. The opt-in switch.
- `FT_RESUMABLE_INDEXING_OAK-<issue>` — feature toggle, default off.
- Chunk config (`chunkSize`, `chunkTimeMs`) — lane-level system properties read by
  `ResumableAsyncIndexUpdate` (chunk boundary is a property of the shared lane traversal,
  not of an individual index). May migrate to the def later; out of scope now.
- The old global master switch `oak.async.resume` is **retired** as a gate; the resume
  lane's existence + per-index `mode=resume` replaces it. `usePathTreeTraversal` /
  `pathTreeSlimFormat` stay as-is on the resume class.

### Scheduling / registration

- **Prod (OSGi):** `AsyncIndexerService` registers a `ResumableAsyncIndexUpdate` for each
  configured `resume_<base>` lane on its own interval. Additive and config-gated — a
  deployment with no `resume_*` lanes configured behaves exactly like trunk.
- **Tests / oak-run:** instantiate `ResumableAsyncIndexUpdate("resume_async", store,
  provider, ...)` directly, like existing `AsyncIndexUpdate` tests.

### Testing

- **Routing:** `isIncluded` skips `mode=resume` on the normal lane and requires it on the
  resume lane; prefix-strip base-lane derivation.
- **Lifecycle:** enable (seed-from-base, no reindex → content preserved + continues
  incrementally); revert (resume lane self-heals: deletes PathTree, sets `reindex=true`,
  normal lane rebuilds).
- **Resumable reindex (toggle ON):** interrupt a chunked reindex mid-build → resume from
  PathTree → full content + `reindex=false` only at completion.
- **Fallback (toggle OFF):** reindex on normal lane while Y/Z run on the resume lane;
  pause → reset → all restart from `C_pause`, no gap for Y/Z, X consistent.
- **Isolation:** two lanes advancing concurrently never double-process or corrupt each
  other's checkpoint / lease.
- **Regression:** existing `AsyncIndexUpdate` / `IndexUpdate` suites stay green (base path
  is trunk-equivalent via no-op hooks).

## Open items / follow-ups

- Exact Jira issue number for the toggle name (`FT_RESUMABLE_INDEXING_OAK-<issue>`).
- Precise mechanism for pausing the resume lane during a toggle-OFF reindex (detection of
  an in-flight native reindex on the base lane, and where the pause/reset is driven).
