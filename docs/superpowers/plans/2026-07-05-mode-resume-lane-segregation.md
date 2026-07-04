# mode=resume Opt-in + Segregated ResumableAsyncIndexUpdate Lane — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an index opt into resumable/chunked async indexing per-index via a `mode=resume` property, processed by a segregated `ResumableAsyncIndexUpdate` running on a `resume_`-prefixed lane, so the stock `AsyncIndexUpdate` path reverts to trunk behavior and resume can be rolled out and reverted one index at a time.

**Architecture:** Routing is by mode, never by rewriting the def's `async` property (so a mode flip preserves index content). The resume process is a subclass of `AsyncIndexUpdate` that overrides a small set of protected seams; the base class keeps trunk-equivalent no-op defaults. Reindex of a `mode=resume` index is resumable when `FT_RESUMABLE_REINDEXING_OAK-<issue>` is ON (Approach A), and falls back to a native reindex on the normal lane + a resume-lane pause/reset when OFF (Approach C, the default).

**Tech Stack:** Java 11 source / built on JDK 17, Apache Jackrabbit Oak (`oak-core`, `oak-lucene`), JUnit 4.13.1, Mockito 5, Maven multi-module.

## Global Constraints

- Build/test on JDK 17: `export JAVA_HOME=/Users/mokatari/installs/jdk-17.0.10.jdk/Contents/Home` (env default is JDK 11 and will fail the enforcer).
- oak-core fast build+test: `mvn test -pl oak-core -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`.
- Before building/testing `oak-lucene`, reinstall the modified upstream modules from source: `mvn install -pl oak-search,oak-core -DskipTests -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`.
- Every new `.java` file must start with the Apache 2.0 license header (see AGENTS.md; RAT enforces it).
- No wildcard imports; import each class individually.
- New code must have >80% test coverage.
- Feature toggle name is exactly `FT_RESUMABLE_REINDEXING_OAK-<issue>`; a new-feature toggle is **disabled by default** (per AGENTS.md). Replace `<issue>` with the assigned Jira issue number before merge (tracked as an open item; use `FT_RESUMABLE_REINDEXING_OAK-0` as the literal until assigned).
- Branch `resumeIndexingV3`; commit messages start with the Jira key once assigned. Never commit to `trunk`.
- Design source of truth: `docs/superpowers/specs/2026-07-04-mode-resume-lane-segregation-design.md`.

---

## File Structure

**Created:**
- `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/ResumableAsyncIndexUpdate.java` — the segregated resume process; owns all resume/chunk logic and lane-name helpers.
- `oak-core/src/test/java/org/apache/jackrabbit/oak/plugins/index/ResumableAsyncIndexUpdateTest.java` — unit + lifecycle tests for the new class and routing.

**Modified:**
- `oak-core/.../plugins/index/IndexConstants.java` — add `MODE_PROPERTY_NAME`, `MODE_RESUME`.
- `oak-core/.../plugins/index/IndexUpdate.java` — mode filter in `isIncluded`, `resumeLane` flag on `IndexUpdateRootState`, and the reindex-routing exemption (Task 7).
- `oak-core/.../plugins/index/AsyncIndexUpdate.java` — extract protected seams; base impls become trunk-equivalent; move resume/chunk logic out (into the subclass).
- `oak-core/.../plugins/index/AsyncIndexerService.java` — register a `ResumableAsyncIndexUpdate` per configured `resume_<base>` lane.
- Existing resume tests (`ResumeChunkFlushTest`, `ResumeIndexingE2ETest`, `AsyncIndexUpdateResumptionTest`) — repoint to `ResumableAsyncIndexUpdate` where they exercise resume behavior.

---

## Task 1: Mode-aware routing filter

Route `mode=resume` indexes away from the normal lane and toward the resume lane, driven by a per-run flag, without touching the def's `async` property. The resume process passes the **base** lane name (`async`) as the `IndexUpdate` match name, so `isIncluded` only needs to add a mode check — no prefix stripping here.

**Files:**
- Modify: `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/IndexConstants.java` (after line 52)
- Modify: `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/IndexUpdate.java` (`isIncluded` at 645-658; `IndexUpdateRootState` at 923-954; call site at 525; public constructors 170-223)
- Test: `oak-core/src/test/java/org/apache/jackrabbit/oak/plugins/index/IndexUpdateTest.java`

**Interfaces:**
- Produces:
  - `IndexConstants.MODE_PROPERTY_NAME = "mode"`, `IndexConstants.MODE_RESUME = "resume"`.
  - `static boolean IndexUpdate.isIncluded(String asyncRef, NodeBuilder definition, boolean resumeLane)` — resume-lane-aware overload.
  - `IndexUpdateRootState.resumeLane` (final boolean), threaded through both `IndexUpdate` public constructors via a new trailing `boolean resumeLane` parameter.

- [ ] **Step 1: Write the failing test**

Add to `IndexUpdateTest.java`:

```java
@Test
public void isIncludedRespectsResumeMode() {
    NodeBuilder normal = EMPTY_NODE.builder();
    normal.setProperty("async", "async");

    NodeBuilder resume = EMPTY_NODE.builder();
    resume.setProperty("async", "async");
    resume.setProperty(IndexConstants.MODE_PROPERTY_NAME, IndexConstants.MODE_RESUME);

    // normal lane (resumeLane=false): takes normal, skips resume-mode
    assertTrue(IndexUpdate.isIncluded("async", normal, false));
    assertFalse(IndexUpdate.isIncluded("async", resume, false));

    // resume lane (resumeLane=true): takes resume-mode, skips normal
    assertFalse(IndexUpdate.isIncluded("async", normal, true));
    assertTrue(IndexUpdate.isIncluded("async", resume, true));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl oak-core -Dtest=IndexUpdateTest#isIncludedRespectsResumeMode -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: FAIL — `isIncluded(String,NodeBuilder,boolean)` and `MODE_PROPERTY_NAME` do not exist (compile error).

- [ ] **Step 3: Add the constants**

In `IndexConstants.java`, after line 52 (`String ASYNC_PROPERTY_NAME = "async";`):

```java
    /**
     * Optional index-definition property selecting a processing mode.
     * When set to {@link #MODE_RESUME} the index is processed by the
     * segregated resumable async indexer instead of the normal lane.
     */
    String MODE_PROPERTY_NAME = "mode";

    /** Value of {@link #MODE_PROPERTY_NAME} that opts an index into resumable indexing. */
    String MODE_RESUME = "resume";
```

- [ ] **Step 4: Add the mode-aware `isIncluded` overload**

In `IndexUpdate.java`, add `import static ...IndexConstants.MODE_PROPERTY_NAME;` and `MODE_RESUME;`, then replace the existing `isIncluded` (645-658) with:

```java
    static boolean isIncluded(String asyncRef, NodeBuilder definition) {
        return isIncluded(asyncRef, definition, false);
    }

    static boolean isIncluded(String asyncRef, NodeBuilder definition, boolean resumeLane) {
        boolean resumeDef = MODE_RESUME.equals(definition.getString(MODE_PROPERTY_NAME));
        // A resume lane only processes resume-mode defs; a normal lane only non-resume defs.
        if (resumeDef != resumeLane) {
            return false;
        }
        if (definition.hasProperty(ASYNC_PROPERTY_NAME)) {
            PropertyState p = definition.getProperty(ASYNC_PROPERTY_NAME);
            Iterable<String> opt = p.getValue(Type.STRINGS);
            if (asyncRef == null) {
                return IterableUtils.contains(opt, INDEXING_MODE_NRT) || IterableUtils.contains(opt, INDEXING_MODE_SYNC);
            } else {
                return IterableUtils.contains(opt, asyncRef);
            }
        } else {
            return asyncRef == null;
        }
    }
```

- [ ] **Step 5: Thread `resumeLane` through rootState and the constructors**

In `IndexUpdateRootState` (923-954): add `final boolean resumeLane;`, add a trailing `boolean resumeLane` param to its constructor, and set `this.resumeLane = resumeLane;`.

In the resume-context `IndexUpdate` constructor (206-223): add a trailing `boolean resumeLane` param and pass it to the `IndexUpdateRootState(...)` call at 216. Update the delegating constructor at 184-190 to pass `false`. Add one more public constructor overload so existing callers compile unchanged:

```java
    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback, NodeTraversalCallback traversalCallback,
            CommitInfo commitInfo, CorruptIndexHandler corruptIndexHandler,
            @Nullable ResumeContext resumeContext, @Nullable NodeStore store) {
        this(provider, async, root, builder, updateCallback, traversalCallback,
             commitInfo, corruptIndexHandler, resumeContext, store, false);
    }
```

and make the real body take the extra trailing `boolean resumeLane`.

At the call site (525) change to:

```java
            if (isIncluded(rootState.async, definition, rootState.resumeLane)) {
```

- [ ] **Step 6: Run test to verify it passes**

Run: `mvn test -pl oak-core -Dtest=IndexUpdateTest#isIncludedRespectsResumeMode -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 7: Run the full IndexUpdateTest to confirm no regression**

Run: `mvn test -pl oak-core -Dtest=IndexUpdateTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS (all existing methods still green; default `resumeLane=false` preserves trunk behavior).

- [ ] **Step 8: Commit**

```bash
git add oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/IndexConstants.java \
        oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/IndexUpdate.java \
        oak-core/src/test/java/org/apache/jackrabbit/oak/plugins/index/IndexUpdateTest.java
git commit -m "OAK-<issue>: mode-aware isIncluded routing (mode=resume)"
```

---

## Task 2: Extract resume seams in AsyncIndexUpdate (pure refactor, no behavior change)

Introduce protected hook methods on `AsyncIndexUpdate` and move the resume-specific decisions behind them, but keep `AsyncIndexUpdate`'s own implementations exactly reproducing the current behavior (still reading the `oak.async.*` system properties). This is a **behavior-preserving extract-method refactor**: all existing tests — including the resume tests — stay green because `AsyncIndexUpdate` still does everything it does today.

**Files:**
- Modify: `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/AsyncIndexUpdate.java` (`updateIndex` 1068-1309; `run`/`runWhenPermitted` around 751-854; the `IndexUpdate` construction at 1140)
- Test: existing `AsyncIndexUpdateTest`, `ResumeChunkFlushTest`

**Interfaces:**
- Produces (all `protected` on `AsyncIndexUpdate`, base impls = current behavior):
  - `protected String indexMatchLaneName()` — the lane name passed as `IndexUpdate`'s `async` arg for def-matching. Base returns `name`.
  - `protected boolean isResumeLane()` — base returns `false`.
  - `protected boolean isChunkedRun(NodeState before)` — base returns the current `resumeEnabled && (configuredChunkSize>0 || chunkTimeMs>0) && before != MISSING_NODE`.
  - `protected ResumeContext buildResumeContext(String resumeFromPath, PathTree pathTree, boolean chunked)` — base returns the current `ResumeContext`.
  - `protected void onChunkComplete(...)` and `protected void afterRun(NodeBuilder builder, IndexUpdate indexUpdate, boolean fullyCompleted)` — base impls reproduce current chunk-commit / no-op cleanup.

- [ ] **Step 1: Confirm current resume tests are green (baseline)**

Run: `mvn test -pl oak-core -Dtest=AsyncIndexUpdateTest,ResumeChunkFlushTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS. Record the counts; they must be identical after this task.

- [ ] **Step 2: Add the `indexMatchLaneName()` / `isResumeLane()` seams and use them at the IndexUpdate call**

In `AsyncIndexUpdate.java` add:

```java
    /** Lane name used to match index definitions (their {@code async} value). */
    protected String indexMatchLaneName() {
        return name;
    }

    /** Whether this process serves {@code mode=resume} definitions. */
    protected boolean isResumeLane() {
        return false;
    }
```

Change the `IndexUpdate` construction (1140) to use them:

```java
            indexUpdate = new IndexUpdate(provider, indexMatchLaneName(), after, builder,
                    callback, callback, info, corruptIndexHandler, resumeContext, null, isResumeLane())
                    .withMissingProviderStrategy(missingStrategy);
```

(This requires the 11-arg `IndexUpdate` constructor from Task 1.)

- [ ] **Step 3: Wrap the chunked-mode decision in `isChunkedRun`**

Replace the inline computation at 1086-1087 with a call:

```java
        boolean isInitialIndex = before == MISSING_NODE;
        boolean chunkedMode = isChunkedRun(before);
```

and add the base method reproducing today's logic verbatim:

```java
    protected boolean isChunkedRun(NodeState before) {
        boolean resumeEnabled = Boolean.getBoolean(PROP_RESUME_ENABLED);
        long chunkTimeMs = Long.getLong(PROP_CHUNK_TIME_MS, 0);
        boolean isInitialIndex = before == MISSING_NODE;
        return resumeEnabled && (configuredChunkSize > 0 || chunkTimeMs > 0) && !isInitialIndex;
    }
```

- [ ] **Step 4: Extract `onChunkComplete` and `afterRun` seams**

Move the body of the `if (isChunkComplete) { ... return false; }` block (1240-1256) into `protected boolean onChunkComplete(CommitFailedException exception, AsyncUpdateCallback callback, ResumeContext resumeContext, IndexUpdate indexUpdate, NodeBuilder builder, String beforeCheckpoint, String afterCheckpoint, AtomicReference<String> checkpointToReleaseRef)` returning `true` when a chunk boundary was handled (caller then `return false`). Base impl = the current block. Add a no-op `protected void afterRun(NodeBuilder builder, IndexUpdate indexUpdate, boolean fullyCompleted)` and call it right before the checkpoint-state update at 1273 with `fullyCompleted=true`.

- [ ] **Step 5: Run tests to verify identical behavior**

Run: `mvn test -pl oak-core -Dtest=AsyncIndexUpdateTest,ResumeChunkFlushTest,IndexUpdateTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS with the same counts as Step 1. No behavior changed — only method boundaries.

- [ ] **Step 6: Commit**

```bash
git add oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/AsyncIndexUpdate.java
git commit -m "OAK-<issue>: extract resume seams in AsyncIndexUpdate (no behavior change)"
```

---

## Task 3: Create ResumableAsyncIndexUpdate; move resume logic into overrides; base → trunk

Create the subclass, move the resume/chunk logic out of `AsyncIndexUpdate`'s base impls into `ResumableAsyncIndexUpdate` overrides, and make `AsyncIndexUpdate`'s base hooks trunk-equivalent (no chunking, no PathTree, `isResumeLane()=false`). Retire the global `oak.async.resume` gate: resume behavior now comes solely from running the subclass. Repoint the resume tests to the subclass.

**Files:**
- Create: `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/ResumableAsyncIndexUpdate.java`
- Modify: `AsyncIndexUpdate.java` (base hook impls → trunk defaults; keep `loadOrCreatePathTree`/`commitChunkAndSaveResumeState`/`clearResumeStateAfterCompletion` accessible — move them to `protected` so the subclass can call them, or relocate into the subclass)
- Modify: `ResumeChunkFlushTest`, `ResumeIndexingE2ETest`, `AsyncIndexUpdateResumptionTest` (instantiate `ResumableAsyncIndexUpdate`)

**Interfaces:**
- Consumes: the Task 2 seams; `PathTree`, `ResumeContext`, `PathTreeEditorDiff`, `ResumableEditorDiff` in the `resume` package; the `PROP_CHUNK_SIZE`/`PROP_CHUNK_TIME_MS`/`PROP_USE_PATHTREE_TRAVERSAL`/`PROP_PATHTREE_SLIM_FORMAT` constants (move these into the subclass).
- Produces:
  - `public class ResumableAsyncIndexUpdate extends AsyncIndexUpdate` with constructors mirroring `AsyncIndexUpdate` (name is the `resume_`-prefixed lane).
  - `public static final String RESUME_LANE_PREFIX = "resume_";`
  - `public static String resumeLaneName(String baseLane)` → `RESUME_LANE_PREFIX + baseLane`.
  - `public static String baseLaneName(String resumeLane)` → strips the prefix (throws `IllegalArgumentException` if absent).
  - `public static boolean isResumeLane(String laneName)` → `laneName.startsWith(RESUME_LANE_PREFIX)`.
  - Overrides: `indexMatchLaneName()` → `baseLaneName(name)`; `isResumeLane()` → `true`; `isChunkedRun(...)` → chunked when chunk config is set (see Task 6 for the reindex term); `onChunkComplete(...)` → the real chunk-commit; `buildResumeContext(...)`.

- [ ] **Step 1: Write the failing lane-helper test**

Create `ResumableAsyncIndexUpdateTest.java` (with the Apache license header) containing:

```java
@Test
public void laneNameHelpers() {
    assertEquals("resume_async", ResumableAsyncIndexUpdate.resumeLaneName("async"));
    assertEquals("resume_fulltext-async", ResumableAsyncIndexUpdate.resumeLaneName("fulltext-async"));
    assertEquals("async", ResumableAsyncIndexUpdate.baseLaneName("resume_async"));
    assertTrue(ResumableAsyncIndexUpdate.isResumeLane("resume_async"));
    assertFalse(ResumableAsyncIndexUpdate.isResumeLane("async"));
}

@Test(expected = IllegalArgumentException.class)
public void baseLaneNameRejectsNonResume() {
    ResumableAsyncIndexUpdate.baseLaneName("async");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#laneNameHelpers -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: FAIL — `ResumableAsyncIndexUpdate` does not exist.

- [ ] **Step 3: Create ResumableAsyncIndexUpdate with lane helpers + constructors + trivial overrides**

Create the class extending `AsyncIndexUpdate`, with the Apache header, the three static helpers, a constructor `public ResumableAsyncIndexUpdate(String resumeLaneName, NodeStore store, IndexEditorProvider provider, StatisticsProvider statsProvider, boolean switchOnSync)` delegating to `super(...)` (note: `checkValidName` already accepts `resume_async` since it ends with `async`), and:

```java
    @Override
    protected String indexMatchLaneName() {
        return baseLaneName(getName());   // add a protected getter getName() on AsyncIndexUpdate if absent
    }

    @Override
    protected boolean isResumeLane() {
        return true;
    }
```

Add `protected String getName() { return name; }` to `AsyncIndexUpdate` if not already present.

- [ ] **Step 4: Run to verify the helper test passes**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#laneNameHelpers -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 5: Move resume logic from AsyncIndexUpdate base impls into the subclass overrides**

- Move `PROP_CHUNK_SIZE`, `PROP_CHUNK_TIME_MS`, `PROP_USE_PATHTREE_TRAVERSAL`, `PROP_PATHTREE_SLIM_FORMAT`, `PROP_PATHTREE_ULTRA_SLIM_FORMAT`, and `configuredChunkSize` into `ResumableAsyncIndexUpdate`.
- Make `AsyncIndexUpdate.isChunkedRun(...)` return `false` (base is never chunked), `onChunkComplete(...)` return `false` (base has no chunk boundaries), `buildResumeContext(...)` return `null`.
- Override in `ResumableAsyncIndexUpdate`: `isChunkedRun` = `(configuredChunkSize > 0 || Long.getLong(PROP_CHUNK_TIME_MS,0) > 0) && before != MISSING_NODE`; `onChunkComplete` = the extracted chunk-commit body (calls `commitChunkAndSaveResumeState`); `buildResumeContext` = the `ResumeContext`/`PathTree` wiring currently at 1124-1161.
- Change `loadOrCreatePathTree`, `commitChunkAndSaveResumeState`, `clearResumeStateAfterCompletion` from `private` to `protected` on `AsyncIndexUpdate` (keep them there — they are generic helpers keyed by `name`), OR relocate them into `ResumableAsyncIndexUpdate`. Prefer relocating to the subclass to keep the base clean.
- Remove the `PROP_RESUME_ENABLED` / `oak.async.resume` reads from `AsyncIndexUpdate`. Resume no longer has a global gate.

- [ ] **Step 6: Repoint the resume tests to the subclass**

In `ResumeChunkFlushTest`, `ResumeIndexingE2ETest`, `AsyncIndexUpdateResumptionTest`, replace `new AsyncIndexUpdate(...)` with `new ResumableAsyncIndexUpdate(ResumableAsyncIndexUpdate.resumeLaneName("async"), store, provider, ...)` for the resume scenarios, and mark the exercised index defs with `mode=resume`. Where a test set `System.setProperty("oak.async.resume", "true")`, remove it (no longer needed) but keep the `oak.async.chunkSize`/`chunkTimeMs` settings.

- [ ] **Step 7: Run the full resume + base suites**

Run: `mvn test -pl oak-core -Dtest=AsyncIndexUpdateTest,IndexUpdateTest,ResumeChunkFlushTest,AsyncIndexUpdateResumptionTest,ResumableAsyncIndexUpdateTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS. `AsyncIndexUpdateTest`/`IndexUpdateTest` prove the base path is trunk-equivalent; resume tests prove the subclass reproduces resume behavior.

- [ ] **Step 8: Run the lucene E2E (rebuild oak-search/oak-core first)**

```bash
export JAVA_HOME=/Users/mokatari/installs/jdk-17.0.10.jdk/Contents/Home
mvn install -pl oak-search,oak-core -DskipTests -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true
mvn test -pl oak-lucene -Dtest=ResumeIndexingE2ETest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true
```
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: segregate resume logic into ResumableAsyncIndexUpdate"
```

---

## Task 4: Seed-from-base checkpoint on first resume run (enable with no reindex)

When an index switches to `mode=resume`, the resume lane's first run has no `:async/resume_<lane>` checkpoint. Instead of starting from `MISSING_NODE` (a de-facto reindex), seed it from the base lane's current `:async/<base>` checkpoint so it continues incrementally.

**Files:**
- Modify: `AsyncIndexUpdate.java` — add a seam `protected String resolveBeforeCheckpoint(NodeState async)` returning `async.getString(name)` (base = trunk, line 751). Use it at 751.
- Modify: `ResumableAsyncIndexUpdate.java` — override to seed from the base lane.
- Test: `ResumableAsyncIndexUpdateTest.java`

**Interfaces:**
- Consumes: `AsyncIndexUpdate.name` (resume lane), `baseLaneName(name)`.
- Produces: `protected String resolveBeforeCheckpoint(NodeState async)`.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void firstResumeRunSeedsFromBaseCheckpoint() throws Exception {
    MemoryNodeStore store = new MemoryNodeStore();
    // simulate the base lane having indexed up to checkpoint "cp-base"
    NodeBuilder b = store.getRoot().builder();
    b.child(":async").setProperty("async", "cp-base");
    store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

    ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
            ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
            new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

    NodeState async = store.getRoot().getChildNode(":async");
    // resume lane has no :async/resume_async yet -> must seed from base "cp-base"
    assertEquals("cp-base", r.resolveBeforeCheckpoint(async));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#firstResumeRunSeedsFromBaseCheckpoint -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: FAIL — `resolveBeforeCheckpoint` not defined.

- [ ] **Step 3: Add the base seam and use it in run()**

In `AsyncIndexUpdate.java`:

```java
    /** Returns the checkpoint this run should start from, or null for initial indexing. */
    protected String resolveBeforeCheckpoint(NodeState async) {
        return async.getString(name);
    }
```

At line 751 replace `beforeCheckpoint = root.getChildNode(ASYNC).getString(name);` with:

```java
                beforeCheckpoint = resolveBeforeCheckpoint(root.getChildNode(ASYNC));
```

- [ ] **Step 4: Override in the subclass**

```java
    @Override
    protected String resolveBeforeCheckpoint(NodeState async) {
        String own = async.getString(getName());
        if (own != null) {
            return own;                       // resume lane already has its own checkpoint
        }
        return async.getString(baseLaneName(getName()));  // seed once from the base lane
    }
```

- [ ] **Step 5: Run to verify it passes**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#firstResumeRunSeedsFromBaseCheckpoint -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 6: Run AsyncIndexUpdateTest to confirm base unchanged**

Run: `mvn test -pl oak-core -Dtest=AsyncIndexUpdateTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS (base `resolveBeforeCheckpoint` = trunk behavior).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: seed resume lane checkpoint from base lane on first run"
```

---

## Task 5: Revert self-heal (mode=resume → null)

When an index no longer has `mode=resume` but the resume lane still holds resume state for its lane, the resume lane cleans up its own state and sets `reindex=true` so the normal lane rebuilds the index cleanly. The resume lane owns this cleanup.

**Files:**
- Modify: `ResumableAsyncIndexUpdate.java` — override `afterRun(...)` (the Task 2 seam) to detect orphaned defs and clean up; add a helper `cleanupRevertedIndexes(NodeBuilder builder)`.
- Test: `ResumableAsyncIndexUpdateTest.java`

**Interfaces:**
- Consumes: `IndexConstants.MODE_PROPERTY_NAME`, `MODE_RESUME`, `REINDEX_PROPERTY_NAME`; the resume-state node `:async/<resumeLane>-resume`.
- Produces: `void cleanupRevertedIndexes(NodeBuilder builder)` — for each index def under the resume lane's scope whose `mode != resume`, sets `reindex=true`; deletes `:async/<resumeLane>-resume` when no resume-mode defs remain on the lane.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void revertDeletesResumeStateAndFlagsReindex() throws Exception {
    MemoryNodeStore store = new MemoryNodeStore();
    NodeBuilder b = store.getRoot().builder();
    // resume state exists for the lane
    b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/x");
    // an index def that was mode=resume but is now reverted (mode removed)
    NodeBuilder def = b.child("oak:index").child("myIndex");
    def.setProperty("type", "property");
    def.setProperty("async", "async");            // still async
    // no "mode" property -> reverted
    store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

    ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
            ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
            new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

    NodeBuilder root = store.getRoot().builder();
    r.cleanupRevertedIndexes(root);

    assertTrue(root.getChildNode("oak:index").getChildNode("myIndex").getBoolean("reindex"));
    assertFalse(root.getChildNode(":async").hasChildNode("resume_async-resume"));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#revertDeletesResumeStateAndFlagsReindex -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: FAIL — `cleanupRevertedIndexes` not defined.

- [ ] **Step 3: Implement `cleanupRevertedIndexes` and wire it into `afterRun`**

```java
    void cleanupRevertedIndexes(NodeBuilder root) {
        String base = baseLaneName(getName());
        boolean anyResumeDefRemains = false;
        NodeBuilder defs = root.getChildNode("oak:index");
        if (defs.exists()) {
            for (String n : defs.getChildNodeNames()) {
                NodeBuilder def = defs.getChildNode(n);
                if (!def.hasProperty(IndexConstants.ASYNC_PROPERTY_NAME)) {
                    continue;
                }
                boolean matchesBase = IterableUtils.contains(
                        def.getProperty(IndexConstants.ASYNC_PROPERTY_NAME).getValue(Type.STRINGS), base);
                if (!matchesBase) {
                    continue;
                }
                if (IndexConstants.MODE_RESUME.equals(def.getString(IndexConstants.MODE_PROPERTY_NAME))) {
                    anyResumeDefRemains = true;
                } else {
                    // reverted: rebuild cleanly on the normal lane
                    def.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
                }
            }
        }
        if (!anyResumeDefRemains) {
            NodeBuilder async = root.getChildNode(ASYNC);
            String resumeNode = getName() + "-resume";
            if (async.hasChildNode(resumeNode)) {
                async.getChildNode(resumeNode).remove();
            }
            async.removeProperty(getName());   // release the resume-lane checkpoint property
        }
    }

    @Override
    protected void afterRun(NodeBuilder builder, IndexUpdate indexUpdate, boolean fullyCompleted) {
        cleanupRevertedIndexes(builder);
    }
```

Note: `oak:index` is the top-level index-definitions node name; if the deployment nests defs, iterate the same way `AsyncIndexUpdate` discovers them. For this plan the top-level `oak:index` scan matches the E2E fixtures.

- [ ] **Step 4: Run to verify it passes**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#revertDeletesResumeStateAndFlagsReindex -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: resume lane self-heals on revert (delete state + reindex=true)"
```

---

## Task 6: Resumable reindex under FT_RESUMABLE_REINDEXING (toggle ON)

When the toggle is ON, a `mode=resume` index with `reindex=true` (or a first-ever build) rebuilds resumably in chunks. `reindex=false` and the reindex-count bump are applied **only on full completion**, never on a partial chunk.

**Files:**
- Modify: `ResumableAsyncIndexUpdate.java` — feature toggle field + `isChunkedRun` override includes the initial/reindex case when the toggle is ON; completion-gating for `reindex`.
- Test: `ResumableAsyncIndexUpdateTest.java`

**Interfaces:**
- Consumes: `org.apache.jackrabbit.oak.spi.toggle.Feature`; the whiteboard passed at registration (Task 8). For unit tests, expose `void setResumableReindexEnabledForTest(boolean)`.
- Produces: `boolean isResumableReindexEnabled()`.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void chunkedRunCoversReindexOnlyWhenToggleOn() {
    ResumableAsyncIndexUpdate r = newResumeUpdateWithChunkSize(100); // helper sets oak.async.chunkSize=100
    NodeState missing = org.apache.jackrabbit.oak.spi.state.EmptyNodeState.MISSING_NODE;

    r.setResumableReindexEnabledForTest(false);
    assertFalse("reindex not chunked when toggle off", r.isChunkedRun(missing));

    r.setResumableReindexEnabledForTest(true);
    assertTrue("reindex chunked when toggle on", r.isChunkedRun(missing));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#chunkedRunCoversReindexOnlyWhenToggleOn -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: FAIL — `setResumableReindexEnabledForTest` / behavior not present.

- [ ] **Step 3: Implement the toggle and reindex-aware `isChunkedRun`**

```java
    private Feature resumableReindexFeature;         // set at registration (Task 8), may be null
    private Boolean resumableReindexEnabledOverride;  // test-only

    void setResumableReindexEnabledForTest(boolean enabled) {
        this.resumableReindexEnabledOverride = enabled;
    }

    boolean isResumableReindexEnabled() {
        if (resumableReindexEnabledOverride != null) {
            return resumableReindexEnabledOverride;
        }
        return resumableReindexFeature != null && resumableReindexFeature.isEnabled();
    }

    @Override
    protected boolean isChunkedRun(NodeState before) {
        boolean chunkConfigured = configuredChunkSize > 0 || Long.getLong(PROP_CHUNK_TIME_MS, 0) > 0;
        if (!chunkConfigured) {
            return false;
        }
        boolean isInitialIndex = before == MISSING_NODE;
        // Toggle ON: reindex/initial build is also chunked (resumable reindex).
        // Toggle OFF: only incremental is chunked (see Task 7 for reindex handling).
        return isInitialIndex ? isResumableReindexEnabled() : true;
    }
```

- [ ] **Step 4: Completion-gate the reindex flag**

Confirm (and add a guarded assertion in the E2E of Task 9) that `reindex=false` / `incrementReIndexCount` run only in the full-completion path (`AsyncIndexUpdate.updateIndex` after the traversal completes without a `CHUNK_COMPLETE`), never inside `onChunkComplete`. In `IndexUpdate.collectIndexEditors` the `reindex=false` at line 602 happens during editor setup for a reindex; for a chunked resumable reindex this must be deferred. Add a seam: `IndexUpdate` skips setting `reindex=false` when `rootState.resumeLane && rootState.chunked` and instead the resume lane sets it on full completion. Implement by passing a `chunked` flag into the rootState (mirror the `resumeLane` plumbing from Task 1) and guarding line 601-603:

```java
                    } else if (!(rootState.resumeLane && rootState.chunked)) {
                        definition.setProperty(REINDEX_PROPERTY_NAME, false);
                        incrementReIndexCount(definition);
                        removeIndexState(definition);
                        clearCorruptFlag(definition, indexPath);
                        reindex.put(concat(getPath(), INDEX_DEFINITIONS_NAME, name), editor);
                    } else {
                        // resumable reindex: keep reindex=true until the build fully completes;
                        // still register the editor so content is (re)built this chunk.
                        removeIndexStateOnce(definition);   // clear stale content only on the first chunk
                        reindex.put(concat(getPath(), INDEX_DEFINITIONS_NAME, name), editor);
                    }
```

`removeIndexStateOnce` clears index content only when starting fresh (no resume state), guarded by a boolean on the rootState set from `resumeFromPath == null`. On full completion, `ResumableAsyncIndexUpdate.afterRun(..., fullyCompleted=true)` sets `reindex=false` and calls `incrementReIndexCount` for the reindexed defs it tracked.

- [ ] **Step 5: Run to verify it passes**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#chunkedRunCoversReindexOnlyWhenToggleOn -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: resumable reindex gated by FT_RESUMABLE_REINDEXING (default off)"
```

---

## Task 7: Fallback C (toggle OFF) — native reindex + resume-lane pause/reset

With the toggle OFF (default), a `mode=resume` index that needs a reindex is rebuilt natively on the normal lane, the resume lane pauses (does not advance `C_pause`), and once the native reindex completes the resume lane resets (deletes its resume cursor, keeps `C_pause`) so all resume-mode indexes restart cleanly from `C_pause`. This resolves the spec's open follow-up on the pause/reset mechanism.

**Files:**
- Modify: `IndexUpdate.java` — normal-lane reindex exemption in `isIncluded`.
- Modify: `ResumableAsyncIndexUpdate.java` — pause detection + reset via a persisted marker.
- Test: `ResumableAsyncIndexUpdateTest.java`

**Interfaces:**
- Consumes: `isResumableReindexEnabled()`, `REINDEX_PROPERTY_NAME`.
- Produces:
  - `isIncluded(..., resumeLane=false)` also returns `true` for a `mode=resume` def **when it has `reindex=true` and resumable-reindex is disabled** — so the normal lane rebuilds it. Implement via a static-settable process flag `IndexUpdate.setResumableReindexEnabled(boolean)` OR a rootState flag `resumableReindexEnabled` plumbed from the running process. Use the rootState flag (no static global).
  - `boolean ResumableAsyncIndexUpdate.shouldPauseForNativeReindex(NodeState root)` — true if any resume-mode def matching this base lane has `reindex=true` and resumable-reindex is disabled.
  - Marker `:async/<resumeLane>-resume/reindexPaused=true` written while paused; the reset (delete cursor, clear marker) runs on the first run where `shouldPauseForNativeReindex` is false but the marker is set.

- [ ] **Step 1: Write the failing routing test**

```java
@Test
public void normalLaneReindexesResumeDefWhenToggleOff() {
    NodeBuilder resumeReindexing = EMPTY_NODE.builder();
    resumeReindexing.setProperty("async", "async");
    resumeReindexing.setProperty(IndexConstants.MODE_PROPERTY_NAME, IndexConstants.MODE_RESUME);
    resumeReindexing.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

    // toggle OFF: normal lane MUST pick it up to run the native reindex
    assertTrue(IndexUpdate.isIncluded("async", resumeReindexing, false, /*resumableReindexEnabled*/ false));
    // toggle ON: normal lane still skips it (resume lane handles the reindex)
    assertFalse(IndexUpdate.isIncluded("async", resumeReindexing, false, /*resumableReindexEnabled*/ true));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#normalLaneReindexesResumeDefWhenToggleOff -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: FAIL — 4-arg `isIncluded` not defined.

- [ ] **Step 3: Add the reindex exemption to `isIncluded`**

Add a 4-arg overload and route the 3-arg one to it with `resumableReindexEnabled=true` (so existing resume-lane behavior is unchanged):

```java
    static boolean isIncluded(String asyncRef, NodeBuilder definition, boolean resumeLane) {
        return isIncluded(asyncRef, definition, resumeLane, true);
    }

    static boolean isIncluded(String asyncRef, NodeBuilder definition,
                              boolean resumeLane, boolean resumableReindexEnabled) {
        boolean resumeDef = MODE_RESUME.equals(definition.getString(MODE_PROPERTY_NAME));
        if (resumeDef != resumeLane) {
            // EXCEPTION: a normal lane adopts a resume-mode def that needs a reindex
            // while resumable-reindex is disabled (fallback C rebuilds it natively).
            boolean normalLaneAdoptsReindex = !resumeLane && resumeDef
                    && !resumableReindexEnabled
                    && definition.getBoolean(REINDEX_PROPERTY_NAME);
            if (!normalLaneAdoptsReindex) {
                return false;
            }
        }
        // ... existing async-match body unchanged ...
    }
```

Plumb `resumableReindexEnabled` onto `IndexUpdateRootState` (mirror the `resumeLane` plumbing from Task 1; the normal `AsyncIndexUpdate` sets it from `false`-safe default `true` — normal lane has no resume feature — wait: the normal lane needs to know the toggle. Set it from a process getter: `AsyncIndexUpdate` base returns `true` (no resumable reindex ⇒ never adopts), and `ResumableAsyncIndexUpdate` is irrelevant here). The relevant decision is on the **normal** `AsyncIndexUpdate`; give it a settable `resumableReindexEnabled` supplied at registration from the same `Feature` (Task 8). Base default `true` means "don't adopt" (safe: preserves trunk). When the feature is present and OFF, it is set to `false` so the normal lane adopts the reindex.

Update the call site (525) to pass `rootState.resumableReindexEnabled`.

- [ ] **Step 4: Run to verify it passes**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#normalLaneReindexesResumeDefWhenToggleOff -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 5: Write the failing pause/reset test**

```java
@Test
public void resumeLanePausesDuringNativeReindexThenResets() throws Exception {
    MemoryNodeStore store = new MemoryNodeStore();
    NodeBuilder b = store.getRoot().builder();
    b.child(":async").setProperty("resume_async", "cp-pause");
    b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/y");
    NodeBuilder def = b.child("oak:index").child("idx");
    def.setProperty("type", "property");
    def.setProperty("async", "async");
    def.setProperty(IndexConstants.MODE_PROPERTY_NAME, IndexConstants.MODE_RESUME);
    def.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);   // native reindex in flight
    store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

    ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
            "resume_async", store, new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);
    r.setResumableReindexEnabledForTest(false);

    // reindex in flight -> pause; checkpoint C_pause untouched; marker set
    assertTrue(r.shouldPauseForNativeReindex(store.getRoot()));

    // native reindex completes: clear reindex flag
    NodeBuilder b2 = store.getRoot().builder();
    b2.child("oak:index").child("idx").setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);
    store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

    assertFalse(r.shouldPauseForNativeReindex(store.getRoot()));
    NodeBuilder root = store.getRoot().builder();
    r.resetAfterNativeReindex(root);   // deletes cursor, keeps C_pause
    assertFalse(root.getChildNode(":async").hasChildNode("resume_async-resume"));
    assertEquals("cp-pause", root.getChildNode(":async").getString("resume_async"));
}
```

- [ ] **Step 6: Implement pause detection + reset**

```java
    boolean shouldPauseForNativeReindex(NodeState root) {
        if (isResumableReindexEnabled()) {
            return false;   // toggle ON: the resume lane owns the reindex, no pause
        }
        String base = baseLaneName(getName());
        NodeState defs = root.getChildNode("oak:index");
        for (String n : defs.getChildNodeNames()) {
            NodeState def = defs.getChildNode(n);
            if (MODE_RESUME.equals(def.getString(MODE_PROPERTY_NAME))
                    && def.getBoolean(REINDEX_PROPERTY_NAME)
                    && def.hasProperty(ASYNC_PROPERTY_NAME)
                    && IterableUtils.contains(def.getProperty(ASYNC_PROPERTY_NAME).getValue(Type.STRINGS), base)) {
                return true;
            }
        }
        return false;
    }

    void resetAfterNativeReindex(NodeBuilder root) {
        NodeBuilder async = root.getChildNode(ASYNC);
        String resumeNode = getName() + "-resume";
        if (async.hasChildNode(resumeNode)) {
            async.getChildNode(resumeNode).remove();   // drop cursor/PathTree; keep :async/<lane> checkpoint (C_pause)
        }
    }
```

Wire into `runWhenPermitted`: at the very start of a run, `if (shouldPauseForNativeReindex(store.getRoot())) { markReindexPaused(); return; }`. On a run where `shouldPauseForNativeReindex` is false but the paused marker is set, call `resetAfterNativeReindex` inside the run's builder before the normal flow, then clear the marker. (Marker = a boolean property `reindexPaused` on `:async/<lane>-resume`; if that node was deleted by reset, treat absence as "not paused".)

- [ ] **Step 7: Run to verify it passes**

Run: `mvn test -pl oak-core -Dtest=ResumableAsyncIndexUpdateTest#resumeLanePausesDuringNativeReindexThenResets -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 8: Run AsyncIndexUpdateTest + IndexUpdateTest for regression**

Run: `mvn test -pl oak-core -Dtest=AsyncIndexUpdateTest,IndexUpdateTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS (normal lane default `resumableReindexEnabled=true` ⇒ never adopts ⇒ trunk behavior).

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: fallback C - native reindex + resume-lane pause/reset (toggle off)"
```

---

## Task 8: OSGi registration + feature toggle wiring

Register a `ResumableAsyncIndexUpdate` for each configured `resume_<base>` lane, and create the `FT_RESUMABLE_REINDEXING_OAK-<issue>` feature toggle on the Whiteboard, supplying it to both the resume process and the normal processes (so the normal lane knows whether to adopt reindexes).

**Files:**
- Modify: `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/AsyncIndexerService.java` (`activate`, 119-156)
- Modify: `ResumableAsyncIndexUpdate.java` / `AsyncIndexUpdate.java` — setters `setResumableReindexFeature(Feature)` (resume) and `setResumableReindexEnabledSupplier(...)`/`setResumableReindexFeature(Feature)` (normal lane, for the adoption decision).
- Test: covered by Task 9 E2E (OSGi activation is integration-level; a unit test for `AsyncIndexerService.activate` is not part of the existing suite).

**Interfaces:**
- Consumes: `org.apache.jackrabbit.oak.spi.toggle.Feature.newFeature(name, whiteboard)`; `AsyncConfig` (existing lane config).
- Produces: registered resume tasks; `Feature` closed in `deactivate`.

- [ ] **Step 1: Create the feature toggle in `activate`**

After the whiteboard is created (122), add:

```java
        Feature resumableReindex = Feature.newFeature("FT_RESUMABLE_REINDEXING_OAK-0", whiteboard);
        closer.register(resumableReindex::close);
```

- [ ] **Step 2: Supply the toggle to the normal lanes**

Inside the existing loop (130-153), after constructing each normal `AsyncIndexUpdate task`, add:

```java
            task.setResumableReindexFeature(resumableReindex);
```

and add to `AsyncIndexUpdate`:

```java
    private Feature resumableReindexFeature;
    public void setResumableReindexFeature(Feature f) { this.resumableReindexFeature = f; }
    protected boolean isResumableReindexEnabled() {
        return resumableReindexFeature != null && resumableReindexFeature.isEnabled();
    }
```

and set `rootState.resumableReindexEnabled = isResumableReindexEnabled()` when building the `IndexUpdate` (so the normal lane's `isIncluded` adoption uses it). Move the subclass's `isResumableReindexEnabled()` from Task 6 to override this base method (keep the test override).

- [ ] **Step 3: Register a resume task per lane**

After `registerAsyncReindexSupport(whiteboard);` (154), add:

```java
        for (AsyncConfig c : asyncIndexerConfig) {
            String resumeLane = ResumableAsyncIndexUpdate.resumeLaneName(c.name);
            ResumableAsyncIndexUpdate rtask = new ResumableAsyncIndexUpdate(
                    resumeLane, nodeStore, indexEditorProvider, statisticsProvider, false);
            rtask.setCorruptIndexHandler(corruptIndexHandler);
            rtask.setValidatorProviders(Collections.singletonList(validatorProvider));
            rtask.setResumableReindexFeature(resumableReindex);
            rtask.setLeaseTimeOut(TimeUnit.MINUTES.toMillis(
                    (nodeStore instanceof Clusterable) ? config.leaseTimeOutMinutes() : 0));
            indexRegistration.registerAsyncIndexer(rtask, c.timeIntervalInSecs);
            closer.register(rtask);
        }
```

(Registration is additive; with no `mode=resume` indexes present the resume tasks find nothing to do and never touch `:async`, so behavior matches trunk for existing deployments.)

- [ ] **Step 4: Build oak-core to confirm compilation**

Run: `mvn install -pl oak-core -DskipTests -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: register resume_ lanes + FT_RESUMABLE_REINDEXING toggle in AsyncIndexerService"
```

---

## Task 9: End-to-end lifecycle test

Prove the whole lifecycle against a real Lucene index: enable (no reindex, incremental resume), resumable reindex (toggle ON), and revert (self-heal). Extends the existing `ResumeIndexingE2ETest` harness in `oak-lucene`.

**Files:**
- Create/Modify: `oak-lucene/src/test/java/org/apache/jackrabbit/oak/plugins/index/lucene/resumeindexing/ResumeModeLifecycleE2ETest.java`

**Interfaces:**
- Consumes: `ResumableAsyncIndexUpdate`, `AsyncIndexUpdate`, the lucene test fixtures used by `ResumeIndexingE2ETest`.

- [ ] **Step 1: Write the enable-no-reindex test**

```java
@Test
public void enableResumeModeContinuesIncrementallyNoReindex() throws Exception {
    // 1. index N nodes on the normal AsyncIndexUpdate("async") lane -> content built, :async/async set
    // 2. set mode=resume on the index def (no reindex flag), refresh
    // 3. run ResumableAsyncIndexUpdate("resume_async"): assert :async/resume_async seeds from :async/async
    //    and the pre-existing content is still queryable (NOT rebuilt from scratch)
    // 4. add M more nodes, run resume lane in chunks: assert all N+M queryable, reindexCount unchanged
    assertEquals(preExistingReindexCount, currentReindexCount);   // no reindex happened
    assertQueryReturns(N + M);
}
```

- [ ] **Step 2: Write the resumable-reindex test (toggle ON)**

```java
@Test
public void resumableReindexResumesAfterInterruption() throws Exception {
    // mode=resume, reindex=true, toggle ON, small chunkSize
    // run resume lane once (one chunk) -> partial content, reindex flag STILL true (completion-gated)
    // run again repeatedly -> on the run that completes traversal, reindex flag flips to false
    // assert full content queryable and reindexCount incremented exactly once
}
```

- [ ] **Step 3: Write the revert test**

```java
@Test
public void revertToNullSelfHeals() throws Exception {
    // mode=resume with resume state present -> remove mode property -> run resume lane
    // assert :async/resume_async-resume deleted and def.reindex==true
    // then run normal AsyncIndexUpdate("async") -> index rebuilt, content queryable
}
```

- [ ] **Step 4: Run the E2E suite (rebuild upstream first)**

```bash
export JAVA_HOME=/Users/mokatari/installs/jdk-17.0.10.jdk/Contents/Home
mvn install -pl oak-search,oak-core -DskipTests -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true
mvn test -pl oak-lucene -Dtest=ResumeModeLifecycleE2ETest,ResumeIndexingE2ETest,ResumeChunkFlushTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true
```
Expected: PASS.

- [ ] **Step 5: Full oak-core regression**

Run: `mvn test -pl oak-core -Dtest=AsyncIndexUpdateTest,IndexUpdateTest,ResumableAsyncIndexUpdateTest,AsyncIndexUpdateResumptionTest -Djacoco.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Drat.skip=true -Danimal.sniffer.skip=true`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "OAK-<issue>: end-to-end lifecycle tests for mode=resume segregation"
```

---

## Open items (carry to review)

- Replace `FT_RESUMABLE_REINDEXING_OAK-0` and `OAK-<issue>` with the assigned Jira issue number across code and commits.
- Confirm the index-definitions scan in `cleanupRevertedIndexes` / `shouldPauseForNativeReindex` matches how the target deployment nests defs (top-level `oak:index` assumed here).
- After landing, request review from committers active in `oak-core` async-indexing (per AGENTS.md).
