/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.resume;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

/**
 * PathTree-aware EditorDiff that uses PathTree for traversal when possible,
 * avoiding SegmentStore calls for fully-processed nodes.
 *
 * <p>This is an optimization for resumable indexing:
 * <ul>
 *   <li>For fully-processed paths: Get child names from PathTree (no SegmentStore)</li>
 *   <li>For not-fully-processed paths: Fall back to NodeState (SegmentStore)</li>
 * </ul>
 */
public class PathTreeEditorDiff {

    private static final Logger LOG = LoggerFactory.getLogger(PathTreeEditorDiff.class);

    /**
     * Per-run traversal and timing counters.
     *
     * <p>These were previously process-global {@code static} counters. Because
     * {@link #process} runs synchronously on the calling thread, multiple async
     * index lanes (each diffing on its own thread) shared the same globals: one
     * lane's {@link #resetStats()} or reads corrupted another's numbers. The
     * counters now live in a {@link ThreadLocal}, giving each lane/run its own
     * isolated set while the static getStats()/getTimingStats()/... facade
     * (which the perf-analysis tooling relies on) keeps working unchanged.
     *
     * <p>Access is single-threaded within a run, but the counters remain plain
     * longs mutated only from the diffing thread; the {@code Stats} instance is
     * threaded through the private recursion to avoid a ThreadLocal lookup per
     * node in the hot path.
     */
    private static final class Stats {
        long pathTreeTraversals;
        long segmentStoreTraversals;
        long pathTreeChildLookups;
        long segmentStoreChildLookups;
        long skippedGetChildCalls;

        // Timing counters (in nanoseconds)
        long segmentStoreReadTimeNanos;
        long pathTreeLookupTimeNanos;
        long editorCallbackTimeNanos;
    }

    private static final ThreadLocal<Stats> STATS = ThreadLocal.withInitial(Stats::new);

    /**
     * Reset traversal statistics (call before each diff).
     */
    public static void resetStats() {
        // Replace the current thread's counters with a fresh set.
        STATS.set(new Stats());
    }

    /**
     * Get traversal statistics string.
     */
    public static String getStats() {
        Stats s = STATS.get();
        return "pathTreeTraversals=" + s.pathTreeTraversals +
               ", segmentStoreTraversals=" + s.segmentStoreTraversals +
               ", pathTreeChildLookups=" + s.pathTreeChildLookups +
               ", segmentStoreChildLookups=" + s.segmentStoreChildLookups +
               ", skippedGetChildCalls=" + s.skippedGetChildCalls;
    }

    /**
     * Get detailed timing statistics.
     */
    public static String getTimingStats() {
        Stats s = STATS.get();
        return String.format("segmentStoreReadTime=%.2fms, pathTreeLookupTime=%.2fms, editorCallbackTime=%.2fms",
            s.segmentStoreReadTimeNanos / 1_000_000.0,
            s.pathTreeLookupTimeNanos / 1_000_000.0,
            s.editorCallbackTimeNanos / 1_000_000.0);
    }

    public static long getSegmentStoreReadTimeMs() {
        return STATS.get().segmentStoreReadTimeNanos / 1_000_000;
    }

    public static int getPathTreeTraversals() {
        return (int) STATS.get().pathTreeTraversals;
    }

    public static int getSegmentStoreTraversals() {
        return (int) STATS.get().segmentStoreTraversals;
    }

    /**
     * Process diff using PathTree for traversal optimization.
     *
     * @param editor the editor to receive callbacks
     * @param pathTree the PathTree for optimized traversal
     * @param before the before state
     * @param after the after state
     * @return null if successful, exception otherwise
     */
    @Nullable
    public static CommitFailedException process(
            @NotNull Editor editor,
            @NotNull PathTree pathTree,
            @NotNull NodeState before,
            @NotNull NodeState after) {

        LOG.debug("[PathTreeEditorDiff] Starting diff with PathTree optimization");
        return processPath(editor, pathTree, "/", before, after, STATS.get());
    }

    @Nullable
    private static CommitFailedException processPath(
            @NotNull Editor editor,
            @NotNull PathTree pathTree,
            @NotNull String path,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Stats stats) {

        try {
            // Check if we can traverse from PathTree (fully processed)
            // This PathTree lookup is very fast compared to SegmentStore
            long lookupStart = System.nanoTime();
            boolean usePathTree = pathTree.canTraverseFromPathTree(path);
            boolean isFullyProcessed = pathTree.isFullyProcessed(path);
            stats.pathTreeLookupTimeNanos += System.nanoTime() - lookupStart;

            if (usePathTree) {
                stats.pathTreeTraversals++;
            } else {
                stats.segmentStoreTraversals++;
            }

            // MAJOR OPTIMIZATION: For fully-processed nodes, skip ALL editor calls
            // The IndexUpdate.enter() would just return immediately anyway, so we avoid:
            // 1. Function call overhead
            // 2. PathTree lookups in enter()
            // 3. Leave() marking (already done)
            // This is safe because fully-processed means the node is already in Lucene
            if (isFullyProcessed) {
                // Skip editor.enter() and editor.leave() entirely
                // Just process children from PathTree (which will also be skipped)
                CommitFailedException childException = processFullyProcessedChildren(
                    editor, pathTree, path, stats);

                if (childException != null) {
                    return childException;
                }
                return null;
            }

            // Not fully processed - need to call editors
            long callbackStart = System.nanoTime();
            editor.enter(before, after);
            stats.editorCallbackTimeNanos += System.nanoTime() - callbackStart;

            // Diff properties, mirroring EditorDiff.compareAgainstBaseState(). This must run for
            // every visited node, including newly-discovered ones (before == MISSING_NODE): those
            // are exactly the nodes whose property values need to be fed to the index editor. The
            // previous guard skipped added/deleted nodes, so a resumed node was marked processed in
            // the PathTree without its properties ever reaching the index (silent data loss). Both
            // MISSING_NODE.getProperties() and getProperty() are empty/null, so the generic diff is
            // correct for added (before MISSING), changed (both present) and deleted (after MISSING).
            long propStart = System.nanoTime();
            for (PropertyState afterProp : after.getProperties()) {
                PropertyState beforeProp = before.getProperty(afterProp.getName());
                if (beforeProp == null) {
                    editor.propertyAdded(afterProp);
                } else if (!beforeProp.equals(afterProp)) {
                    editor.propertyChanged(beforeProp, afterProp);
                }
            }
            for (PropertyState beforeProp : before.getProperties()) {
                if (!after.hasProperty(beforeProp.getName())) {
                    editor.propertyDeleted(beforeProp);
                }
            }
            stats.segmentStoreReadTimeNanos += System.nanoTime() - propStart;

            // Process child nodes
            CommitFailedException childException = processChildren(
                editor, pathTree, path, before, after, stats);

            if (childException != null) {
                return childException;
            }

            // Call leave
            callbackStart = System.nanoTime();
            editor.leave(before, after);
            stats.editorCallbackTimeNanos += System.nanoTime() - callbackStart;

            return null;

        } catch (CommitFailedException e) {
            return e;
        }
    }

    /**
     * Process children of a fully-processed node using only PathTree.
     * No SegmentStore calls at all.
     *
     * OPTIMIZATION: Since the parent is fully processed, all children in PathTree
     * must also be fully processed. We can skip the editor callbacks entirely!
     */
    @Nullable
    private static CommitFailedException processFullyProcessedChildren(
            @NotNull Editor editor,
            @NotNull PathTree pathTree,
            @NotNull String parentPath,
            @NotNull Stats stats) throws CommitFailedException {

        Set<String> childNames = pathTree.getChildNamesFromPathTree(parentPath);

        for (String childName : childNames) {
            String childPath = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;

            stats.skippedGetChildCalls += 2; // Saved 2 getChildNode calls

            // MAJOR OPTIMIZATION: Check if child is also fully processed
            // If so, skip ALL editor calls for this entire subtree
            boolean childFullyProcessed = pathTree.isFullyProcessed(childPath);

            if (childFullyProcessed) {
                // Child is fully processed - recursively process its children from PathTree
                // WITHOUT calling any editor methods (no enter/leave overhead)
                stats.pathTreeTraversals++;
                CommitFailedException e = processFullyProcessedChildren(editor, pathTree, childPath, stats);
                if (e != null) return e;
            } else {
                // Child NOT fully processed - need to call editor
                // This handles edge cases where parent is marked but child isn't
                long callbackStart = System.nanoTime();
                Editor childEditor = editor.childNodeChanged(childName, MISSING_NODE, MISSING_NODE);
                stats.editorCallbackTimeNanos += System.nanoTime() - callbackStart;

                if (childEditor != null) {
                    CommitFailedException e = processPath(
                        childEditor, pathTree, childPath, MISSING_NODE, MISSING_NODE, stats);
                    if (e != null) return e;
                }
            }
        }

        return null;
    }

    @Nullable
    private static CommitFailedException processChildren(
            @NotNull Editor editor,
            @NotNull PathTree pathTree,
            @NotNull String parentPath,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Stats stats) throws CommitFailedException {

        // Always enumerate the REAL children from the node states, never from the
        // PathTree. Only fully-processed nodes may be traversed from the tree, and those
        // are short-circuited earlier (processFullyProcessedChildren); by the time we get
        // here the node is a frontier node whose children may include brand-new content
        // that no prior chunk has seen — and such nodes are not in the PathTree. Driving
        // enumeration from the tree would silently drop them (verified data loss). The
        // PathTree is still consulted per child, but only to cheaply SKIP fully-processed
        // subtrees, not to decide which children exist.
        stats.segmentStoreChildLookups++;
        LOG.trace("[PathTreeDiff] Enumerating real children for: {}", parentPath);

        for (String childName : after.getChildNodeNames()) {
            String childPath = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;

            // Check the PathTree FIRST: a fully-processed child needs no SegmentStore read
            // and no descent — the editor would skip it anyway.
            long lookupStart = System.nanoTime();
            boolean childFullyProcessed = pathTree.isFullyProcessed(childPath);
            stats.pathTreeLookupTimeNanos += System.nanoTime() - lookupStart;

            if (childFullyProcessed) {
                LOG.trace("[PathTreeDiff] Child {} fully processed - SKIPPING SegmentStore entirely", childPath);
                stats.skippedGetChildCalls += 2; // Saved 2 getChildNode calls (before + after)

                long callbackStart = System.nanoTime();
                Editor childEditor = editor.childNodeChanged(childName, MISSING_NODE, MISSING_NODE);
                stats.editorCallbackTimeNanos += System.nanoTime() - callbackStart;

                if (childEditor != null) {
                    CommitFailedException e = processPath(
                        childEditor, pathTree, childPath, MISSING_NODE, MISSING_NODE, stats);
                    if (e != null) return e;
                }
            } else {
                // Frontier / unprocessed child - read the real states.
                long readStart = System.nanoTime();
                NodeState beforeChild = before.getChildNode(childName);
                NodeState afterChild = after.getChildNode(childName);
                stats.segmentStoreReadTimeNanos += System.nanoTime() - readStart;

                boolean beforeExists = beforeChild.exists();

                if (!beforeExists) {
                    // Child added since the base state.
                    long callbackStart = System.nanoTime();
                    Editor childEditor = editor.childNodeAdded(childName, afterChild);
                    stats.editorCallbackTimeNanos += System.nanoTime() - callbackStart;

                    if (childEditor != null) {
                        CommitFailedException e = processPath(
                            childEditor, pathTree, childPath, MISSING_NODE, afterChild, stats);
                        if (e != null) return e;
                    }
                } else {
                    // Prune unchanged subtrees, mirroring EditorDiff's
                    // compareAgainstBaseState(): descend only into children that
                    // actually differ. Without this, resume walks every node in the
                    // (large, static) /jcr:system subtree, exhausting the chunk
                    // budget there and never returning to finish the changed content.
                    if (afterChild.equals(beforeChild)) {
                        continue;
                    }
                    long callbackStart = System.nanoTime();
                    Editor childEditor = editor.childNodeChanged(childName, beforeChild, afterChild);
                    stats.editorCallbackTimeNanos += System.nanoTime() - callbackStart;

                    if (childEditor != null) {
                        CommitFailedException e = processPath(
                            childEditor, pathTree, childPath, beforeChild, afterChild, stats);
                        if (e != null) return e;
                    }
                }
            }
        }

        // Children present in the base state but gone in after: deletions.
        for (String childName : before.getChildNodeNames()) {
            if (!after.hasChildNode(childName)) {
                String childPath = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
                NodeState beforeChild = before.getChildNode(childName);

                Editor childEditor = editor.childNodeDeleted(childName, beforeChild);
                if (childEditor != null) {
                    CommitFailedException e = processPath(
                        childEditor, pathTree, childPath, beforeChild, MISSING_NODE, stats);
                    if (e != null) return e;
                }
            }
        }

        return null;
    }
}
