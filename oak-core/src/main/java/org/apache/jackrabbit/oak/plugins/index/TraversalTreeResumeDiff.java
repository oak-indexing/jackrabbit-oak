/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resume diff processor that uses {@link TraversalTree} metadata to avoid NodeStore I/O
 * for already-indexed nodes before the resume point.
 *
 * <p>This is similar in spirit to {@link CachedResumeDiff}, but drives iteration and
 * skip decisions from the in-memory/pruned {@link TraversalTree} and uses {@link VirtualNodeState}
 * for skip traversal (no SegmentStore reads for skipped subtrees).
 *
 * <h2>Design</h2>
 * <ul>
 *   <li><b>Before resume point</b>: traverse using {@link VirtualNodeState} and do not read child states from NodeStore.</li>
 *   <li><b>At/after resume point</b>: traverse real {@link NodeState} and call {@link EditorDiff#process} for indexing.</li>
 *   <li><b>Indexed subtrees</b> (from tree): treated as leaf stubs by default (enter/leave only) to keep CPU low.</li>
 * </ul>
 */
public final class TraversalTreeResumeDiff {

    private static final Logger log = LoggerFactory.getLogger(TraversalTreeResumeDiff.class);
    private static final boolean DEBUG = Boolean.getBoolean("oak.async.treeResumeDebug");

    private TraversalTreeResumeDiff() {}

    @FunctionalInterface
    public interface ResumePointHandler {
        void onResumePointReached() throws CommitFailedException;
    }

    @Nullable
    public static CommitFailedException process(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull TraversalTree traversalTree,
            @NotNull String resumePath,
            @Nullable ResumePointHandler onResumePointReached) throws CommitFailedException {

        String[] pathSegments = parseResumePath(resumePath);
        boolean[] reachedResume = {false};
        boolean[] firedCallback = {false};

        long start = System.currentTimeMillis();

        // Enter root with real states: allows IndexUpdate (if wired to skipMode) to defer init but keep real deferred states.
        editor.enter(before, after);

        CommitFailedException ex = processChildrenOptimized(
                editor, before, after, traversalTree, pathSegments, 0, reachedResume, firedCallback, onResumePointReached);

        if (ex != null) {
            return ex;
        }

        editor.leave(before, after);

        if (DEBUG) {
            log.info("[TREE-RESUME] Completed in {}ms, resumePath={}", (System.currentTimeMillis() - start), resumePath);
        }
        return null;
    }

    private static String[] parseResumePath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return new String[0];
        }
        return java.util.Arrays.stream(path.split("/"))
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }

    @Nullable
    private static CommitFailedException processChildrenOptimized(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull TraversalTree tree,
            @NotNull String[] pathSegments,
            int pathIndex,
            boolean[] reachedResume,
            boolean[] firedCallback,
            @Nullable ResumePointHandler onResumePointReached) throws CommitFailedException {

        boolean onResumePath = pathIndex < pathSegments.length;
        String targetChild = onResumePath ? pathSegments[pathIndex] : null;

        // Prefer captured child order from the tree (works even after pruning to stubs).
        Iterable<String> childNames = getChildNames(tree, after);

        for (String childName : childNames) {
            TraversalTree childTree = tree.getChild(childName);
            boolean childIndexed = childTree != null && childTree.isIndexed();

            // On resume path: navigate down until the final segment.
            if (!reachedResume[0] && onResumePath && childName.equals(targetChild)) {
                boolean isLast = (pathIndex + 1) == pathSegments.length;
                if (isLast) {
                    // Resume point reached: switch to real processing from here.
                    reachedResume[0] = true;
                    fireResumeCallbackOnce(firedCallback, onResumePointReached);

                    NodeState childBefore = before.getChildNode(childName);
                    NodeState childAfter = after.getChildNode(childName);
                    Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
                    if (childEditor != null) {
                        CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                        if (ex != null) {
                            return ex;
                        }
                    }
                } else {
                    // Continue navigating towards resume point with virtual state (no NodeStore reads for siblings).
                    NodeState virtualChild = (childTree != null) ? childTree.toVirtualNodeState() : MISSING_NODE;
                    Editor childEditor = editor.childNodeAdded(childName, virtualChild);
                    if (childEditor != null) {
                        childEditor.enter(MISSING_NODE, virtualChild);

                        // Only read the real states along the resume path (O(depth) total), for correctness after resume.
                        NodeState realChildBefore = before.getChildNode(childName);
                        NodeState realChildAfter = after.getChildNode(childName);
                        CommitFailedException ex = processChildrenOptimized(
                                childEditor, realChildBefore, realChildAfter,
                                childTree != null ? childTree : new TraversalTree(childName, 0, tree),
                                pathSegments, pathIndex + 1, reachedResume, firedCallback, onResumePointReached);
                        if (ex != null) {
                            return ex;
                        }

                        childEditor.leave(MISSING_NODE, virtualChild);
                    }
                }
                continue;
            }

            // Before resume point: if the subtree is already indexed, replay just enter/leave with a virtual state.
            if (!reachedResume[0] && childIndexed) {
                NodeState virtualChild = childTree.toVirtualNodeState();
                Editor childEditor = editor.childNodeAdded(childName, virtualChild);
                if (childEditor != null) {
                    childEditor.enter(MISSING_NODE, virtualChild);
                    // Recurse into indexed subtree using virtual state to preserve DFS structure.
                    // With pruning/stubbing enabled, most indexed children will have no descendants,
                    // so this is still cheap but keeps traversal semantics correct.
                    CommitFailedException ex = processChildrenOptimized(
                            childEditor,
                            MISSING_NODE,
                            virtualChild,
                            childTree,
                            pathSegments,
                            // IMPORTANT: disable resume-path matching inside already-indexed subtrees.
                            // The resume target is not inside these subtrees, and allowing matching here
                            // can prematurely trigger "reached resume" and skip real indexing.
                            pathSegments.length,
                            reachedResume,
                            firedCallback,
                            onResumePointReached);
                    if (ex != null) {
                        return ex;
                    }
                    childEditor.leave(MISSING_NODE, virtualChild);
                }
                continue;
            }

            // After resume point (or no tree info): do full real processing.
            if (!reachedResume[0]) {
                // If we didn't find the target via tree (e.g., pruning/partial tree), we must start real processing now.
                reachedResume[0] = true;
                fireResumeCallbackOnce(firedCallback, onResumePointReached);
            }

            NodeState childBefore = before.getChildNode(childName);
            NodeState childAfter = after.getChildNode(childName);
            Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
            if (childEditor != null) {
                CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                if (ex != null) {
                    return ex;
                }
            }
        }

        // Process children that exist in the real state but not in the tree's captured list (e.g., newly added).
        // Only relevant after resume point is reached.
        if (reachedResume[0]) {
            List<String> captured = tree.getChildNames();
            for (String childName : after.getChildNodeNames()) {
                if (captured != null && !captured.isEmpty() && captured.contains(childName)) {
                    continue;
                }
                NodeState childBefore = before.getChildNode(childName);
                NodeState childAfter = after.getChildNode(childName);
                Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
                if (childEditor != null) {
                    CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                    if (ex != null) {
                        return ex;
                    }
                }
            }
        }

        return null;
    }

    private static Iterable<String> getChildNames(@NotNull TraversalTree tree, @NotNull NodeState after) {
        List<String> captured = tree.getChildNames();
        if (captured != null && !captured.isEmpty()) {
            return captured;
        }
        // Fallback – may read from NodeStore (only happens when tree lacks metadata)
        return after.getChildNodeNames();
    }

    private static void fireResumeCallbackOnce(boolean[] fired, @Nullable ResumePointHandler cb) throws CommitFailedException {
        if (fired[0]) {
            return;
        }
        fired[0] = true;
        if (cb != null) {
            cb.onResumePointReached();
        }
    }

    @Nullable
    private static Editor getChildEditor(Editor editor, String name, NodeState before, NodeState after)
            throws CommitFailedException {
        if (!before.exists() && after.exists()) {
            return editor.childNodeAdded(name, after);
        } else if (before.exists() && !after.exists()) {
            return editor.childNodeDeleted(name, before);
        } else if (before.exists() && after.exists()) {
            return editor.childNodeChanged(name, before, after);
        }
        return null;
    }
}


