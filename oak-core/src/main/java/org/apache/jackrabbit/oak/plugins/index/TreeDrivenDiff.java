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

import java.util.ArrayList;
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
 * Tree-Driven Diff processor for resume operations.
 * 
 * <h2>Simplified Algorithm</h2>
 * This version navigates directly to the resume point using the in-memory tree,
 * then delegates to a custom diff that skips indexed siblings.
 * 
 * The key insight is that we can use the TraversalTree to know which siblings
 * have been indexed, and skip them without reading from SegmentStore.
 */
public class TreeDrivenDiff {
    
    private static final Logger log = LoggerFactory.getLogger(TreeDrivenDiff.class);
    private static final boolean DEBUG = Boolean.getBoolean("oak.async.treeDrivenDebug");
    
    /**
     * Statistics for performance tracking.
     */
    public static class Stats {
        public long virtualTraversalTimeMs;
        public long realTraversalTimeMs;
        public int nodesSkippedVirtual;
        public int nodesProcessedReal;
        public int editorEnterCalls;
        public int editorLeaveCalls;
    }
    
    private static void debug(String msg) {
        if (DEBUG) {
            System.out.println("[TDD] " + msg);
        }
    }
    
    /**
     * Resume from a saved path using direct navigation and smart sibling skipping.
     * 
     * This creates a ResumingEditor that uses the TraversalTree to skip indexed
     * siblings during traversal, then delegates to the real editor.
     */
    @Nullable
    public static CommitFailedException resume(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull TraversalTree tree,
            @NotNull String resumePath,
            @Nullable Stats stats,
            @Nullable Runnable onResumePointReached) throws CommitFailedException {
        
        long startTime = System.currentTimeMillis();
        
        String[] pathSegments = parsePath(resumePath);
        
        debug("=== TreeDrivenDiff.resume() START ===");
        debug("Resume path: " + resumePath);
        debug("Path segments: " + java.util.Arrays.toString(pathSegments));
        debug("Tree nodes: " + tree.countNodes() + ", indexed: " + tree.countIndexedNodes());
        
        if (pathSegments.length == 0) {
            debug("No resume path - using normal EditorDiff");
            if (onResumePointReached != null) {
                onResumePointReached.run();
            }
            return EditorDiff.process(editor, before, after);
        }
        
        // Create a tree-aware ResumingEditor that skips indexed siblings
        TreeAwareResumingEditor resumingEditor = new TreeAwareResumingEditor(
            editor, tree, resumePath, onResumePointReached, stats);
        
        CommitFailedException exception = EditorDiff.process(resumingEditor, before, after);
        
        long totalTime = System.currentTimeMillis() - startTime;
        debug("=== TreeDrivenDiff COMPLETE: " + totalTime + "ms ===");
        
        if (stats != null) {
            stats.virtualTraversalTimeMs = resumingEditor.skipTimeMs;
            stats.realTraversalTimeMs = totalTime - resumingEditor.skipTimeMs;
            stats.nodesSkippedVirtual = resumingEditor.nodesSkipped;
            stats.nodesProcessedReal = resumingEditor.nodesProcessed;
            stats.editorEnterCalls = resumingEditor.enterCalls;
            stats.editorLeaveCalls = resumingEditor.leaveCalls;
        }
        
        System.out.println("[TREE-DRIVEN] Complete: " + totalTime + "ms" +
            ", skipped=" + resumingEditor.nodesSkipped + 
            ", real=" + resumingEditor.nodesProcessed +
            ", enter=" + resumingEditor.enterCalls + 
            ", leave=" + resumingEditor.leaveCalls);
        
        return exception;
    }
    
    /**
     * Parse path into segments.
     */
    private static String[] parsePath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return new String[0];
        }
        return java.util.Arrays.stream(path.split("/"))
                               .filter(s -> !s.isEmpty())
                               .toArray(String[]::new);
    }
    
    /**
     * Editor that delegates enter/leave/childNode operations but skips property operations.
     * This allows Lucene to see all nodes while not re-indexing already-indexed content.
     */
    private static class SkipPropertiesEditor implements Editor {
        private final Editor delegate;
        
        SkipPropertiesEditor(Editor delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            delegate.enter(before, after);
        }
        
        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            delegate.leave(before, after);
        }
        
        @Override
        public void propertyAdded(org.apache.jackrabbit.oak.api.PropertyState after) {
            // Skip - already indexed
        }
        
        @Override
        public void propertyChanged(org.apache.jackrabbit.oak.api.PropertyState before, 
                org.apache.jackrabbit.oak.api.PropertyState after) {
            // Skip - already indexed
        }
        
        @Override
        public void propertyDeleted(org.apache.jackrabbit.oak.api.PropertyState before) {
            // Skip - already indexed
        }
        
        @Override
        @Nullable
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            Editor child = delegate.childNodeAdded(name, after);
            return child != null ? new SkipPropertiesEditor(child) : null;
        }
        
        @Override
        @Nullable
        public Editor childNodeChanged(String name, NodeState before, NodeState after) 
                throws CommitFailedException {
            Editor child = delegate.childNodeChanged(name, before, after);
            return child != null ? new SkipPropertiesEditor(child) : null;
        }
        
        @Override
        @Nullable
        public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            Editor child = delegate.childNodeDeleted(name, before);
            return child != null ? new SkipPropertiesEditor(child) : null;
        }
    }
    
    /**
     * A ResumingEditor that uses the TraversalTree to skip indexed siblings.
     */
    private static class TreeAwareResumingEditor implements Editor {
        private final Editor delegate;
        private final TraversalTree rootTree;
        private final String[] targetPath;
        private final Runnable onTargetReached;
        private final Stats stats;
        
        // Current state
        private int currentDepth = 0;
        private boolean targetReached = false;
        private TraversalTree currentTree;
        private List<String> pathSoFar = new ArrayList<>();
        
        // Statistics
        int nodesSkipped = 0;
        int nodesProcessed = 0;
        int enterCalls = 0;
        int leaveCalls = 0;
        long skipTimeMs = 0;
        
        TreeAwareResumingEditor(Editor delegate, TraversalTree tree, String resumePath, 
                Runnable onTargetReached, Stats stats) {
            this.delegate = delegate;
            this.rootTree = tree;
            this.targetPath = parsePath(resumePath);
            this.onTargetReached = onTargetReached;
            this.stats = stats;
            this.currentTree = tree;
        }
        
        private boolean isOnTargetPath(String name) {
            if (currentDepth >= targetPath.length) {
                return false;
            }
            return targetPath[currentDepth].equals(name);
        }
        
        private boolean isBeforeTargetOnPath(String name) {
            if (currentDepth >= targetPath.length) {
                return false;
            }
            // Check if this name comes before the target segment in iteration order
            String targetSegment = targetPath[currentDepth];
            
            // Use tree to determine order
            if (currentTree != null) {
                List<String> childNames = currentTree.getChildNames();
                int namePos = childNames.indexOf(name);
                int targetPos = childNames.indexOf(targetSegment);
                if (namePos >= 0 && targetPos >= 0) {
                    return namePos < targetPos;
                }
            }
            
            return false;
        }
        
        private boolean isIndexedInTree(String name) {
            if (currentTree == null) {
                return false;
            }
            TraversalTree childTree = currentTree.getChild(name);
            return childTree != null && childTree.isIndexed();
        }
        
        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            enterCalls++;
            debug("ROOT enter(), targetReached=" + targetReached);
            delegate.enter(before, after);
        }
        
        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            leaveCalls++;
            debug("ROOT leave(), targetReached=" + targetReached);
            delegate.leave(before, after);
        }
        
        @Override
        public void propertyAdded(org.apache.jackrabbit.oak.api.PropertyState after) 
                throws CommitFailedException {
            // Properties are only processed after target is reached
            if (targetReached) {
                delegate.propertyAdded(after);
            }
        }
        
        @Override
        public void propertyChanged(org.apache.jackrabbit.oak.api.PropertyState before, 
                org.apache.jackrabbit.oak.api.PropertyState after) throws CommitFailedException {
            if (targetReached) {
                delegate.propertyChanged(before, after);
            }
        }
        
        @Override
        public void propertyDeleted(org.apache.jackrabbit.oak.api.PropertyState before) 
                throws CommitFailedException {
            if (targetReached) {
                delegate.propertyDeleted(before);
            }
        }
        
        @Override
        @Nullable
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            long startSkip = System.nanoTime();
            
            try {
                // If we've reached the target, process all children normally
                if (targetReached) {
                    nodesProcessed++;
                    return delegate.childNodeAdded(name, after);
                }
                
                // Check if this child is on the path to the target
                if (isOnTargetPath(name)) {
                    debug("On target path: " + name + " at depth " + currentDepth);
                    
                    // Navigate into this child
                    Editor childDelegate = delegate.childNodeAdded(name, after);
                    if (childDelegate == null) {
                        return null;
                    }
                    
                    // Check if we've reached the target
                    if (currentDepth == targetPath.length - 1) {
                        debug("TARGET REACHED: " + name);
                        targetReached = true;
                        if (onTargetReached != null) {
                            onTargetReached.run();
                        }
                        nodesProcessed++;
                    }
                    
                    // Move tree pointer down
                    TraversalTree childTree = currentTree != null ? currentTree.getChild(name) : null;
                    
                    return new TreeAwareChildEditor(childDelegate, childTree, currentDepth + 1);
                }
                
                // Not on target path - check if we should skip this sibling
                if (isBeforeTargetOnPath(name) && isIndexedInTree(name)) {
                    // Skip this indexed sibling - it was already processed
                    // IMPORTANT: We still call delegate.childNodeAdded so Lucene sees the node
                    // But we return a SkipPropertiesEditor that won't index properties
                    debug("Skipping indexed sibling: " + name);
                    TraversalTree sibTree = currentTree != null ? currentTree.getChild(name) : null;
                    nodesSkipped += (sibTree != null ? sibTree.getCachedSubtreeSize() : 1);
                    
                    // Get delegate editor so Lucene sees this node
                    Editor sibDelegate = delegate.childNodeAdded(name, after);
                    if (sibDelegate == null) {
                        return null;
                    }
                    // Wrap with SkipPropertiesEditor to avoid re-indexing
                    return new SkipPropertiesEditor(sibDelegate);
                }
                
                // This is a sibling AFTER the target path, or an unindexed sibling before
                // Process it normally
                debug("Processing sibling: " + name + " (after target or unindexed)");
                nodesProcessed++;
                return delegate.childNodeAdded(name, after);
                
            } finally {
                skipTimeMs += (System.nanoTime() - startSkip) / 1_000_000;
            }
        }
        
        @Override
        @Nullable
        public Editor childNodeChanged(String name, NodeState before, NodeState after) 
                throws CommitFailedException {
            // For changed nodes, similar logic
            if (targetReached) {
                return delegate.childNodeChanged(name, before, after);
            }
            
            if (isOnTargetPath(name)) {
                Editor childDelegate = delegate.childNodeChanged(name, before, after);
                if (childDelegate == null) {
                    return null;
                }
                
                if (currentDepth == targetPath.length - 1) {
                    targetReached = true;
                    if (onTargetReached != null) {
                        onTargetReached.run();
                    }
                }
                
                TraversalTree childTree = currentTree != null ? currentTree.getChild(name) : null;
                return new TreeAwareChildEditor(childDelegate, childTree, currentDepth + 1);
            }
            
            // Skip indexed siblings before target
            if (isBeforeTargetOnPath(name) && isIndexedInTree(name)) {
                nodesSkipped++;
                return null;
            }
            
            return delegate.childNodeChanged(name, before, after);
        }
        
        @Override
        @Nullable
        public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            if (targetReached) {
                return delegate.childNodeDeleted(name, before);
            }
            return null;  // Don't process deletions during resume
        }
        
        /**
         * Child editor that maintains the tree navigation state.
         */
        private class TreeAwareChildEditor implements Editor {
            private final Editor childDelegate;
            private final TraversalTree childTree;
            private final int childDepth;
            
            TreeAwareChildEditor(Editor delegate, TraversalTree tree, int depth) {
                this.childDelegate = delegate;
                this.childTree = tree;
                this.childDepth = depth;
            }
            
            @Override
            public void enter(NodeState before, NodeState after) throws CommitFailedException {
                enterCalls++;
                debug("CHILD enter() depth=" + childDepth + ", targetReached=" + targetReached);
                childDelegate.enter(before, after);
            }
            
            @Override
            public void leave(NodeState before, NodeState after) throws CommitFailedException {
                leaveCalls++;
                debug("CHILD leave() depth=" + childDepth + ", targetReached=" + targetReached);
                childDelegate.leave(before, after);
            }
            
            @Override
            public void propertyAdded(org.apache.jackrabbit.oak.api.PropertyState after) 
                    throws CommitFailedException {
                if (targetReached) {
                    childDelegate.propertyAdded(after);
                }
            }
            
            @Override
            public void propertyChanged(org.apache.jackrabbit.oak.api.PropertyState before, 
                    org.apache.jackrabbit.oak.api.PropertyState after) throws CommitFailedException {
                if (targetReached) {
                    childDelegate.propertyChanged(before, after);
                }
            }
            
            @Override
            public void propertyDeleted(org.apache.jackrabbit.oak.api.PropertyState before) 
                    throws CommitFailedException {
                if (targetReached) {
                    childDelegate.propertyDeleted(before);
                }
            }
            
            @Override
            @Nullable
            public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
                // If target reached, process normally
                if (targetReached) {
                    nodesProcessed++;
                    return childDelegate.childNodeAdded(name, after);
                }
                
                // Check if on target path
                if (childDepth < targetPath.length && targetPath[childDepth].equals(name)) {
                    debug("Child on target path: " + name + " at depth " + childDepth);
                    
                    Editor grandchildDelegate = childDelegate.childNodeAdded(name, after);
                    if (grandchildDelegate == null) {
                        return null;
                    }
                    
                    // Check if this is the target
                    if (childDepth == targetPath.length - 1) {
                        debug("TARGET REACHED in child: " + name);
                        targetReached = true;
                        if (onTargetReached != null) {
                            onTargetReached.run();
                        }
                        nodesProcessed++;
                    }
                    
                    TraversalTree grandchildTree = childTree != null ? childTree.getChild(name) : null;
                    return new TreeAwareChildEditor(grandchildDelegate, grandchildTree, childDepth + 1);
                }
                
                // Check if should skip
                String targetSegment = childDepth < targetPath.length ? targetPath[childDepth] : null;
                if (targetSegment != null && childTree != null) {
                    List<String> childNames = childTree.getChildNames();
                    int namePos = childNames.indexOf(name);
                    int targetPos = childNames.indexOf(targetSegment);
                    
                    if (namePos >= 0 && targetPos >= 0 && namePos < targetPos) {
                        // Before target - check if indexed
                        TraversalTree sibTree = childTree.getChild(name);
                        if (sibTree != null && sibTree.isIndexed()) {
                            debug("Skipping indexed: " + name);
                            nodesSkipped += sibTree.getCachedSubtreeSize();
                            // Use SkipPropertiesEditor instead of returning null
                            Editor sibDelegate = childDelegate.childNodeAdded(name, after);
                            return sibDelegate != null ? new SkipPropertiesEditor(sibDelegate) : null;
                        }
                    }
                }
                
                // Process normally
                nodesProcessed++;
                return childDelegate.childNodeAdded(name, after);
            }
            
            @Override
            @Nullable
            public Editor childNodeChanged(String name, NodeState before, NodeState after) 
                    throws CommitFailedException {
                if (targetReached) {
                    return childDelegate.childNodeChanged(name, before, after);
                }
                
                // Similar logic as childNodeAdded
                if (childDepth < targetPath.length && targetPath[childDepth].equals(name)) {
                    Editor grandchildDelegate = childDelegate.childNodeChanged(name, before, after);
                    if (grandchildDelegate == null) {
                        return null;
                    }
                    
                    if (childDepth == targetPath.length - 1) {
                        targetReached = true;
                        if (onTargetReached != null) {
                            onTargetReached.run();
                        }
                    }
                    
                    TraversalTree grandchildTree = childTree != null ? childTree.getChild(name) : null;
                    return new TreeAwareChildEditor(grandchildDelegate, grandchildTree, childDepth + 1);
                }
                
                return childDelegate.childNodeChanged(name, before, after);
            }
            
            @Override
            @Nullable
            public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
                if (targetReached) {
                    return childDelegate.childNodeDeleted(name, before);
                }
                return null;
            }
        }
    }
}
