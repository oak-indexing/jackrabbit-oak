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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Tree-driven indexer that uses the in-memory TraversalTree to navigate
 * directly to the resume point WITHOUT traversing the NodeStore.
 * 
 * The key insight:
 * - Traditional approach: EditorDiff traverses ALL nodes → O(all_nodes) NodeStore reads
 * - Tree-driven approach: Use tree to navigate → O(depth) NodeStore reads
 * 
 * Flow:
 * 1. Load TraversalTree from previous chunk
 * 2. Navigate to resume path using tree (NO NodeStore reads for indexed nodes!)
 * 3. Build minimal editor hierarchy for path segments only
 * 4. At resume point, switch to normal EditorDiff for REMAINING nodes
 * 5. Continue building tree during processing
 * 6. On chunk limit, save tree for next iteration
 */
public class TreeDrivenIndexer {
    
    private static final Logger log = LoggerFactory.getLogger(TreeDrivenIndexer.class);
    
    /**
     * Result of tree-driven navigation.
     */
    public static class NavigationResult {
        public final boolean success;
        public final NodeState beforeAtResume;
        public final NodeState afterAtResume;
        public final Editor editorAtResume;
        public final TraversalTree treeNodeAtResume;
        public final List<EditorContext> editorStack;  // For unwinding on leave()
        public final long nodesSkipped;
        public final long navigationTimeMs;
        
        public NavigationResult(boolean success, NodeState beforeAtResume, NodeState afterAtResume, 
                               Editor editorAtResume, TraversalTree treeNodeAtResume,
                               List<EditorContext> editorStack, long nodesSkipped, long navigationTimeMs) {
            this.success = success;
            this.beforeAtResume = beforeAtResume;
            this.afterAtResume = afterAtResume;
            this.editorAtResume = editorAtResume;
            this.treeNodeAtResume = treeNodeAtResume;
            this.editorStack = editorStack;
            this.nodesSkipped = nodesSkipped;
            this.navigationTimeMs = navigationTimeMs;
        }
    }
    
    /**
     * Context for an editor at a specific level - used for unwinding.
     */
    public static class EditorContext {
        public final Editor editor;
        public final NodeState before;
        public final NodeState after;
        public final TraversalTree treeNode;
        
        public EditorContext(Editor editor, NodeState before, NodeState after, TraversalTree treeNode) {
            this.editor = editor;
            this.before = before;
            this.after = after;
            this.treeNode = treeNode;
        }
    }
    
    /**
     * Navigate to resume path using the TraversalTree.
     * 
     * This method:
     * 1. Uses tree to identify path segments to resume point
     * 2. Reads ONLY those path segments from NodeStore (O(depth) reads!)
     * 3. Builds minimal editor hierarchy
     * 4. Returns state at resume point for continued processing
     * 
     * @param rootEditor the root editor to start with
     * @param rootBefore the before state (source checkpoint)
     * @param rootAfter the after state (target checkpoint)
     * @param traversalTree the in-memory tree from previous chunk
     * @param resumePath the path to navigate to
     * @return NavigationResult with state at resume point, or null if failed
     */
    @Nullable
    public static NavigationResult navigateToResumePath(
            @NotNull Editor rootEditor,
            @NotNull NodeState rootBefore,
            @NotNull NodeState rootAfter,
            @NotNull TraversalTree traversalTree,
            @NotNull String resumePath) throws CommitFailedException {
        
        long startTime = System.currentTimeMillis();
        long nodesSkipped = 0;
        
        if (resumePath == null || resumePath.isEmpty() || "/".equals(resumePath)) {
            // No resume - start from root
            return new NavigationResult(true, rootBefore, rootAfter, rootEditor, traversalTree,
                    new ArrayList<>(), 0, System.currentTimeMillis() - startTime);
        }
        
        // Parse path into segments
        String[] segments = resumePath.split("/");
        List<String> pathSegments = new ArrayList<>();
        for (String s : segments) {
            if (!s.isEmpty()) pathSegments.add(s);
        }
        
        if (pathSegments.isEmpty()) {
            return new NavigationResult(true, rootBefore, rootAfter, rootEditor, traversalTree,
                    new ArrayList<>(), 0, System.currentTimeMillis() - startTime);
        }
        
        log.info("[TREE-NAV] Navigating to {} ({} segments) using tree", resumePath, pathSegments.size());
        System.out.println("[TREE-NAV] Starting navigation to: " + resumePath);
        
        // Track editor stack for unwinding
        List<EditorContext> editorStack = new ArrayList<>();
        
        // Current state as we navigate
        NodeState currentBefore = rootBefore;
        NodeState currentAfter = rootAfter;
        Editor currentEditor = rootEditor;
        TraversalTree currentTreeNode = traversalTree;
        
        try {
            // Call enter on root
            currentEditor.enter(currentBefore, currentAfter);
            editorStack.add(new EditorContext(currentEditor, currentBefore, currentAfter, currentTreeNode));
            
            // Navigate through each path segment
            for (int i = 0; i < pathSegments.size(); i++) {
                String segment = pathSegments.get(i);
                boolean isLastSegment = (i == pathSegments.size() - 1);
                
                // Get tree node for this segment (if exists)
                TraversalTree childTreeNode = currentTreeNode != null ? currentTreeNode.getChild(segment) : null;
                
                // Count indexed siblings we're skipping (for metrics)
                if (currentTreeNode != null) {
                    for (TraversalTree sibling : iterateChildren(currentTreeNode)) {
                        if (sibling.isIndexed() && !sibling.getName().equals(segment)) {
                            nodesSkipped += sibling.getCachedSubtreeSize();
                        }
                    }
                }
                
                // Read ONLY this segment from NodeStore (O(1) per segment = O(depth) total!)
                NodeState childBefore = currentBefore.getChildNode(segment);
                NodeState childAfter = currentAfter.getChildNode(segment);
                
                // Determine if this is added, changed, or deleted
                Editor childEditor;
                if (!childBefore.exists() && childAfter.exists()) {
                    childEditor = currentEditor.childNodeAdded(segment, childAfter);
                } else if (childBefore.exists() && !childAfter.exists()) {
                    childEditor = currentEditor.childNodeDeleted(segment, childBefore);
                } else if (childBefore.exists() && childAfter.exists()) {
                    childEditor = currentEditor.childNodeChanged(segment, childBefore, childAfter);
                } else {
                    // Neither exists - path doesn't exist
                    log.warn("[TREE-NAV] Path segment {} doesn't exist at depth {}", segment, i);
                    return null;
                }
                
                if (childEditor == null) {
                    // Editor doesn't care about this path
                    log.info("[TREE-NAV] Editor returned null for segment {}", segment);
                    return null;
                }
                
                // Move to child
                currentBefore = childBefore;
                currentAfter = childAfter;
                currentEditor = childEditor;
                currentTreeNode = childTreeNode;
                
                // Call enter on child
                currentEditor.enter(currentBefore, currentAfter);
                editorStack.add(new EditorContext(currentEditor, currentBefore, currentAfter, currentTreeNode));
                
                if (!isLastSegment) {
                    log.debug("[TREE-NAV] Navigated to segment {}/{}: {}", i + 1, pathSegments.size(), segment);
                }
            }
            
            long navigationTime = System.currentTimeMillis() - startTime;
            log.info("[TREE-NAV] Reached {} in {}ms, skipped {} indexed nodes (0 NodeStore reads for skipped!)",
                    resumePath, navigationTime, nodesSkipped);
            System.out.println("[TREE-NAV] SUCCESS! Reached " + resumePath + " in " + navigationTime + 
                    "ms, skipped " + nodesSkipped + " indexed nodes");
            
            return new NavigationResult(true, currentBefore, currentAfter, currentEditor, currentTreeNode,
                    editorStack, nodesSkipped, navigationTime);
            
        } catch (CommitFailedException e) {
            log.error("[TREE-NAV] Failed to navigate to {}: {}", resumePath, e.getMessage());
            throw e;
        }
    }
    
    /**
     * Continue processing from the resume point.
     * 
     * After navigating to resume point:
     * 1. Process remaining children of current node (siblings of next path segment)
     * 2. Use TraversalTree to SKIP indexed siblings (no NodeStore read!)
     * 3. Use normal EditorDiff for unindexed siblings
     * 
     * @param navResult result from navigateToResumePath
     * @param trackingEditor editor that tracks tree (wraps actual editor)
     * @return exception if any, null on success
     */
    @Nullable
    public static CommitFailedException continueFromResumePoint(
            @NotNull NavigationResult navResult,
            @NotNull TraversalTrackingEditor trackingEditor) throws CommitFailedException {
        
        if (!navResult.success) {
            return null;
        }
        
        long startTime = System.currentTimeMillis();
        
        // The current editor is at the resume point (parent of where we need to continue)
        // We need to process remaining siblings
        
        NodeState before = navResult.beforeAtResume;
        NodeState after = navResult.afterAtResume;
        Editor editor = navResult.editorAtResume;
        TraversalTree treeNode = navResult.treeNodeAtResume;
        
        System.out.println("[TREE-NAV] Continuing with EditorDiff from resume point...");
        
        // Use EditorDiff to process remaining children
        // The TraversalTreeSkippingEditor will skip indexed children
        CommitFailedException exception = EditorDiff.process(editor, before, after);
        
        // Call leave() on the editor at resume point
        if (exception == null) {
            editor.leave(before, after);
        }
        
        long processTime = System.currentTimeMillis() - startTime;
        System.out.println("[TREE-NAV] EditorDiff processing took " + processTime + "ms");
        
        return exception;
    }
    
    /**
     * Unwind the editor stack by calling leave() on each editor.
     * Called after processing is complete or on error.
     */
    public static void unwindEditorStack(List<EditorContext> editorStack) throws CommitFailedException {
        // Unwind in reverse order (leaf to root)
        for (int i = editorStack.size() - 1; i >= 0; i--) {
            EditorContext ctx = editorStack.get(i);
            ctx.editor.leave(ctx.before, ctx.after);
        }
    }
    
    // Helper to iterate children
    private static Iterable<TraversalTree> iterateChildren(TraversalTree node) {
        List<TraversalTree> children = new ArrayList<>();
        java.util.Iterator<TraversalTree> it = node.childrenIterator();
        while (it.hasNext()) {
            children.add(it.next());
        }
        return children;
    }
}

