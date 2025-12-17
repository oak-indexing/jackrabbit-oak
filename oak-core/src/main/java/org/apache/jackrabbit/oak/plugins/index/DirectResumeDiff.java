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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Direct navigation diff processor for resume operations.
 * 
 * <h2>Key Insight</h2>
 * When resuming from a saved path, we don't need to traverse all previously
 * indexed nodes. We can navigate DIRECTLY to the resume point in O(depth)
 * instead of O(indexed_nodes).
 * 
 * <h2>Algorithm</h2>
 * <pre>
 * Given resumePath = "/content/dam/asset-500/jcr:content"
 * 
 * 1. Parse path: ["content", "dam", "asset-500", "jcr:content"]
 * 
 * 2. Direct descent - for each segment:
 *    - Call enter() on current editor
 *    - Skip properties (already indexed)
 *    - Get child editor for next segment
 *    - Track editor stack for proper leave() calls
 * 
 * 3. At resume point:
 *    - Switch to full EditorDiff for remaining siblings
 *    - Continue normal DFS from this point
 * 
 * 4. Unwind stack - call leave() on each editor in reverse
 * </pre>
 * 
 * <h2>Performance</h2>
 * - Traditional resume: O(indexed_nodes) - visits every indexed node
 * - DirectResumeDiff: O(depth) + O(remaining_nodes)
 * 
 * For a tree with 50,000 indexed nodes and depth 4:
 * - Traditional: 50,000 enter/leave calls
 * - Direct: 4 enter/leave calls + remaining work
 */
public class DirectResumeDiff {
    
    private static final Logger log = LoggerFactory.getLogger(DirectResumeDiff.class);
    
    /**
     * Statistics holder for performance tracking.
     */
    public static class Stats {
        public long directNavigationTimeMs;
        public long remainingTraversalTimeMs;
        public int nodesSkipped;
        public int nodesProcessed;
        public int editorEnterCalls;
    }
    
    /**
     * Resume from a saved path using direct navigation.
     * 
     * @param editor the root editor
     * @param before the before state
     * @param after the after state
     * @param tree the traversal tree with indexed/unindexed info
     * @param resumePath path to resume from (e.g., "/content/dam/asset-500")
     * @param stats optional stats collector
     * @return exception if any, null on success
     */
    @Nullable
    public static CommitFailedException resume(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull TraversalTree tree,
            @NotNull String resumePath,
            @Nullable Stats stats) throws CommitFailedException {
        
        long startTime = System.currentTimeMillis();
        
        // Parse resume path
        String[] pathSegments = parsePath(resumePath);
        
        if (pathSegments.length == 0) {
            // No resume path - just do normal diff
            return EditorDiff.process(editor, before, after);
        }
        
        log.info("[DIRECT-RESUME] Starting direct navigation to: {} (depth={})", 
                resumePath, pathSegments.length);
        System.out.println("[DIRECT-RESUME] Navigating directly to: " + resumePath + 
                " (depth=" + pathSegments.length + ")");
        
        // Stack of editors for proper leave() unwinding
        Deque<EditorFrame> editorStack = new ArrayDeque<>();
        
        try {
            // Phase 1: Direct descent to resume point
            NodeState currentBefore = before;
            NodeState currentAfter = after;
            Editor currentEditor = editor;
            TraversalTree currentTree = tree;
            
            // Enter root
            currentEditor.enter(MISSING_NODE, currentAfter);
            editorStack.push(new EditorFrame(currentEditor, MISSING_NODE, currentAfter, ""));
            if (stats != null) stats.editorEnterCalls++;
            
            // Navigate down the path - only initialize editors on the path
            for (int i = 0; i < pathSegments.length; i++) {
                String segment = pathSegments[i];
                boolean isLastSegment = (i == pathSegments.length - 1);
                
                // Get child states
                NodeState childBefore = currentBefore.getChildNode(segment);
                NodeState childAfter = currentAfter.getChildNode(segment);
                
                // Get child editor
                Editor childEditor = getChildEditor(currentEditor, segment, childBefore, childAfter);
                
                if (childEditor == null) {
                    log.warn("[DIRECT-RESUME] No editor for child: {}", segment);
                    break;
                }
                
                // Enter the child
                childEditor.enter(childBefore, childAfter);
                editorStack.push(new EditorFrame(childEditor, childBefore, childAfter, segment));
                if (stats != null) stats.editorEnterCalls++;
                
                // Move to next level
                currentBefore = childBefore;
                currentAfter = childAfter;
                currentEditor = childEditor;
                
                if (currentTree != null) {
                    currentTree = currentTree.getChild(segment);
                }
                
                // Track skipped nodes (all indexed siblings before this one)
                if (stats != null && currentTree != null) {
                    // Siblings that were indexed are skipped
                    stats.nodesSkipped += countIndexedSiblingsBefore(tree, pathSegments, i);
                }
            }
            
            long navigationTime = System.currentTimeMillis() - startTime;
            if (stats != null) stats.directNavigationTimeMs = navigationTime;
            
            log.info("[DIRECT-RESUME] Reached resume point in {}ms with {} enter() calls", 
                    navigationTime, editorStack.size());
            System.out.println("[DIRECT-RESUME] Reached target in " + navigationTime + 
                    "ms, " + editorStack.size() + " editor levels");
            
            // Phase 2: Process remaining siblings at resume point
            long traversalStart = System.currentTimeMillis();
            
            // Get the resume point's parent and process remaining siblings
            CommitFailedException result = processRemainingSiblings(
                    editorStack, pathSegments, before, after, tree, stats);
            
            if (result != null) {
                return result;
            }
            
            if (stats != null) {
                stats.remainingTraversalTimeMs = System.currentTimeMillis() - traversalStart;
            }
            
            // Phase 3: Unwind the stack - leave all editors in reverse order
            while (!editorStack.isEmpty()) {
                EditorFrame frame = editorStack.pop();
                frame.editor.leave(frame.before, frame.after);
            }
            
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("[DIRECT-RESUME] Complete in {}ms (nav={}ms, traverse={}ms)", 
                    totalTime, 
                    stats != null ? stats.directNavigationTimeMs : "?",
                    stats != null ? stats.remainingTraversalTimeMs : "?");
            System.out.println("[DIRECT-RESUME] Total time: " + totalTime + "ms");
            
            return null;
            
        } catch (CommitFailedException e) {
            // On exception, still need to unwind the stack
            while (!editorStack.isEmpty()) {
                EditorFrame frame = editorStack.pop();
                try {
                    frame.editor.leave(frame.before, frame.after);
                } catch (Exception ignored) {
                    // Best effort cleanup
                }
            }
            return e;
        }
    }
    
    /**
     * Process remaining siblings after the resume point.
     */
    private static CommitFailedException processRemainingSiblings(
            Deque<EditorFrame> editorStack,
            String[] pathSegments,
            NodeState rootBefore,
            NodeState rootAfter,
            TraversalTree tree,
            Stats stats) throws CommitFailedException {
        
        // Navigate to each level and process remaining children
        // Start from deepest level (resume point) and work up
        
        // The last entry in stack is the resume point
        // We need to process:
        // 1. Remaining children of resume point
        // 2. Remaining siblings at each parent level
        
        // Convert stack to array for index-based access
        EditorFrame[] frames = editorStack.toArray(new EditorFrame[0]);
        
        // Process from resume point level upward
        for (int level = frames.length - 1; level >= 0; level--) {
            EditorFrame frame = frames[level];
            
            // Get the tree node for this level
            TraversalTree treeNode = tree;
            for (int i = 0; i < level && i < pathSegments.length; i++) {
                if (treeNode != null) {
                    treeNode = treeNode.getChild(pathSegments[i]);
                }
            }
            
            // At resume point level - process all children that weren't indexed
            if (level == frames.length - 1) {
                // Process properties first (they may not have been done)
                NodeState afterState = frame.after;
                for (PropertyState prop : afterState.getProperties()) {
                    frame.editor.propertyAdded(prop);
                }
                
                // Then process all children
                for (String childName : afterState.getChildNodeNames()) {
                    NodeState childBefore = frame.before.getChildNode(childName);
                    NodeState childAfter = afterState.getChildNode(childName);
                    
                    // Check if already indexed via tree
                    boolean alreadyIndexed = false;
                    if (treeNode != null) {
                        TraversalTree childTree = treeNode.getChild(childName);
                        alreadyIndexed = childTree != null && childTree.isIndexed();
                    }
                    
                    if (alreadyIndexed) {
                        if (stats != null) stats.nodesSkipped++;
                        continue;
                    }
                    
                    // Process this child with full EditorDiff
                    if (stats != null) stats.nodesProcessed++;
                    Editor childEditor = getChildEditor(frame.editor, childName, childBefore, childAfter);
                    if (childEditor != null) {
                        CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                        if (ex != null) return ex;
                    }
                }
            } else {
                // At parent levels - process siblings AFTER the path we came from
                String pathChild = pathSegments[level];
                boolean foundPathChild = false;
                
                NodeState afterState = frame.after;
                for (String childName : afterState.getChildNodeNames()) {
                    if (childName.equals(pathChild)) {
                        foundPathChild = true;
                        continue; // Skip - this is on our descent path
                    }
                    
                    if (!foundPathChild) {
                        // Sibling before our path - should be indexed already
                        if (stats != null) stats.nodesSkipped++;
                        continue;
                    }
                    
                    // Sibling after our path - needs processing
                    NodeState childBefore = frame.before.getChildNode(childName);
                    NodeState childAfter = afterState.getChildNode(childName);
                    
                    // Check tree for indexed status
                    boolean alreadyIndexed = false;
                    if (treeNode != null) {
                        TraversalTree childTree = treeNode.getChild(childName);
                        alreadyIndexed = childTree != null && childTree.isIndexed();
                    }
                    
                    if (alreadyIndexed) {
                        if (stats != null) stats.nodesSkipped++;
                        continue;
                    }
                    
                    // Process with full EditorDiff
                    if (stats != null) stats.nodesProcessed++;
                    Editor childEditor = getChildEditor(frame.editor, childName, childBefore, childAfter);
                    if (childEditor != null) {
                        CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                        if (ex != null) return ex;
                    }
                }
            }
        }
        
        return null;
    }
    
    /**
     * Count indexed siblings before the given path segment.
     */
    private static int countIndexedSiblingsBefore(TraversalTree tree, String[] path, int level) {
        TraversalTree parent = tree;
        for (int i = 0; i < level; i++) {
            parent = parent.getChild(path[i]);
            if (parent == null) return 0;
        }
        
        String targetName = path[level];
        int count = 0;
        
        for (java.util.Map.Entry<String, TraversalTree> entry : getChildEntries(parent)) {
            if (entry.getKey().equals(targetName)) {
                break; // Stop at target
            }
            if (entry.getValue().isIndexed()) {
                count += entry.getValue().getCachedSubtreeSize();
            }
        }
        
        return count;
    }
    
    /**
     * Helper to get child entries from tree (accessing internal LinkedHashMap).
     */
    @SuppressWarnings("unchecked")
    private static Iterable<java.util.Map.Entry<String, TraversalTree>> getChildEntries(TraversalTree tree) {
        // Use reflection or add a method to TraversalTree
        // For now, iterate through children
        java.util.List<java.util.Map.Entry<String, TraversalTree>> entries = new java.util.ArrayList<>();
        java.util.Iterator<TraversalTree> it = tree.childrenIterator();
        while (it.hasNext()) {
            TraversalTree child = it.next();
            entries.add(new java.util.AbstractMap.SimpleEntry<>(child.getName(), child));
        }
        return entries;
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
     * Get child editor based on node existence.
     */
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
    
    /**
     * Frame to track editor state for stack unwinding.
     */
    private static class EditorFrame {
        final Editor editor;
        final NodeState before;
        final NodeState after;
        final String name;
        
        EditorFrame(Editor editor, NodeState before, NodeState after, String name) {
            this.editor = editor;
            this.before = before;
            this.after = after;
            this.name = name;
        }
    }
}

