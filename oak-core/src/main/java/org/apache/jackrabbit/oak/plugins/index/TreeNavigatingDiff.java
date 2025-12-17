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

import java.util.Arrays;

/**
 * Tree-navigating diff that uses TraversalTree's indexed flags to skip SegmentStore reads.
 * 
 * Key insight:
 * - Nodes with indexed=true in tree have been FULLY processed (leave() called)
 * - These nodes don't need SegmentStore reads - we can skip them entirely
 * - For unvisited children (not in tree), we iterate SegmentStore but only read content for unindexed ones
 * 
 * Flow:
 * 1. For each child in SegmentStore:
 *    - Check if it exists in tree
 *    - If in tree AND indexed=true → SKIP (no SegmentStore read!)
 *    - Otherwise → need to process (read from SegmentStore)
 * 2. Also use resume path to skip siblings BEFORE current path segment
 * 
 * This eliminates SegmentStore reads for already-indexed subtrees!
 */
public class TreeNavigatingDiff {
    
    private static final Logger log = LoggerFactory.getLogger(TreeNavigatingDiff.class);
    
    /**
     * Process diff using tree for navigation, avoiding SegmentStore reads for indexed nodes.
     * 
     * @param editor the editor to apply changes to
     * @param before the before state
     * @param after the after state  
     * @param tree the traversal tree with indexed flags
     * @param resumePath the path to resume from (e.g., "/content/dam/asset-5")
     * @param onResumePathReached callback when we start processing from resume point
     * @return exception if any, null on success
     */
    @Nullable
    public static CommitFailedException process(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull TraversalTree tree,
            @NotNull String resumePath,
            @Nullable Runnable onResumePathReached) throws CommitFailedException {
        
        long startTime = System.currentTimeMillis();
        long[] stats = new long[3]; // [nodesSkipped, nodesProcessed, segmentReads]
        
        // Parse resume path into segments
        String[] pathSegments = parseResumePath(resumePath);
        
        try {
            // Call enter on root
            editor.enter(before, after);
            
            // Process children using path-guided navigation
            processChildrenWithPath(editor, before, after, tree, pathSegments, 0, stats, onResumePathReached);
            
            // Call leave on root
            editor.leave(before, after);
            
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("[TREE-NAV-DIFF] Completed in " + totalTime + "ms: " +
                "skipped=" + stats[0] + ", processed=" + stats[1] + ", segmentReads=" + stats[2]);
            
            return null;
            
        } catch (CommitFailedException e) {
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("[TREE-NAV-DIFF] Exception after " + totalTime + "ms: " +
                "skipped=" + stats[0] + ", processed=" + stats[1] + ", segmentReads=" + stats[2] +
                " - " + e.getMessage());
            return e;
        }
    }
    
    /**
     * Parse resume path into segments, filtering empty strings.
     */
    private static String[] parseResumePath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return new String[0];
        }
        return Arrays.stream(path.split("/"))
                     .filter(s -> !s.isEmpty())
                     .toArray(String[]::new);
    }
    
    /**
     * Process children using path-guided navigation.
     * 
     * Two-phase skip approach:
     * 1. If we have a path segment: skip siblings BEFORE it in iteration order
     * 2. For any child: if tree says indexed=true, SKIP (already fully processed)
     */
    private static void processChildrenWithPath(
            Editor editor,
            NodeState before,
            NodeState after,
            TraversalTree tree,
            String[] pathSegments,
            int pathIndex,
            long[] stats,
            Runnable onResumePathReached) throws CommitFailedException {
        
        boolean hasPathSegment = pathIndex < pathSegments.length;
        String targetSegment = hasPathSegment ? pathSegments[pathIndex] : null;
        boolean reachedTarget = !hasPathSegment; // If no target, we've already "reached" it
        boolean calledResumeCallback = false;
        
        // Iterate children from NodeStore to ensure we see ALL children
        // (tree might be incomplete - only has nodes visited before CHUNK_COMPLETE)
        for (String childName : after.getChildNodeNames()) {
            
            // Check if we should skip based on resume path position
            if (!reachedTarget) {
                if (childName.equals(targetSegment)) {
                    // Found target segment - will navigate into it
                    reachedTarget = true;
                } else {
                    // Before target in iteration order
                    // ONLY skip if tree confirms it was indexed!
                    // If not in tree, it might be a new node that needs processing
                    TraversalTree childTree2 = tree.getChild(childName);
                    if (childTree2 != null && childTree2.isIndexed()) {
                        // Tree confirms indexed - safe to skip!
                        stats[0] += childTree2.getCachedSubtreeSize();
                        continue;
                    }
                    // Not in tree or not indexed - can't skip, need to process
                }
            }
            
            // Check if tree says this child is fully indexed (can skip!)
            TraversalTree childTree = tree.getChild(childName);
            if (childTree != null && childTree.isIndexed()) {
                // Tree says fully indexed - SKIP without SegmentStore read!
                stats[0] += childTree.getCachedSubtreeSize();
                // Debug: track which asset nodes are skipped
                if (childName.startsWith("asset-")) {
                    System.out.println("[TREE-NAV-SKIP-INDEXED] " + childName);
                }
                continue;
            }
            
            // Debug: track which asset nodes are processed
            if (childName.startsWith("asset-")) {
                System.out.println("[TREE-NAV-PROCESS] " + childName + " (in_tree=" + (childTree != null) + ", indexed=" + (childTree != null ? childTree.isIndexed() : "N/A") + ")");
            }
            
            // Need to process this child - read from SegmentStore
            stats[2]++;
            NodeState childBefore = before.getChildNode(childName);
            NodeState childAfter = after.getChildNode(childName);
            
            Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
            if (childEditor == null) {
                continue;
            }
            
            stats[1]++;
            
            // Call resume callback when we reach the resume point
            boolean isTargetSegment = hasPathSegment && childName.equals(targetSegment);
            boolean isLastSegment = (pathIndex == pathSegments.length - 1);
            
            if (isTargetSegment && isLastSegment && !calledResumeCallback && onResumePathReached != null) {
                onResumePathReached.run();
                calledResumeCallback = true;
            }
            
            if (isTargetSegment && !isLastSegment) {
                // Not at leaf yet - continue navigating into resume path
                // We manage enter/leave ourselves since we're doing custom traversal
                childEditor.enter(childBefore, childAfter);
                
                // Process properties on this node
                compareProperties(childEditor, childBefore, childAfter);
                
                // Only recurse into the target child
                TraversalTree subtree = (childTree != null) ? childTree : new TraversalTree();
                processChildrenWithPath(childEditor, childBefore, childAfter, subtree,
                                        pathSegments, pathIndex + 1, stats, onResumePathReached);
                
                childEditor.leave(childBefore, childAfter);
            } else {
                // At or past resume point - use standard EditorDiff for full processing
                // NOTE: EditorDiff.process() handles enter/leave internally, don't call them here!
                CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                if (ex != null) {
                    throw ex;
                }
            }
        }
    }
    
    /**
     * Compare properties between before and after states, calling editor methods.
     */
    private static void compareProperties(
            Editor editor,
            NodeState before,
            NodeState after) throws CommitFailedException {
        
        // Check for added/changed properties
        for (org.apache.jackrabbit.oak.api.PropertyState afterProperty : after.getProperties()) {
            String name = afterProperty.getName();
            org.apache.jackrabbit.oak.api.PropertyState beforeProperty = before.getProperty(name);
            if (beforeProperty == null) {
                editor.propertyAdded(afterProperty);
            } else if (!beforeProperty.equals(afterProperty)) {
                editor.propertyChanged(beforeProperty, afterProperty);
            }
        }
        
        // Check for deleted properties
        for (org.apache.jackrabbit.oak.api.PropertyState beforeProperty : before.getProperties()) {
            if (!after.hasProperty(beforeProperty.getName())) {
                editor.propertyDeleted(beforeProperty);
            }
        }
    }
    
    /**
     * Get child editor based on change type.
     */
    @Nullable
    private static Editor getChildEditor(
            Editor editor, 
            String childName,
            NodeState childBefore,
            NodeState childAfter) throws CommitFailedException {
        
        if (!childBefore.exists() && childAfter.exists()) {
            return editor.childNodeAdded(childName, childAfter);
        } else if (childBefore.exists() && !childAfter.exists()) {
            return editor.childNodeDeleted(childName, childBefore);
        } else if (childBefore.exists() && childAfter.exists()) {
            return editor.childNodeChanged(childName, childBefore, childAfter);
        }
        return null;
    }
}
