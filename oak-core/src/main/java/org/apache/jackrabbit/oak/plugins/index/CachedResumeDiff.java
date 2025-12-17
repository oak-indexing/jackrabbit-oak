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
 * Optimized diff processor that uses CachedNodeInfo to ELIMINATE SegmentStore reads during skip phase.
 * 
 * <h2>Key Insight</h2>
 * During the skip phase (traversing from root to resume point), the indexing editor only needs:
 * <ul>
 *   <li>Node type (jcr:primaryType) - for index rule matching</li>
 *   <li>Child names in iteration order - for consistent DFS traversal</li>
 * </ul>
 * 
 * We DON'T need:
 * <ul>
 *   <li>Actual property values (ResumingEditor skips propertyAdded/Changed during skip)</li>
 *   <li>Full NodeState content</li>
 * </ul>
 * 
 * <h2>Optimization Strategy</h2>
 * <ol>
 *   <li>For nodes ON the resume path: Use virtual NodeState from cache (ZERO SegmentStore reads)</li>
 *   <li>For nodes BEFORE the resume path (already indexed): Use virtual state for enter/leave only</li>
 *   <li>For nodes AFTER the resume path: Use real SegmentStore state (actual indexing)</li>
 * </ol>
 * 
 * <h2>Critical Behavior</h2>
 * This PRESERVES Lucene state by calling all editor methods (enter, leave, childNode*).
 * It only reduces SegmentStore reads, not editor calls. This is essential because:
 * <ul>
 *   <li>IndexUpdate needs enter/leave to manage its internal state</li>
 *   <li>FulltextIndexEditor needs childNode* callbacks to close writers properly</li>
 *   <li>Lucene segments need to be merged correctly across chunks</li>
 * </ul>
 */
public class CachedResumeDiff {
    
    private static final Logger log = LoggerFactory.getLogger(CachedResumeDiff.class);
    
    // Stats indices for tracking performance
    private static final int STAT_CACHE_HITS = 0;
    private static final int STAT_SEGMENT_READS = 1;
    private static final int STAT_NODES_PROCESSED = 2;
    private static final int STAT_NODES_SKIPPED = 3;
    
    /**
     * Process diff using cached info for skip phase optimization.
     * 
     * This eliminates SegmentStore reads for nodes on the path to the resume point
     * by using CachedNodeInfo's virtual NodeState.
     * 
     * @param editor the editor (should be wrapped with ResumingEditor)
     * @param before the before state from SegmentStore (only used after resume point)
     * @param after the after state from SegmentStore (only used after resume point)
     * @param cache the cached node info from previous chunks
     * @param resumePath path we're resuming from
     * @return exception if any, null on success
     */
    @Nullable
    public static CommitFailedException process(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull CachedNodeInfo cache,
            @NotNull String resumePath) throws CommitFailedException {
        
        long startTime = System.currentTimeMillis();
        long[] stats = new long[4];
        
        String[] pathSegments = parseResumePath(resumePath);
        boolean[] reachedResume = {false};  // Track when we reach the resume point
        
        try {
            // Use virtual state from cache during skip phase
            NodeState virtualAfter = cache.toVirtualNodeState();
            
            editor.enter(MISSING_NODE, virtualAfter);
            stats[STAT_CACHE_HITS]++;
            
            // Process children with optimized skip traversal
            CommitFailedException result = processChildrenOptimized(
                    editor, before, after, cache, pathSegments, 0, stats, reachedResume);
            
            if (result != null) {
                return result;
            }
            
            editor.leave(MISSING_NODE, virtualAfter);
            
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("[CACHED-SKIP] Completed in {}ms: cacheHits={}, segmentReads={}, processed={}, skipped={}",
                    totalTime, stats[STAT_CACHE_HITS], stats[STAT_SEGMENT_READS], 
                    stats[STAT_NODES_PROCESSED], stats[STAT_NODES_SKIPPED]);
            System.out.println("[CACHED-SKIP] Completed in " + totalTime + "ms: " +
                "cacheHits=" + stats[STAT_CACHE_HITS] + ", segmentReads=" + stats[STAT_SEGMENT_READS] + 
                ", processed=" + stats[STAT_NODES_PROCESSED] + ", skipped=" + stats[STAT_NODES_SKIPPED]);
            
            return null;
            
        } catch (CommitFailedException e) {
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("[CACHED-SKIP] Exception after {}ms: {} - {}", totalTime, e.getType(), e.getMessage());
            System.out.println("[CACHED-SKIP] Exception after " + totalTime + "ms: " + e.getMessage());
            return e;
        }
    }
    
    private static String[] parseResumePath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return new String[0];
        }
        return java.util.Arrays.stream(path.split("/"))
                               .filter(s -> !s.isEmpty())
                               .toArray(String[]::new);
    }
    
    /**
     * Optimized child processing with minimal SegmentStore reads.
     * 
     * This method is the heart of the optimization. It:
     * 1. Uses cached child names for iteration (no getChildNodeNames() from SegmentStore)
     * 2. Uses virtual NodeState for nodes on the resume path
     * 3. Only reads from SegmentStore when we reach nodes that need actual indexing
     */
    @Nullable
    private static CommitFailedException processChildrenOptimized(
            Editor editor,
            NodeState before,
            NodeState after,
            CachedNodeInfo cache,
            String[] pathSegments,
            int pathIndex,
            long[] stats,
            boolean[] reachedResume) throws CommitFailedException {
        
        // Determine where we are relative to the resume path
        boolean onResumePath = pathIndex < pathSegments.length;
        String targetChild = onResumePath ? pathSegments[pathIndex] : null;
        
        // Skip properties during skip phase (ResumingEditor handles this anyway)
        // BUT we still need to call them to maintain editor state
        if (!reachedResume[0] && cache.getChildCount() > 0) {
            // Use cached virtual state for property iteration during skip
            NodeState virtualState = cache.toVirtualNodeState();
            // Properties are minimal in virtual state - just jcr:primaryType
            for (PropertyState prop : virtualState.getProperties()) {
                editor.propertyAdded(prop);
            }
            stats[STAT_CACHE_HITS]++;
        } else if (reachedResume[0]) {
            // After resume point - need real property processing
            compareProperties(editor, before, after);
        }
        
        // Iterate children - use cache for iteration order
        Iterable<String> childNames;
        if (cache.getChildCount() > 0) {
            // Use cached child names - ZERO SegmentStore reads!
            childNames = cache.getChildNames();
            stats[STAT_CACHE_HITS]++;
        } else {
            // Cache miss or empty node - fall back to SegmentStore
            childNames = after.getChildNodeNames();
            stats[STAT_SEGMENT_READS]++;
        }
        
        for (String childName : childNames) {
            CachedNodeInfo childCache = cache.getChild(childName);
            stats[STAT_NODES_PROCESSED]++;
            
            if (onResumePath && childName.equals(targetChild)) {
                // ON the resume path - use cached state, no SegmentStore reads
                CommitFailedException ex = processOnResumePath(
                        editor, before, after, childName, childCache, 
                        pathSegments, pathIndex, stats, reachedResume);
                if (ex != null) return ex;
                
            } else if (!reachedResume[0] && childCache != null && childCache.isIndexed()) {
                // BEFORE resume point, already indexed - call enter/leave with virtual state
                // This maintains editor hierarchy but skips actual work
                stats[STAT_NODES_SKIPPED]++;
                
                NodeState virtualChild = childCache.toVirtualNodeState();
                Editor childEditor = editor.childNodeAdded(childName, virtualChild);
                
                if (childEditor != null) {
                    childEditor.enter(MISSING_NODE, virtualChild);
                    
                    // Recursively process cached children
                    CommitFailedException ex = processChildrenOptimized(
                            childEditor, MISSING_NODE, virtualChild, childCache,
                            pathSegments, pathIndex, stats, reachedResume);
                    if (ex != null) return ex;
                    
                    childEditor.leave(MISSING_NODE, virtualChild);
                }
                stats[STAT_CACHE_HITS]++;
                
            } else {
                // AFTER resume point or not in cache - full SegmentStore processing
                reachedResume[0] = true;
                stats[STAT_SEGMENT_READS]++;
                
                NodeState childBefore = before.getChildNode(childName);
                NodeState childAfter = after.getChildNode(childName);
                
                Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
                if (childEditor != null) {
                    CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                    if (ex != null) return ex;
                }
            }
        }
        
        // Check for new children not in cache (added since cache was built)
        if (reachedResume[0] || cache.getChildCount() == 0) {
            for (String childName : after.getChildNodeNames()) {
                if (cache.hasChild(childName)) {
                    continue; // Already processed
                }
                
                // New child - full SegmentStore processing
                stats[STAT_SEGMENT_READS]++;
                stats[STAT_NODES_PROCESSED]++;
                
                NodeState childBefore = before.getChildNode(childName);
                NodeState childAfter = after.getChildNode(childName);
                
                Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
                if (childEditor != null) {
                    CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                    if (ex != null) return ex;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Process a child that is ON the resume path - navigate towards resume point.
     */
    @Nullable
    private static CommitFailedException processOnResumePath(
            Editor editor,
            NodeState before,
            NodeState after,
            String childName,
            CachedNodeInfo childCache,
            String[] pathSegments,
            int pathIndex,
            long[] stats,
            boolean[] reachedResume) throws CommitFailedException {
        
        boolean isLastSegment = pathIndex + 1 == pathSegments.length;
        
        if (childCache != null) {
            // Have cache - use virtual state
            NodeState virtualChild = childCache.toVirtualNodeState();
            Editor childEditor = editor.childNodeAdded(childName, virtualChild);
            stats[STAT_CACHE_HITS]++;
            
            if (childEditor != null) {
                childEditor.enter(MISSING_NODE, virtualChild);
                
                if (isLastSegment) {
                    // Reached the resume point - switch to full processing
                    reachedResume[0] = true;
                    System.out.println("[CACHED-SKIP] Reached resume point at: " + 
                            String.join("/", java.util.Arrays.copyOfRange(pathSegments, 0, pathIndex + 1)));
                    
                    // Process remaining children with EditorDiff
                    NodeState realAfter = after;
                    for (int i = 0; i <= pathIndex; i++) {
                        realAfter = realAfter.getChildNode(pathSegments[i]);
                    }
                    
                    compareProperties(childEditor, MISSING_NODE, realAfter);
                    
                    for (String grandchildName : realAfter.getChildNodeNames()) {
                        stats[STAT_SEGMENT_READS]++;
                        NodeState grandchildBefore = MISSING_NODE;
                        NodeState grandchildAfter = realAfter.getChildNode(grandchildName);
                        
                        Editor grandchildEditor = childEditor.childNodeAdded(grandchildName, grandchildAfter);
                        if (grandchildEditor != null) {
                            CommitFailedException ex = EditorDiff.process(grandchildEditor, grandchildBefore, grandchildAfter);
                            if (ex != null) return ex;
                        }
                    }
                } else {
                    // Continue on resume path
                    CommitFailedException ex = processChildrenOptimized(
                            childEditor, MISSING_NODE, after.getChildNode(childName), childCache,
                            pathSegments, pathIndex + 1, stats, reachedResume);
                    if (ex != null) return ex;
                }
                
                childEditor.leave(MISSING_NODE, virtualChild);
            }
        } else {
            // No cache for this child - fall back to SegmentStore
            stats[STAT_SEGMENT_READS]++;
            reachedResume[0] = true;
            
            NodeState childBefore = before.getChildNode(childName);
            NodeState childAfter = after.getChildNode(childName);
            
            Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
            if (childEditor != null) {
                CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
                if (ex != null) return ex;
            }
        }
        
        return null;
    }
    
    /**
     * Compare and process properties (for nodes after resume point).
     */
    private static void compareProperties(Editor editor, NodeState before, NodeState after) 
            throws CommitFailedException {
        for (PropertyState afterProp : after.getProperties()) {
            String name = afterProp.getName();
            PropertyState beforeProp = before.getProperty(name);
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

