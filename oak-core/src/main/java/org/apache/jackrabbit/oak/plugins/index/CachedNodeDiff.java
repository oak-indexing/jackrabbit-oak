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
 * Diff processor that uses CachedNodeInfo to avoid SegmentStore reads during skip phase.
 * 
 * Key optimization:
 * - Use cached node info to determine node types and child order
 * - Only read from SegmentStore when we reach nodes not in cache or need actual properties
 * - Skip indexed subtrees entirely (no SegmentStore read!)
 * 
 * Flow:
 * 1. Navigate using cached info (fast, in-memory)
 * 2. For indexed nodes in cache: SKIP (no SegmentStore read!)
 * 3. For unindexed nodes in cache: use cached info for editor hierarchy, then read properties
 * 4. For nodes not in cache: read from SegmentStore
 */
public class CachedNodeDiff {
    
    private static final Logger log = LoggerFactory.getLogger(CachedNodeDiff.class);
    
    /**
     * Process diff using cached info to avoid SegmentStore reads.
     * 
     * @param editor the editor to apply changes to
     * @param before the before state (from SegmentStore)
     * @param after the after state (from SegmentStore)
     * @param cache the cached node info from previous chunks
     * @param resumePath path to resume from
     * @param onResumeReached callback when resume point is reached
     * @return exception if any, null on success
     */
    @Nullable
    public static CommitFailedException process(
            @NotNull Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull CachedNodeInfo cache,
            @NotNull String resumePath,
            @Nullable Runnable onResumeReached) throws CommitFailedException {
        
        long startTime = System.currentTimeMillis();
        long[] stats = new long[4]; // [cacheHits, cacheMisses, nodesSkipped, segmentReads]
        
        String[] pathSegments = parseResumePath(resumePath);
        
        try {
            // Use virtual state from cache if available
            NodeState virtualAfter = cache.toVirtualNodeState();
            
            editor.enter(before, virtualAfter);
            
            // Process children using cache-guided navigation
            processChildrenWithCache(editor, before, after, cache, pathSegments, 0, stats, onResumeReached);
            
            editor.leave(before, virtualAfter);
            
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("[CACHED-DIFF] Completed in " + totalTime + "ms: " +
                "cacheHits=" + stats[0] + ", cacheMisses=" + stats[1] + 
                ", skipped=" + stats[2] + ", segmentReads=" + stats[3]);
            
            return null;
            
        } catch (CommitFailedException e) {
            long totalTime = System.currentTimeMillis() - startTime;
            System.out.println("[CACHED-DIFF] Exception after " + totalTime + "ms: " +
                "cacheHits=" + stats[0] + ", cacheMisses=" + stats[1] + 
                ", skipped=" + stats[2] + ", segmentReads=" + stats[3] +
                " - " + e.getMessage());
            return e;
        }
    }
    
    private static String[] parseResumePath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return new String[0];
        }
        return Arrays.stream(path.split("/"))
                     .filter(s -> !s.isEmpty())
                     .toArray(String[]::new);
    }
    
    private static void processChildrenWithCache(
            Editor editor,
            NodeState before,
            NodeState after,
            CachedNodeInfo cache,
            String[] pathSegments,
            int pathIndex,
            long[] stats,
            Runnable onResumeReached) throws CommitFailedException {
        
        boolean hasPathSegment = pathIndex < pathSegments.length;
        String targetSegment = hasPathSegment ? pathSegments[pathIndex] : null;
        boolean reachedTarget = !hasPathSegment;
        boolean calledCallback = false;
        
        // First, iterate children from cache (NO SegmentStore read for iteration!)
        for (String childName : cache.getChildNames()) {
            CachedNodeInfo childCache = cache.getChild(childName);
            
            // Check if we should skip based on resume path position
            if (!reachedTarget) {
                if (childName.equals(targetSegment)) {
                    reachedTarget = true;
                } else if (childCache != null && childCache.isIndexed()) {
                    // Before target AND indexed - SKIP entirely!
                    stats[2] += childCache.getSubtreeSize();
                    stats[0]++; // Cache hit
                    continue;
                }
                // If not indexed, we need to process it even if before target
            }
            
            // Check if indexed (can skip!)
            if (childCache != null && childCache.isIndexed() && reachedTarget && !childName.equals(targetSegment)) {
                stats[2] += childCache.getSubtreeSize();
                stats[0]++; // Cache hit
                continue;
            }
            
            stats[0]++; // Cache hit (we found info in cache)
            
            // Need to process this child
            // Use cached info for node type, but may need SegmentStore for properties
            boolean isTargetSegment = hasPathSegment && childName.equals(targetSegment);
            boolean isLastSegment = (pathIndex == pathSegments.length - 1);
            
            NodeState childBefore = before.getChildNode(childName);
            NodeState childAfter;
            
            if (childCache != null) {
                // Use virtual state from cache (no SegmentStore read for type info!)
                childAfter = childCache.toVirtualNodeState();
            } else {
                // Cache miss - need SegmentStore
                stats[1]++;
                stats[3]++;
                childAfter = after.getChildNode(childName);
            }
            
            Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
            if (childEditor == null) {
                continue;
            }
            
            if (isTargetSegment && isLastSegment && !calledCallback && onResumeReached != null) {
                onResumeReached.run();
                calledCallback = true;
            }
            
            if (isTargetSegment && !isLastSegment) {
                // Continue navigating with cache
                childEditor.enter(childBefore, childAfter);
                
                if (childCache != null) {
                    processChildrenWithCache(childEditor, childBefore, after.getChildNode(childName),
                                             childCache, pathSegments, pathIndex + 1, stats, onResumeReached);
                } else {
                    // No cache - fall back to EditorDiff
                    stats[3]++;
                    CommitFailedException ex = EditorDiff.process(childEditor, childBefore, after.getChildNode(childName));
                    if (ex != null) throw ex;
                }
                
                childEditor.leave(childBefore, childAfter);
            } else {
                // At or past resume point - need full processing from SegmentStore
                stats[3]++;
                NodeState realChildAfter = after.getChildNode(childName);
                CommitFailedException ex = EditorDiff.process(childEditor, childBefore, realChildAfter);
                if (ex != null) {
                    throw ex;
                }
            }
        }
        
        // Check for NEW children not in cache (added since cache was built)
        for (String childName : after.getChildNodeNames()) {
            if (cache.hasChild(childName)) {
                continue; // Already processed above
            }
            
            stats[1]++; // Cache miss
            stats[3]++; // SegmentStore read
            
            NodeState childBefore = before.getChildNode(childName);
            NodeState childAfter = after.getChildNode(childName);
            
            Editor childEditor = getChildEditor(editor, childName, childBefore, childAfter);
            if (childEditor == null) {
                continue;
            }
            
            // New child - use EditorDiff for full processing
            CommitFailedException ex = EditorDiff.process(childEditor, childBefore, childAfter);
            if (ex != null) {
                throw ex;
            }
        }
    }
    
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

