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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    
    // Statistics counters
    private static final AtomicInteger pathTreeTraversals = new AtomicInteger(0);
    private static final AtomicInteger segmentStoreTraversals = new AtomicInteger(0);
    private static final AtomicInteger pathTreeChildLookups = new AtomicInteger(0);
    private static final AtomicInteger segmentStoreChildLookups = new AtomicInteger(0);
    private static final AtomicInteger skippedGetChildCalls = new AtomicInteger(0);
    
    // Timing counters (in nanoseconds)
    private static final AtomicLong segmentStoreReadTimeNanos = new AtomicLong(0);
    private static final AtomicLong pathTreeLookupTimeNanos = new AtomicLong(0);
    private static final AtomicLong editorCallbackTimeNanos = new AtomicLong(0);
    
    /**
     * Reset traversal statistics (call before each diff).
     */
    public static void resetStats() {
        pathTreeTraversals.set(0);
        segmentStoreTraversals.set(0);
        pathTreeChildLookups.set(0);
        segmentStoreChildLookups.set(0);
        skippedGetChildCalls.set(0);
        segmentStoreReadTimeNanos.set(0);
        pathTreeLookupTimeNanos.set(0);
        editorCallbackTimeNanos.set(0);
    }
    
    /**
     * Get traversal statistics string.
     */
    public static String getStats() {
        return "pathTreeTraversals=" + pathTreeTraversals.get() + 
               ", segmentStoreTraversals=" + segmentStoreTraversals.get() +
               ", pathTreeChildLookups=" + pathTreeChildLookups.get() +
               ", segmentStoreChildLookups=" + segmentStoreChildLookups.get() +
               ", skippedGetChildCalls=" + skippedGetChildCalls.get();
    }
    
    /**
     * Get detailed timing statistics.
     */
    public static String getTimingStats() {
        return String.format("segmentStoreReadTime=%.2fms, pathTreeLookupTime=%.2fms, editorCallbackTime=%.2fms",
            segmentStoreReadTimeNanos.get() / 1_000_000.0,
            pathTreeLookupTimeNanos.get() / 1_000_000.0,
            editorCallbackTimeNanos.get() / 1_000_000.0);
    }
    
    public static long getSegmentStoreReadTimeMs() {
        return segmentStoreReadTimeNanos.get() / 1_000_000;
    }
    
    public static int getPathTreeTraversals() {
        return pathTreeTraversals.get();
    }
    
    public static int getSegmentStoreTraversals() {
        return segmentStoreTraversals.get();
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
        return processPath(editor, pathTree, "/", before, after);
    }
    
    @Nullable
    private static CommitFailedException processPath(
            @NotNull Editor editor,
            @NotNull PathTree pathTree,
            @NotNull String path,
            @NotNull NodeState before,
            @NotNull NodeState after) {
        
        try {
            // Check if we can traverse from PathTree (fully processed)
            // This PathTree lookup is very fast compared to SegmentStore
            long lookupStart = System.nanoTime();
            boolean usePathTree = pathTree.canTraverseFromPathTree(path);
            boolean isFullyProcessed = pathTree.isFullyProcessed(path);
            pathTreeLookupTimeNanos.addAndGet(System.nanoTime() - lookupStart);
            
            if (usePathTree) {
                pathTreeTraversals.incrementAndGet();
            } else {
                segmentStoreTraversals.incrementAndGet();
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
                    editor, pathTree, path);
                
                if (childException != null) {
                    return childException;
                }
                return null;
            }
            
            // Not fully processed - need to call editors
            long callbackStart = System.nanoTime();
            editor.enter(before, after);
            editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
            
            // Process properties from after state (involves SegmentStore reads)
            if (!usePathTree && before != MISSING_NODE && after != MISSING_NODE) {
                long propStart = System.nanoTime();
                for (PropertyState afterProp : after.getProperties()) {
                    PropertyState beforeProp = before.getProperty(afterProp.getName());
                    if (beforeProp == null) {
                        editor.propertyAdded(afterProp);
                    } else if (!beforeProp.equals(afterProp)) {
                        editor.propertyChanged(beforeProp, afterProp);
                    }
                }
                
                // Check for deleted properties
                for (PropertyState beforeProp : before.getProperties()) {
                    if (!after.hasProperty(beforeProp.getName())) {
                        editor.propertyDeleted(beforeProp);
                    }
                }
                segmentStoreReadTimeNanos.addAndGet(System.nanoTime() - propStart);
            }
            
            // Process child nodes
            CommitFailedException childException = processChildren(
                editor, pathTree, path, before, after, usePathTree);
            
            if (childException != null) {
                return childException;
            }
            
            // Call leave
            callbackStart = System.nanoTime();
            editor.leave(before, after);
            editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
            
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
            @NotNull String parentPath) throws CommitFailedException {
        
        Set<String> childNames = pathTree.getChildNamesFromPathTree(parentPath);
        
        for (String childName : childNames) {
            String childPath = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
            
            skippedGetChildCalls.addAndGet(2); // Saved 2 getChildNode calls
            
            // MAJOR OPTIMIZATION: Check if child is also fully processed
            // If so, skip ALL editor calls for this entire subtree
            boolean childFullyProcessed = pathTree.isFullyProcessed(childPath);
            
            if (childFullyProcessed) {
                // Child is fully processed - recursively process its children from PathTree
                // WITHOUT calling any editor methods (no enter/leave overhead)
                pathTreeTraversals.incrementAndGet();
                CommitFailedException e = processFullyProcessedChildren(editor, pathTree, childPath);
                if (e != null) return e;
            } else {
                // Child NOT fully processed - need to call editor
                // This handles edge cases where parent is marked but child isn't
                long callbackStart = System.nanoTime();
                Editor childEditor = editor.childNodeChanged(childName, MISSING_NODE, MISSING_NODE);
                editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
                
                if (childEditor != null) {
                    CommitFailedException e = processPath(
                        childEditor, pathTree, childPath, MISSING_NODE, MISSING_NODE);
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
            boolean usePathTree) throws CommitFailedException {
        
        // Get child names - either from PathTree or SegmentStore
        Iterable<String> childNames;
        
        if (usePathTree) {
            // Get children from PathTree (no SegmentStore call!)
            Set<String> pathTreeChildren = pathTree.getChildNamesFromPathTree(parentPath);
            pathTreeChildLookups.addAndGet(pathTreeChildren.size());
            childNames = pathTreeChildren;
            
            LOG.trace("[PathTreeDiff] Got {} children from PathTree for: {}", 
                pathTreeChildren.size(), parentPath);
        } else {
            // Get children from SegmentStore
            childNames = after.getChildNodeNames();
            segmentStoreChildLookups.incrementAndGet();
            
            LOG.trace("[PathTreeDiff] Got children from SegmentStore for: {}", parentPath);
        }
        
        // Process each child
        for (String childName : childNames) {
            String childPath = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
            
            // CRITICAL OPTIMIZATION: Check PathTree FIRST, BEFORE any SegmentStore calls!
            // This is the key to avoiding expensive I/O for fully-processed nodes.
            long lookupStart = System.nanoTime();
            boolean childFullyProcessed = pathTree.isFullyProcessed(childPath);
            pathTreeLookupTimeNanos.addAndGet(System.nanoTime() - lookupStart);
            
            if (childFullyProcessed) {
                // Child is fully processed - use dummy NodeStates to avoid SegmentStore
                // The editor will skip processing anyway due to PathTree skip logic
                LOG.trace("[PathTreeDiff] Child {} fully processed - SKIPPING SegmentStore entirely", childPath);
                skippedGetChildCalls.addAndGet(2); // Saved 2 getChildNode calls (before + after)
                
                // Call childNodeChanged with dummy states - editor will skip
                long callbackStart = System.nanoTime();
                Editor childEditor = editor.childNodeChanged(childName, MISSING_NODE, MISSING_NODE);
                editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
                
                if (childEditor != null) {
                    CommitFailedException e = processPath(
                        childEditor, pathTree, childPath, MISSING_NODE, MISSING_NODE);
                    if (e != null) return e;
                }
            } else {
                // Child NOT fully processed - need to read from SegmentStore
                // This is the expensive path that we want to minimize
                long readStart = System.nanoTime();
                NodeState beforeChild = before.getChildNode(childName);
                NodeState afterChild = after.getChildNode(childName);
                segmentStoreReadTimeNanos.addAndGet(System.nanoTime() - readStart);
                
                // Determine if this is add, change, or exists in both
                boolean beforeExists = beforeChild.exists();
                boolean afterExists = afterChild.exists();
                
                if (!beforeExists && afterExists) {
                    // Child added
                    long callbackStart = System.nanoTime();
                    Editor childEditor = editor.childNodeAdded(childName, afterChild);
                    editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
                    
                    if (childEditor != null) {
                        CommitFailedException e = processPath(
                            childEditor, pathTree, childPath, MISSING_NODE, afterChild);
                        if (e != null) return e;
                    }
                } else if (beforeExists && afterExists) {
                    // Child changed (or unchanged - editor decides)
                    long callbackStart = System.nanoTime();
                    Editor childEditor = editor.childNodeChanged(childName, beforeChild, afterChild);
                    editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
                    
                    if (childEditor != null) {
                        CommitFailedException e = processPath(
                            childEditor, pathTree, childPath, beforeChild, afterChild);
                        if (e != null) return e;
                    }
                } else if (beforeExists && !afterExists) {
                    // Child deleted
                    long callbackStart = System.nanoTime();
                    Editor childEditor = editor.childNodeDeleted(childName, beforeChild);
                    editorCallbackTimeNanos.addAndGet(System.nanoTime() - callbackStart);
                    
                    if (childEditor != null) {
                        CommitFailedException e = processPath(
                            childEditor, pathTree, childPath, beforeChild, MISSING_NODE);
                        if (e != null) return e;
                    }
                }
                // else: neither exists, skip
            }
        }
        
        // If NOT using PathTree, also check for children only in before state (deleted)
        if (!usePathTree) {
            for (String childName : before.getChildNodeNames()) {
                if (!after.hasChildNode(childName)) {
                    String childPath = parentPath.equals("/") ? "/" + childName : parentPath + "/" + childName;
                    NodeState beforeChild = before.getChildNode(childName);
                    
                    Editor childEditor = editor.childNodeDeleted(childName, beforeChild);
                    if (childEditor != null) {
                        CommitFailedException e = processPath(
                            childEditor, pathTree, childPath, beforeChild, MISSING_NODE);
                        if (e != null) return e;
                    }
                }
            }
        }
        
        return null;
    }
}

