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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal tree structure for tracking visited/indexed paths during resumable indexing.
 * 
 * <p>This tree is built during the first indexing run and used in subsequent runs
 * to skip already-indexed nodes without making expensive NodeStore calls.
 * 
 * <p>The tree is serializable to/from NodeState for persistence under :async node.
 */
public class PathTree {

    /**
     * Node in the path tree representing a visited path.
     */
    public static class PathNode {
        private final String name;
        private final Map<String, PathNode> children;
        private boolean indexed;  // True if this node was actually indexed (not just traversed)
        private String primaryType;  // Cached primary type for skip decisions
        
        // Phase flags for tracking traversal state
        private boolean enterCompleted;  // True when enter() has been called and completed
        private boolean leaveCompleted;  // True when leave() has been called and completed
        
        public PathNode(String name) {
            this.name = name;
            this.children = new ConcurrentHashMap<>();
            this.indexed = false;
            this.enterCompleted = false;
            this.leaveCompleted = false;
        }
        
        public String getName() {
            return name;
        }
        
        public boolean isIndexed() {
            return indexed;
        }
        
        public void setIndexed(boolean indexed) {
            this.indexed = indexed;
        }
        
        public String getPrimaryType() {
            return primaryType;
        }
        
        public void setPrimaryType(String primaryType) {
            this.primaryType = primaryType;
        }
        
        // ========== Enter/Leave Phase Tracking ==========
        
        public boolean isEnterCompleted() {
            return enterCompleted;
        }
        
        public void setEnterCompleted(boolean enterCompleted) {
            this.enterCompleted = enterCompleted;
        }
        
        public boolean isLeaveCompleted() {
            return leaveCompleted;
        }
        
        public void setLeaveCompleted(boolean leaveCompleted) {
            this.leaveCompleted = leaveCompleted;
        }
        
        /**
         * Check if this node is fully processed (both enter and leave completed).
         * Only fully processed nodes can be safely skipped during resume.
         */
        public boolean isFullyProcessed() {
            return enterCompleted && leaveCompleted;
        }
        
        public PathNode getChild(String name) {
            return children.get(name);
        }
        
        public PathNode getOrCreateChild(String name) {
            return children.computeIfAbsent(name, PathNode::new);
        }
        
        public boolean hasChild(String name) {
            return children.containsKey(name);
        }
        
        public Set<String> getChildNames() {
            return children.keySet();
        }
        
        public Map<String, PathNode> getChildren() {
            return children;
        }
        
        public int getChildCount() {
            return children.size();
        }
        
        /**
         * Prune this node's indexed children to save memory.
         * Once a subtree is fully indexed, we don't need to keep it.
         */
        public void pruneIndexedChildren() {
            children.entrySet().removeIf(e -> e.getValue().isIndexed() && 
                                              e.getValue().getChildCount() == 0);
        }
    }
    
    private final PathNode root;
    private int totalNodes;
    private int indexedNodes;
    
    // Track last fully processed path for ultra-slim serialization (Approach 2)
    // In DFS order, any path that comes "before" this is fully processed
    private String lastFullyProcessedPath;
    
    // Flag to enable DFS order comparison - only true when resuming from a PREVIOUS chunk
    // During the current run, we use tree-based lookup because hash-order traversal != DFS order
    private boolean useDfsOrderComparison = false;
    
    public PathTree() {
        this.root = new PathNode("");
        this.totalNodes = 0;
        this.indexedNodes = 0;
        this.lastFullyProcessedPath = null;
    }
    
    // ========== DFS Order Path Comparison (Approach 2) ==========
    
    /**
     * Compare two paths in DFS (Depth-First Search) traversal order.
     * 
     * DFS order properties:
     * - Parent comes before its children: /a < /a/b
     * - Siblings are ordered alphabetically: /a/b < /a/c
     * - All children of a node come before the next sibling: /a/b/c < /a/d
     * 
     * @return negative if path1 < path2, 0 if equal, positive if path1 > path2
     */
    public static int compareDfsOrder(@NotNull String path1, @NotNull String path2) {
        if (path1.equals(path2)) return 0;
        
        // Convert Iterable to array
        java.util.List<String> list1 = new java.util.ArrayList<>();
        java.util.List<String> list2 = new java.util.ArrayList<>();
        for (String s : PathUtils.elements(path1)) list1.add(s);
        for (String s : PathUtils.elements(path2)) list2.add(s);
        
        // Compare segment by segment
        int minLen = Math.min(list1.size(), list2.size());
        for (int i = 0; i < minLen; i++) {
            int cmp = list1.get(i).compareTo(list2.get(i));
            if (cmp != 0) return cmp;
        }
        
        // If all common segments are equal, shorter path (ancestor) comes first
        return Integer.compare(list1.size(), list2.size());
    }
    
    /**
     * Check if path1 comes before path2 in DFS order.
     */
    public static boolean isBeforeInDfsOrder(@NotNull String path1, @NotNull String path2) {
        return compareDfsOrder(path1, path2) < 0;
    }
    
    /**
     * Check if path1 comes at or before path2 in DFS order.
     */
    public static boolean isAtOrBeforeInDfsOrder(@NotNull String path1, @NotNull String path2) {
        return compareDfsOrder(path1, path2) <= 0;
    }
    
    /**
     * Get the last fully processed path.
     */
    @Nullable
    public String getLastFullyProcessedPath() {
        return lastFullyProcessedPath;
    }
    
    /**
     * Check if DFS order comparison is enabled for isFullyProcessed() checks.
     */
    public boolean isUsingDfsOrderComparison() {
        return useDfsOrderComparison;
    }
    
    /**
     * Set the last fully processed path (used when loading from serialized state).
     */
    public void setLastFullyProcessedPath(@Nullable String path) {
        this.lastFullyProcessedPath = path;
    }
    
    /**
     * Check if a path is fully processed using DFS order comparison.
     * This is ultra-fast: just one string comparison instead of tree traversal!
     * 
     * @param path the path to check
     * @return true if path was fully processed (comes before or equals lastFullyProcessedPath)
     */
    public boolean isFullyProcessedByDfsOrder(@NotNull String path) {
        if (lastFullyProcessedPath == null) {
            return false;
        }
        // A path is fully processed if it comes at or before the last processed path in DFS order
        return isAtOrBeforeInDfsOrder(path, lastFullyProcessedPath);
    }
    
    /**
     * Get or create a node at the given path.
     * 
     * @param path absolute path starting with /
     * @return the PathNode at the path
     */
    @NotNull
    public PathNode getOrCreateNode(@NotNull String path) {
        if ("/".equals(path)) {
            return root;
        }
        
        PathNode current = root;
        for (String segment : PathUtils.elements(path)) {
            PathNode child = current.getChild(segment);
            if (child == null) {
                child = current.getOrCreateChild(segment);
                totalNodes++;  // Increment for EACH new node created
            }
            current = child;
        }
        return current;
    }
    
    /**
     * Get a node at the given path, or null if it doesn't exist.
     * 
     * @param path absolute path starting with /
     * @return the PathNode at the path, or null
     */
    @Nullable
    public PathNode getNode(@NotNull String path) {
        if ("/".equals(path)) {
            return root;
        }
        
        PathNode current = root;
        for (String segment : PathUtils.elements(path)) {
            current = current.getChild(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
    
    /**
     * Check if a path has been visited (exists in tree).
     */
    public boolean hasPath(@NotNull String path) {
        return getNode(path) != null;
    }
    
    /**
     * Check if a path has been indexed.
     */
    public boolean isIndexed(@NotNull String path) {
        PathNode node = getNode(path);
        return node != null && node.isIndexed();
    }
    
    /**
     * Mark a path as indexed.
     */
    public void markIndexed(@NotNull String path) {
        PathNode node = getOrCreateNode(path);
        if (!node.isIndexed()) {
            node.setIndexed(true);
            indexedNodes++;
        }
    }
    
    // ========== Enter/Leave Phase Tracking Methods ==========
    
    /**
     * Mark that enter() has been completed for this path.
     */
    public void markEnterCompleted(@NotNull String path) {
        PathNode node = getOrCreateNode(path);
        node.setEnterCompleted(true);
    }
    
    /**
     * Mark that leave() has been completed for this path.
     * This also marks the node as indexed and updates lastFullyProcessedPath.
     */
    public void markLeaveCompleted(@NotNull String path) {
        PathNode node = getOrCreateNode(path);
        node.setLeaveCompleted(true);
        if (!node.isIndexed()) {
            node.setIndexed(true);
            indexedNodes++;
        }
        
        // Update lastFullyProcessedPath for DFS-order tracking (Approach 2)
        // We only update if this path is "after" the current last path in DFS order
        // This ensures we always track the furthest progress in the traversal
        if (lastFullyProcessedPath == null || isBeforeInDfsOrder(lastFullyProcessedPath, path)) {
            lastFullyProcessedPath = path;
        }
    }
    
    /**
     * Check if enter() has been completed for this path.
     */
    public boolean isEnterCompleted(@NotNull String path) {
        PathNode node = getNode(path);
        return node != null && node.isEnterCompleted();
    }
    
    /**
     * Check if leave() has been completed for this path.
     */
    public boolean isLeaveCompleted(@NotNull String path) {
        PathNode node = getNode(path);
        return node != null && node.isLeaveCompleted();
    }
    
    /**
     * Check if a path is fully processed (both enter and leave completed).
     * Only fully processed nodes can be safely skipped without NodeStore calls.
     * 
     * Uses the most efficient check available:
     * 1. If useDfsOrderComparison is enabled (resuming from previous chunk): O(path_length) string comparison
     * 2. Otherwise, tree-based ancestor checking: O(depth) tree traversal
     * 
     * IMPORTANT: DFS order comparison is ONLY used when resuming from a previous chunk.
     * During the current run, we use tree-based lookup because SegmentStore traverses
     * children in hash order, not DFS order. Using DFS comparison would cause nodes
     * that come "later" in hash order but "earlier" in DFS order to be incorrectly skipped.
     */
    public boolean isFullyProcessed(@NotNull String path) {
        // FAST PATH: Use DFS order comparison if resuming from a previous chunk
        // This is O(path_length) string comparison instead of O(depth) tree traversal
        // NOTE: Only use this when resuming - during current run, hash order != DFS order
        if (useDfsOrderComparison && lastFullyProcessedPath != null) {
            return isFullyProcessedByDfsOrder(path);
        }
        
        // SLOW PATH: Use ancestor checking (frontier-based)
        return isFullyProcessedInternal(path, 0);
    }
    
    private boolean isFullyProcessedInternal(String path, int depth) {
        // Safety limit to prevent infinite recursion
        if (depth > 100) {
            return false;
        }
        
        // Check if exact path is fully processed
        PathNode node = getNode(path);
        if (node != null && node.isFullyProcessed()) {
            return true;
        }
        
        // Check ancestors - if any ancestor is fully processed, so is this path
        // This enables frontier-based storage: we only store the first fully-processed
        // node in each subtree, and all descendants are implicitly fully processed
        if (!"/".equals(path)) {
            String parentPath = PathUtils.getParentPath(path);
            return isFullyProcessedInternal(parentPath, depth + 1);
        }
        
        return false;
    }
    
    /**
     * Check if exact path is fully processed (no ancestor checking, no DFS order).
     */
    public boolean isExactPathFullyProcessed(@NotNull String path) {
        PathNode node = getNode(path);
        return node != null && node.isFullyProcessed();
    }
    
    /**
     * Get count of fully processed nodes.
     */
    public int getFullyProcessedCount() {
        return countFullyProcessed(root);
    }
    
    private int countFullyProcessed(PathNode node) {
        int count = node.isFullyProcessed() ? 1 : 0;
        for (PathNode child : node.getChildren().values()) {
            count += countFullyProcessed(child);
        }
        return count;
    }
    
    /**
     * Get the root node.
     */
    public PathNode getRoot() {
        return root;
    }
    
    public int getTotalNodes() {
        return totalNodes;
    }
    
    public int getIndexedNodes() {
        return indexedNodes;
    }
    
    /**
     * Serialize the tree to a NodeBuilder for persistence.
     * 
     * @param builder the node builder to write to
     */
    public void serializeTo(@NotNull NodeBuilder builder) {
        builder.setProperty("totalNodes", totalNodes);
        builder.setProperty("indexedNodes", indexedNodes);
        // Save lastFullyProcessedPath for DFS order comparison on resume
        if (lastFullyProcessedPath != null) {
            builder.setProperty("lastFullyProcessedPath", lastFullyProcessedPath);
        }
        serializeNode(root, builder.child("tree"));
    }
    
    private void serializeNode(PathNode node, NodeBuilder builder) {
        if (node.isIndexed()) {
            builder.setProperty("indexed", true);
        }
        if (node.getPrimaryType() != null) {
            builder.setProperty("primaryType", node.getPrimaryType());
        }
        // Serialize enter/leave flags
        if (node.isEnterCompleted()) {
            builder.setProperty("enterCompleted", true);
        }
        if (node.isLeaveCompleted()) {
            builder.setProperty("leaveCompleted", true);
        }
        
        for (Map.Entry<String, PathNode> entry : node.getChildren().entrySet()) {
            serializeNode(entry.getValue(), builder.child(entry.getKey()));
        }
    }
    
    /**
     * Deserialize a tree from NodeState.
     * 
     * @param state the node state to read from
     * @return the deserialized PathTree
     */
    @NotNull
    public static PathTree deserializeFrom(@NotNull NodeState state) {
        PathTree tree = new PathTree();
        
        PropertyState totalProp = state.getProperty("totalNodes");
        if (totalProp != null) {
            tree.totalNodes = totalProp.getValue(Type.LONG).intValue();
        }
        
        PropertyState indexedProp = state.getProperty("indexedNodes");
        if (indexedProp != null) {
            tree.indexedNodes = indexedProp.getValue(Type.LONG).intValue();
        }
        
        NodeState treeState = state.getChildNode("tree");
        if (treeState.exists()) {
            deserializeNode(tree.root, treeState);
        }
        
        // NOTE: We don't use DFS order comparison for FULL format because
        // SegmentStore traversal is hash-based, not DFS-order based.
        // DFS comparison would incorrectly skip nodes that come "later" in hash order
        // but "earlier" in DFS order.
        tree.useDfsOrderComparison = false;
        
        // Read lastFullyProcessedPath but don't use it for comparison
        PropertyState lastPathProp = state.getProperty("lastFullyProcessedPath");
        if (lastPathProp != null) {
            tree.lastFullyProcessedPath = lastPathProp.getValue(Type.STRING);
        }
        
        return tree;
    }
    
    private static void deserializeNode(PathNode node, NodeState state) {
        PropertyState indexedProp = state.getProperty("indexed");
        if (indexedProp != null && indexedProp.getValue(Type.BOOLEAN)) {
            node.setIndexed(true);
        }
        
        PropertyState typeProp = state.getProperty("primaryType");
        if (typeProp != null) {
            node.setPrimaryType(typeProp.getValue(Type.STRING));
        }
        
        // Deserialize enter/leave flags
        PropertyState enterProp = state.getProperty("enterCompleted");
        if (enterProp != null && enterProp.getValue(Type.BOOLEAN)) {
            node.setEnterCompleted(true);
        }
        
        PropertyState leaveProp = state.getProperty("leaveCompleted");
        if (leaveProp != null && leaveProp.getValue(Type.BOOLEAN)) {
            node.setLeaveCompleted(true);
        }
        
        for (String childName : state.getChildNodeNames()) {
            // Skip property-like child names
            if (!"indexed".equals(childName) && !"primaryType".equals(childName) 
                && !"enterCompleted".equals(childName) && !"leaveCompleted".equals(childName)) {
                PathNode child = node.getOrCreateChild(childName);
                deserializeNode(child, state.getChildNode(childName));
            }
        }
    }
    
    /**
     * Clear the tree.
     */
    public void clear() {
        root.getChildren().clear();
        totalNodes = 0;
        indexedNodes = 0;
    }
    
    // ========== Slim Serialization (Unprocessed Nodes Only) ==========
    
    /**
     * Serialize using FRONTIER-BASED pruning to NodeBuilder.
     * 
     * Key optimization: We store:
     * 1. All NOT fully processed nodes (the in-progress chain)
     * 2. FRONTIER nodes: first-level fully processed children of in-progress nodes
     * 
     * With ancestor checking in isFullyProcessed(), storing a frontier node
     * means all its descendants are implicitly fully processed.
     * 
     * Storage: O(chain_depth + frontier_size) instead of O(all_nodes)
     */
    public void serializeSlimTo(@NotNull NodeBuilder builder) {
        builder.setProperty("totalNodes", totalNodes);
        builder.setProperty("indexedNodes", indexedNodes);
        builder.setProperty("fullyProcessedCount", getFullyProcessedCount());
        
        // Collect paths for serialization
        java.util.List<String> paths = new java.util.ArrayList<>();
        java.util.List<String> enterFlags = new java.util.ArrayList<>();
        java.util.List<String> leaveFlags = new java.util.ArrayList<>();
        java.util.List<String> frontierFlags = new java.util.ArrayList<>(); // Mark frontier nodes
        
        collectFrontierPaths(root, "/", paths, enterFlags, leaveFlags, frontierFlags);
        
        // Serialize as arrays
        builder.setProperty("paths", paths, Type.STRINGS);
        builder.setProperty("enterFlags", enterFlags, Type.STRINGS);
        builder.setProperty("leaveFlags", leaveFlags, Type.STRINGS);
        builder.setProperty("frontierFlags", frontierFlags, Type.STRINGS);
        
        builder.setProperty("slimFormat", true);
        builder.setProperty("frontierFormat", true); // New flag to indicate frontier format
        builder.setProperty("pathCount", paths.size());
        
        // Count frontier vs in-progress
        int frontierCount = 0;
        int inProgressCount = 0;
        for (String flag : frontierFlags) {
            if ("true".equals(flag)) frontierCount++;
            else inProgressCount++;
        }
        
        System.out.println("[DEBUG-PATHTREE-FRONTIER] Serialized " + paths.size() + 
            " paths (frontier=" + frontierCount + ", inProgress=" + inProgressCount +
            ") vs " + totalNodes + " total nodes (savings: " + 
            (totalNodes > 0 ? (100 - paths.size() * 100 / totalNodes) : 0) + "%)");
    }
    
    /**
     * Collect paths for frontier-based serialization:
     * - NOT fully processed nodes (in-progress chain)
     * - FRONTIER: fully processed children of NOT fully processed parents
     */
    private void collectFrontierPaths(PathNode node, String path, 
            java.util.List<String> paths, 
            java.util.List<String> enterFlags,
            java.util.List<String> leaveFlags,
            java.util.List<String> frontierFlags) {
        
        boolean nodeIsFullyProcessed = node.isFullyProcessed();
        boolean parentIsFullyProcessed = false;
        
        // Check if parent is fully processed (for frontier detection)
        if (!"/".equals(path)) {
            String parentPath = PathUtils.getParentPath(path);
            PathNode parentNode = getNode(parentPath);
            parentIsFullyProcessed = (parentNode != null && parentNode.isFullyProcessed());
        }
        
        // Determine if this node should be stored
        boolean isFrontier = nodeIsFullyProcessed && !parentIsFullyProcessed && !"/".equals(path);
        boolean isInProgress = !nodeIsFullyProcessed && !"/".equals(path);
        
        if (isFrontier || isInProgress) {
            paths.add(path);
            enterFlags.add(String.valueOf(node.isEnterCompleted()));
            leaveFlags.add(String.valueOf(node.isLeaveCompleted()));
            frontierFlags.add(String.valueOf(isFrontier));
        }
        
        // If this node is fully processed, DON'T recurse to children
        // (they're all implicitly fully processed via ancestor check)
        // Only recurse if NOT fully processed
        if (!nodeIsFullyProcessed) {
            for (Map.Entry<String, PathNode> entry : node.getChildren().entrySet()) {
                String childPath = "/".equals(path) ? "/" + entry.getKey() : path + "/" + entry.getKey();
                collectFrontierPaths(entry.getValue(), childPath, paths, enterFlags, leaveFlags, frontierFlags);
            }
        }
    }
    
    // ========== ULTRA-SLIM Serialization (Approach 2) ==========
    
    /**
     * Serialize using ULTRA-SLIM format: just the last fully processed path!
     * 
     * This is the most efficient format:
     * - Storage: O(1) - just ONE path string (~50-100 bytes)
     * - Skip logic: O(path_depth) string comparison instead of tree traversal
     * 
     * We also store the in-progress chain (not fully processed nodes) to maintain
     * correct resume state.
     */
    public void serializeUltraSlimTo(@NotNull NodeBuilder builder) {
        builder.setProperty("totalNodes", totalNodes);
        builder.setProperty("indexedNodes", indexedNodes);
        builder.setProperty("fullyProcessedCount", getFullyProcessedCount());
        
        // Store the last fully processed path - this is the key!
        if (lastFullyProcessedPath != null) {
            builder.setProperty("lastFullyProcessedPath", lastFullyProcessedPath);
        }
        
        // Collect only in-progress paths (not fully processed)
        java.util.List<String> inProgressPaths = new java.util.ArrayList<>();
        java.util.List<String> enterFlags = new java.util.ArrayList<>();
        java.util.List<String> leaveFlags = new java.util.ArrayList<>();
        
        collectInProgressPaths(root, "/", inProgressPaths, enterFlags, leaveFlags);
        
        if (!inProgressPaths.isEmpty()) {
            builder.setProperty("inProgressPaths", inProgressPaths, Type.STRINGS);
            builder.setProperty("inProgressEnterFlags", enterFlags, Type.STRINGS);
            builder.setProperty("inProgressLeaveFlags", leaveFlags, Type.STRINGS);
        }
        
        builder.setProperty("slimFormat", true);
        builder.setProperty("ultraSlimFormat", true);
        
        int estimatedSize = (lastFullyProcessedPath != null ? lastFullyProcessedPath.length() : 0) + 
                           inProgressPaths.stream().mapToInt(String::length).sum() + 100;
        
        System.out.println("[DEBUG-PATHTREE-ULTRASLIM] Serialized: lastPath=" + lastFullyProcessedPath +
            ", inProgress=" + inProgressPaths.size() + " paths" +
            ", estimatedSize=~" + estimatedSize + " bytes" +
            " (vs " + totalNodes + " total nodes)");
    }
    
    /**
     * Collect only in-progress paths (not fully processed).
     */
    private void collectInProgressPaths(PathNode node, String path,
            java.util.List<String> paths,
            java.util.List<String> enterFlags,
            java.util.List<String> leaveFlags) {
        
        // Collect if not fully processed (but has at least started)
        if (!"/".equals(path) && !node.isFullyProcessed() && node.isEnterCompleted()) {
            paths.add(path);
            enterFlags.add(String.valueOf(node.isEnterCompleted()));
            leaveFlags.add(String.valueOf(node.isLeaveCompleted()));
        }
        
        // Only recurse if not fully processed
        if (!node.isFullyProcessed()) {
            for (Map.Entry<String, PathNode> entry : node.getChildren().entrySet()) {
                String childPath = "/".equals(path) ? "/" + entry.getKey() : path + "/" + entry.getKey();
                collectInProgressPaths(entry.getValue(), childPath, paths, enterFlags, leaveFlags);
            }
        }
    }
    
    /**
     * Deserialize from ultra-slim format (last path + in-progress).
     */
    @NotNull
    public static PathTree deserializeUltraSlimFrom(@NotNull NodeState state) {
        PathTree tree = new PathTree();
        
        // Read counters
        PropertyState totalProp = state.getProperty("totalNodes");
        if (totalProp != null) {
            tree.totalNodes = totalProp.getValue(Type.LONG).intValue();
        }
        
        PropertyState indexedProp = state.getProperty("indexedNodes");
        if (indexedProp != null) {
            tree.indexedNodes = indexedProp.getValue(Type.LONG).intValue();
        }
        
        // Read last fully processed path - this is the key!
        PropertyState lastPathProp = state.getProperty("lastFullyProcessedPath");
        if (lastPathProp != null) {
            tree.lastFullyProcessedPath = lastPathProp.getValue(Type.STRING);
        }
        
        // Read in-progress paths
        PropertyState inProgressProp = state.getProperty("inProgressPaths");
        PropertyState enterProp = state.getProperty("inProgressEnterFlags");
        PropertyState leaveProp = state.getProperty("inProgressLeaveFlags");
        
        int inProgressCount = 0;
        if (inProgressProp != null) {
            Iterable<String> paths = inProgressProp.getValue(Type.STRINGS);
            Iterable<String> enters = enterProp != null ? enterProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
            Iterable<String> leaves = leaveProp != null ? leaveProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
            
            java.util.Iterator<String> pathIt = paths.iterator();
            java.util.Iterator<String> enterIt = enters.iterator();
            java.util.Iterator<String> leaveIt = leaves.iterator();
            
            while (pathIt.hasNext()) {
                String path = pathIt.next();
                boolean enterCompleted = enterIt.hasNext() && Boolean.parseBoolean(enterIt.next());
                boolean leaveCompleted = leaveIt.hasNext() && Boolean.parseBoolean(leaveIt.next());
                
                PathNode node = tree.getOrCreateNode(path);
                node.setEnterCompleted(enterCompleted);
                node.setLeaveCompleted(leaveCompleted);
                inProgressCount++;
            }
        }
        
        // NOTE: We don't enable DFS order comparison because SegmentStore traversal
        // is hash-based, not DFS-order based. DFS comparison would incorrectly skip
        // nodes that come "later" in hash order but "earlier" in DFS order.
        tree.useDfsOrderComparison = false;
        
        System.out.println("[DEBUG-PATHTREE-ULTRASLIM] Loaded: lastPath=" + tree.lastFullyProcessedPath +
            ", inProgress=" + inProgressCount + " paths, useDfsOrder=" + tree.useDfsOrderComparison)
        
        return tree;
    }
    
    /**
     * Check if state contains ultra-slim format.
     */
    public static boolean isUltraSlimFormat(@NotNull NodeState state) {
        PropertyState prop = state.getProperty("ultraSlimFormat");
        return prop != null && prop.getValue(Type.BOOLEAN);
    }
    
    /**
     * Deserialize from frontier format.
     * 
     * The frontier format stores:
     * - In-progress nodes (not fully processed)
     * - Frontier nodes (fully processed children of in-progress parents)
     * 
     * On resume, frontier nodes enable skip optimization via ancestor checking.
     */
    @NotNull
    public static PathTree deserializeSlimFrom(@NotNull NodeState state) {
        PathTree tree = new PathTree();
        
        // Read counters
        PropertyState totalProp = state.getProperty("totalNodes");
        if (totalProp != null) {
            tree.totalNodes = totalProp.getValue(Type.LONG).intValue();
        }
        
        PropertyState indexedProp = state.getProperty("indexedNodes");
        if (indexedProp != null) {
            tree.indexedNodes = indexedProp.getValue(Type.LONG).intValue();
        }
        
        // Check for frontier format (new) vs legacy unprocessedPaths format
        PropertyState frontierProp = state.getProperty("frontierFormat");
        boolean isFrontierFormat = frontierProp != null && frontierProp.getValue(Type.BOOLEAN);
        
        if (isFrontierFormat) {
            // New frontier format
            PropertyState pathsProp = state.getProperty("paths");
            PropertyState enterProp = state.getProperty("enterFlags");
            PropertyState leaveProp = state.getProperty("leaveFlags");
            PropertyState frontierFlagsProp = state.getProperty("frontierFlags");
            
            if (pathsProp != null) {
                Iterable<String> paths = pathsProp.getValue(Type.STRINGS);
                Iterable<String> enters = enterProp != null ? enterProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
                Iterable<String> leaves = leaveProp != null ? leaveProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
                Iterable<String> frontiers = frontierFlagsProp != null ? frontierFlagsProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
                
                java.util.Iterator<String> pathIt = paths.iterator();
                java.util.Iterator<String> enterIt = enters.iterator();
                java.util.Iterator<String> leaveIt = leaves.iterator();
                java.util.Iterator<String> frontierIt = frontiers.iterator();
                
                int frontierCount = 0;
                int inProgressCount = 0;
                
                while (pathIt.hasNext()) {
                    String path = pathIt.next();
                    boolean enterCompleted = enterIt.hasNext() && Boolean.parseBoolean(enterIt.next());
                    boolean leaveCompleted = leaveIt.hasNext() && Boolean.parseBoolean(leaveIt.next());
                    boolean isFrontier = frontierIt.hasNext() && Boolean.parseBoolean(frontierIt.next());
                    
                    // Create the path in the tree
                    PathNode node = tree.getOrCreateNode(path);
                    node.setEnterCompleted(enterCompleted);
                    node.setLeaveCompleted(leaveCompleted);
                    
                    // Frontier nodes are fully processed
                    if (isFrontier) {
                        node.setIndexed(true);
                        frontierCount++;
                    } else {
                        inProgressCount++;
                    }
                }
                
                System.out.println("[DEBUG-PATHTREE-FRONTIER] Loaded " + (frontierCount + inProgressCount) + 
                    " paths (frontier=" + frontierCount + ", inProgress=" + inProgressCount + ")");
            }
        } else {
            // Legacy unprocessedPaths format (for backwards compatibility)
            PropertyState pathsProp = state.getProperty("unprocessedPaths");
            PropertyState enterProp = state.getProperty("enterFlags");
            PropertyState leaveProp = state.getProperty("leaveFlags");
            
            if (pathsProp != null) {
                Iterable<String> paths = pathsProp.getValue(Type.STRINGS);
                Iterable<String> enters = enterProp != null ? enterProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
                Iterable<String> leaves = leaveProp != null ? leaveProp.getValue(Type.STRINGS) : java.util.Collections.emptyList();
                
                java.util.Iterator<String> pathIt = paths.iterator();
                java.util.Iterator<String> enterIt = enters.iterator();
                java.util.Iterator<String> leaveIt = leaves.iterator();
                
                int loadedCount = 0;
                while (pathIt.hasNext()) {
                    String path = pathIt.next();
                    boolean enterCompleted = enterIt.hasNext() && Boolean.parseBoolean(enterIt.next());
                    boolean leaveCompleted = leaveIt.hasNext() && Boolean.parseBoolean(leaveIt.next());
                    
                    PathNode node = tree.getOrCreateNode(path);
                    node.setEnterCompleted(enterCompleted);
                    node.setLeaveCompleted(leaveCompleted);
                    if (enterCompleted || leaveCompleted) {
                        node.setIndexed(true);
                    }
                    loadedCount++;
                }
                
                System.out.println("[DEBUG-PATHTREE-SLIM] Loaded " + loadedCount + " unprocessed paths (legacy format)");
            }
        }
        
        // Slim format uses tree-based ancestor checking, not DFS order comparison
        // But if we have frontier nodes, ancestor checking works correctly
        tree.useDfsOrderComparison = false;
        
        return tree;
    }
    
    /**
     * Check if state contains slim format.
     */
    public static boolean isSlimFormat(@NotNull NodeState state) {
        PropertyState slimProp = state.getProperty("slimFormat");
        return slimProp != null && slimProp.getValue(Type.BOOLEAN);
    }
    
    /**
     * Deserialize from either ultra-slim, slim, or full format.
     */
    @NotNull
    public static PathTree deserializeAuto(@NotNull NodeState state) {
        if (isUltraSlimFormat(state)) {
            return deserializeUltraSlimFrom(state);
        } else if (isSlimFormat(state)) {
            return deserializeSlimFrom(state);
        } else {
            return deserializeFrom(state);
        }
    }
    
    /**
     * Get estimated serialized size in bytes.
     * Slim format: ~50 bytes per unprocessed path
     * Full format: ~50 bytes per node
     */
    public int getEstimatedSerializedSize(boolean slimFormat) {
        if (slimFormat) {
            return getNotFullyProcessedCount() * 80; // path string + flags
        } else {
            return totalNodes * 50; // all nodes
        }
    }
    
    /**
     * Save PathTree to a file for analysis.
     */
    public void saveToFile(@NotNull String filePath) {
        try (java.io.PrintWriter writer = new java.io.PrintWriter(filePath)) {
            writer.println("{");
            writer.println("  \"totalNodes\": " + totalNodes + ",");
            writer.println("  \"indexedNodes\": " + indexedNodes + ",");
            writer.println("  \"fullyProcessedCount\": " + getFullyProcessedCount() + ",");
            writer.println("  \"notFullyProcessedCount\": " + getNotFullyProcessedCount() + ",");
            writer.println("  \"unprocessedPaths\": [");
            
            java.util.List<String> unprocessedPaths = new java.util.ArrayList<>();
            collectUnprocessedPathsOnly(root, "/", unprocessedPaths);
            
            for (int i = 0; i < unprocessedPaths.size(); i++) {
                writer.print("    \"" + unprocessedPaths.get(i) + "\"");
                if (i < unprocessedPaths.size() - 1) writer.print(",");
                writer.println();
            }
            
            writer.println("  ]");
            writer.println("}");
            
            System.out.println("[DEBUG-PATHTREE] Saved PathTree to " + filePath + 
                " (total=" + totalNodes + ", unprocessed=" + unprocessedPaths.size() + ")");
        } catch (java.io.IOException e) {
            System.err.println("[ERROR] Failed to save PathTree to file: " + e.getMessage());
        }
    }
    
    private void collectUnprocessedPathsOnly(PathNode node, String path, java.util.List<String> paths) {
        if (!"/".equals(path) && !node.isFullyProcessed()) {
            paths.add(path);
        }
        for (Map.Entry<String, PathNode> entry : node.getChildren().entrySet()) {
            String childPath = "/".equals(path) ? "/" + entry.getKey() : path + "/" + entry.getKey();
            collectUnprocessedPathsOnly(entry.getValue(), childPath, paths);
        }
    }
    
    // ========== PathTree-Driven Traversal Support ==========
    
    /**
     * Check if this path can be traversed using PathTree instead of SegmentStore.
     * A path can be traversed from PathTree if:
     * 1. The path exists in PathTree
     * 2. The node is fully processed (enter+leave completed)
     * 3. All children are known (we've seen them before)
     */
    public boolean canTraverseFromPathTree(@NotNull String path) {
        PathNode node = getNode(path);
        if (node == null) {
            return false;
        }
        // Can traverse if fully processed - we know all its children
        return node.isFullyProcessed();
    }
    
    /**
     * Get child names from PathTree (without calling SegmentStore).
     * Only call this if canTraverseFromPathTree() returns true.
     * 
     * @param path the parent path
     * @return set of child names, or empty set if path not found
     */
    @NotNull
    public Set<String> getChildNamesFromPathTree(@NotNull String path) {
        PathNode node = getNode(path);
        if (node == null) {
            return Set.of();
        }
        return node.getChildNames();
    }
    
    /**
     * Get count of nodes that are NOT fully processed (need SegmentStore).
     */
    public int getNotFullyProcessedCount() {
        return countNotFullyProcessed(root);
    }
    
    private int countNotFullyProcessed(PathNode node) {
        // Count this node if it exists but is not fully processed
        int count = (!node.getName().isEmpty() && !node.isFullyProcessed()) ? 1 : 0;
        for (PathNode child : node.getChildren().values()) {
            count += countNotFullyProcessed(child);
        }
        return count;
    }
    
    /**
     * Check if PathTree is empty (no nodes).
     */
    public boolean isEmpty() {
        return root.getChildCount() == 0;
    }
    
    /**
     * Get traversal statistics.
     */
    public TraversalStats getTraversalStats() {
        TraversalStats stats = new TraversalStats();
        collectStats(root, stats);
        return stats;
    }
    
    private void collectStats(PathNode node, TraversalStats stats) {
        if (!node.getName().isEmpty()) { // Don't count root
            stats.totalNodes++;
            if (node.isFullyProcessed()) {
                stats.fullyProcessed++;
            } else {
                stats.notFullyProcessed++;
            }
            if (node.isEnterCompleted() && !node.isLeaveCompleted()) {
                stats.enterOnlyCompleted++;
            }
        }
        for (PathNode child : node.getChildren().values()) {
            collectStats(child, stats);
        }
    }
    
    /**
     * Statistics about PathTree traversal state.
     */
    public static class TraversalStats {
        public int totalNodes = 0;
        public int fullyProcessed = 0;
        public int notFullyProcessed = 0;
        public int enterOnlyCompleted = 0;  // Enter done, leave pending (interrupted?)
        
        @Override
        public String toString() {
            return "TraversalStats{total=" + totalNodes + 
                   ", fullyProcessed=" + fullyProcessed + 
                   ", notFullyProcessed=" + notFullyProcessed +
                   ", enterOnly=" + enterOnlyCompleted + "}";
        }
    }
    
    // ========== Pruning Methods ==========
    
    /**
     * Prune fully processed leaf nodes to reduce storage size.
     * Keeps non-leaf nodes (needed for traversal structure) and not-fully-processed nodes.
     * 
     * @return the number of nodes pruned
     */
    public int pruneFullyProcessedLeaves() {
        int[] prunedCount = {0};
        pruneRecursive(root, prunedCount);
        return prunedCount[0];
    }
    
    private void pruneRecursive(PathNode node, int[] prunedCount) {
        // First, recursively prune children
        for (PathNode child : new java.util.ArrayList<>(node.getChildren().values())) {
            pruneRecursive(child, prunedCount);
        }
        
        // Then, remove fully processed leaf children
        java.util.Iterator<Map.Entry<String, PathNode>> it = node.getChildren().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, PathNode> entry = it.next();
            PathNode child = entry.getValue();
            
            // Prune if: fully processed AND has no children (leaf)
            if (child.isFullyProcessed() && child.getChildCount() == 0) {
                it.remove();
                prunedCount[0]++;
                totalNodes--;
                // NOTE: Don't decrement indexedNodes - pruning removes tree structure
                // for storage efficiency, but the content remains indexed in Lucene
            }
        }
    }
    
    /**
     * Get pruning statistics - how many nodes could be pruned.
     */
    public int getPrunableNodeCount() {
        return countPrunable(root);
    }
    
    private int countPrunable(PathNode node) {
        int count = 0;
        for (PathNode child : node.getChildren().values()) {
            count += countPrunable(child);
            if (child.isFullyProcessed() && child.getChildCount() == 0) {
                count++;
            }
        }
        return count;
    }
    
    @Override
    public String toString() {
        return "PathTree{totalNodes=" + totalNodes + ", indexedNodes=" + indexedNodes + 
               ", fullyProcessed=" + getFullyProcessedCount() + "}";
    }
}

