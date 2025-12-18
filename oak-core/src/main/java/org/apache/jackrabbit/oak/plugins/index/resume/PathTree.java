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
    
    public PathTree() {
        this.root = new PathNode("");
        this.totalNodes = 0;
        this.indexedNodes = 0;
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
            current = current.getOrCreateChild(segment);
        }
        totalNodes++;
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
     * This also marks the node as indexed.
     */
    public void markLeaveCompleted(@NotNull String path) {
        PathNode node = getOrCreateNode(path);
        node.setLeaveCompleted(true);
        if (!node.isIndexed()) {
            node.setIndexed(true);
            indexedNodes++;
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
     */
    public boolean isFullyProcessed(@NotNull String path) {
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
                indexedNodes--;
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

