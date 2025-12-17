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
        
        public PathNode(String name) {
            this.name = name;
            this.children = new ConcurrentHashMap<>();
            this.indexed = false;
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
        
        for (String childName : state.getChildNodeNames()) {
            if (!"indexed".equals(childName) && !"primaryType".equals(childName)) {
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
    
    @Override
    public String toString() {
        return "PathTree{totalNodes=" + totalNodes + ", indexedNodes=" + indexedNodes + "}";
    }
}

