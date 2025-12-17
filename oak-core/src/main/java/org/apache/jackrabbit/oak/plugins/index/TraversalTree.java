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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Lightweight in-memory tree structure that tracks:
 * 1. Tree structure (node names and parent-child relationships)
 * 2. Iteration order (LinkedHashMap preserves insertion order)
 * 3. Indexing status (which nodes have been indexed)
 * 4. Node metadata (primaryType, childNames) for virtual navigation
 * 
 * This enables O(depth) resume instead of O(visited_nodes) traversal.
 * 
 * <h2>Tree-Driven Resume</h2>
 * When resuming from a saved path, we use this tree to navigate during the
 * skip phase WITHOUT reading from SegmentStore:
 * <pre>
 * 1. For nodes we've already LEFT (fully indexed):
 *    - Use VirtualNodeState backed by TraversalTree data
 *    - No SegmentStore I/O required
 *    - enter()/leave() calls happen in same order as original
 * 
 * 2. For nodes we're resuming AT and beyond:
 *    - Use real NodeState from SegmentStore
 *    - Normal EditorDiff processing
 * </pre>
 */
public class TraversalTree implements Serializable {
    private static final long serialVersionUID = 2L;  // Updated version
    
    private final String name;
    private final int iterationPosition;  // Position in parent's child iteration
    private boolean indexed;
    private boolean hasContent;  // True if this node or any descendant has content properties
    
    // Node metadata for virtual navigation (captured when node is entered)
    private String primaryType;  // jcr:primaryType value
    private List<String> childNames;  // Ordered list of child names (captured from iteration)
    
    // Cached subtree size - O(1) instead of O(N) for skip decisions
    // Updated incrementally when children are added/removed
    private int cachedSubtreeSize = 1;  // Count self
    
    // LinkedHashMap preserves insertion (iteration) order!
    private final LinkedHashMap<String, TraversalTree> children = new LinkedHashMap<>();
    
    // Parent reference (transient - rebuilt on deserialization)
    private transient TraversalTree parent;
    
    /**
     * Create root node.
     */
    public TraversalTree() {
        this.name = "";
        this.iterationPosition = 0;
        this.parent = null;
    }
    
    /**
     * Create child node.
     */
    public TraversalTree(String name, int iterationPosition, TraversalTree parent) {
        this.name = name;
        this.iterationPosition = iterationPosition;
        this.parent = parent;
    }
    
    /**
     * Add a child node, preserving iteration order.
     */
    public TraversalTree addChild(String name, int position) {
        TraversalTree child = new TraversalTree(name, position, this);
        children.put(name, child);
        
        // Update cached size up the tree (O(depth) instead of O(N))
        updateAncestorSize(1);
        
        return child;
    }
    
    /**
     * Update cached subtree size for this node and all ancestors.
     * Called when children are added (+delta) or removed (-delta).
     */
    private void updateAncestorSize(int delta) {
        TraversalTree node = this;
        while (node != null) {
            node.cachedSubtreeSize += delta;
            node = node.parent;
        }
    }
    
    /**
     * Get cached subtree size (O(1) operation).
     * Much faster than countNodes() which is O(N).
     */
    public int getCachedSubtreeSize() {
        return cachedSubtreeSize;
    }
    
    /**
     * Get or create child node.
     */
    public TraversalTree getOrCreateChild(String name, int position) {
        TraversalTree existing = children.get(name);
        if (existing != null) {
            return existing;
        }
        return addChild(name, position);
    }
    
    /**
     * Get child by name.
     */
    public TraversalTree getChild(String name) {
        return children.get(name);
    }
    
    /**
     * Check if has child.
     */
    public boolean hasChild(String name) {
        return children.containsKey(name);
    }
    
    /**
     * Mark this node as indexed.
     */
    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }
    
    /**
     * Check if indexed (traversal complete).
     */
    public boolean isIndexed() {
        return indexed;
    }
    
    /**
     * Check if any children are indexed.
     * This is used to determine if a parent node should be marked as indexed
     * even if it had no properties processed (because it contains indexed children).
     */
    public boolean hasIndexedChildren() {
        if (children == null || children.isEmpty()) {
            return false;
        }
        for (TraversalTree child : children.values()) {
            if (child.isIndexed()) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Mark this node as having content (non-system properties).
     */
    public void setHasContent(boolean hasContent) {
        this.hasContent = hasContent;
    }
    
    /**
     * Check if this node or any descendant has content properties.
     */
    public boolean hasContent() {
        return hasContent;
    }
    
    /**
     * Prune indexed children to save memory.
     * Once a subtree is fully indexed (leave() called), we don't need to keep it in memory.
     * We only need to track:
     * 1. The active path (nodes currently being processed - entered but not left)
     * 2. Unvisited siblings (nodes not yet entered)
     * 
     * This reduces memory from O(all nodes) to O(tree depth + unvisited siblings).
     * 
     * Example:
     * Before pruning:
     *   Root (indexed=true, 100K children all indexed)
     *     ├─ child-0 (indexed=true) ... DONE - prune this
     *     ├─ child-1 (indexed=true) ... DONE - prune this
     *     ...
     *     ├─ child-99998 (indexed=true) ... DONE - prune this
     *     └─ child-99999 (indexed=false) ... ACTIVE - keep this
     * 
     * After pruning:
     *   Root (indexed=true, 1 child)
     *     └─ child-99999 (indexed=false) ... ACTIVE - keep this
     * 
     * Memory saved: 99,999 nodes * ~46 bytes = ~4.6 MB
     */
    public void pruneIndexedChildren() {
        if (children.isEmpty()) {
            return;
        }
        
        // Count nodes to be pruned for size update
        int prunedNodes = 0;
        int stubbedChildren = 0;
        
        // Convert fully-indexed children into lightweight stubs instead of removing them.
        //
        // Why stubs?
        // - We want to preserve DFS iteration order for resume replay
        // - We want VirtualNodeState.getChildNode() to keep working even after pruning
        // - We don't need the full subtree in memory once it's indexed
        //
        // A stub keeps only: name, primaryType, indexed flag, cachedSubtreeSize metadata
        // and drops its children map + captured childNames to save memory.
        Iterator<java.util.Map.Entry<String, TraversalTree>> it = children.entrySet().iterator();
        while (it.hasNext()) {
            TraversalTree child = it.next().getValue();
            // Only remove if fully indexed (no unindexed descendants)
            if (child.indexed && !child.hasUnindexedDescendants()) {
                int oldSize = child.cachedSubtreeSize;
                // Keep a stub node (size=1) and drop its subtree
                child.children.clear();
                if (child.childNames != null) {
                    child.childNames = Collections.emptyList();
                }
                child.cachedSubtreeSize = 1;
                // Update ancestor sizes by the nodes we no longer keep in-memory
                int delta = Math.max(0, oldSize - 1);
                if (delta > 0) {
                    prunedNodes += delta;
                }
                stubbedChildren++;
            }
        }
        
        // Update cached size (O(depth) instead of recounting)
        if (prunedNodes > 0) {
            updateAncestorSize(-prunedNodes);
        }
        
        // Log significant pruning operations (threshold: 100 nodes)
        if (stubbedChildren > 100) {
            System.out.println("[TREE PRUNE] " + getPath() + ": stubbed " + stubbedChildren + " children (" + prunedNodes + " nodes freed)");
        }
    }
    
    /**
     * Check if this node or any descendant is not yet indexed.
     * Used to determine if we can safely prune a subtree.
     */
    private boolean hasUnindexedDescendants() {
        if (!indexed) {
            return true;
        }
        
        for (TraversalTree child : children.values()) {
            if (child.hasUnindexedDescendants()) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Get node name.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get iteration position in parent.
     */
    public int getIterationPosition() {
        return iterationPosition;
    }
    
    /**
     * Get parent node.
     */
    public TraversalTree getParent() {
        return parent;
    }
    
    /**
     * Get children iterator (in iteration order).
     */
    public Iterator<TraversalTree> childrenIterator() {
        return children.values().iterator();
    }
    
    /**
     * Get children starting from a specific position.
     */
    public Iterator<TraversalTree> childrenIteratorFrom(int startPosition) {
        List<TraversalTree> remaining = new ArrayList<>();
        for (TraversalTree child : children.values()) {
            if (child.iterationPosition >= startPosition) {
                remaining.add(child);
            }
        }
        return remaining.iterator();
    }
    
    /**
     * Get number of children.
     */
    public int getChildCount() {
        return children.size();
    }
    
    /**
     * Find first unindexed node in iteration order.
     * Returns null if all nodes are indexed.
     */
    public TraversalTree findFirstUnindexed() {
        if (!indexed) {
            return this;
        }
        for (TraversalTree child : children.values()) {
            TraversalTree found = child.findFirstUnindexed();
            if (found != null) {
                return found;
            }
        }
        return null;
    }
    
    /**
     * Find node at the given path.
     */
    public TraversalTree findByPath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return this;
        }
        
        String[] segments = path.split("/");
        TraversalTree current = this;
        
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.getChild(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
    
    /**
     * Get path as string array for navigation.
     */
    public String[] getPathSegments() {
        List<String> segments = new ArrayList<>();
        TraversalTree current = this;
        while (current != null && current.parent != null) {
            segments.add(0, current.name);
            current = current.parent;
        }
        return segments.toArray(new String[0]);
    }
    
    /**
     * Get full path as string.
     */
    public String getPath() {
        String[] segments = getPathSegments();
        if (segments.length == 0) {
            return "/";
        }
        StringBuilder sb = new StringBuilder();
        for (String segment : segments) {
            sb.append("/").append(segment);
        }
        return sb.toString();
    }
    
    /**
     * Count total nodes in tree.
     */
    public int countNodes() {
        int count = 1;
        for (TraversalTree child : children.values()) {
            count += child.countNodes();
        }
        return count;
    }
    
    /**
     * Count indexed nodes.
     */
    public int countIndexedNodes() {
        int count = indexed ? 1 : 0;
        for (TraversalTree child : children.values()) {
            count += child.countIndexedNodes();
        }
        return count;
    }
    
    /**
     * Count nodes with content (non-system properties).
     */
    public int countContentNodes() {
        int count = hasContent ? 1 : 0;
        for (TraversalTree child : children.values()) {
            count += child.countContentNodes();
        }
        return count;
    }
    
    /**
     * Rebuild parent references after deserialization.
     */
    public void rebuildParentReferences() {
        for (TraversalTree child : children.values()) {
            child.parent = this;
            child.rebuildParentReferences();
        }
    }
    
    /**
     * Serialize to compact JSON format.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        toJson(sb);
        return sb.toString();
    }
    
    private void toJson(StringBuilder sb) {
        sb.append("{\"n\":\"").append(escapeJson(name)).append("\"");
        sb.append(",\"p\":").append(iterationPosition);
        sb.append(",\"i\":").append(indexed);
        sb.append(",\"h\":").append(hasContent);  // hasContent flag
        if (!children.isEmpty()) {
            sb.append(",\"c\":[");
            boolean first = true;
            for (TraversalTree child : children.values()) {
                if (!first) sb.append(",");
                child.toJson(sb);
                first = false;
            }
            sb.append("]");
        }
        sb.append("}");
    }
    
    /**
     * Parse from JSON format.
     */
    public static TraversalTree fromJson(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        TraversalTree tree = parseJson(json, 0, null).tree;
        tree.rebuildParentReferences();
        return tree;
    }
    
    private static class ParseResult {
        TraversalTree tree;
        int endPos;
        ParseResult(TraversalTree tree, int endPos) {
            this.tree = tree;
            this.endPos = endPos;
        }
    }
    
    private static ParseResult parseJson(String json, int start, TraversalTree parent) {
        // Find name
        int nameStart = json.indexOf("\"n\":\"", start) + 5;
        int nameEnd = json.indexOf("\"", nameStart);
        String name = unescapeJson(json.substring(nameStart, nameEnd));
        
        // Find position
        int posStart = json.indexOf("\"p\":", nameEnd) + 4;
        int posEnd = posStart;
        while (posEnd < json.length() && Character.isDigit(json.charAt(posEnd))) posEnd++;
        int position = Integer.parseInt(json.substring(posStart, posEnd));
        
        // Find indexed
        int indexedStart = json.indexOf("\"i\":", posEnd) + 4;
        boolean indexed = json.substring(indexedStart, indexedStart + 4).startsWith("true");
        
        // Find hasContent
        int hasContentStart = json.indexOf("\"h\":", indexedStart);
        boolean hasContent = false;
        if (hasContentStart != -1 && hasContentStart < json.indexOf("}", indexedStart)) {
            hasContentStart += 4;
            hasContent = json.substring(hasContentStart, hasContentStart + 4).startsWith("true");
        }
        
        TraversalTree tree = parent == null ? new TraversalTree() : new TraversalTree(name, position, parent);
        tree.indexed = indexed;
        tree.hasContent = hasContent;
        
        // Find children - look for "c":[ after hasContent
        int searchFrom = hasContentStart > 0 ? hasContentStart : indexedStart;
        int childrenStart = json.indexOf("\"c\":[", searchFrom);
        int objEnd = json.indexOf("}", searchFrom);
        
        if (childrenStart != -1 && childrenStart < objEnd) {
            int pos = childrenStart + 5;
            while (pos < json.length() && json.charAt(pos) != ']') {
                if (json.charAt(pos) == '{') {
                    ParseResult childResult = parseJson(json, pos, tree);
                    tree.children.put(childResult.tree.name, childResult.tree);
                    pos = childResult.endPos;
                }
                pos++;
            }
            objEnd = json.indexOf("}", pos);
        }
        
        return new ParseResult(tree, objEnd);
    }
    
    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
    
    private static String unescapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\\"", "\"").replace("\\\\", "\\");
    }
    
    @Override
    public String toString() {
        return "TraversalTree{path=" + getPath() + ", indexed=" + indexed + 
               ", hasContent=" + hasContent + ", children=" + children.size() + "}";
    }
    
    /**
     * Check if this subtree can be safely skipped during resume.
     * A subtree can be skipped if:
     * 1. It has been fully indexed (traversal complete)
     * 2. It has no content properties (only system/structure nodes)
     * OR
     * 1. It has been fully indexed (traversal complete)
     * 2. It has content but was already processed (indexed + hasContent)
     */
    public boolean canSkip() {
        // Only skip if fully indexed AND either:
        // - Has no content (pure structure like jcr:nodeTypes)
        // - Has content that was already indexed
        return indexed;
    }
    
    // =========================================================================
    // TREE-DRIVEN RESUME SUPPORT
    // =========================================================================
    
    /**
     * Set the primary type (jcr:primaryType) of this node.
     * Captured during enter() for use in virtual navigation.
     */
    public void setPrimaryType(String primaryType) {
        this.primaryType = primaryType;
    }
    
    /**
     * Get the primary type of this node.
     */
    public String getPrimaryType() {
        return primaryType;
    }
    
    /**
     * Capture child names from the real NodeState.
     * Called during enter() to record the iteration order for virtual replay.
     */
    public void captureChildNames(NodeState nodeState) {
        if (nodeState == null || !nodeState.exists()) {
            this.childNames = Collections.emptyList();
            return;
        }
        
        List<String> names = new ArrayList<>();
        for (String childName : nodeState.getChildNodeNames()) {
            names.add(childName);
        }
        this.childNames = names;
    }
    
    /**
     * Get the captured child names in iteration order.
     */
    public List<String> getChildNames() {
        return childNames != null ? childNames : Collections.emptyList();
    }
    
    /**
     * Check if this tree node has captured metadata for virtual navigation.
     */
    public boolean hasMetadata() {
        return childNames != null;
    }
    
    /**
     * Create a VirtualNodeState backed by this TraversalTree node.
     * Used during skip phase to avoid SegmentStore reads.
     * 
     * The virtual state provides:
     * - exists() = true (we know it exists because we indexed it)
     * - getChildNodeNames() = captured child names in order
     * - getChildNode(name) = virtual child states for indexed children
     * - Properties = empty (not needed for navigation, we skip properties)
     * 
     * @return A NodeState backed by this tree's metadata
     */
    public NodeState toVirtualNodeState() {
        return new VirtualNodeState(this);
    }
    
    /**
     * Get child node entries in iteration order as Iterable.
     * Returns children in the same order they were originally visited.
     */
    public Iterable<TraversalTree> getChildrenInOrder() {
        return children.values();
    }
}

