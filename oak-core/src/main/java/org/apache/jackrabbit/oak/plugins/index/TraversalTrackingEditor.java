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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

/**
 * Editor wrapper that builds an in-memory TraversalTree during indexing.
 * 
 * This tracks:
 * 1. Which nodes were visited (in iteration order)
 * 2. Which nodes were indexed (marked on leave())
 * 
 * The TraversalTree can then be serialized and used for fast resume.
 */
public class TraversalTrackingEditor implements Editor {
    
    // Cache system property at class loading time (not on every leave() call)
    private static final boolean PRUNING_DISABLED = Boolean.getBoolean("oak.async.disableTreePruning");
    
    private final Editor delegate;
    private final TraversalTree currentNode;
    private int childPosition = 0;
    
    // Track if any properties were processed (indicates real indexing happened)
    private boolean anyPropertiesProcessed = false;
    
    /**
     * Create tracking editor for a given tree node.
     * 
     * @param delegate the wrapped editor
     * @param treeNode the tree node (root or child)
     */
    public TraversalTrackingEditor(Editor delegate, TraversalTree treeNode) {
        this.delegate = delegate;
        this.currentNode = treeNode;
    }
    
    /**
     * Get the tracking tree.
     */
    public TraversalTree getTraversalTree() {
        // Navigate to root
        TraversalTree root = currentNode;
        while (root.getParent() != null) {
            root = root.getParent();
        }
        return root;
    }
    
    /**
     * Get current node in tree.
     */
    public TraversalTree getCurrentNode() {
        return currentNode;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        // Capture child names for tree-driven resume
        // This preserves the iteration order for virtual traversal
        if (after != null && after.exists()) {
            currentNode.captureChildNames(after);
            
            // Also capture primaryType for potential future use
            String primaryType = after.getString("jcr:primaryType");
            if (primaryType != null) {
                currentNode.setPrimaryType(primaryType);
            }
        }
        
        delegate.enter(before, after);
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        // IMPORTANT: Call delegate.leave() FIRST to ensure actual indexing completes
        // Then mark as indexed. This ensures that if leave() throws (e.g., CHUNK_COMPLETE),
        // the node is NOT marked as indexed (so it will be reprocessed in next chunk)
        delegate.leave(before, after);
        
        // Check if any children were indexed (by inspecting the tree)
        boolean hasIndexedChildren = currentNode.hasIndexedChildren();
        
        // Only mark as indexed if REAL indexing happened:
        // - At least one property was processed, OR
        // - At least one child was indexed (this node is a parent of indexed content)
        // This prevents ResumingEditor's skip phase from marking nodes as indexed
        // when properties were actually skipped
        if (anyPropertiesProcessed || hasIndexedChildren) {
            currentNode.setIndexed(true);
            
            // Prune indexed children to save memory
            // Once a subtree is fully indexed, we don't need to keep it in memory
            // This reduces memory from O(all nodes) to O(tree depth + unvisited siblings)
            // Can be disabled with -Doak.async.disableTreePruning=true for comparison
            if (!PRUNING_DISABLED) {
                currentNode.pruneIndexedChildren();
            }
        }
    }
    
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        anyPropertiesProcessed = true;  // Track that real indexing happened
        // Mark node as having content if property is not a system property
        // System properties (jcr:*, rep:*, oak:*) are not typically indexed by Lucene
        if (isContentProperty(after.getName())) {
            markHasContent();
        }
        delegate.propertyAdded(after);
    }
    
    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        anyPropertiesProcessed = true;  // Track that real indexing happened
        // Mark node as having content if property is not a system property
        if (isContentProperty(after.getName())) {
            markHasContent();
        }
        delegate.propertyChanged(before, after);
    }
    
    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        anyPropertiesProcessed = true;  // Track that real indexing happened
        // Mark node as having content if property is not a system property
        if (isContentProperty(before.getName())) {
            markHasContent();
        }
        delegate.propertyDeleted(before);
    }
    
    /**
     * Check if a property is a content property (not a system property).
     * System properties (jcr:*, rep:*, oak:*) are typically not indexed by Lucene.
     * 
     * Optimized to use single char check first for common cases.
     */
    private static boolean isContentProperty(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        
        // Fast path: check first character
        char first = name.charAt(0);
        
        // Hidden properties start with ':'
        if (first == ':') {
            return false;
        }
        
        // jcr:* and oak:* properties - check for common prefixes
        if (first == 'j' && name.startsWith("jcr:")) {
            return false;
        }
        if (first == 'o' && name.startsWith("oak:")) {
            return false;
        }
        if (first == 'r' && name.startsWith("rep:")) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Mark current node and all ancestors as having content.
     * This propagates up the tree so that parent nodes know they contain
     * content somewhere in their subtree.
     */
    private void markHasContent() {
        TraversalTree node = currentNode;
        while (node != null && !node.hasContent()) {
            node.setHasContent(true);
            node = node.getParent();
        }
    }
    
    @Override
    @Nullable
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        // Track this child in our in-memory tree
        // Use getOrCreateChild to preserve existing nodes (important for resume!)
        TraversalTree childTreeNode = currentNode.getOrCreateChild(name, childPosition++);
        
        // Get delegate's child editor
        Editor childDelegate = delegate.childNodeAdded(name, after);
        if (childDelegate == null) {
            // Still track it even if delegate doesn't care
            return null;
        }
        
        // Wrap child editor to continue tracking
        return new TraversalTrackingEditor(childDelegate, childTreeNode);
    }
    
    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // Track this child
        TraversalTree childTreeNode = currentNode.getOrCreateChild(name, childPosition++);
        
        Editor childDelegate = delegate.childNodeChanged(name, before, after);
        if (childDelegate == null) {
            return null;
        }
        
        return new TraversalTrackingEditor(childDelegate, childTreeNode);
    }
    
    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // Track this child
        TraversalTree childTreeNode = currentNode.getOrCreateChild(name, childPosition++);
        
        Editor childDelegate = delegate.childNodeDeleted(name, before);
        if (childDelegate == null) {
            return null;
        }
        
        return new TraversalTrackingEditor(childDelegate, childTreeNode);
    }
}

