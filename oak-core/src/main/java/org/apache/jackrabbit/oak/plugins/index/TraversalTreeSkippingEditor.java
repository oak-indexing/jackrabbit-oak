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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Editor that uses the TraversalTree to skip already-indexed subtrees.
 * 
 * When a subtree is marked as indexed in the TraversalTree, this editor
 * returns null from childNode* methods, causing EditorDiff to skip the
 * entire subtree WITHOUT reading from NodeStore.
 * 
 * This reduces skip phase from O(indexed_nodes) to O(1) for each indexed subtree.
 */
public class TraversalTreeSkippingEditor implements Editor {
    
    private static final Logger log = LoggerFactory.getLogger(TraversalTreeSkippingEditor.class);
    
    private final Editor delegate;
    private final TraversalTree currentTreeNode;
    private final AtomicLong skippedSubtrees;
    private final AtomicLong skippedNodes;
    private final AtomicLong processedNodes;
    private final AtomicLong skippedApproved;  // Track skipped "approved" nodes for verification
    private int childPosition = 0;
    
    /**
     * Create root skipping editor.
     * 
     * @param delegate the wrapped editor (usually TraversalTrackingEditor -> ResumingEditor -> IndexUpdate)
     * @param tree the traversal tree with indexed flags
     */
    public TraversalTreeSkippingEditor(Editor delegate, TraversalTree tree) {
        this(delegate, tree, new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong());
        // Debug: show tree structure
        if (tree != null) {
            log.info("TraversalTreeSkippingEditor created with tree: {} children at root", tree.getChildCount());
        }
    }
    
    private TraversalTreeSkippingEditor(Editor delegate, TraversalTree treeNode,
                                        AtomicLong skippedSubtrees, AtomicLong skippedNodes,
                                        AtomicLong processedNodes, AtomicLong skippedApproved) {
        this.delegate = delegate;
        this.currentTreeNode = treeNode;
        this.skippedSubtrees = skippedSubtrees;
        this.skippedNodes = skippedNodes;
        this.processedNodes = processedNodes;
        this.skippedApproved = skippedApproved;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        delegate.enter(before, after);
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        delegate.leave(before, after);
    }
    
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        delegate.propertyAdded(after);
    }
    
    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        delegate.propertyChanged(before, after);
    }
    
    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        delegate.propertyDeleted(before);
    }
    
    @Override
    @Nullable
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        childPosition++;
        
        // Check if this child is already indexed in the TraversalTree
        // IMPORTANT: Only skip STRUCTURE nodes (hasContent=false)!
        // Content nodes MUST be processed by IndexUpdate because:
        // 1. Each chunk creates a NEW LuceneIndexWriter
        // 2. If we skip content, the writer never sees those nodes
        // 3. The writer's leave() is never called, so no index accumulation
        // Tree skip only works for structure-only subtrees (jcr:system, oak:index, etc.)
        if (currentTreeNode != null) {
            TraversalTree childTree = currentTreeNode.getChild(name);
            if (childTree != null && childTree.isIndexed() && !childTree.hasContent()) {
                // Structure-only subtree - safe to skip entirely
                // No content properties means no Lucene docs to worry about
                int subtreeSize = childTree.getCachedSubtreeSize();
                skippedSubtrees.incrementAndGet();
                skippedNodes.addAndGet(subtreeSize);
                
                if (log.isDebugEnabled()) {
                    log.debug("[TREE-SKIP] SKIP structure: {} ({} nodes)", 
                            childTree.getPath(), subtreeSize);
                }
                return null;
            }
        }
        
        // Content node or not indexed - process normally
        processedNodes.incrementAndGet();
        
        Editor childDelegate = delegate.childNodeAdded(name, after);
        if (childDelegate == null) {
            return null;
        }
        
        // Get tree node AFTER delegate call (TraversalTrackingEditor may have created it)
        TraversalTree childTree = currentTreeNode != null ? currentTreeNode.getChild(name) : null;
        
        return new TraversalTreeSkippingEditor(childDelegate, childTree, 
                                               skippedSubtrees, skippedNodes, processedNodes, skippedApproved);
    }
    
    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        childPosition++;
        
        // Only skip STRUCTURE nodes - content nodes must be processed
        if (currentTreeNode != null) {
            TraversalTree childTree = currentTreeNode.getChild(name);
            if (childTree != null && childTree.isIndexed() && !childTree.hasContent()) {
                int subtreeSize = childTree.getCachedSubtreeSize();
                skippedSubtrees.incrementAndGet();
                skippedNodes.addAndGet(subtreeSize);
                
                if (log.isDebugEnabled()) {
                    log.debug("[TREE-SKIP] SKIP structure changed: {} ({} nodes)", childTree.getPath(), subtreeSize);
                }
                return null;
            }
        }
        
        processedNodes.incrementAndGet();
        Editor childDelegate = delegate.childNodeChanged(name, before, after);
        if (childDelegate == null) {
            return null;
        }
        
        // Use existing tree node if found
        TraversalTree childTree = currentTreeNode != null ? currentTreeNode.getChild(name) : null;
        
        return new TraversalTreeSkippingEditor(childDelegate, childTree,
                                               skippedSubtrees, skippedNodes, processedNodes, skippedApproved);
    }
    
    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        childPosition++;
        
        // Only skip STRUCTURE nodes - content deletions must be processed
        if (currentTreeNode != null) {
            TraversalTree childTree = currentTreeNode.getChild(name);
            if (childTree != null && childTree.isIndexed() && !childTree.hasContent()) {
                int subtreeSize = childTree.getCachedSubtreeSize();
                skippedSubtrees.incrementAndGet();
                skippedNodes.addAndGet(subtreeSize);
                
                if (log.isDebugEnabled()) {
                    log.debug("[TREE-SKIP] SKIP structure deleted: {} ({} nodes)", childTree.getPath(), subtreeSize);
                }
                return null;
            }
        }
        
        processedNodes.incrementAndGet();
        Editor childDelegate = delegate.childNodeDeleted(name, before);
        if (childDelegate == null) {
            return null;
        }
        
        // Use existing tree node if found
        TraversalTree childTree = currentTreeNode != null ? currentTreeNode.getChild(name) : null;
        
        return new TraversalTreeSkippingEditor(childDelegate, childTree,
                                               skippedSubtrees, skippedNodes, processedNodes, skippedApproved);
    }
    
    /**
     * Get statistics about skipped subtrees.
     * @return array of [skippedSubtrees, skippedNodes, processedNodes]
     */
    public long[] getSkipStats() {
        return new long[] {
            skippedSubtrees.get(),
            skippedNodes.get(),
            processedNodes.get()
        };
    }
    
    /**
     * Log skip statistics.
     */
    public void logStats() {
        log.info("TraversalTreeSkipping stats: skippedSubtrees={}, skippedNodes={}, processedNodes={}, skippedApproved={}",
                skippedSubtrees.get(), skippedNodes.get(), processedNodes.get(), skippedApproved.get());
        System.out.println("[TREE SKIP] Skipped " + skippedSubtrees.get() + " subtrees (" + 
                          skippedNodes.get() + " nodes), processed " + processedNodes.get() + " nodes" +
                          (skippedApproved.get() > 0 ? " [WARNING: " + skippedApproved.get() + " approved skipped!]" : ""));
    }
}

