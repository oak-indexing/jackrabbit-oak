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

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A virtual NodeState backed by TraversalTree metadata.
 * 
 * <h2>Purpose</h2>
 * During resume, we need to replay enter()/leave() calls for nodes we've
 * already indexed. Instead of reading from SegmentStore (expensive I/O),
 * we use this virtual state that provides enough information for:
 * <ul>
 *   <li>Tree navigation (getChildNode, getChildNodeNames)</li>
 *   <li>Existence checks (exists)</li>
 * </ul>
 * 
 * <h2>Limitations</h2>
 * This is a minimal implementation. Properties are not available (empty).
 * This is fine because we skip property processing during the skip phase.
 * 
 * <h2>Example Usage</h2>
 * <pre>
 * // During skip phase, create virtual state from tree
 * TraversalTree treeNode = tree.findByPath("/content/dam");
 * NodeState virtualState = treeNode.toVirtualNodeState();
 * 
 * // Navigate children without SegmentStore reads
 * for (String childName : virtualState.getChildNodeNames()) {
 *     NodeState child = virtualState.getChildNode(childName);
 *     // child is also a VirtualNodeState
 * }
 * </pre>
 */
public class VirtualNodeState extends AbstractNodeState {
    
    private final TraversalTree treeNode;
    
    /**
     * Create a virtual node state backed by tree metadata.
     * 
     * @param treeNode The TraversalTree node providing metadata
     */
    public VirtualNodeState(@NotNull TraversalTree treeNode) {
        this.treeNode = treeNode;
    }
    
    /**
     * Get the underlying TraversalTree node.
     */
    public TraversalTree getTreeNode() {
        return treeNode;
    }
    
    @Override
    public boolean exists() {
        // If we have this in the tree, it existed when we indexed it
        return true;
    }
    
    @Override
    public boolean hasProperty(@NotNull String name) {
        // We provide just enough to let index rule matching work without NodeStore reads.
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
            return treeNode.getPrimaryType() != null;
        }
        return false;
    }
    
    @Override
    public PropertyState getProperty(@NotNull String name) {
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
            String pt = treeNode.getPrimaryType();
            if (pt != null) {
                return PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, pt, Type.NAME);
            }
        }
        return null;
    }
    
    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        PropertyState pt = getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (pt != null) {
            List<PropertyState> props = new ArrayList<>(1);
            props.add(pt);
            return props;
        }
        return emptyList();
    }
    
    @Override
    public long getPropertyCount() {
        return treeNode.getPrimaryType() != null ? 1 : 0;
    }
    
    @Override
    public boolean hasChildNode(@NotNull String name) {
        // Check if we have this child in our tree
        return treeNode.hasChild(name);
    }
    
    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) {
        AbstractNodeState.checkValidName(name);
        
        // Look up child in tree
        TraversalTree childTree = treeNode.getChild(name);
        if (childTree != null) {
            // Return virtual state for indexed child
            return childTree.toVirtualNodeState();
        }
        
        // Child not in tree - return MISSING_NODE
        return MISSING_NODE;
    }
    
    @Override
    public long getChildNodeCount(long max) {
        // Use captured child names if available
        List<String> names = treeNode.getChildNames();
        if (!names.isEmpty()) {
            return Math.min(names.size(), max);
        }
        // Fall back to tree children count
        return Math.min(treeNode.getChildCount(), max);
    }
    
    @NotNull
    @Override
    public Iterable<String> getChildNodeNames() {
        // Prefer captured names (preserves original iteration order)
        List<String> capturedNames = treeNode.getChildNames();
        if (!capturedNames.isEmpty()) {
            return capturedNames;
        }
        
        // Fall back to tree children
        List<String> names = new ArrayList<>();
        for (TraversalTree child : treeNode.getChildrenInOrder()) {
            names.add(child.getName());
        }
        return names;
    }
    
    @NotNull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        List<ChildNodeEntry> entries = new ArrayList<>();
        
        // Prefer captured names for correct order
        List<String> capturedNames = treeNode.getChildNames();
        if (!capturedNames.isEmpty()) {
            for (String name : capturedNames) {
                TraversalTree childTree = treeNode.getChild(name);
                if (childTree != null) {
                    entries.add(new VirtualChildNodeEntry(name, childTree));
                }
            }
        } else {
            // Fall back to tree children
            for (TraversalTree child : treeNode.getChildrenInOrder()) {
                entries.add(new VirtualChildNodeEntry(child.getName(), child));
            }
        }
        
        return entries;
    }
    
    @NotNull
    @Override
    public NodeBuilder builder() {
        // Return a memory builder - virtual states are read-only for navigation
        return new MemoryNodeBuilder(this);
    }
    
    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        // For virtual states during skip phase, we report as "added" node
        // This allows editors to receive enter()/leave() calls
        return AbstractNodeState.compareAgainstBaseState(this, base, diff);
    }
    
    @Override
    @Nullable
    public String getString(@NotNull String name) {
        PropertyState ps = getProperty(name);
        return ps != null ? ps.getValue(Type.STRING) : null;
    }
    
    @Override
    public String toString() {
        return "VirtualNodeState{path=" + treeNode.getPath() + 
               ", indexed=" + treeNode.isIndexed() + 
               ", children=" + treeNode.getChildCount() + "}";
    }
    
    /**
     * Virtual child node entry backed by TraversalTree.
     */
    private static class VirtualChildNodeEntry implements ChildNodeEntry {
        private final String name;
        private final TraversalTree treeNode;
        
        VirtualChildNodeEntry(String name, TraversalTree treeNode) {
            this.name = name;
            this.treeNode = treeNode;
        }
        
        @NotNull
        @Override
        public String getName() {
            return name;
        }
        
        @NotNull
        @Override
        public NodeState getNodeState() {
            return treeNode.toVirtualNodeState();
        }
        
        @Override
        public String toString() {
            return name + " = " + treeNode.getPath();
        }
    }
}

