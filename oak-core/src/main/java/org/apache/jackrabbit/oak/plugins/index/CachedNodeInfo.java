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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cached information about a node, sufficient for building editor hierarchy
 * during skip phase without reading from SegmentStore.
 * 
 * Key insight: During skip phase, the indexing editor only needs:
 * 1. Path (to build editor hierarchy)
 * 2. Node type (jcr:primaryType) for determining indexing rules
 * 3. Child names in iteration order (for consistent DFS traversal)
 * 
 * We DON'T need full node content - that's only needed for actual indexing.
 */
public class CachedNodeInfo {
    
    private String name;
    private String primaryType;
    private List<String> mixinTypes;
    private LinkedHashMap<String, CachedNodeInfo> children;
    private boolean indexed;  // True if leave() was called (subtree complete)
    
    // Transient - not serialized
    private transient CachedNodeInfo parent;
    private transient int cachedSubtreeSize = -1;
    
    public CachedNodeInfo() {
        this.name = "";
        this.children = new LinkedHashMap<>();
        this.indexed = false;
    }
    
    public CachedNodeInfo(String name) {
        this.name = name;
        this.children = new LinkedHashMap<>();
        this.indexed = false;
    }
    
    /**
     * Create from a real NodeState, extracting only the needed info.
     */
    public static CachedNodeInfo from(String name, NodeState state) {
        CachedNodeInfo info = new CachedNodeInfo(name);
        
        // Extract primary type
        PropertyState primaryType = state.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (primaryType != null) {
            info.primaryType = primaryType.getValue(Type.STRING);
        }
        
        // Extract mixin types
        PropertyState mixins = state.getProperty(JcrConstants.JCR_MIXINTYPES);
        if (mixins != null) {
            info.mixinTypes = new ArrayList<>();
            for (String mixin : mixins.getValue(Type.STRINGS)) {
                info.mixinTypes.add(mixin);
            }
        }
        
        return info;
    }
    
    public String getName() {
        return name;
    }
    
    public String getPrimaryType() {
        return primaryType;
    }
    
    public List<String> getMixinTypes() {
        return mixinTypes != null ? mixinTypes : Collections.emptyList();
    }
    
    public boolean isIndexed() {
        return indexed;
    }
    
    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }
    
    public CachedNodeInfo getParent() {
        return parent;
    }
    
    /**
     * Add a child, preserving iteration order.
     */
    public CachedNodeInfo addChild(String childName, NodeState childState) {
        CachedNodeInfo child = CachedNodeInfo.from(childName, childState);
        child.parent = this;
        children.put(childName, child);
        invalidateSubtreeSize();
        return child;
    }
    
    /**
     * Get or create a child (for resume scenarios).
     */
    public CachedNodeInfo getOrCreateChild(String childName) {
        CachedNodeInfo child = children.get(childName);
        if (child == null) {
            child = new CachedNodeInfo(childName);
            child.parent = this;
            children.put(childName, child);
            invalidateSubtreeSize();
        }
        return child;
    }
    
    public CachedNodeInfo getChild(String childName) {
        return children.get(childName);
    }
    
    public boolean hasChild(String childName) {
        return children.containsKey(childName);
    }
    
    public Iterable<String> getChildNames() {
        return children.keySet();
    }
    
    public int getChildCount() {
        return children.size();
    }
    
    /**
     * Get subtree size (cached for performance).
     */
    public int getSubtreeSize() {
        if (cachedSubtreeSize < 0) {
            int size = 1; // This node
            for (CachedNodeInfo child : children.values()) {
                size += child.getSubtreeSize();
            }
            cachedSubtreeSize = size;
        }
        return cachedSubtreeSize;
    }
    
    private void invalidateSubtreeSize() {
        cachedSubtreeSize = -1;
        if (parent != null) {
            parent.invalidateSubtreeSize();
        }
    }
    
    /**
     * Find a node by path.
     */
    public CachedNodeInfo findByPath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return this;
        }
        
        String[] segments = path.split("/");
        CachedNodeInfo current = this;
        
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
     * Create a "virtual" NodeState that returns cached info.
     * This allows editors to work with cached data without SegmentStore reads.
     */
    public NodeState toVirtualNodeState() {
        return new VirtualNodeState(this);
    }
    
    /**
     * Serialize to a simple format for persistence.
     * Format: path|primaryType|indexed|children...
     */
    public String toSerializedForm() {
        StringBuilder sb = new StringBuilder();
        serialize(sb, "");
        return sb.toString();
    }
    
    private void serialize(StringBuilder sb, String path) {
        String currentPath = path.isEmpty() ? "/" : path;
        sb.append(currentPath).append("|");
        sb.append(primaryType != null ? primaryType : "").append("|");
        sb.append(indexed ? "1" : "0").append("\n");
        
        for (Map.Entry<String, CachedNodeInfo> entry : children.entrySet()) {
            String childPath = "/".equals(currentPath) ? "/" + entry.getKey() : currentPath + "/" + entry.getKey();
            entry.getValue().serialize(sb, childPath);
        }
    }
    
    /**
     * Deserialize from the simple format.
     */
    public static CachedNodeInfo fromSerializedForm(String data) {
        if (data == null || data.isEmpty()) {
            return null;
        }
        
        CachedNodeInfo root = new CachedNodeInfo();
        Map<String, CachedNodeInfo> nodesByPath = new LinkedHashMap<>();
        nodesByPath.put("/", root);
        
        for (String line : data.split("\n")) {
            if (line.isEmpty()) continue;
            
            String[] parts = line.split("\\|", -1);
            if (parts.length < 3) continue;
            
            String path = parts[0];
            String primaryType = parts[1].isEmpty() ? null : parts[1];
            boolean indexed = "1".equals(parts[2]);
            
            if ("/".equals(path)) {
                root.primaryType = primaryType;
                root.indexed = indexed;
            } else {
                // Find parent and add child
                String parentPath = path.substring(0, path.lastIndexOf('/'));
                if (parentPath.isEmpty()) parentPath = "/";
                String childName = path.substring(path.lastIndexOf('/') + 1);
                
                CachedNodeInfo parent = nodesByPath.get(parentPath);
                if (parent != null) {
                    CachedNodeInfo child = new CachedNodeInfo(childName);
                    child.primaryType = primaryType;
                    child.indexed = indexed;
                    child.parent = parent;
                    parent.children.put(childName, child);
                    nodesByPath.put(path, child);
                }
            }
        }
        
        return root;
    }
    
    private void fixParentReferences() {
        for (CachedNodeInfo child : children.values()) {
            child.parent = this;
            child.fixParentReferences();
        }
    }
    
    /**
     * Virtual NodeState that returns data from cache.
     * Only provides the minimum needed for indexing rule determination.
     */
    private static class VirtualNodeState implements NodeState {
        private final CachedNodeInfo info;
        
        VirtualNodeState(CachedNodeInfo info) {
            this.info = info;
        }
        
        @Override
        public boolean exists() {
            return true;
        }
        
        @Override
        public boolean hasProperty(@NotNull String name) {
            if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
                return info.primaryType != null;
            }
            if (JcrConstants.JCR_MIXINTYPES.equals(name)) {
                return info.mixinTypes != null && !info.mixinTypes.isEmpty();
            }
            return false;
        }
        
        @Override
        @Nullable
        public PropertyState getProperty(@NotNull String name) {
            if (JcrConstants.JCR_PRIMARYTYPE.equals(name) && info.primaryType != null) {
                return PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, info.primaryType, Type.NAME);
            }
            if (JcrConstants.JCR_MIXINTYPES.equals(name) && info.mixinTypes != null && !info.mixinTypes.isEmpty()) {
                return PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, info.mixinTypes, Type.NAMES);
            }
            return null;
        }
        
        @Override
        public long getPropertyCount() {
            int count = 0;
            if (info.primaryType != null) count++;
            if (info.mixinTypes != null && !info.mixinTypes.isEmpty()) count++;
            return count;
        }
        
        @Override
        @NotNull
        public Iterable<? extends PropertyState> getProperties() {
            List<PropertyState> props = new ArrayList<>();
            PropertyState pt = getProperty(JcrConstants.JCR_PRIMARYTYPE);
            if (pt != null) props.add(pt);
            PropertyState mt = getProperty(JcrConstants.JCR_MIXINTYPES);
            if (mt != null) props.add(mt);
            return props;
        }
        
        @Override
        public boolean hasChildNode(@NotNull String name) {
            return info.hasChild(name);
        }
        
        @Override
        @NotNull
        public NodeState getChildNode(@NotNull String name) {
            CachedNodeInfo child = info.getChild(name);
            if (child != null) {
                return child.toVirtualNodeState();
            }
            return EmptyNodeState.MISSING_NODE;
        }
        
        @Override
        public long getChildNodeCount(long max) {
            return info.getChildCount();
        }
        
        @Override
        @NotNull
        public Iterable<String> getChildNodeNames() {
            return info.getChildNames();
        }
        
        @Override
        @NotNull
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            List<ChildNodeEntry> entries = new ArrayList<>();
            for (String childName : info.getChildNames()) {
                CachedNodeInfo child = info.getChild(childName);
                if (child != null) {
                    entries.add(new MemoryChildNodeEntry(childName, child.toVirtualNodeState()));
                }
            }
            return entries;
        }
        
        @Override
        @NotNull
        public NodeBuilder builder() {
            return new MemoryNodeBuilder(this);
        }
        
        @Override
        public boolean compareAgainstBaseState(NodeState base, org.apache.jackrabbit.oak.spi.state.NodeStateDiff diff) {
            // Not needed for skip phase
            return true;
        }
        
        // Additional required methods - return defaults since we only care about type info
        
        @Override
        @Nullable
        public String getString(@NotNull String name) {
            PropertyState prop = getProperty(name);
            return prop != null ? prop.getValue(Type.STRING) : null;
        }
        
        @Override
        @Nullable
        public String getName(@NotNull String name) {
            PropertyState prop = getProperty(name);
            return prop != null ? prop.getValue(Type.NAME) : null;
        }
        
        @Override
        @NotNull
        public Iterable<String> getStrings(@NotNull String name) {
            PropertyState prop = getProperty(name);
            return prop != null ? prop.getValue(Type.STRINGS) : Collections.emptyList();
        }
        
        @Override
        @NotNull
        public Iterable<String> getNames(@NotNull String name) {
            PropertyState prop = getProperty(name);
            return prop != null ? prop.getValue(Type.NAMES) : Collections.emptyList();
        }
        
        @Override
        public boolean getBoolean(@NotNull String name) {
            PropertyState prop = getProperty(name);
            return prop != null && prop.getValue(Type.BOOLEAN);
        }
        
        @Override
        public long getLong(@NotNull String name) {
            PropertyState prop = getProperty(name);
            return prop != null ? prop.getValue(Type.LONG) : 0L;
        }
    }
}

