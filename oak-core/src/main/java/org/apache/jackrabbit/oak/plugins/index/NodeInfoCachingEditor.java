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
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

/**
 * Editor that builds a CachedNodeInfo tree during traversal.
 * 
 * This collects the minimal information needed for skip-phase navigation:
 * - Node types (jcr:primaryType, jcr:mixinTypes)
 * - Child names in iteration order
 * - Indexed flag (set when leave() is called)
 * 
 * The cached info can be used in subsequent chunks to navigate without
 * reading from SegmentStore.
 */
public class NodeInfoCachingEditor extends DefaultEditor {
    
    private final Editor delegate;
    private final CachedNodeInfo rootInfo;
    private CachedNodeInfo currentInfo;
    
    /**
     * Create a caching editor that wraps the delegate.
     * 
     * @param delegate the actual indexing editor
     * @param existingCache optional existing cache to extend (for resume scenarios)
     */
    public NodeInfoCachingEditor(Editor delegate, @Nullable CachedNodeInfo existingCache) {
        this.delegate = delegate;
        this.rootInfo = existingCache != null ? existingCache : new CachedNodeInfo();
        this.currentInfo = rootInfo;
    }
    
    private NodeInfoCachingEditor(NodeInfoCachingEditor parent, Editor delegate, CachedNodeInfo currentInfo) {
        this.delegate = delegate;
        this.rootInfo = parent.rootInfo;
        this.currentInfo = currentInfo;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        delegate.enter(before, after);
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        delegate.leave(before, after);
        // Mark this node as indexed (subtree complete)
        currentInfo.setIndexed(true);
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
        Editor childDelegate = delegate.childNodeAdded(name, after);
        if (childDelegate == null) {
            return null;
        }
        
        // Add to cache, extracting only needed info
        CachedNodeInfo childInfo = currentInfo.getChild(name);
        if (childInfo == null) {
            childInfo = currentInfo.addChild(name, after);
        }
        
        return new NodeInfoCachingEditor(this, childDelegate, childInfo);
    }
    
    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        Editor childDelegate = delegate.childNodeChanged(name, before, after);
        if (childDelegate == null) {
            return null;
        }
        
        // Get or create in cache
        CachedNodeInfo childInfo = currentInfo.getOrCreateChild(name);
        
        return new NodeInfoCachingEditor(this, childDelegate, childInfo);
    }
    
    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        Editor childDelegate = delegate.childNodeDeleted(name, before);
        if (childDelegate == null) {
            return null;
        }
        
        // Get or create in cache
        CachedNodeInfo childInfo = currentInfo.getOrCreateChild(name);
        
        return new NodeInfoCachingEditor(this, childDelegate, childInfo);
    }
    
    /**
     * Get the root of the cached node info tree.
     */
    public CachedNodeInfo getCachedInfo() {
        return rootInfo;
    }
}

