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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context for resumable indexing operations.
 * 
 * <p>This context holds:
 * <ul>
 *   <li>The path to resume from (where last indexing stopped)</li>
 *   <li>Skip mode flag - when true, editors should skip Lucene document creation</li>
 *   <li>PathTree - minimal tree for tracking visited/indexed paths</li>
 *   <li>Chunk limit and current count for resumable indexing</li>
 * </ul>
 * 
 * <p>The context is passed through the editor chain during diff processing.
 */
public class ResumeContext {
    
    private static final Logger LOG = LoggerFactory.getLogger(ResumeContext.class);
    
    private final String resumeFromPath;
    private final PathTree pathTree;
    private final int chunkLimit;
    
    private boolean inSkipMode;
    private String currentPath;
    private int nodesProcessed;
    private String lastIndexedPath;
    private boolean chunkLimitReached;
    
    // Callbacks
    private Runnable onResumePointReached;
    private Runnable onChunkLimitReached;
    
    /**
     * Create a new ResumeContext.
     * 
     * @param resumeFromPath path to resume from (null or "/" for no resume)
     * @param pathTree the path tree for tracking visited nodes (can be null for new run)
     * @param chunkLimit max nodes to process before chunk commit (0 for unlimited)
     */
    public ResumeContext(@Nullable String resumeFromPath, 
                         @Nullable PathTree pathTree,
                         int chunkLimit) {
        this.resumeFromPath = resumeFromPath;
        this.pathTree = pathTree != null ? pathTree : new PathTree();
        this.chunkLimit = chunkLimit;
        
        // If we have a resume path, start in skip mode
        this.inSkipMode = resumeFromPath != null && !"/".equals(resumeFromPath);
        this.currentPath = "/";
        this.nodesProcessed = 0;
        this.chunkLimitReached = false;
        
        if (inSkipMode) {
            LOG.info("Resume context initialized - skip mode enabled until path: {}", resumeFromPath);
        }
    }
    
    /**
     * Create a context for first-time indexing (no resume).
     */
    public static ResumeContext createForFirstRun(int chunkLimit) {
        return new ResumeContext(null, new PathTree(), chunkLimit);
    }
    
    /**
     * Create a context for resuming from a saved state.
     */
    public static ResumeContext createForResume(@NotNull String resumePath, 
                                                 @NotNull PathTree pathTree,
                                                 int chunkLimit) {
        return new ResumeContext(resumePath, pathTree, chunkLimit);
    }
    
    // ========== Getters ==========
    
    @Nullable
    public String getResumeFromPath() {
        return resumeFromPath;
    }
    
    @NotNull
    public PathTree getPathTree() {
        return pathTree;
    }
    
    public int getChunkLimit() {
        return chunkLimit;
    }
    
    public boolean isInSkipMode() {
        return inSkipMode;
    }
    
    @NotNull
    public String getCurrentPath() {
        return currentPath;
    }
    
    public int getNodesProcessed() {
        return nodesProcessed;
    }
    
    @Nullable
    public String getLastIndexedPath() {
        return lastIndexedPath;
    }
    
    public boolean isChunkLimitReached() {
        return chunkLimitReached;
    }
    
    // ========== State Updates ==========
    
    /**
     * Update the current path being processed.
     */
    public void setCurrentPath(@NotNull String path) {
        this.currentPath = path;
        
        // Check if we've reached the resume point
        if (inSkipMode && resumeFromPath != null && path.equals(resumeFromPath)) {
            LOG.info("Resume point reached: {}", path);
            inSkipMode = false;
            if (onResumePointReached != null) {
                onResumePointReached.run();
            }
        }
    }
    
    /**
     * Called when a node is successfully indexed.
     * Updates counters and checks chunk limit.
     * 
     * @param path the indexed node path
     * @return true if chunk limit was reached
     */
    public boolean nodeIndexed(@NotNull String path) {
        if (inSkipMode) {
            // Don't count skipped nodes
            return false;
        }
        
        nodesProcessed++;
        lastIndexedPath = path;
        pathTree.markIndexed(path);
        
        // Check chunk limit
        if (chunkLimit > 0 && nodesProcessed >= chunkLimit && !chunkLimitReached) {
            chunkLimitReached = true;
            LOG.info("Chunk limit reached: {} nodes processed, last path: {}", nodesProcessed, path);
            if (onChunkLimitReached != null) {
                onChunkLimitReached.run();
            }
            return true;
        }
        
        return false;
    }
    
    /**
     * Check if the given path should be skipped (already indexed in previous run).
     */
    public boolean shouldSkipPath(@NotNull String path) {
        if (inSkipMode) {
            // In skip mode, we're traversing to the resume point
            // Skip nodes that are NOT on the path to the resume point
            if (resumeFromPath != null && !resumeFromPath.startsWith(path)) {
                return true;
            }
        }
        
        // Check if already indexed in the path tree
        return pathTree.isIndexed(path);
    }
    
    /**
     * Mark that we've visited a node (may not be indexed yet).
     */
    public void nodeVisited(@NotNull String path, @Nullable String primaryType) {
        PathTree.PathNode node = pathTree.getOrCreateNode(path);
        if (primaryType != null) {
            node.setPrimaryType(primaryType);
        }
    }
    
    // ========== Callbacks ==========
    
    public void setOnResumePointReached(Runnable callback) {
        this.onResumePointReached = callback;
    }
    
    public void setOnChunkLimitReached(Runnable callback) {
        this.onChunkLimitReached = callback;
    }
    
    // ========== Serialization ==========
    
    /**
     * Serialize the context to a NodeBuilder for persistence.
     */
    public void serializeTo(@NotNull NodeBuilder builder) {
        if (lastIndexedPath != null) {
            builder.setProperty("lastIndexedPath", lastIndexedPath);
        }
        builder.setProperty("nodesProcessed", nodesProcessed);
        builder.setProperty("chunkLimit", chunkLimit);
        
        // Serialize the path tree
        pathTree.serializeTo(builder.child("pathTree"));
    }
    
    /**
     * Deserialize a context from NodeState.
     */
    @NotNull
    public static ResumeContext deserializeFrom(@NotNull NodeState state, int chunkLimit) {
        PropertyState lastPathProp = state.getProperty("lastIndexedPath");
        String resumePath = lastPathProp != null ? lastPathProp.getValue(Type.STRING) : null;
        
        // Load the path tree
        PathTree pathTree = new PathTree();
        NodeState treeState = state.getChildNode("pathTree");
        if (treeState.exists()) {
            pathTree = PathTree.deserializeFrom(treeState);
        }
        
        return new ResumeContext(resumePath, pathTree, chunkLimit);
    }
    
    /**
     * Check if there's a saved resume state.
     */
    public static boolean hasResumeState(@NotNull NodeState state) {
        return state.hasProperty("lastIndexedPath");
    }
    
    @Override
    public String toString() {
        return "ResumeContext{" +
               "resumeFromPath='" + resumeFromPath + '\'' +
               ", inSkipMode=" + inSkipMode +
               ", currentPath='" + currentPath + '\'' +
               ", nodesProcessed=" + nodesProcessed +
               ", chunkLimit=" + chunkLimit +
               ", pathTree=" + pathTree +
               '}';
    }
}

