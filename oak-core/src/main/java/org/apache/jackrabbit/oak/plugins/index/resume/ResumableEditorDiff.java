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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resumable version of EditorDiff that passes ResumeContext through the editor chain.
 * 
 * <p>This diff processor:
 * <ul>
 *   <li>Tracks the current path during traversal</li>
 *   <li>Passes ResumeContext to editors that implement ResumableEditor</li>
 *   <li>Checks chunk limits and can trigger early exit</li>
 *   <li>Builds the PathTree during traversal</li>
 * </ul>
 */
public class ResumableEditorDiff implements NodeStateDiff {
    
    private static final Logger LOG = LoggerFactory.getLogger(ResumableEditorDiff.class);
    
    private final Editor editor;
    private final ResumeContext context;
    private final String currentPath;
    
    private CommitFailedException exception;
    
    /**
     * Process the diff between two states with resume support.
     * 
     * @param editor the root editor
     * @param before the before state
     * @param after the after state
     * @param context the resume context
     * @return exception if processing failed, null otherwise
     */
    @Nullable
    public static CommitFailedException process(
            @Nullable Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull ResumeContext context) {
        return processWithPath(editor, before, after, context, "/");
    }
    
    /**
     * Process the diff starting at a specific path.
     */
    @Nullable
    private static CommitFailedException processWithPath(
            @Nullable Editor editor,
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull ResumeContext context,
            @NotNull String path) {
        requireNonNull(before);
        requireNonNull(after);
        requireNonNull(context);
        requireNonNull(path);
        
        if (editor == null) {
            return null;
        }
        
        // Set context on editor if it supports it
        if (editor instanceof ResumableEditor) {
            ((ResumableEditor) editor).setResumeContext(context);
        }
        
        // Update current path in context
        context.setCurrentPath(path);
        
        // Track visited node
        context.nodeVisited(path, null);
        
        try {
            editor.enter(before, after);
            
            // Check if chunk limit was reached
            if (context.isChunkLimitReached()) {
                LOG.debug("Chunk limit reached at path: {}", path);
                return createChunkCompleteException(context);
            }
            
            ResumableEditorDiff diff = new ResumableEditorDiff(editor, context, path);
            if (!after.compareAgainstBaseState(before, diff)) {
                return diff.exception;
            }
            
            // Check again after processing children
            if (context.isChunkLimitReached()) {
                LOG.debug("Chunk limit reached after children at path: {}", path);
                return createChunkCompleteException(context);
            }
            
            editor.leave(before, after);
            
        } catch (CommitFailedException e) {
            return e;
        }
        
        return null;
    }
    
    /**
     * Create a special exception to signal chunk completion.
     */
    private static CommitFailedException createChunkCompleteException(ResumeContext context) {
        return new CommitFailedException(
            CommitFailedException.OAK, 1001, 
            "CHUNK_COMPLETE:" + context.getLastIndexedPath()
        );
    }
    
    /**
     * Check if an exception is a chunk complete signal.
     */
    public static boolean isChunkCompleteException(@Nullable CommitFailedException e) {
        return e != null && e.getMessage() != null && 
               e.getMessage().startsWith("CHUNK_COMPLETE:");
    }
    
    /**
     * Extract the last indexed path from a chunk complete exception.
     */
    @Nullable
    public static String getChunkCompletePath(@NotNull CommitFailedException e) {
        if (isChunkCompleteException(e)) {
            return e.getMessage().substring("CHUNK_COMPLETE:".length());
        }
        return null;
    }
    
    private ResumableEditorDiff(Editor editor, ResumeContext context, String currentPath) {
        this.editor = editor;
        this.context = context;
        this.currentPath = currentPath;
    }
    
    private String childPath(String childName) {
        if ("/".equals(currentPath)) {
            return "/" + childName;
        }
        return currentPath + "/" + childName;
    }
    
    // ========== NodeStateDiff Implementation ==========
    
    @Override
    public boolean propertyAdded(PropertyState after) {
        // Skip properties if in skip mode
        if (context.isInSkipMode()) {
            return true;
        }
        
        try {
            editor.propertyAdded(after);
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }
    
    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        // Skip properties if in skip mode
        if (context.isInSkipMode()) {
            return true;
        }
        
        try {
            editor.propertyChanged(before, after);
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }
    
    @Override
    public boolean propertyDeleted(PropertyState before) {
        // Skip properties if in skip mode
        if (context.isInSkipMode()) {
            return true;
        }
        
        try {
            editor.propertyDeleted(before);
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }
    
    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        String childPath = childPath(name);
        
        // Check if we should skip this subtree
        if (context.shouldSkipPath(childPath) && !isOnResumePathPrefix(childPath)) {
            LOG.trace("Skipping already-indexed path: {}", childPath);
            return true;
        }
        
        try {
            NodeState before = MISSING_NODE;
            Editor childEditor = editor.childNodeAdded(name, after);
            
            if (childEditor != null) {
                CommitFailedException ex = processWithPath(
                    childEditor, before, after, context, childPath);
                if (ex != null) {
                    exception = ex;
                    return false;
                }
            }
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }
    
    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        String childPath = childPath(name);
        
        // Check if we should skip this subtree (but always traverse if on resume path)
        if (context.shouldSkipPath(childPath) && !isOnResumePathPrefix(childPath)) {
            LOG.trace("Skipping already-indexed path: {}", childPath);
            return true;
        }
        
        try {
            Editor childEditor = editor.childNodeChanged(name, before, after);
            
            if (childEditor != null) {
                CommitFailedException ex = processWithPath(
                    childEditor, before, after, context, childPath);
                if (ex != null) {
                    exception = ex;
                    return false;
                }
            }
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }
    
    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        String childPath = childPath(name);
        
        // Skip deletions in skip mode
        if (context.isInSkipMode()) {
            return true;
        }
        
        try {
            NodeState after = MISSING_NODE;
            Editor childEditor = editor.childNodeDeleted(name, before);
            
            if (childEditor != null) {
                CommitFailedException ex = processWithPath(
                    childEditor, before, after, context, childPath);
                if (ex != null) {
                    exception = ex;
                    return false;
                }
            }
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }
    
    /**
     * Check if the given path is a prefix of the resume path.
     * We must traverse this path to reach the resume point.
     */
    private boolean isOnResumePathPrefix(String path) {
        String resumePath = context.getResumeFromPath();
        if (resumePath == null || "/".equals(resumePath)) {
            return false;
        }
        // path is a prefix of resumePath if resumePath starts with path
        return resumePath.startsWith(path + "/") || resumePath.equals(path);
    }
}

