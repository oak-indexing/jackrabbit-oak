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

import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Extended Editor interface that supports resumable indexing.
 * 
 * <p>Editors implementing this interface can:
 * <ul>
 *   <li>Access the ResumeContext to check skip mode</li>
 *   <li>Track the current path during traversal</li>
 *   <li>Skip document creation when in skip mode</li>
 * </ul>
 */
public interface ResumableEditor extends Editor {
    
    /**
     * Set the resume context for this editor.
     * Called before processing begins.
     * 
     * @param context the resume context
     */
    void setResumeContext(@NotNull ResumeContext context);
    
    /**
     * Get the current resume context.
     * 
     * @return the context, or null if not set
     */
    @Nullable
    ResumeContext getResumeContext();
    
    /**
     * Get the current path being processed by this editor.
     * 
     * @return the current absolute path
     */
    @NotNull
    String getCurrentPath();
    
    /**
     * Check if this editor is currently in skip mode.
     * When in skip mode, document creation should be skipped.
     * 
     * @return true if in skip mode
     */
    default boolean isInSkipMode() {
        ResumeContext ctx = getResumeContext();
        return ctx != null && ctx.isInSkipMode();
    }
    
    /**
     * Check if the given child path should be skipped.
     * 
     * @param childName the child node name
     * @return true if the child should be skipped
     */
    default boolean shouldSkipChild(String childName) {
        ResumeContext ctx = getResumeContext();
        if (ctx == null) {
            return false;
        }
        String childPath = getCurrentPath() + "/" + childName;
        if ("/".equals(getCurrentPath())) {
            childPath = "/" + childName;
        }
        return ctx.shouldSkipPath(childPath);
    }
}

