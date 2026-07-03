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
package org.apache.jackrabbit.oak.spi.commit;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;


import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EditorDiff implements NodeStateDiff {
    
    private static final Logger LOG = LoggerFactory.getLogger(EditorDiff.class);

    /**
     * Validates and possibly edits the given subtree by diffing
     * and recursing through it.
     *
     * @param editor editor for the root of the subtree
     * @param before state of the original subtree
     * @param after state of the modified subtree
     * @return exception if the processing failed, {@code null} otherwise
     */
    @Nullable
    public static CommitFailedException process(
            @Nullable Editor editor,
            @NotNull NodeState before, @NotNull NodeState after) {
        requireNonNull(before);
        requireNonNull(after);
        if (editor != null) {
            try {
                editor.enter(before, after);

                EditorDiff diff = new EditorDiff(editor);
                if (!after.compareAgainstBaseState(before, diff)) {
                    return diff.exception;
                }

                editor.leave(before, after);
            } catch (CommitFailedException e) {
                return e;
            }
        }
        return null;
    }
    
    /**
     * Process diff starting from a specific path, skipping already-indexed nodes.
     * This delegates to the normal process() method since the editor should already
     * be wrapped with appropriate resuming logic by the caller.
     * 
     * <p>Note: This method exists for API compatibility. The caller (e.g., AsyncIndexUpdate)
     * should wrap the editor with ResumingEditor before calling this method.
     * 
     * @param editor editor for the root of the subtree (should be wrapped with ResumingEditor if resuming)
     * @param rootBefore state of the original tree root
     * @param rootAfter state of the modified tree root
     * @param resumePath path to resume from (logged for debugging)
     * @param onResumePointReached callback when resume point is reached (not used - should be in ResumingEditor)
     * @return exception if the processing failed, {@code null} otherwise
     */
    @Nullable
    public static CommitFailedException processFromPath(
            @Nullable Editor editor,
            @NotNull NodeState rootBefore, 
            @NotNull NodeState rootAfter,
            @Nullable String resumePath,
            @Nullable Runnable onResumePointReached) {
        requireNonNull(rootBefore);
        requireNonNull(rootAfter);
        
        if (resumePath != null && !"/".equals(resumePath)) {
            LOG.info("Processing diff with resume target: {}", resumePath);
        }
        
        // Delegate to normal processing - caller should have wrapped editor with ResumingEditor
        return process(editor, rootBefore, rootAfter);
    }

    private final Editor editor;

    /**
     * Checked exceptions don't compose. So we need to hack around.
     * See http://markmail.org/message/ak67n5k7mr3vqylm and
     * http://markmail.org/message/bhocbruikljpuhu6
     */
    private CommitFailedException exception;

    private EditorDiff(Editor editor) {
        this.editor = editor;
    }

    //-------------------------------------------------< NodeStateDiff >--

    @Override
    public boolean propertyAdded(PropertyState after) {
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
        try {
            NodeState before = MISSING_NODE;
            Editor childEditor = editor.childNodeAdded(name, after);
            // NOTE: This piece of code is duplicated across this and the
            // other child node diff methods. The reason for the duplication
            // is to simplify the frequently occurring long stack traces
            // in diff processing.
            if (childEditor != null) {
                childEditor.enter(before, after);

                EditorDiff diff = new EditorDiff(childEditor);
                if (!after.compareAgainstBaseState(before, diff)) {
                    exception = diff.exception;
                    return false;
                }

                childEditor.leave(before, after);
            }
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeChanged(
            String name, NodeState before, NodeState after) {
        try {
            Editor childEditor = editor.childNodeChanged(name, before, after);
            if (childEditor != null) {
                childEditor.enter(before, after);

                EditorDiff diff = new EditorDiff(childEditor);
                if (!after.compareAgainstBaseState(before, diff)) {
                    exception = diff.exception;
                    return false;
                }

                childEditor.leave(before, after);
            }
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        try {
            NodeState after = MISSING_NODE;
            Editor childEditor = editor.childNodeDeleted(name, before);
            if (childEditor != null) {
                childEditor.enter(before, after);

                EditorDiff diff = new EditorDiff(childEditor);
                if (!after.compareAgainstBaseState(before, diff)) {
                    exception = diff.exception;
                    return false;
                }

                childEditor.leave(before, after);
            }
            return true;
        } catch (CommitFailedException e) {
            exception = e;
            return false;
        }
    }

}
