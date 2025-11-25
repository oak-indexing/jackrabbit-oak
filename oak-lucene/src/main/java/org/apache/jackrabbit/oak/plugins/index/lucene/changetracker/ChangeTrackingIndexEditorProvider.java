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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker;

import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.IndexWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IndexEditorProvider for the change tracking mechanism.
 * This provider creates ChangeTrackingIndexEditor instances that record
 * all changed paths into a Lucene index for later chunked processing.
 * 
 * <p><strong>Simplified Design:</strong> Since we only store paths (not content), 
 * we don't need checkpoint information. The editor only needs a timestamp for
 * ordering and retention purposes.
 */
public class ChangeTrackingIndexEditorProvider implements IndexEditorProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingIndexEditorProvider.class);
    private static final String CHANGE_TRACKING_INDEX_NAME = "changeTrackingIndex";

    private final IndexWriter changeTrackingWriter;

    /**
     * Creates a change tracking index editor provider.
     *
     * @param changeTrackingWriter The Lucene IndexWriter for the change tracking index
     */
    public ChangeTrackingIndexEditorProvider(@NotNull IndexWriter changeTrackingWriter) {
        this.changeTrackingWriter = changeTrackingWriter;
        LOG.info("ChangeTrackingIndexEditorProvider initialized");
    }

    @Override
    @Nullable
    public Editor getIndexEditor(@NotNull String type,
                                  @NotNull NodeBuilder definition,
                                  @NotNull NodeState root,
                                  @NotNull IndexUpdateCallback callback) {
        
        // Only handle lucene type indexes
        if (!"lucene".equals(type)) {
            return null;
        }

        // Check if this is the changeTrackingIndex by looking at its async property
        String asyncValue = definition.getString("async");
        if (!"change-tracker-async".equals(asyncValue)) {
            // This provider only handles indexes in the change-tracker-async lane
            return null;
        }

        LOG.info("Creating ChangeTrackingIndexEditor for change tracking index");

        // Get timestamp for this diff processing - used for ordering and retention
        long diffProcessingTime = System.currentTimeMillis();
        
        LOG.debug("Change tracking diff at timestamp: {}", diffProcessingTime);
        
        // Create and return the change tracking editor
        // Note: No checkpoint info needed since we only store paths, not content
        return new ChangeTrackingIndexEditor(
            changeTrackingWriter,
            diffProcessingTime
        );
    }
}

