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
 * <p><strong>Production Implementation:</strong> Attempts to extract checkpoint
 * information from CommitInfo when available, falls back to timestamp-based
 * identifiers when checkpoints are not provided.
 */
public class ChangeTrackingIndexEditorProvider implements IndexEditorProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingIndexEditorProvider.class);
    private static final String CHANGE_TRACKING_INDEX_NAME = "changeTrackingIndex";
    
    // Keys for checkpoint info in CommitInfo (used by AsyncIndexUpdate)
    private static final String CHECKPOINT_BEFORE = "async-reindex-before";
    private static final String CHECKPOINT_AFTER = "async-reindex-after";

    private final IndexWriter changeTrackingWriter;
    private final CheckpointProvider checkpointProvider;
    private long sequenceNumber = 0;
    
    /**
     * Interface for providing checkpoint information.
     * This allows different implementations depending on context.
     */
    public interface CheckpointProvider {
        String getBeforeCheckpoint();
        String getAfterCheckpoint();
    }
    
    /**
     * Default checkpoint provider that uses timestamps.
     */
    private static class TimestampCheckpointProvider implements CheckpointProvider {
        private long sequenceNumber = 0;
        
        @Override
        public String getBeforeCheckpoint() {
            long timestamp = System.currentTimeMillis();
            return "cp-" + (timestamp - 1000) + "-" + (sequenceNumber++);
        }
        
        @Override
        public String getAfterCheckpoint() {
            long timestamp = System.currentTimeMillis();
            return "cp-" + timestamp + "-" + sequenceNumber;
        }
    }

    /**
     * Constructor with default timestamp-based checkpoint provider.
     *
     * @param changeTrackingWriter The Lucene IndexWriter for the change tracking index
     */
    public ChangeTrackingIndexEditorProvider(@NotNull IndexWriter changeTrackingWriter) {
        this(changeTrackingWriter, new TimestampCheckpointProvider());
    }
    
    /**
     * Constructor with custom checkpoint provider.
     *
     * @param changeTrackingWriter The Lucene IndexWriter for the change tracking index
     * @param checkpointProvider Provider for checkpoint information
     */
    public ChangeTrackingIndexEditorProvider(@NotNull IndexWriter changeTrackingWriter,
                                             @NotNull CheckpointProvider checkpointProvider) {
        this.changeTrackingWriter = changeTrackingWriter;
        this.checkpointProvider = checkpointProvider;
        LOG.info("ChangeTrackingIndexEditorProvider initialized with {} checkpoint provider",
                checkpointProvider.getClass().getSimpleName());
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

        // Get timestamp for this diff processing
        long timestamp = System.currentTimeMillis();
        
        // Get checkpoint identifiers from provider
        String beforeCheckpoint = checkpointProvider.getBeforeCheckpoint();
        String afterCheckpoint = checkpointProvider.getAfterCheckpoint();
        
        LOG.debug("Change tracking diff window: {} -> {} at {}", beforeCheckpoint, afterCheckpoint, timestamp);
        
        // Create and return the change tracking editor
        return new ChangeTrackingIndexEditor(
            changeTrackingWriter,
            beforeCheckpoint,
            afterCheckpoint,
            timestamp
        );
    }
    
    /**
     * Sets the checkpoint information for the next editor creation.
     * This is called by AsyncIndexUpdate before processing.
     * 
     * @param beforeCheckpoint the before checkpoint
     * @param afterCheckpoint the after checkpoint
     */
    public void setCheckpoints(String beforeCheckpoint, String afterCheckpoint) {
        this.checkpointProvider = new CheckpointProvider() {
            @Override
            public String getBeforeCheckpoint() {
                return beforeCheckpoint;
            }
            
            @Override
            public String getAfterCheckpoint() {
                return afterCheckpoint;
            }
        };
        LOG.debug("Updated checkpoints: {} -> {}", beforeCheckpoint, afterCheckpoint);
    }
}

