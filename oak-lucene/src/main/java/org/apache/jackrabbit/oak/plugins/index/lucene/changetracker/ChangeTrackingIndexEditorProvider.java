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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
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
 * <p><strong>Timestamp Strategy:</strong> Uses checkpoint1's (beforeCheckpoint) creation timestamp
 * to tag change entries. The checkpoint1 ID is passed through CommitInfo by AsyncIndexUpdate,
 * and this provider extracts the timestamp from the checkpoint.
 * 
 * <p>This provides accurate traceability: diffProcessingTime represents the repository state
 * FROM which changes are being recorded, making it clear which checkpoint's diff produced
 * these changes.
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

        // Get CommitInfo from callback (must be ContextAwareCallback)
        if (!(callback instanceof ContextAwareCallback)) {
            LOG.warn("Callback is not ContextAwareCallback, using current time");
            return new ChangeTrackingIndexEditor(changeTrackingWriter, System.currentTimeMillis());
        }
        
        IndexingContext context = ((ContextAwareCallback) callback).getIndexingContext();
        CommitInfo info = context.getCommitInfo();
        
        // Extract checkpoint1 (beforeCheckpoint) ID from CommitInfo
        // AsyncIndexUpdate passes this via IndexConstants.BEFORE_CHECKPOINT_ID
        String checkpoint1Id = (String) info.getInfo().get(IndexConstants.BEFORE_CHECKPOINT_ID);
        
        long diffProcessingTime;
        if (checkpoint1Id != null) {
            // Extract timestamp from checkpoint1
            diffProcessingTime = extractCheckpointTimestamp(root, checkpoint1Id);
            if (diffProcessingTime > 0) {
                LOG.debug("Change tracking diff FROM checkpoint1: {} (timestamp: {})", 
                         checkpoint1Id, diffProcessingTime);
            } else {
                LOG.warn("Could not extract timestamp from checkpoint1: {}, using current time", 
                        checkpoint1Id);
                diffProcessingTime = System.currentTimeMillis();
            }
        } else {
            // Initial indexing - no checkpoint1 exists yet
            LOG.debug("No checkpoint1 found (initial indexing), using current time");
            diffProcessingTime = System.currentTimeMillis();
        }
        
        // Create and return the change tracking editor
        // diffProcessingTime = checkpoint1's timestamp (the "before" state in the diff)
        return new ChangeTrackingIndexEditor(
            changeTrackingWriter,
            diffProcessingTime
        );
    }
    
    /**
     * Extracts the creation timestamp from a checkpoint.
     * Handles different NodeStore implementations (Segment, Document, Memory).
     * 
     * @param root the root NodeState
     * @param checkpointId the checkpoint ID
     * @return the checkpoint creation timestamp, or 0 if not found
     */
    private long extractCheckpointTimestamp(NodeState root, String checkpointId) {
        try {
            NodeState checkpointsNode = root.getChildNode("checkpoints");
            
            if (!checkpointsNode.exists()) {
                LOG.debug("No checkpoints node found");
                return 0;
            }
            
            // Iterate through all checkpoints to find the matching one
            for (ChildNodeEntry entry : checkpointsNode.getChildNodeEntries()) {
                if (entry.getName().equals(checkpointId)) {
                    NodeState checkpoint = entry.getNodeState();
                    
                    // Try "created" property (SegmentNodeStore style)
                    PropertyState createdProp = checkpoint.getProperty("created");
                    if (createdProp != null) {
                        return createdProp.getValue(Type.LONG);
                    }
                    
                    // Try extracting from the checkpoint ID itself
                    // DocumentNodeStore uses revision strings that contain timestamps
                    // Format: "r<timestamp>-<counter>-<clusterId>"
                    if (checkpointId.startsWith("r")) {
                        String[] parts = checkpointId.substring(1).split("-");
                        if (parts.length >= 1) {
                            try {
                                // Convert hex timestamp to long
                                return Long.parseLong(parts[0], 16);
                            } catch (NumberFormatException e) {
                                LOG.debug("Could not parse timestamp from revision: {}", checkpointId);
                            }
                        }
                    }
                    
                    LOG.debug("No timestamp found in checkpoint: {}", checkpointId);
                    return 0;
                }
            }
            
            LOG.debug("Checkpoint not found: {}", checkpointId);
            return 0;
            
        } catch (Exception e) {
            LOG.warn("Error extracting checkpoint timestamp for {}", checkpointId, e);
            return 0;
        }
    }
}

