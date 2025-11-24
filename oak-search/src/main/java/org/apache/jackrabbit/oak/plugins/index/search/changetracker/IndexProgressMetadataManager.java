/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.search.changetracker;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manages metadata for change tracking in the repository.
 * 
 * <p>This class handles two types of metadata:
 * <ol>
 *   <li><strong>Change Tracker State</strong> - at /var/oak/indexing/progress/changeTracker
 *       <ul>
 *         <li>lastProcessedCheckpoint - last checkpoint processed by change tracker</li>
 *         <li>lastDiffProcessingTime - timestamp of last diff run</li>
 *         <li>registeredIndexes - list of indexes using change tracking</li>
 *       </ul>
 *   </li>
 *   <li><strong>Per-Index Progress</strong> - at configurable path (default: /var/oak/indexing/progress/indexes)
 *       <ul>
 *         <li>lastProcessedTimestamp - last fully processed timestamp</li>
 *         <li>lastProcessedSerialNumber - last fully processed serial number</li>
 *         <li>Statistics - processed counts, chunk info, etc.</li>
 *       </ul>
 *   </li>
 * </ol>
 * 
 * <p>All updates are atomic and use the NodeStore merge operation for consistency.
 */
public class IndexProgressMetadataManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(IndexProgressMetadataManager.class);
    
    // Fixed path for change tracker state
    private static final String CHANGE_TRACKER_PATH = "/var/oak/indexing/progress/changeTracker";
    
    // Configurable path for per-index progress (defaults to /var/oak/indexing/progress/indexes)
    private static final String DEFAULT_INDEXES_METADATA_PATH = "/var/oak/indexing/progress/indexes";
    private static final String METADATA_PATH_PROPERTY = "oak.changeTracker.metadataPath";
    
    // Property names for change tracker state
    private static final String PROP_LAST_PROCESSED_CHECKPOINT = "lastProcessedCheckpoint";
    private static final String PROP_LAST_DIFF_PROCESSING_TIME = "lastDiffProcessingTime";
    private static final String PROP_LAST_RUN_START = "lastRunStart";
    private static final String PROP_LAST_RUN_COMPLETE = "lastRunComplete";
    private static final String PROP_LAST_RUN_CHANGE_COUNT = "lastRunChangeCount";
    
    // Property names for per-index progress
    private static final String PROP_INDEX_PATH = "indexPath";
    private static final String PROP_LAST_PROCESSED_TIMESTAMP = "lastProcessedTimestamp";
    private static final String PROP_LAST_PROCESSED_SERIAL_NUMBER = "lastProcessedSerialNumber";
    private static final String PROP_CURRENT_CHUNK_START = "currentChunkStart";
    private static final String PROP_CURRENT_CHUNK_END = "currentChunkEnd";
    private static final String PROP_PROCESSING_STARTED = "processingStarted";
    private static final String PROP_LAST_CHUNK_COMMIT = "lastChunkCommit";
    
    // Statistics node
    private static final String NODE_STATS = "stats";
    private static final String PROP_TOTAL_PROCESSED = "totalProcessed";
    private static final String PROP_TOTAL_CHUNKS = "totalChunks";
    private static final String PROP_AVERAGE_CHUNK_TIME = "averageChunkTime";
    private static final String PROP_LAST_CHUNK_SIZE = "lastChunkSize";
    
    private final NodeStore nodeStore;
    private final String indexesMetadataPath;
    
    /**
     * Creates a metadata manager with default paths.
     * 
     * @param nodeStore the node store for persistence
     */
    public IndexProgressMetadataManager(@NotNull NodeStore nodeStore) {
        this(nodeStore, System.getProperty(METADATA_PATH_PROPERTY, DEFAULT_INDEXES_METADATA_PATH));
    }
    
    /**
     * Creates a metadata manager with custom index metadata path.
     * 
     * @param nodeStore the node store for persistence
     * @param indexesMetadataPath the path where per-index metadata is stored
     */
    public IndexProgressMetadataManager(@NotNull NodeStore nodeStore, 
                                        @NotNull String indexesMetadataPath) {
        this.nodeStore = nodeStore;
        this.indexesMetadataPath = indexesMetadataPath;
        LOG.info("IndexProgressMetadataManager initialized with metadata path: {}", indexesMetadataPath);
    }
    
    /**
     * @return the configured path for index metadata
     */
    @NotNull
    public String getIndexesMetadataPath() {
        return indexesMetadataPath;
    }
    
    /**
     * Registers an index to use change tracking.
     * Initializes progress metadata for the index if it doesn't exist.
     * 
     * @param indexPath the path of the index definition
     * @throws CommitFailedException if the registration fails
     */
    public void registerIndex(@NotNull String indexPath) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        
        // Ensure change tracker node exists
        ensureNode(builder, CHANGE_TRACKER_PATH);
        
        // Initialize index progress node
        String indexNodeName = sanitizeIndexName(indexPath);
        NodeBuilder indexNode = ensureNode(builder, indexesMetadataPath + "/" + indexNodeName);
        
        if (!indexNode.hasProperty(PROP_INDEX_PATH)) {
            indexNode.setProperty(PROP_INDEX_PATH, indexPath);
            indexNode.setProperty(PROP_LAST_PROCESSED_TIMESTAMP, 0L);
            indexNode.setProperty(PROP_LAST_PROCESSED_SERIAL_NUMBER, 0L);
            indexNode.setProperty(PROP_CURRENT_CHUNK_START, 0L);
            indexNode.setProperty(PROP_CURRENT_CHUNK_END, 0L);
            indexNode.setProperty(PROP_PROCESSING_STARTED, 0L);
            indexNode.setProperty(PROP_LAST_CHUNK_COMMIT, 0L);
            
            // Initialize statistics
            NodeBuilder stats = indexNode.child(NODE_STATS);
            stats.setProperty(PROP_TOTAL_PROCESSED, 0L);
            stats.setProperty(PROP_TOTAL_CHUNKS, 0L);
            stats.setProperty(PROP_AVERAGE_CHUNK_TIME, 0L);
            stats.setProperty(PROP_LAST_CHUNK_SIZE, 0);
            
            LOG.info("Initialized progress metadata for index: {}", indexPath);
        }
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    /**
     * Gets the progress metadata for an index.
     * 
     * @param indexPath the path of the index definition
     * @return the progress metadata, or null if not found
     */
    @Nullable
    public IndexProgressMetadata getIndexProgress(@NotNull String indexPath) {
        NodeState root = nodeStore.getRoot();
        String indexNodeName = sanitizeIndexName(indexPath);
        NodeState indexNode = getNode(root, indexesMetadataPath + "/" + indexNodeName);
        
        if (indexNode == null || !indexNode.exists()) {
            return null;
        }
        
        NodeState stats = indexNode.getChildNode(NODE_STATS);
        
        return new IndexProgressMetadata.Builder()
            .indexPath(indexPath)
            .lastProcessedTimestamp(getLong(indexNode, PROP_LAST_PROCESSED_TIMESTAMP, 0L))
            .lastProcessedSerialNumber(getLong(indexNode, PROP_LAST_PROCESSED_SERIAL_NUMBER, 0L))
            .currentChunkStart(getLong(indexNode, PROP_CURRENT_CHUNK_START, 0L))
            .currentChunkEnd(getLong(indexNode, PROP_CURRENT_CHUNK_END, 0L))
            .processingStarted(getLong(indexNode, PROP_PROCESSING_STARTED, 0L))
            .lastChunkCommit(getLong(indexNode, PROP_LAST_CHUNK_COMMIT, 0L))
            .totalProcessed(getLong(stats, PROP_TOTAL_PROCESSED, 0L))
            .totalChunks(getLong(stats, PROP_TOTAL_CHUNKS, 0L))
            .build();
    }
    
    /**
     * Updates the progress for an index after processing a chunk.
     * 
     * @param indexPath the path of the index definition
     * @param lastTimestamp the last processed timestamp
     * @param lastSerialNumber the last processed serial number
     * @param processedCount the number of entries processed in this chunk
     * @throws CommitFailedException if the update fails
     */
    public void updateProgress(@NotNull String indexPath,
                              long lastTimestamp,
                              long lastSerialNumber,
                              int processedCount) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        
        String indexNodeName = sanitizeIndexName(indexPath);
        NodeBuilder indexNode = ensureNode(builder, indexesMetadataPath + "/" + indexNodeName);
        
        // Update progress
        indexNode.setProperty(PROP_LAST_PROCESSED_TIMESTAMP, lastTimestamp);
        indexNode.setProperty(PROP_LAST_PROCESSED_SERIAL_NUMBER, lastSerialNumber);
        indexNode.setProperty(PROP_LAST_CHUNK_COMMIT, System.currentTimeMillis());
        
        // Update statistics
        NodeBuilder stats = indexNode.child(NODE_STATS);
        long totalProcessed = getLong(stats.getNodeState(), PROP_TOTAL_PROCESSED, 0L) + processedCount;
        long totalChunks = getLong(stats.getNodeState(), PROP_TOTAL_CHUNKS, 0L) + 1;
        
        stats.setProperty(PROP_TOTAL_PROCESSED, totalProcessed);
        stats.setProperty(PROP_TOTAL_CHUNKS, totalChunks);
        stats.setProperty(PROP_LAST_CHUNK_SIZE, processedCount);
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        LOG.debug("Updated progress for {}: timestamp={}, serial={}, processed={}",
                indexPath, lastTimestamp, lastSerialNumber, processedCount);
    }
    
    /**
     * Updates the change tracker state after a diff run.
     * 
     * @param checkpoint the checkpoint processed
     * @param diffProcessingTime the timestamp of the diff run
     * @param changeCount the number of changes recorded
     * @throws CommitFailedException if the update fails
     */
    public void updateChangeTrackerState(@NotNull String checkpoint,
                                        long diffProcessingTime,
                                        int changeCount) throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        
        NodeBuilder changeTracker = ensureNode(builder, CHANGE_TRACKER_PATH);
        changeTracker.setProperty(PROP_LAST_PROCESSED_CHECKPOINT, checkpoint);
        changeTracker.setProperty(PROP_LAST_DIFF_PROCESSING_TIME, diffProcessingTime);
        changeTracker.setProperty(PROP_LAST_RUN_COMPLETE, System.currentTimeMillis());
        changeTracker.setProperty(PROP_LAST_RUN_CHANGE_COUNT, changeCount);
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        LOG.info("Updated change tracker state: checkpoint={}, time={}, changes={}",
                checkpoint, diffProcessingTime, changeCount);
    }
    
    /**
     * Marks the start of change tracker processing.
     * 
     * @throws CommitFailedException if the update fails
     */
    public void markChangeTrackerStart() throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder builder = root.builder();
        
        NodeBuilder changeTracker = ensureNode(builder, CHANGE_TRACKER_PATH);
        changeTracker.setProperty(PROP_LAST_RUN_START, System.currentTimeMillis());
        
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    /**
     * Gets the list of indexes using change tracking.
     * 
     * <p>This scans the indexes metadata directory to discover which indexes
     * have been registered for change tracking.
     * 
     * @return the list of index paths using change tracking
     */
    @NotNull
    public List<String> getRegisteredIndexes() {
        NodeState root = nodeStore.getRoot();
        NodeState indexesNode = getNode(root, indexesMetadataPath);
        
        if (indexesNode == null || !indexesNode.exists()) {
            return Collections.emptyList();
        }
        
        List<String> indexes = new ArrayList<>();
        for (String indexNodeName : indexesNode.getChildNodeNames()) {
            NodeState indexNode = indexesNode.getChildNode(indexNodeName);
            PropertyState indexPathProp = indexNode.getProperty(PROP_INDEX_PATH);
            if (indexPathProp != null) {
                indexes.add(indexPathProp.getValue(Type.STRING));
            }
        }
        
        return indexes;
    }
    
    /**
     * Gets the last diff processing time.
     * 
     * @return the timestamp, or 0 if not set
     */
    public long getLastDiffProcessingTime() {
        NodeState root = nodeStore.getRoot();
        NodeState changeTracker = getNode(root, CHANGE_TRACKER_PATH);
        
        if (changeTracker == null || !changeTracker.exists()) {
            return 0;
        }
        
        return getLong(changeTracker, PROP_LAST_DIFF_PROCESSING_TIME, 0L);
    }
    
    // Helper methods
    
    private NodeBuilder ensureNode(NodeBuilder root, String path) {
        NodeBuilder current = root;
        String[] segments = path.split("/");
        
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.child(segment);
        }
        
        return current;
    }
    
    @Nullable
    private NodeState getNode(NodeState root, String path) {
        NodeState current = root;
        String[] segments = path.split("/");
        
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.getChildNode(segment);
            if (!current.exists()) {
                return null;
            }
        }
        
        return current;
    }
    
    private long getLong(NodeState node, String property, long defaultValue) {
        if (node == null || !node.exists()) {
            return defaultValue;
        }
        PropertyState prop = node.getProperty(property);
        return prop != null ? prop.getValue(Type.LONG) : defaultValue;
    }
    
    private List<String> getStringList(NodeState node, String property) {
        if (node == null || !node.exists()) {
            return Collections.emptyList();
        }
        PropertyState prop = node.getProperty(property);
        if (prop == null) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String value : prop.getValue(Type.STRINGS)) {
            result.add(value);
        }
        return result;
    }
    
    private List<String> getStringList(NodeBuilder node, String property) {
        return getStringList(node.getNodeState(), property);
    }
    
    private String sanitizeIndexName(String indexPath) {
        // Convert /oak:index/damAssetLucene to damAssetLucene
        String name = indexPath;
        if (name.startsWith("/")) {
            int lastSlash = name.lastIndexOf('/');
            if (lastSlash >= 0) {
                name = name.substring(lastSlash + 1);
            }
        }
        // Replace any remaining special characters
        return name.replaceAll("[^a-zA-Z0-9_-]", "_");
    }
}

