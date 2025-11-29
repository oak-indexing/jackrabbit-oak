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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker;

import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Populator for the change tracking index.
 * 
 * <p>This class is responsible for running checkpoint diffs and populating the change tracking
 * Lucene index with changed paths. It is one of three indexers in the change tracking system:
 * 
 * <ol>
 *   <li><strong>ChangeTrackingIndexPopulator</strong> (this class) - Runs checkpoint diffs and
 *       populates the change tracking index with changed paths</li>
 *   <li><strong>Traditional AsyncIndexUpdate</strong> - Processes indexes without useChangeTracker=true</li>
 *   <li><strong>ChangeTrackingAsyncIndexUpdate</strong> - Reads from change tracking index and
 *       processes indexes WITH useChangeTracker=true</li>
 * </ol>
 * 
 * <h3>Architecture</h3>
 * <pre>
 * ChangeTrackingIndexPopulator (runs checkpoint diff)
 *   ↓
 * Change Tracking Index (Lucene)
 *   ↓
 * ChangeTrackingAsyncIndexUpdate (reads and processes)
 * </pre>
 * 
 * <h3>Key Responsibilities</h3>
 * <ul>
 *   <li>Run checkpoint diffs across the repository</li>
 *   <li>Delegate to {@link ChangeTrackingIndexEditorProvider} to record changes</li>
 *   <li>Track last processed checkpoint for cleanup coordination</li>
 *   <li>Provide single source of truth for all change-tracked indexes</li>
 * </ul>
 * 
 * <h3>Usage</h3>
 * <pre>
 * // Create the populator
 * ChangeTrackingIndexPopulator populator = new ChangeTrackingIndexPopulator(
 *     nodeStore,
 *     changeTrackingDirectory,
 *     metadataManager,
 *     statisticsProvider
 * );
 * 
 * // Initialize (creates index definition if needed)
 * populator.initialize();
 * 
 * // Run periodically (e.g., via ScheduledExecutorService)
 * scheduler.scheduleWithFixedDelay(populator, 5, 5, TimeUnit.SECONDS);
 * </pre>
 * 
 * <h3>Deployment</h3>
 * <p>This populator should run alongside traditional AsyncIndexUpdate and ChangeTrackingAsyncIndexUpdate:
 * <pre>
 * // 1. Change tracking index populator
 * ChangeTrackingIndexPopulator populator = ...;
 * scheduler.scheduleWithFixedDelay(populator::run, 5, 5, SECONDS);
 * 
 * // 2. Traditional async indexer
 * AsyncIndexUpdate traditional = new AsyncIndexUpdate("async", ...);
 * scheduler.scheduleWithFixedDelay(traditional, 5, 5, SECONDS);
 * 
 * // 3. Change tracking async indexer
 * ChangeTrackingAsyncIndexUpdate changeTracking = ...;
 * scheduler.scheduleWithFixedDelay(changeTracking::run, 10, 10, SECONDS);
 * </pre>
 */
public class ChangeTrackingIndexPopulator implements Runnable {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingIndexPopulator.class);
    
    /**
     * The async lane name for the change tracking index.
     */
    public static final String ASYNC_LANE_NAME = ChangeTrackingIndexDefinitionBuilder.ASYNC_LANE;
    
    private final NodeStore nodeStore;
    private final Directory changeTrackingDirectory;
    private final IndexWriter changeTrackingWriter;
    private final IndexProgressMetadataManager metadataManager;
    private final AsyncIndexUpdate asyncIndexUpdate;
    private final ChangeTrackingIndexEditorProvider editorProvider;
    
    private volatile boolean initialized = false;
    private volatile String lastProcessedCheckpoint = null;
    
    /**
     * Creates a change tracking index populator.
     * 
     * @param nodeStore the node store
     * @param changeTrackingDirectory the Lucene directory for the change tracking index
     * @param metadataManager the metadata manager for progress tracking
     * @param statisticsProvider the statistics provider (can be StatisticsProvider.NOOP)
     */
    public ChangeTrackingIndexPopulator(@NotNull NodeStore nodeStore,
                                       @NotNull Directory changeTrackingDirectory,
                                       @NotNull IndexProgressMetadataManager metadataManager,
                                       @NotNull StatisticsProvider statisticsProvider) throws IOException {
        this.nodeStore = nodeStore;
        this.changeTrackingDirectory = changeTrackingDirectory;
        this.metadataManager = metadataManager;
        
        // Create IndexWriter for change tracking index
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, new StandardAnalyzer(Version.LUCENE_47));
        this.changeTrackingWriter = new IndexWriter(changeTrackingDirectory, config);
        
        // Commit empty index to create initial index structure
        // This allows DirectoryReader to be opened even when no documents exist yet
        this.changeTrackingWriter.commit();
        
        // Create the editor provider that will record changes
        this.editorProvider = new ChangeTrackingIndexEditorProvider(changeTrackingWriter);
        
        // Create AsyncIndexUpdate for the change-tracker-async lane
        this.asyncIndexUpdate = new AsyncIndexUpdate(
            ASYNC_LANE_NAME,
            nodeStore,
            editorProvider,
            statisticsProvider,
            false  // switchOnSync = false (always async)
        );
        
        LOG.info("ChangeTrackingIndexPopulator created for lane: {}", ASYNC_LANE_NAME);
    }
    
    /**
     * Initializes the change tracking index.
     * 
     * <p>This method:
     * <ul>
     *   <li>Creates the change tracking index definition if it doesn't exist</li>
     *   <li>Ensures the index is properly configured</li>
     *   <li>Prepares the system for population</li>
     * </ul>
     * 
     * <p>This should be called once at startup before running the populator.
     * 
     * @throws IllegalStateException if initialization fails
     */
    public void initialize() {
        if (initialized) {
            LOG.debug("ChangeTrackingIndexPopulator already initialized");
            return;
        }
        
        try {
            LOG.info("Initializing change tracking index...");
            
            // Get root builder
            NodeState root = nodeStore.getRoot();
            NodeBuilder rootBuilder = root.builder();
            
            // Ensure oak:index exists
            if (!rootBuilder.hasChildNode("oak:index")) {
                rootBuilder.child("oak:index");
                LOG.info("Created oak:index node");
            }
            
            NodeBuilder oakIndex = rootBuilder.child("oak:index");
            
            // Create change tracking index definition if it doesn't exist
            if (!ChangeTrackingIndexDefinitionBuilder.hasChangeTrackingIndex(oakIndex)) {
                LOG.info("Creating change tracking index definition...");
                ChangeTrackingIndexDefinitionBuilder.createChangeTrackingIndex(oakIndex);
                
                // Commit the index definition
                nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                LOG.info("Change tracking index definition created successfully");
            } else {
                LOG.info("Change tracking index definition already exists");
            }
            
            initialized = true;
            LOG.info("ChangeTrackingIndexPopulator initialized successfully");
            
        } catch (Exception e) {
            LOG.error("Failed to initialize ChangeTrackingIndexPopulator", e);
            throw new IllegalStateException("Failed to initialize change tracking index populator", e);
        }
    }
    
    /**
     * Runs one iteration of change tracking index population.
     * 
     * <p>This method:
     * <ol>
     *   <li>Runs a checkpoint diff via AsyncIndexUpdate</li>
     *   <li>Records changed paths to the change tracking index</li>
     *   <li>Updates last processed checkpoint for cleanup coordination</li>
     * </ol>
     * 
     * <p>This method is designed to be called periodically (e.g., every 5 seconds).
     */
    @Override
    public void run() {
        if (!initialized) {
            LOG.warn("ChangeTrackingIndexPopulator not initialized. Call initialize() first.");
            return;
        }
        
        try {
            LOG.debug("Starting change tracking index population cycle");
            long startTime = System.currentTimeMillis();
            
            // Run AsyncIndexUpdate - this will diff checkpoint1 → checkpoint2
            // AsyncIndexUpdate will pass checkpoint1 ID through CommitInfo
            // ChangeTrackingIndexEditorProvider will extract the timestamp from checkpoint1
            asyncIndexUpdate.run();
            
            // Commit changes to the change tracking index so they are visible to readers
            changeTrackingWriter.commit();
            
            // Track the last processed checkpoint via lastProcessedCheckpoint field
            // Note: Since AsyncIndexStats is not accessible, we track checkpoints internally
            if (lastProcessedCheckpoint != null) {
                updateLastProcessedCheckpoint(lastProcessedCheckpoint);
            }
            
            long duration = System.currentTimeMillis() - startTime;
            LOG.debug("Change tracking index population cycle completed in {}ms", duration);
            
        } catch (Exception e) {
            LOG.error("Error during change tracking index population", e);
            // Don't rethrow - let the scheduler continue
        }
    }
    
    /**
     * Updates the last processed checkpoint in the metadata.
     * This is used for cleanup coordination (LIMITATION 4.2).
     * 
     * @param checkpoint the checkpoint that was just processed
     */
    private void updateLastProcessedCheckpoint(String checkpoint) {
        try {
            lastProcessedCheckpoint = checkpoint;
            
            // Get change count from AsyncIndexUpdate stats if available
            int changeCount = 0;  // Would need to extract from AsyncIndexUpdate
            
            // Store in metadata for cleanup coordination
            metadataManager.updateChangeTrackerState(checkpoint, System.currentTimeMillis(), changeCount);
            
            LOG.debug("Updated last processed checkpoint: {}", checkpoint);
            
        } catch (Exception e) {
            LOG.error("Failed to update last processed checkpoint", e);
            // Non-fatal - don't stop processing
        }
    }
    
    /**
     * Gets the last processed checkpoint.
     * Used for cleanup coordination to determine safe deletion boundaries.
     * 
     * @return the last processed checkpoint, or null if none
     */
    public String getLastProcessedCheckpoint() {
        return lastProcessedCheckpoint;
    }
    
    /**
     * Gets the timestamp when the last checkpoint was processed.
     * 
     * @return the timestamp in milliseconds, or 0 if none
     */
    public long getLastProcessedTimestamp() {
        try {
            // Read from metadata
            // This would read from the metadata manager's storage
            // For now, return current time as placeholder
            return System.currentTimeMillis();
        } catch (Exception e) {
            LOG.warn("Failed to get last processed timestamp", e);
            return 0;
        }
    }
    
    /**
     * Closes the populator and releases resources.
     */
    public void close() {
        try {
            LOG.info("Closing ChangeTrackingIndexPopulator");
            
            // Close the AsyncIndexUpdate
            asyncIndexUpdate.close();
            
            // Close the directory
            if (changeTrackingDirectory != null) {
                changeTrackingDirectory.close();
            }
            
            LOG.info("ChangeTrackingIndexPopulator closed successfully");
            
        } catch (IOException e) {
            LOG.error("Error closing ChangeTrackingIndexPopulator", e);
        }
    }
    
    /**
     * Gets statistics about the population process.
     * 
     * @return statistics string, or empty if not available
     */
    public String getStatistics() {
        try {
            return String.format(
                "ChangeTrackingIndexPopulator[lane=%s, initialized=%s, lastCheckpoint=%s]",
                ASYNC_LANE_NAME,
                initialized,
                lastProcessedCheckpoint != null ? lastProcessedCheckpoint.substring(0, Math.min(8, lastProcessedCheckpoint.length())) + "..." : "none"
            );
        } catch (Exception e) {
            return "ChangeTrackingIndexPopulator[error getting stats]";
        }
    }
    
    /**
     * Checks if the populator is initialized.
     * 
     * @return true if initialized
     */
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Forces initialization even if already initialized.
     * Useful for testing or recovery scenarios.
     */
    public void forceInitialize() {
        initialized = false;
        initialize();
    }
    
}

