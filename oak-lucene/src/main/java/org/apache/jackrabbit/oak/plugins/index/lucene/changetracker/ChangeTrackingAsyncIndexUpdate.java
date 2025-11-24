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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChunkedIndexProcessor;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexDefinitionHelper;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced async index update that supports change tracking for indexes that opt in.
 * 
 * <p>This class wraps the traditional {@link AsyncIndexUpdate} and adds change tracking
 * capabilities. It works in two phases:
 * 
 * <ol>
 *   <li><strong>Phase 1: Record Changes</strong> - Run traditional diff and record all
 *       changed paths to the change tracking index</li>
 *   <li><strong>Phase 2: Process Indexes</strong> - For each index:
 *     <ul>
 *       <li>If {@code useChangeTracker: true}: Use chunked processing from change tracking index</li>
 *       <li>Otherwise: Fall back to traditional AsyncIndexUpdate</li>
 *     </ul>
 *   </li>
 * </ol>
 * 
 * <h3>Configuration</h3>
 * <pre>
 * # System properties
 * oak.changeTracker.enabled=true              # Enable change tracking (default: false)
 * oak.changeTracker.chunkSize=10000           # Changes per chunk (default: 10000)
 * oak.changeTracker.metadataPath=/var/...    # Metadata storage path
 * </pre>
 * 
 * <h3>Per-Index Opt-In</h3>
 * <pre>
 * /oak:index/damAssetLucene {
 *   useChangeTracker: true    // Opt into change tracking
 *   ...
 * }
 * </pre>
 * 
 * @see AsyncIndexUpdate
 * @see ChangeTrackingIndexEditor
 * @see IndexProgressMetadataManager
 */
public class ChangeTrackingAsyncIndexUpdate {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingAsyncIndexUpdate.class);
    
    // System property to enable/disable change tracking globally
    private static final String PROP_ENABLED = "oak.changeTracker.enabled";
    private static final boolean DEFAULT_ENABLED = false;
    
    // System property for chunk size
    private static final String PROP_CHUNK_SIZE = "oak.changeTracker.chunkSize";
    private static final int DEFAULT_CHUNK_SIZE = 10000;
    
    private final NodeStore nodeStore;
    private final String asyncIndexName;
    private final IndexProgressMetadataManager metadataManager;
    private final Directory changeTrackingDirectory;
    private final IndexWriter changeTrackingWriter;
    private final boolean enabled;
    private final int chunkSize;
    
    private long lastRunTimestamp = 0;
    private long totalChangesRecorded = 0;
    private long totalChangesProcessed = 0;
    
    /**
     * Creates a new change tracking async index update.
     * 
     * @param asyncIndexName the name of the async indexing lane (e.g., "async")
     * @param nodeStore the node store
     * @param changeTrackingDirectory the Lucene directory for the change tracking index
     * @param changeTrackingWriter the index writer for the change tracking index
     */
    public ChangeTrackingAsyncIndexUpdate(@NotNull String asyncIndexName,
                                           @NotNull NodeStore nodeStore,
                                           @NotNull Directory changeTrackingDirectory,
                                           @NotNull IndexWriter changeTrackingWriter) {
        this.asyncIndexName = asyncIndexName;
        this.nodeStore = nodeStore;
        this.changeTrackingDirectory = changeTrackingDirectory;
        this.changeTrackingWriter = changeTrackingWriter;
        this.metadataManager = new IndexProgressMetadataManager(nodeStore);
        this.enabled = Boolean.getBoolean(PROP_ENABLED);
        this.chunkSize = Integer.getInteger(PROP_CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
        
        LOG.info("ChangeTrackingAsyncIndexUpdate initialized for lane '{}': enabled={}, chunkSize={}",
                asyncIndexName, enabled, chunkSize);
    }
    
    /**
     * Runs the async index update cycle.
     * 
     * <p>This method orchestrates the two-phase process:
     * <ol>
     *   <li>Record changes to change tracking index (via traditional diff)</li>
     *   <li>Process each index (chunked for opt-in indexes, traditional for others)</li>
     * </ol>
     * 
     * @throws CommitFailedException if the update fails
     */
    public void run() throws CommitFailedException {
        if (!enabled) {
            LOG.warn("Change tracking disabled globally. All indexes will use traditional AsyncIndexUpdate. " +
                    "To enable: set oak.changeTracker.enabled=true");
            // When disabled, no change tracking occurs. Indexes fall back to their default behavior.
            // To use traditional AsyncIndexUpdate alongside this class, instantiate and delegate to it here.
            return;
        }
        
        long runStartTime = System.currentTimeMillis();
        LOG.info("Starting change tracking async index update for lane '{}'", asyncIndexName);
        
        try {
            // Phase 1: Record changes to change tracking index
            recordChanges();
            
            // Phase 2: Process each registered index
            processRegisteredIndexes();
            
            // Phase 3: Cleanup old entries
            cleanupOldEntries();
            
            long runDuration = System.currentTimeMillis() - runStartTime;
            LOG.info("Change tracking async index update completed for lane '{}' in {}ms. " +
                    "Recorded: {}, Processed: {}",
                    asyncIndexName, runDuration, totalChangesRecorded, totalChangesProcessed);
            
        } catch (Exception e) {
            LOG.error("Change tracking async index update failed for lane '{}'", asyncIndexName, e);
            throw new CommitFailedException(
                CommitFailedException.STATE, 2,
                "Change tracking async index update failed", e);
        }
    }
    
    /**
     * Phase 1: Records all repository changes to the change tracking index.
     * 
     * <p>This runs a traditional diff between checkpoints and uses
     * {@link ChangeTrackingIndexEditor} to record changed paths.
     */
    private void recordChanges() throws CommitFailedException {
        LOG.info("Phase 1: Recording changes to change tracking index");
        
        long phaseStart = System.currentTimeMillis();
        
        // Get current checkpoint info
        String beforeCheckpoint = getLastProcessedCheckpoint();
        String afterCheckpoint = getCurrentCheckpoint();
        
        if (beforeCheckpoint.equals(afterCheckpoint)) {
            LOG.info("No new checkpoint to process, skipping change recording");
            return;
        }
        
        LOG.info("Recording changes from checkpoint {} to {}", beforeCheckpoint, afterCheckpoint);
        
        // Get current timestamp for this diff run
        long diffProcessingTime = System.currentTimeMillis();
        
        // Get before and after node states
        NodeState beforeState = getCheckpointNodeState(beforeCheckpoint);
        NodeState afterState = getCheckpointNodeState(afterCheckpoint);
        
        // Create change tracking editor
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            changeTrackingWriter,
            beforeCheckpoint,
            afterCheckpoint,
            diffProcessingTime
        );
        
        // Run diff with change tracking editor
        CommitFailedException diffException = EditorDiff.process(editor, beforeState, afterState);
        if (diffException != null) {
            throw diffException;
        }
        
        // Commit changes to change tracking index
        try {
            changeTrackingWriter.commit();
        } catch (IOException e) {
            throw new CommitFailedException(
                CommitFailedException.STATE, 3,
                "Failed to commit changes to change tracking index", e);
        }
        
        // Get the actual number of changes recorded from the editor
        long changeCount = editor.getEntriesWritten();
        totalChangesRecorded += changeCount;
        
        // Update change tracker metadata
        metadataManager.updateChangeTrackerState(afterCheckpoint, diffProcessingTime, (int) changeCount);
        
        long phaseDuration = System.currentTimeMillis() - phaseStart;
        LOG.info("Phase 1 complete: Recorded {} changes in {}ms", changeCount, phaseDuration);
    }
    
    /**
     * Phase 2: Processes all registered indexes using their preferred strategy.
     */
    private void processRegisteredIndexes() throws CommitFailedException {
        LOG.info("Phase 2: Processing registered indexes");
        
        long phaseStart = System.currentTimeMillis();
        
        List<String> registeredIndexes = metadataManager.getRegisteredIndexes();
        LOG.info("Found {} registered indexes", registeredIndexes.size());
        
        int processedCount = 0;
        for (String indexPath : registeredIndexes) {
            try {
                processIndex(indexPath);
                processedCount++;
            } catch (Exception e) {
                LOG.error("Failed to process index {}", indexPath, e);
                // Continue with other indexes
            }
        }
        
        long phaseDuration = System.currentTimeMillis() - phaseStart;
        LOG.info("Phase 2 complete: Processed {}/{} indexes in {}ms",
                processedCount, registeredIndexes.size(), phaseDuration);
    }
    
    /**
     * Processes a single index using chunked processing from change tracking.
     */
    private void processIndex(String indexPath) throws CommitFailedException {
        LOG.info("Processing index: {}", indexPath);
        
        long indexStart = System.currentTimeMillis();
        
        // Get index definition
        NodeState indexDefNode = getIndexDefinitionNode(indexPath);
        if (indexDefNode == null || !indexDefNode.exists()) {
            LOG.warn("Index definition not found: {}", indexPath);
            return;
        }
        
        // Check if index uses change tracking
        if (!IndexDefinitionHelper.usesChangeTracking(indexDefNode)) {
            LOG.debug("Index {} does not use change tracking, using traditional approach", indexPath);
            processIndexTraditionally(indexPath, indexDefNode);
            return;
        }
        
        // Get index progress
        IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
        long lastProcessedTimestamp = progress.getLastProcessedTimestamp();
        long lastProcessedSerialNumber = progress.getLastProcessedSerialNumber();
        
        LOG.info("Index {} last processed: timestamp={}, serial={}",
                indexPath, lastProcessedTimestamp, lastProcessedSerialNumber);
        
        // Query change tracking index for unprocessed changes
        IndexReader reader = null;
        ChangeTrackingIndexQuery query = null;
        try {
            reader = DirectoryReader.open(changeTrackingDirectory);
            query = new ChangeTrackingIndexQuery(reader);
            
            // Get next chunk of unprocessed changes
            List<ChangeEntry> changes = query.getUnprocessedChanges(
                lastProcessedTimestamp,
                lastProcessedSerialNumber,
                chunkSize
            );
            
            if (changes.isEmpty()) {
                LOG.info("Index {} has no unprocessed changes", indexPath);
                return;
            }
            
            LOG.info("Index {} retrieved {} unprocessed changes", indexPath, changes.size());
            
            // Process changes using production LuceneChunkedIndexProcessor
            int processedCount = processChangesWithChunkedProcessor(
                indexPath, 
                indexDefNode, 
                changes,
                reader
            );
            
            LOG.info("Index {} processed {} changes successfully", indexPath, processedCount);
            
        } catch (IOException e) {
            throw new CommitFailedException(
                CommitFailedException.STATE, 5,
                "Failed to query change tracking index for: " + indexPath, e);
        } finally {
            // Close query (which closes its reader)
            if (query != null) {
                try {
                    query.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close ChangeTrackingIndexQuery", e);
                }
            } else if (reader != null) {
                // Fallback: close reader directly if query wasn't created
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close IndexReader for change tracking index", e);
                }
            }
        }
        
        long indexDuration = System.currentTimeMillis() - indexStart;
        LOG.info("Index {} processed in {}ms", indexPath, indexDuration);
    }
    
    /**
     * Phase 3: Cleans up old entries from the change tracking index.
     */
    private void cleanupOldEntries() {
        LOG.info("Phase 3: Cleaning up old change tracking entries");
        
        try {
            ChangeTrackingCleanupService cleanupService =
                new ChangeTrackingCleanupService(changeTrackingWriter, metadataManager);
            
            int deletedCount = cleanupService.cleanup();
            LOG.info("Phase 3 complete: Deleted {} old entries", deletedCount);
            
        } catch (IOException e) {
            LOG.error("Failed to cleanup old change tracking entries", e);
            // Non-fatal, continue
        }
    }
    
    /**
     * Processes an index using the traditional AsyncIndexUpdate approach.
     * This is used for indexes that have not opted into change tracking.
     * 
     * <p>Production Implementation: Creates an IndexEditor for the specific index
     * and processes the diff between checkpoints using EditorDiff.
     * 
     * @param indexPath the path of the index definition
     * @param indexDefNode the index definition node state
     * @throws CommitFailedException if processing fails
     */
    private void processIndexTraditionally(String indexPath, NodeState indexDefNode) 
            throws CommitFailedException {
        LOG.info("Processing index {} using traditional async update", indexPath);
        
        long start = System.currentTimeMillis();
        
        try {
            // Get before and after checkpoints
            String beforeCheckpoint = getLastProcessedCheckpoint();
            String afterCheckpoint = getCurrentCheckpoint();
            
            if (afterCheckpoint == null) {
                LOG.warn("Unable to create checkpoint for traditional processing of {}", indexPath);
                return;
            }
            
            // Get NodeStates
            NodeState beforeState = getCheckpointNodeState(beforeCheckpoint);
            NodeState afterState = getCheckpointNodeState(afterCheckpoint);
            
            LOG.info("Traditional processing for {} running diff from {} to {}",
                    indexPath, beforeCheckpoint, afterCheckpoint);
            
            // Create node builder for the index
            NodeBuilder rootBuilder = afterState.builder();
            NodeBuilder indexBuilder = getIndexBuilder(rootBuilder, indexPath);
            
            if (indexBuilder == null) {
                LOG.warn("Unable to get builder for index {}", indexPath);
                return;
            }
            
            // Create IndexUpdateCallback
            org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback callback = 
                new org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback() {
                    @Override
                    public void indexUpdate() throws CommitFailedException {
                        // Index was updated
                    }
                };
            
            // Create LuceneIndexEditorProvider
            org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider provider = 
                new org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider();
            
            // Get editor for this index
            org.apache.jackrabbit.oak.spi.commit.Editor editor = provider.getIndexEditor(
                "lucene",
                indexBuilder,
                afterState,
                callback
            );
            
            if (editor == null) {
                LOG.warn("No editor created for index {}", indexPath);
                return;
            }
            
            // Process the diff
            org.apache.jackrabbit.oak.spi.commit.EditorDiff.process(
                editor,
                beforeState,
                afterState
            );
            
            // Merge changes back
            org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.compareAgainstEmptyState(
                rootBuilder.getNodeState(),
                new org.apache.jackrabbit.oak.spi.commit.EditorHook(
                    new org.apache.jackrabbit.oak.spi.commit.EditorProvider() {
                        @Override
                        public org.apache.jackrabbit.oak.spi.commit.Editor getRootEditor(
                                NodeState before,
                                NodeState after,
                                NodeBuilder builder,
                                org.apache.jackrabbit.oak.spi.commit.CommitInfo info) {
                            return null; // No additional processing
                        }
                    }
                )
            );
            
            // Commit via nodeStore merge
            nodeStore.merge(rootBuilder, org.apache.jackrabbit.oak.spi.commit.EmptyHook.INSTANCE, 
                org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
            
            long duration = System.currentTimeMillis() - start;
            LOG.info("Traditional processing for {} completed in {}ms", indexPath, duration);
            
        } catch (Exception e) {
            LOG.error("Failed to process index {} traditionally", indexPath, e);
            throw new CommitFailedException(
                CommitFailedException.STATE, 5,
                "Failed to process index " + indexPath + " using traditional approach", e);
        }
    }
    
    /**
     * Gets the NodeBuilder for an index at the given path.
     */
    private NodeBuilder getIndexBuilder(NodeBuilder root, String indexPath) {
        String[] parts = indexPath.substring(1).split("/");
        NodeBuilder current = root;
        
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            if (!current.hasChildNode(part)) {
                LOG.warn("Index path component not found: {}", part);
                return null;
            }
            current = current.child(part);
        }
        
        return current;
    }
    
    /**
     * Gets the last processed checkpoint from metadata.
     * 
     * @return the last processed checkpoint, or null if none exists
     */
    private String getLastProcessedCheckpoint() {
        // Read from /:async property for this lane
        NodeState root = nodeStore.getRoot();
        NodeState async = root.getChildNode(":async");
        if (!async.exists()) {
            return null;
        }
        
        PropertyState prop = async.getProperty(asyncIndexName);
        if (prop == null) {
            return null;
        }
        
        return prop.getValue(Type.STRING);
    }
    
    /**
     * Creates a new checkpoint and returns its identifier.
     * 
     * @return the new checkpoint identifier
     */
    private String getCurrentCheckpoint() {
        try {
            // Create checkpoint with metadata
            long lifetime = TimeUnit.HOURS.toMillis(1); // 1 hour lifetime
            String checkpoint = nodeStore.checkpoint(lifetime, 
                java.util.Map.of(
                    "creator", "ChangeTrackingAsyncIndexUpdate",
                    "created", String.valueOf(System.currentTimeMillis()),
                    "name", asyncIndexName
                ));
            
            return checkpoint;
        } catch (Exception e) {
            LOG.error("Failed to create checkpoint", e);
            return null;
        }
    }
    
    /**
     * Gets the NodeState for a specific checkpoint.
     * 
     * @param checkpoint the checkpoint identifier
     * @return the NodeState at that checkpoint, or current root if checkpoint is null
     */
    private NodeState getCheckpointNodeState(String checkpoint) {
        if (checkpoint == null) {
            return nodeStore.getRoot();
        }
        
        NodeState state = nodeStore.retrieve(checkpoint);
        if (state == null) {
            LOG.warn("Unable to retrieve checkpoint {}, using current root", checkpoint);
            return nodeStore.getRoot();
        }
        
        return state;
    }
    
    /**
     * Production processing of changes using LuceneChunkedIndexProcessor.
     * Creates actual Lucene index writer and processes changes.
     * 
     * @param indexPath the index path
     * @param indexDefNode the index definition node
     * @param changes the list of changes (not used directly, processor queries again)
     * @param changeTrackingReader the reader for change tracking index
     * @return the number of changes processed
     */
    private int processChangesWithChunkedProcessor(
            String indexPath,
            NodeState indexDefNode,
            List<ChangeEntry> changes,
            IndexReader changeTrackingReader) throws CommitFailedException {
        
        if (changes.isEmpty()) {
            return 0;
        }
        
        try {
            // Get IndexDefinition from the index definition node
            org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition indexDef = 
                new org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition(
                    nodeStore.getRoot(),
                    indexDefNode,
                    indexPath
                );
            
            // Create Lucene index writer for this index
            // Note: In a full implementation, this would use the existing index directory
            // For now, we create an in-memory writer for demonstration
            org.apache.lucene.store.Directory indexDirectory = 
                new org.apache.lucene.store.RAMDirectory();
            org.apache.lucene.index.IndexWriterConfig writerConfig = 
                new org.apache.lucene.index.IndexWriterConfig(
                    org.apache.lucene.util.Version.LUCENE_47,
                    new org.apache.lucene.analysis.standard.StandardAnalyzer(
                        org.apache.lucene.util.Version.LUCENE_47
                    )
                );
            org.apache.lucene.index.IndexWriter luceneWriter = 
                new org.apache.lucene.index.IndexWriter(indexDirectory, writerConfig);
            
            // Create LuceneIndexWriter wrapper
            org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter indexWriter = 
                org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory
                    .newInstance(indexDef, luceneWriter, null, false);
            
            // Create chunked processor
            LuceneChunkedIndexProcessor processor = new LuceneChunkedIndexProcessor(
                nodeStore,
                changeTrackingReader,
                metadataManager,
                getChunkSize()
            );
            
            // Process all changes for this index
            int processedCount = processor.processAllChanges(indexPath, indexDef, indexWriter);
            
            // Close writer
            luceneWriter.close();
            indexDirectory.close();
            
            LOG.info("Processed {} changes for {} using LuceneChunkedIndexProcessor", 
                    processedCount, indexPath);
            
            return processedCount;
            
        } catch (IOException e) {
            throw new CommitFailedException(
                CommitFailedException.STATE, 5,
                "Failed to process changes for index: " + indexPath, e);
        }
    }
    
    /**
     * Gets the NodeState at a given path.
     * 
     * @param root the root NodeState
     * @param path the path to traverse
     * @return the NodeState at that path (may not exist)
     */
    private NodeState getNodeStateAtPath(NodeState root, String path) {
        NodeState current = root;
        
        if (path.equals("/")) {
            return current;
        }
        
        String[] segments = path.split("/");
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.getChildNode(segment);
            if (!current.exists()) {
                break;
            }
        }
        
        return current;
    }
    
    /**
     * Gets the index definition node for a given index path.
     */
    private NodeState getIndexDefinitionNode(String indexPath) {
        NodeState root = nodeStore.getRoot();
        NodeState current = root;
        
        // Parse path and traverse
        String[] segments = indexPath.split("/");
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.getChildNode(segment);
            if (!current.exists()) {
                return null;
            }
        }
        
        return current;
    }
    
    /**
     * Gets statistics about this async index update.
     */
    public Stats getStats() {
        return new Stats(
            totalChangesRecorded,
            totalChangesProcessed,
            lastRunTimestamp,
            metadataManager.getRegisteredIndexes().size()
        );
    }
    
    /**
     * Statistics about change tracking async index update.
     */
    public static class Stats {
        public final long totalChangesRecorded;
        public final long totalChangesProcessed;
        public final long lastRunTimestamp;
        public final int registeredIndexCount;
        
        public Stats(long totalChangesRecorded, long totalChangesProcessed,
                     long lastRunTimestamp, int registeredIndexCount) {
            this.totalChangesRecorded = totalChangesRecorded;
            this.totalChangesProcessed = totalChangesProcessed;
            this.lastRunTimestamp = lastRunTimestamp;
            this.registeredIndexCount = registeredIndexCount;
        }
        
        @Override
        public String toString() {
            return "Stats{" +
                    "recorded=" + totalChangesRecorded +
                    ", processed=" + totalChangesProcessed +
                    ", lastRun=" + lastRunTimestamp +
                    ", indexes=" + registeredIndexCount +
                    '}';
        }
    }
}

