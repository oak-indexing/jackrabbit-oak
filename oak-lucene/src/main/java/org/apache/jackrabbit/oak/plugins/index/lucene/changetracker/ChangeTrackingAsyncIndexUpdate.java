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
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChunkedIndexProcessor;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexDefinitionHelper;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.util.ISO8601;

import java.util.Calendar;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
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
 * Change tracking async index update that processes only indexes that opt into change tracking.
 * 
 * <p><strong>Architecture:</strong> This class runs <strong>independently alongside</strong> two other indexers:
 * <ul>
 *   <li><strong>Change Tracking Index Populator</strong> - A dedicated {@link AsyncIndexUpdate} instance for
 *       the "change-tracker-async" lane that runs checkpoint diffs and populates the change tracking index
 *       with changed paths. This is the shared source that all change-tracked indexes read from.</li>
 *   <li><strong>Traditional AsyncIndexUpdate</strong> - Handles indexes without {@code useChangeTracker=true}</li>
 *   <li><strong>ChangeTrackingAsyncIndexUpdate</strong> (this class) - Reads from the pre-populated change
 *       tracking index and processes indexes WITH {@code useChangeTracker=true}</li>
 * </ul>
 * 
 * <p><strong>Key Design Point:</strong> This class does NOT perform checkpoint diffs. It only reads
 * changed paths from the pre-populated change tracking index, then indexes the current content
 * from NodeStore. The checkpoint diffing is done once by the dedicated change tracking populator,
 * and multiple indexes benefit from that single diff.
 * 
 * <p><strong>Two-Phase Processing:</strong>
 * <ol>
 *   <li><strong>Phase 1: Process Change-Tracked Indexes</strong> - Read changed paths from change tracking
 *       index and process them in chunks for each opt-in index</li>
 *   <li><strong>Phase 2: Cleanup</strong> - Remove old entries from change tracking index that have
 *       been processed by all registered indexes</li>
 * </ol>
 * 
 * <h3>System Configuration</h3>
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
 *   async: "async"             // Still part of async lane
 *   ...
 * }
 * </pre>
 * 
 * <h3>Deployment Example</h3>
 * <pre>
 * // 1. Change Tracking Index Populator (runs checkpoint diffs, populates change tracking index)
 * AsyncIndexUpdate changeTrackingPopulator = new AsyncIndexUpdate(
 *     "change-tracker-async", 
 *     store, 
 *     changeTrackingIndexEditorProvider  // Uses ChangeTrackingIndexEditorProvider
 * );
 * scheduler.scheduleWithFixedDelay(changeTrackingPopulator, 5, 5, SECONDS);
 * 
 * // 2. Traditional indexer for non-change-tracked indexes
 * AsyncIndexUpdate traditional = new AsyncIndexUpdate("async", store, luceneProvider);
 * scheduler.scheduleWithFixedDelay(traditional, 5, 5, SECONDS);
 * 
 * // 3. Change tracking processor for opt-in indexes (reads from pre-populated tracking index)
 * ChangeTrackingAsyncIndexUpdate changeTrackingProcessor = 
 *     new ChangeTrackingAsyncIndexUpdate("async", store, changeTrackingDirectory, changeTrackingWriter);
 * scheduler.scheduleWithFixedDelay(changeTrackingProcessor, 5, 5, SECONDS);
 * </pre>
 * 
 * @see AsyncIndexUpdate
 * @see ChangeTrackingIndexEditor
 * @see IndexProgressMetadataManager
 * @see IndexDefinitionHelper#usesChangeTracking(NodeState)
 */
public class ChangeTrackingAsyncIndexUpdate {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingAsyncIndexUpdate.class);
    
    // System property for chunk size
    private static final String PROP_CHUNK_SIZE = "oak.changeTracker.chunkSize";
    private static final int DEFAULT_CHUNK_SIZE = 10000;
    
    private final NodeStore nodeStore;
    private final String asyncIndexName;
    private final IndexProgressMetadataManager metadataManager;
    private final Directory changeTrackingDirectory;
    private final IndexWriter changeTrackingWriter;
    private final int chunkSize;
    
    private long lastRunTimestamp = 0;
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
                                           IndexWriter changeTrackingWriter) {
        this.asyncIndexName = asyncIndexName;
        this.nodeStore = nodeStore;
        this.changeTrackingDirectory = changeTrackingDirectory;
        this.changeTrackingWriter = changeTrackingWriter;
        this.metadataManager = new IndexProgressMetadataManager(nodeStore);
        this.chunkSize = Integer.getInteger(PROP_CHUNK_SIZE, DEFAULT_CHUNK_SIZE);
        
        LOG.info("ChangeTrackingAsyncIndexUpdate initialized for lane '{}': chunkSize={}",
                asyncIndexName, chunkSize);
    }
    
    /**
     * Runs the async index update cycle.
     * 
     * <p><strong>Important:</strong> This method does NOT run checkpoint diffs. 
     * It assumes the change tracking index is already populated by a separate process
     * (a dedicated AsyncIndexUpdate instance for the change-tracker-async lane).
     * 
     * <p>This method orchestrates the two-phase process:
     * <ol>
     *   <li>Process each change-tracked index (read from change tracking index)</li>
     *   <li>Cleanup old entries</li>
     * </ol>
     * 
     * @throws CommitFailedException if the update fails
     */
    public void run() throws CommitFailedException {
        long runStartTime = System.currentTimeMillis();
        LOG.info("Starting change tracking async index update for lane '{}'", asyncIndexName);
        
        try {
            // Phase 1: Process each registered change-tracked index
            // (reads from pre-populated change tracking index)
            processRegisteredIndexes();
            
            // Phase 2: Cleanup old entries
            cleanupOldEntries();
            
            long runDuration = System.currentTimeMillis() - runStartTime;
            LOG.info("Change tracking async index update completed for lane '{}' in {}ms. " +
                    "Processed: {}",
                    asyncIndexName, runDuration, totalChangesProcessed);
            
        } catch (Exception e) {
            LOG.error("Change tracking async index update failed for lane '{}'", asyncIndexName, e);
            throw new CommitFailedException(
                CommitFailedException.STATE, 2,
                "Change tracking async index update failed", e);
        }
    }
    
    
    /**
     * Phase 1: Processes all change-tracked indexes.
     * 
     * <p><strong>Important:</strong> This method reads from the pre-populated change tracking index.
     * It does NOT run checkpoint diffs - that's done by a separate AsyncIndexUpdate instance
     * dedicated to the change-tracker-async lane.
     * 
     * <p>Only processes indexes with {@code useChangeTracker=true}.
     * Other indexes are handled by the traditional AsyncIndexUpdate running in parallel.
     */
    private void processRegisteredIndexes() throws CommitFailedException {
        LOG.info("Phase 1: Processing change-tracked indexes (reading from pre-populated change tracking index)");
        
        long phaseStart = System.currentTimeMillis();

        //TODO we should not explicitly register indexes
        List<String> allIndexes = metadataManager.getRegisteredIndexes();
        LOG.info("Found {} registered indexes", allIndexes.size());
        
        // Filter to only change-tracked indexes
        List<String> changeTrackedIndexes = getChangeTrackedIndexes(allIndexes);
        LOG.info("Found {} change-tracked indexes (out of {} total)", 
                changeTrackedIndexes.size(), allIndexes.size());
        
        if (changeTrackedIndexes.isEmpty()) {
            LOG.info("No change-tracked indexes to process. Ensure indexes have useChangeTracker=true");
            return;
        }
        
        int processedCount = 0;
        for (String indexPath : changeTrackedIndexes) {
            try {
                processIndex(indexPath);
                processedCount++;
            } catch (Exception e) {
                LOG.error("Failed to process change-tracked index {}", indexPath, e);
                // Continue with other indexes
            }
        }
        
        long phaseDuration = System.currentTimeMillis() - phaseStart;
        LOG.info("Phase 1 complete: Processed {}/{} change-tracked indexes in {}ms",
                processedCount, changeTrackedIndexes.size(), phaseDuration);
    }
    
    /**
     * Filters the list of registered indexes to only those with change tracking enabled.
     * 
     * @param allIndexes all registered index paths
     * @return list of index paths that have {@code useChangeTracker=true}
     */
    private List<String> getChangeTrackedIndexes(List<String> allIndexes) {
        List<String> changeTracked = new ArrayList<>();
        
        for (String indexPath : allIndexes) {
            NodeState indexDefNode = getIndexDefinitionNode(indexPath);
            if (indexDefNode != null && indexDefNode.exists() 
                    && IndexDefinitionHelper.usesChangeTracking(indexDefNode)) {
                changeTracked.add(indexPath);
            }
        }
        
        return changeTracked;
    }
    
    /**
     * Processes a single index using chunked processing from the change tracking index.
     * 
     * <p><strong>Important:</strong> This method reads changed paths from the pre-populated
     * change tracking index, then indexes the current content from NodeStore.
     * It does NOT perform checkpoint diffs.
     * 
     * <p>Only processes indexes with {@code useChangeTracker=true}.
     * Indexes without this flag should be handled by the traditional {@link AsyncIndexUpdate}.
     */
    private void processIndex(String indexPath) throws CommitFailedException {
        LOG.info("Processing index: {} (reading changes from change tracking index)", indexPath);
        
        long indexStart = System.currentTimeMillis();
        
        // Get index definition
        NodeState indexDefNode = getIndexDefinitionNode(indexPath);
        if (indexDefNode == null || !indexDefNode.exists()) {
            LOG.warn("Index definition not found: {}", indexPath);
            return;
        }
        
        // Check if index uses change tracking - SKIP if not
        if (!IndexDefinitionHelper.usesChangeTracking(indexDefNode)) {
            LOG.debug("Index {} does not use change tracking. Skipping - it will be handled by traditional AsyncIndexUpdate", indexPath);
            return;  // ✅ Skip, don't process traditionally
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
            if (changeTrackingWriter != null) {
                // Use NRT reader if writer is available
                reader = DirectoryReader.open(changeTrackingWriter, true);
            } else {
                reader = DirectoryReader.open(changeTrackingDirectory);
            }
            query = new ChangeTrackingIndexQuery(reader);
            
            // Get unprocessed changes
            // Loop until no more changes are returned
            List<ChangeEntry> changes;
            do {
                // Get next chunk of unprocessed changes
                changes = query.getUnprocessedChanges(
                    lastProcessedTimestamp,
                    lastProcessedSerialNumber,
                    chunkSize
                );
                
                if (changes.isEmpty()) {
                    LOG.info("Index {} has no unprocessed changes", indexPath);
                    break; // Exit loop
                }
                
                LOG.info("Index {} retrieved {} unprocessed changes", indexPath, changes.size());
                
                // Process changes using production LuceneChunkedIndexProcessor
                int processedCount = processChangesWithChunkedProcessor(
                    indexPath, 
                    indexDefNode, 
                    changes,
                    reader
                );
                
                // Update total processed count
                this.totalChangesProcessed += processedCount;
                
                LOG.info("Index {} processed {} changes successfully", indexPath, processedCount);

                // IMPORTANT: Update progress tracking variables for the next iteration
                if (!changes.isEmpty()) {
                   ChangeEntry lastEntry = changes.get(changes.size() - 1);
                   lastProcessedTimestamp = lastEntry.getDiffProcessingTime();
                   lastProcessedSerialNumber = lastEntry.getSerialNumber();
                }
                
                // Persist progress to metadata manager
                metadataManager.updateProgress(
                    indexPath, 
                    lastProcessedTimestamp, 
                    lastProcessedSerialNumber, 
                    processedCount
                );
                
            } while (changes.size() == chunkSize); // If full chunk, there might be more
            // Note: Even if changes.size() < chunkSize, we break next iteration on changes.isEmpty()
            // but checking size == chunkSize is a small optimization to avoid one empty query if exactly chunk size returned.
            // However, to be safe and simple (e.g. concurrent updates), let's just loop until empty.
            
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
     * Phase 2: Cleans up old entries from the change tracking index.
     */
    private void cleanupOldEntries() {
        LOG.info("Phase 2: Cleaning up old change tracking entries");
        
        // Skip cleanup if changeTrackingWriter is not provided
        if (changeTrackingWriter == null) {
            LOG.warn("Change tracking writer not available, skipping cleanup. " +
                    "Cleanup should be performed by the change tracking populator.");
            return;
        }
        
        try {
            ChangeTrackingCleanupService cleanupService =
                new ChangeTrackingCleanupService(changeTrackingWriter, metadataManager);
            
            int deletedCount = cleanupService.cleanup();
            LOG.info("Phase 2 complete: Deleted {} old entries", deletedCount);
            
        } catch (IOException e) {
            LOG.error("Failed to cleanup old change tracking entries", e);
            // Non-fatal, continue
        }
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
            // Get mutable access to the repository to create/access the index directory
            org.apache.jackrabbit.oak.spi.state.NodeBuilder rootBuilder = nodeStore.getRoot().builder();
            
            // Get IndexDefinition from the index definition node
            org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition indexDef = 
                new org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition(
                    nodeStore.getRoot(),
                    indexDefNode,
                    indexPath
                );
            
            // Get or create the index directory for this index
            // Pass rootBuilder so we can merge changes later
            org.apache.lucene.store.Directory indexDirectory = getIndexDirectory(indexPath, indexDef, rootBuilder);
            
            // Create Lucene index writer config
            // IMPORTANT: Use the analyzer from the index definition (OakAnalyzer), not StandardAnalyzer
            // otherwise tokenization will not match query time analysis
            org.apache.lucene.analysis.Analyzer analyzer = 
                ((org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition)indexDef).getAnalyzer();
            
            org.apache.lucene.index.IndexWriterConfig writerConfig = 
                new org.apache.lucene.index.IndexWriterConfig(
                    org.apache.lucene.util.Version.LUCENE_47,
                    analyzer
                );
            
            // Configure writer for production use
            writerConfig.setOpenMode(org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            writerConfig.setRAMBufferSizeMB(32); // Reasonable buffer size
            
            org.apache.lucene.index.IndexWriter luceneWriter = 
                new org.apache.lucene.index.IndexWriter(indexDirectory, writerConfig);
            
            // Create chunked processor with production implementation
            LuceneChunkedIndexProcessor processor = new LuceneChunkedIndexProcessor(
                nodeStore,
                changeTrackingReader,
                metadataManager,
                getChunkSize()
            );
            
            LOG.info("Processing changes for index {} using LuceneChunkedIndexProcessor", indexPath);
            
            // Wrap the IndexWriter in our LuceneIndexWriter interface
            LuceneIndexWriter luceneIndexWriter = new SimpleIndexWriterWrapper(luceneWriter);
            
            // Actually process the changes using the chunked processor
            int processedCount = processor.processChangesChunk(indexPath, indexDef, luceneIndexWriter);
            
            LOG.info("Processed {} changes for index {} using LuceneChunkedIndexProcessor", 
                    processedCount, indexPath);
            
            // Commit and close writer
            luceneWriter.commit();
            luceneWriter.close();
            indexDirectory.close();
            
            // Update the index status so IndexTracker detects the change
            // This is critical for the query engine to see the updates
            NodeBuilder indexNode = getNodeBuilderAtPath(rootBuilder, indexPath);
            NodeBuilder status = indexNode.child(":status");
            status.setProperty("lastUpdated", ISO8601.format(Calendar.getInstance()), Type.DATE);
            status.setProperty("indexed", true);
            
            // CRITICAL: Merge the changes back to the NodeStore!
            // OakDirectory writes to a NodeBuilder, but those changes need to be persisted
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            LOG.info("Persisted index changes to NodeStore for {}", indexPath);
            
            return processedCount;
            
        } catch (IOException e) {
            throw new CommitFailedException(
                CommitFailedException.STATE, 5,
                "Failed to process changes for index: " + indexPath, e);
        }
    }
    
    /**
     * Gets or creates the Lucene directory for an index using Oak's directory management.
     * 
     * <p>This implementation uses {@link OakDirectory} which stores the Lucene index
     * data directly in the NodeStore (as blobs), which is Oak's default behavior.
     * This provides:
     * <ul>
     *   <li>Persistence - index data survives restarts</li>
     *   <li>Clustering - works in multi-instance deployments</li>
     *   <li>Backup - included in repository backups</li>
     * </ul>
     * 
     * <p><strong>Note:</strong> For higher performance, Oak can be configured to use
     * filesystem-based storage via {@link org.apache.jackrabbit.oak.plugins.index.lucene.directory.FSDirectoryFactory}
     * or hybrid storage via {@link org.apache.jackrabbit.oak.plugins.index.IndexCopier}.
     * This would require injecting a DirectoryFactory into this class.
     * 
     * @param indexPath the index path (e.g., "/oak:index/damAssetLucene")
     * @param indexDef the index definition
     * @param rootBuilder the root node builder (used to merge changes back to NodeStore)
     * @return the Lucene directory for this index
     * @throws IOException if directory creation fails
     */
    private org.apache.lucene.store.Directory getIndexDirectory(
            String indexPath,
            org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition indexDef,
            org.apache.jackrabbit.oak.spi.state.NodeBuilder rootBuilder) 
            throws IOException {
        
        // Navigate to the index definition node (using passed rootBuilder)
        NodeBuilder indexDefBuilder = getNodeBuilderAtPath(rootBuilder, indexPath);
        
        // Ensure the :data child node exists (this is where Lucene data is stored)
        // IMPORTANT: Must use :data (INDEX_DATA_CHILD_NAME) for LuceneIndexProvider to find it
        if (!indexDefBuilder.hasChildNode(FulltextIndexConstants.INDEX_DATA_CHILD_NAME)) {
            LOG.info("Creating :data node for {} (first time indexing)", indexPath);
            indexDefBuilder.child(FulltextIndexConstants.INDEX_DATA_CHILD_NAME);
        }
        
        // Create OakDirectory for persistent storage in NodeStore
        // Uses the same pattern as LuceneIndexEditor
        LuceneIndexDefinition luceneDef = (LuceneIndexDefinition) indexDef;
        OakDirectory directory = new OakDirectory(
            indexDefBuilder,
            FulltextIndexConstants.INDEX_DATA_CHILD_NAME,
            luceneDef,
            false  // readOnly = false (we need to write)
        );
        
        LOG.debug("Created OakDirectory for index {} (persistent storage in NodeStore)", indexPath);
        return directory;
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
     * Gets a NodeBuilder at a given path, creating intermediate nodes if needed.
     * 
     * @param root the root NodeBuilder
     * @param path the path to traverse
     * @return the NodeBuilder at that path
     */
    private NodeBuilder getNodeBuilderAtPath(NodeBuilder root, String path) {
        NodeBuilder current = root;
        
        if (path.equals("/")) {
            return current;
        }
        
        String[] segments = path.split("/");
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current = current.child(segment);
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
     * Gets the chunk size for processing.
     */
    private int getChunkSize() {
        return chunkSize;
    }
    
    /**
     * Gets statistics about this async index update.
     */
    public Stats getStats() {
        return new Stats(
            totalChangesProcessed,
            lastRunTimestamp,
            metadataManager.getRegisteredIndexes().size()
        );
    }
    
    /**
     * Statistics about change tracking async index update.
     */
    public static class Stats {
        public final long totalChangesProcessed;
        public final long lastRunTimestamp;
        public final int registeredIndexCount;
        
        public Stats(long totalChangesProcessed,
                     long lastRunTimestamp, int registeredIndexCount) {
            this.totalChangesProcessed = totalChangesProcessed;
            this.lastRunTimestamp = lastRunTimestamp;
            this.registeredIndexCount = registeredIndexCount;
        }
        
        @Override
        public String toString() {
            return "Stats{" +
                    "processed=" + totalChangesProcessed +
                    ", lastRun=" + lastRunTimestamp +
                    ", indexes=" + registeredIndexCount +
                    '}';
        }
    }
}

