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

import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Service that cleans up old change tracking entries.
 * 
 * <p>Once all registered indexes have processed a change entry, it can be
 * safely deleted from the change tracking index to prevent unbounded growth.
 * 
 * <p>This service:
 * <ol>
 *   <li>Queries all registered indexes to find their lastProcessedTimestamp</li>
 *   <li>Finds the minimum timestamp across all indexes</li>
 *   <li>Deletes all change entries older than (minimum - retention buffer)</li>
 * </ol>
 * 
 * <p>The retention buffer ensures we don't delete entries that might still
 * be needed if an index is temporarily behind.
 */
public class ChangeTrackingCleanupService {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingCleanupService.class);
    
    private static final String FIELD_DIFF_PROCESSING_TIME = "ct:diffProcessingTime";
    
    // Default: Keep entries for at least 24 hours after all indexes process them
    private static final long DEFAULT_RETENTION_BUFFER_MS = TimeUnit.HOURS.toMillis(24);
    
    // System property to configure retention buffer
    private static final String RETENTION_BUFFER_PROPERTY = "oak.changeTracker.retentionBufferHours";
    
    private final IndexWriter changeTrackingIndexWriter;
    private final IndexProgressMetadataManager metadataManager;
    private final long retentionBufferMs;
    
    /**
     * Creates a cleanup service with default retention buffer.
     * 
     * @param changeTrackingIndexWriter the index writer for the change tracking index
     * @param metadataManager the metadata manager
     */
    public ChangeTrackingCleanupService(@NotNull IndexWriter changeTrackingIndexWriter,
                                         @NotNull IndexProgressMetadataManager metadataManager) {
        this(changeTrackingIndexWriter, metadataManager,
             TimeUnit.HOURS.toMillis(
                 Integer.getInteger(RETENTION_BUFFER_PROPERTY, 24)));
    }
    
    /**
     * Creates a cleanup service with custom retention buffer.
     * 
     * @param changeTrackingIndexWriter the index writer
     * @param metadataManager the metadata manager
     * @param retentionBufferMs the retention buffer in milliseconds
     */
    public ChangeTrackingCleanupService(@NotNull IndexWriter changeTrackingIndexWriter,
                                         @NotNull IndexProgressMetadataManager metadataManager,
                                         long retentionBufferMs) {
        this.changeTrackingIndexWriter = changeTrackingIndexWriter;
        this.metadataManager = metadataManager;
        this.retentionBufferMs = retentionBufferMs;
        LOG.info("ChangeTrackingCleanupService initialized with retention buffer: {} hours",
                TimeUnit.MILLISECONDS.toHours(retentionBufferMs));
    }
    
    /**
     * Runs the cleanup process.
     * 
     * <p>This addresses LIMITATION 8.1 - Cleanup Integration Issues by using
     * coordinated safe deletion timestamp across all registered indexes.
     * 
     * @return the number of entries deleted
     * @throws IOException if cleanup fails
     */
    public int cleanup() throws IOException {
        LOG.info("Starting change tracking cleanup...");
        
        // Get all registered indexes
        List<String> indexes = metadataManager.getRegisteredIndexes();
        if (indexes.isEmpty()) {
            LOG.info("No registered indexes, skipping cleanup");
            return 0;
        }
        
        LOG.info("Found {} registered indexes for cleanup coordination", indexes.size());
        
        // Get safe deletion timestamp using coordinated approach (LIMITATION 8.1)
        // This ensures no index is still processing entries we're about to delete
        long safeDeletionTimestamp = metadataManager.getSafeDeletionTimestamp(retentionBufferMs);
        
        if (safeDeletionTimestamp == 0) {
            LOG.info("No indexes have processed any changes yet, or safe deletion timestamp is invalid - skipping cleanup");
            return 0;
        }
        
        LOG.info("Cleaning up entries older than safe deletion timestamp: {} (retention buffer: {}ms)",
                safeDeletionTimestamp, retentionBufferMs);
        
        // Log per-index status for debugging
        if (LOG.isDebugEnabled()) {
            logIndexStatus(indexes);
        }
        
        // Delete entries older than safe deletion timestamp
        int deleted = deleteEntriesOlderThan(safeDeletionTimestamp);
        
        LOG.info("Cleanup complete: deleted {} entries (safe deletion timestamp: {})",
                deleted, safeDeletionTimestamp);
        return deleted;
    }
    
    /**
     * Logs the status of each registered index for debugging.
     * 
     * @param indexes the list of registered indexes
     */
    private void logIndexStatus(List<String> indexes) {
        LOG.debug("Index status for cleanup coordination:");
        for (String indexPath : indexes) {
            IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
            if (progress != null) {
                LOG.debug("  Index: {}, LastProcessedTimestamp: {}, ProcessedChanges: {}",
                        indexPath,
                        progress.getLastProcessedTimestamp(),
                        progress.hasProcessedChanges());
            } else {
                LOG.debug("  Index: {}, No progress metadata found", indexPath);
            }
        }
    }
    
    /**
     * Finds the minimum lastProcessedTimestamp across all registered indexes.
     * 
     * @param indexes the list of registered index paths
     * @return the minimum timestamp, or 0 if no indexes have processed anything
     */
    private long findMinimumProcessedTimestamp(List<String> indexes) {
        long minTimestamp = Long.MAX_VALUE;
        boolean foundAny = false;
        
        for (String indexPath : indexes) {
            IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
            if (progress != null && progress.hasProcessedChanges()) {
                long timestamp = progress.getLastProcessedTimestamp();
                if (timestamp > 0) {
                    minTimestamp = Math.min(minTimestamp, timestamp);
                    foundAny = true;
                    LOG.debug("Index {} has processed up to timestamp: {}", indexPath, timestamp);
                }
            }
        }
        
        return foundAny ? minTimestamp : 0;
    }
    
    /**
     * Deletes all change entries with diffProcessingTime < cutoffTimestamp.
     * 
     * @param cutoffTimestamp the cutoff timestamp (exclusive)
     * @return the number of entries deleted
     * @throws IOException if deletion fails
     */
    private int deleteEntriesOlderThan(long cutoffTimestamp) throws IOException {
        // Create query: ct:diffProcessingTime < cutoffTimestamp
        // Lucene 4.7: Use NumericRangeQuery instead of LongPoint
        Query query = NumericRangeQuery.newLongRange(
            FIELD_DIFF_PROCESSING_TIME,
            Long.MIN_VALUE,      // min
            cutoffTimestamp - 1, // max (exclusive)
            true,                // minInclusive
            true                 // maxInclusive
        );
        
        // Get document count before deletion for reporting
        int docCountBefore = changeTrackingIndexWriter.maxDoc();
        
        // Delete documents matching the query (returns void in Lucene 4.7)
        changeTrackingIndexWriter.deleteDocuments(query);
        
        // Commit the deletion
        changeTrackingIndexWriter.commit();
        
        // Approximate count of deleted documents
        int docCountAfter = changeTrackingIndexWriter.maxDoc();
        int deleted = docCountBefore - docCountAfter;
        
        return deleted;
    }
    
    /**
     * Gets the current retention buffer in milliseconds.
     * 
     * @return the retention buffer
     */
    public long getRetentionBufferMs() {
        return retentionBufferMs;
    }
}

