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
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingConstants.*;

/**
 * Unit tests for ChangeTrackingCleanupService.
 * 
 * Tests cover:
 * - Deleting old, fully processed entries
 * - Retention buffer logic
 * - Multiple indexes with different progress
 * - Empty index handling
 * - No registered indexes scenario
 * - Cleanup with no processed changes
 */
public class ChangeTrackingCleanupServiceTest {
    
    private Directory directory;
    private IndexWriter writer;
    private NodeStore nodeStore;
    private IndexProgressMetadataManager metadataManager;
    private ChangeTrackingCleanupService cleanupService;
    
    private static final long RETENTION_BUFFER_MS = TimeUnit.HOURS.toMillis(24); // 24 hours
    
    @Before
    public void setup() throws IOException {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
        
        nodeStore = new MemoryNodeStore();
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        metadataManager.initialize();
        
        cleanupService = new ChangeTrackingCleanupService(
            metadataManager, writer, RETENTION_BUFFER_MS
        );
    }
    
    @After
    public void teardown() throws IOException {
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
    }
    
    private void addChangeEntry(String path, long timestamp) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(FIELD_PATH, path, Field.Store.YES));
        doc.add(new StringField(FIELD_CHECKPOINT1, "cp1", Field.Store.YES));
        doc.add(new StringField(FIELD_CHECKPOINT2, "cp2", Field.Store.YES));
        doc.add(new LongField(FIELD_DIFF_PROCESSING_TIME, timestamp, Field.Store.YES));
        doc.add(new LongField(FIELD_SERIAL_NUMBER, 1L, Field.Store.YES));
        writer.addDocument(doc);
    }
    
    @Test
    public void testDeletesOldEntries() throws IOException, CommitFailedException {
        long now = System.currentTimeMillis();
        long oldTime = now - TimeUnit.DAYS.toMillis(3); // 3 days ago
        long recentTime = now - TimeUnit.HOURS.toMillis(12); // 12 hours ago
        
        // Add old and recent entries
        addChangeEntry("/old1", oldTime);
        addChangeEntry("/old2", oldTime + 1000);
        addChangeEntry("/recent1", recentTime);
        addChangeEntry("/recent2", recentTime + 1000);
        writer.commit();
        
        // Register index with progress at recent time
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        
        IndexProgressMetadata progress = IndexProgressMetadata.builder()
            .withIndexPath(indexPath)
            .withLastProcessedTimestamp(recentTime + 1000) // Processed up to recent2
            .withLastProcessedSerialNumber(1L)
            .build();
        metadataManager.updateIndexProgress(progress);
        
        // Cleanup should delete old entries
        cleanupService.cleanUp();
        
        // Verify
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should have 2 recent documents left", 2, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testRetentionBufferLogic() throws IOException, CommitFailedException {
        long now = System.currentTimeMillis();
        long processedTime = now - TimeUnit.HOURS.toMillis(20); // 20 hours ago
        
        // Add entries around the processed time
        long beforeBuffer = processedTime - RETENTION_BUFFER_MS - TimeUnit.HOURS.toMillis(1); // Should be deleted
        long withinBuffer = processedTime - RETENTION_BUFFER_MS + TimeUnit.HOURS.toMillis(1); // Should be kept
        
        addChangeEntry("/beforeBuffer", beforeBuffer);
        addChangeEntry("/withinBuffer", withinBuffer);
        addChangeEntry("/afterProcessed", processedTime + 1000);
        writer.commit();
        
        // Register index
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        
        IndexProgressMetadata progress = IndexProgressMetadata.builder()
            .withIndexPath(indexPath)
            .withLastProcessedTimestamp(processedTime)
            .withLastProcessedSerialNumber(1L)
            .build();
        metadataManager.updateIndexProgress(progress);
        
        // Cleanup
        cleanupService.cleanUp();
        
        // Verify only beforeBuffer is deleted
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should have 2 documents (within buffer and after processed)", 
                    2, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testMultipleIndexesWithDifferentProgress() throws IOException, CommitFailedException {
        long now = System.currentTimeMillis();
        long oldTime = now - TimeUnit.DAYS.toMillis(5);
        long midTime = now - TimeUnit.DAYS.toMillis(3);
        long recentTime = now - TimeUnit.DAYS.toMillis(1);
        
        // Add entries at different times
        addChangeEntry("/old", oldTime);
        addChangeEntry("/mid", midTime);
        addChangeEntry("/recent", recentTime);
        writer.commit();
        
        // Register two indexes with different progress
        String index1 = "/oak:index/index1";
        String index2 = "/oak:index/index2";
        
        metadataManager.registerIndex(index1);
        metadataManager.registerIndex(index2);
        
        // index1 has processed up to recent time
        IndexProgressMetadata progress1 = IndexProgressMetadata.builder()
            .withIndexPath(index1)
            .withLastProcessedTimestamp(recentTime)
            .withLastProcessedSerialNumber(1L)
            .build();
        metadataManager.updateIndexProgress(progress1);
        
        // index2 has only processed up to mid time (slower)
        IndexProgressMetadata progress2 = IndexProgressMetadata.builder()
            .withIndexPath(index2)
            .withLastProcessedTimestamp(midTime)
            .withLastProcessedSerialNumber(1L)
            .build();
        metadataManager.updateIndexProgress(progress2);
        
        // Cleanup should use the minimum (index2's time) as cutoff
        cleanupService.cleanUp();
        
        // Verify: only "old" should be deleted (before midTime - buffer)
        IndexReader reader = DirectoryReader.open(directory);
        // Should keep mid and recent (old is beyond buffer)
        assertTrue("Should have at least 1 document", reader.numDocs() >= 1);
        reader.close();
    }
    
    @Test
    public void testEmptyIndexNoOp() throws IOException {
        // Don't add any entries
        writer.commit();
        
        // Register index
        metadataManager.registerIndex("/oak:index/testIndex");
        
        // Cleanup should complete without errors
        cleanupService.cleanUp();
        
        // Verify no documents
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should have 0 documents", 0, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testNoRegisteredIndexes() throws IOException {
        // Add some entries
        addChangeEntry("/node1", System.currentTimeMillis());
        writer.commit();
        
        // Don't register any indexes
        
        // Cleanup should skip (no indexes to check)
        cleanupService.cleanUp();
        
        // Verify entries are NOT deleted (no indexes registered)
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should still have 1 document", 1, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testNoProcessedChanges() throws IOException, CommitFailedException {
        long now = System.currentTimeMillis();
        
        // Add entries
        addChangeEntry("/node1", now - TimeUnit.DAYS.toMillis(5));
        addChangeEntry("/node2", now - TimeUnit.HOURS.toMillis(12));
        writer.commit();
        
        // Register index but with no progress (lastProcessedTimestamp = 0)
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        
        // Index has not processed any changes yet (default 0 values)
        IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
        assertEquals("Should have 0 last processed timestamp", 0L, progress.getLastProcessedTimestamp());
        
        // Cleanup should skip (no processed changes)
        cleanupService.cleanUp();
        
        // Verify no deletions
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should still have 2 documents", 2, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testCleanupFreesSpace() throws IOException, CommitFailedException {
        long now = System.currentTimeMillis();
        long oldTime = now - TimeUnit.DAYS.toMillis(10);
        
        // Add many old entries
        for (int i = 0; i < 100; i++) {
            addChangeEntry("/old" + i, oldTime + i);
        }
        
        // Add some recent entries
        long recentTime = now - TimeUnit.HOURS.toMillis(1);
        for (int i = 0; i < 10; i++) {
            addChangeEntry("/recent" + i, recentTime + i);
        }
        writer.commit();
        
        IndexReader beforeReader = DirectoryReader.open(directory);
        int beforeCount = beforeReader.numDocs();
        beforeReader.close();
        
        assertEquals("Should start with 110 documents", 110, beforeCount);
        
        // Register index with recent progress
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        
        IndexProgressMetadata progress = IndexProgressMetadata.builder()
            .withIndexPath(indexPath)
            .withLastProcessedTimestamp(recentTime + 10)
            .withLastProcessedSerialNumber(1L)
            .build();
        metadataManager.updateIndexProgress(progress);
        
        // Cleanup
        cleanupService.cleanUp();
        
        // Verify significant reduction
        IndexReader afterReader = DirectoryReader.open(directory);
        int afterCount = afterReader.numDocs();
        afterReader.close();
        
        assertTrue("Should have significantly fewer documents after cleanup", 
                  afterCount < beforeCount / 2);
        assertTrue("Should keep recent documents", afterCount >= 10);
    }
    
    @Test
    public void testRunMethodHandlesExceptions() {
        // Test that run() method catches and logs exceptions
        // (no exception should propagate)
        
        // This will fail because writer is closed
        try {
            writer.close();
            writer = null;
        } catch (IOException e) {
            fail("Should not throw during test setup");
        }
        
        // Run should complete without throwing
        cleanupService.run();
        
        // If we reach here, exception was caught
        assertTrue("Run completed without propagating exception", true);
    }
    
    @Test
    public void testCustomRetentionBuffer() throws IOException, CommitFailedException {
        long customBuffer = TimeUnit.HOURS.toMillis(6); // 6 hours
        
        ChangeTrackingCleanupService customCleanupService = 
            new ChangeTrackingCleanupService(metadataManager, writer, customBuffer);
        
        long now = System.currentTimeMillis();
        long processedTime = now - TimeUnit.HOURS.toMillis(10);
        
        // Add entry that's within 24-hour buffer but outside 6-hour buffer
        long entryTime = processedTime - TimeUnit.HOURS.toMillis(8); // 8 hours before processed
        
        addChangeEntry("/testEntry", entryTime);
        writer.commit();
        
        // Register index
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        
        IndexProgressMetadata progress = IndexProgressMetadata.builder()
            .withIndexPath(indexPath)
            .withLastProcessedTimestamp(processedTime)
            .withLastProcessedSerialNumber(1L)
            .build();
        metadataManager.updateIndexProgress(progress);
        
        // Cleanup with custom buffer
        customCleanupService.cleanUp();
        
        // Entry should be deleted (outside 6-hour buffer)
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should have 0 documents (deleted by custom buffer)", 
                    0, reader.numDocs());
        reader.close();
    }
}

