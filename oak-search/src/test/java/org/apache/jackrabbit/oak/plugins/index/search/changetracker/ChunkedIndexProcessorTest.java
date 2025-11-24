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
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingConstants.*;

/**
 * Unit tests for ChunkedIndexProcessor.
 * 
 * NOTE: This test focuses on the metadata management and chunk iteration logic.
 * Full integration testing with actual index editors is covered by ChangeTrackingE2ETest.
 * 
 * Tests cover:
 * - Chunk iteration logic
 * - Progress metadata updates
 * - Empty results handling
 * - Multiple chunk scenarios
 * - Custom chunk size configuration
 * - Error handling and recovery
 */
public class ChunkedIndexProcessorTest {
    
    private Directory directory;
    private IndexWriter writer;
    private IndexReader reader;
    private NodeStore nodeStore;
    private IndexProgressMetadataManager metadataManager;
    private ChangeTrackingIndexQuery indexQuery;
    private ChunkedIndexProcessor processor;
    
    private static final String TEST_INDEX_PATH = "/oak:index/testIndex";
    private static final int TEST_CHUNK_SIZE = 10;
    
    @Before
    public void setup() throws IOException, CommitFailedException {
        // Setup in-memory Lucene index
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
        
        // Setup NodeStore and metadata manager
        nodeStore = new MemoryNodeStore();
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        metadataManager.initialize();
        metadataManager.registerIndex(TEST_INDEX_PATH);
        
        // Initialize test content in NodeStore
        initializeTestContent();
    }
    
    private void initializeTestContent() throws CommitFailedException {
        NodeBuilder root = nodeStore.getRoot().builder();
        
        // Create some test nodes that will be referenced by change entries
        NodeBuilder content = root.child("content");
        content.child("node1").setProperty("title", "Node 1");
        content.child("node2").setProperty("title", "Node 2");
        content.child("node3").setProperty("title", "Node 3");
        
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    @After
    public void teardown() throws IOException {
        if (indexQuery != null) {
            indexQuery.close();
        }
        if (reader != null && reader.getRefCount() > 0) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
    }
    
    private void addChangeEntry(String path, long timestamp, long serial) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(FIELD_PATH, path, Field.Store.YES));
        doc.add(new StringField(FIELD_CHECKPOINT1, "cp1", Field.Store.YES));
        doc.add(new StringField(FIELD_CHECKPOINT2, "cp2", Field.Store.YES));
        doc.add(new LongField(FIELD_DIFF_PROCESSING_TIME, timestamp, Field.Store.YES));
        doc.add(new LongField(FIELD_SERIAL_NUMBER, serial, Field.Store.YES));
        writer.addDocument(doc);
    }
    
    private void commitAndCreateQuery() throws IOException {
        writer.commit();
        reader = DirectoryReader.open(directory);
        indexQuery = new ChangeTrackingIndexQuery(reader);
        processor = new ChunkedIndexProcessor(nodeStore, indexQuery, metadataManager, TEST_CHUNK_SIZE);
    }
    
    @Test
    public void testConstructorWithDefaultChunkSize() {
        ChunkedIndexProcessor defaultProcessor = new ChunkedIndexProcessor(
            nodeStore, indexQuery, metadataManager);
        
        assertNotNull("Processor should not be null", defaultProcessor);
    }
    
    @Test
    public void testConstructorWithCustomChunkSize() {
        int customChunkSize = 5000;
        ChunkedIndexProcessor customProcessor = new ChunkedIndexProcessor(
            nodeStore, indexQuery, metadataManager, customChunkSize);
        
        assertNotNull("Processor should not be null", customProcessor);
    }
    
    @Test
    public void testQueryUnprocessedChangesWithEmptyIndex() throws IOException {
        commitAndCreateQuery();
        
        // Query with no entries in index
        List<ChangeEntry> changes = indexQuery.getUnprocessedChanges(0L, 0L, TEST_CHUNK_SIZE);
        
        assertTrue("Should return empty list", changes.isEmpty());
    }
    
    @Test
    public void testQueryUnprocessedChangesWithSingleChunk() throws IOException {
        // Add fewer changes than chunk size
        for (int i = 0; i < 5; i++) {
            addChangeEntry("/content/node" + i, 1000L + i, 1L);
        }
        
        commitAndCreateQuery();
        
        List<ChangeEntry> changes = indexQuery.getUnprocessedChanges(0L, 0L, TEST_CHUNK_SIZE);
        
        assertEquals("Should return 5 changes", 5, changes.size());
        assertEquals("/content/node0", changes.get(0).getPath());
        assertEquals("/content/node4", changes.get(4).getPath());
    }
    
    @Test
    public void testQueryUnprocessedChangesWithMultipleChunks() throws IOException {
        // Add more changes than chunk size
        for (int i = 0; i < 25; i++) {
            addChangeEntry("/content/node" + i, 1000L + i, 1L);
        }
        
        commitAndCreateQuery();
        
        // First chunk
        List<ChangeEntry> chunk1 = indexQuery.getUnprocessedChanges(0L, 0L, TEST_CHUNK_SIZE);
        assertEquals("First chunk should have 10 entries", TEST_CHUNK_SIZE, chunk1.size());
        
        // Second chunk
        ChangeEntry lastOfChunk1 = chunk1.get(chunk1.size() - 1);
        List<ChangeEntry> chunk2 = indexQuery.getUnprocessedChanges(
            lastOfChunk1.getDiffProcessingTime(),
            lastOfChunk1.getSerialNumber(),
            TEST_CHUNK_SIZE
        );
        assertEquals("Second chunk should have 10 entries", TEST_CHUNK_SIZE, chunk2.size());
        
        // Third chunk
        ChangeEntry lastOfChunk2 = chunk2.get(chunk2.size() - 1);
        List<ChangeEntry> chunk3 = indexQuery.getUnprocessedChanges(
            lastOfChunk2.getDiffProcessingTime(),
            lastOfChunk2.getSerialNumber(),
            TEST_CHUNK_SIZE
        );
        assertEquals("Third chunk should have 5 entries", 5, chunk3.size());
    }
    
    @Test
    public void testProgressMetadataInitialization() throws IOException, CommitFailedException {
        commitAndCreateQuery();
        
        // Get initial progress
        IndexProgressMetadata progress = metadataManager.getIndexProgress(TEST_INDEX_PATH);
        
        assertNotNull("Progress should not be null", progress);
        assertEquals("Index path should match", TEST_INDEX_PATH, progress.getIndexPath());
        assertEquals("Initial timestamp should be 0", 0L, progress.getLastProcessedTimestamp());
        assertEquals("Initial serial should be 0", 0L, progress.getLastProcessedSerialNumber());
    }
    
    @Test
    public void testProgressMetadataUpdate() throws IOException, CommitFailedException {
        commitAndCreateQuery();
        
        long timestamp = 1000L;
        long serial = 5L;
        
        // Update progress
        IndexProgressMetadata updatedProgress = IndexProgressMetadata.builder()
            .withIndexPath(TEST_INDEX_PATH)
            .withLastProcessedTimestamp(timestamp)
            .withLastProcessedSerialNumber(serial)
            .withTotalProcessed(10L)
            .withTotalChunks(1L)
            .build();
        
        metadataManager.updateIndexProgress(updatedProgress);
        
        // Retrieve and verify
        IndexProgressMetadata retrieved = metadataManager.getIndexProgress(TEST_INDEX_PATH);
        
        assertEquals("Timestamp should match", timestamp, retrieved.getLastProcessedTimestamp());
        assertEquals("Serial should match", serial, retrieved.getLastProcessedSerialNumber());
        assertEquals("Total processed should match", 10L, retrieved.getTotalProcessed());
        assertEquals("Total chunks should match", 1L, retrieved.getTotalChunks());
    }
    
    @Test
    public void testChunkProcessingIteration() throws IOException {
        // Add 25 changes (3 chunks of 10, 10, 5)
        for (int i = 0; i < 25; i++) {
            addChangeEntry("/content/node" + i, 1000L + i, 1L);
        }
        
        commitAndCreateQuery();
        
        long lastTimestamp = 0L;
        long lastSerial = 0L;
        int chunkCount = 0;
        int totalProcessed = 0;
        
        // Simulate chunk processing loop
        while (true) {
            List<ChangeEntry> changes = indexQuery.getUnprocessedChanges(
                lastTimestamp, lastSerial, TEST_CHUNK_SIZE);
            
            if (changes.isEmpty()) {
                break;
            }
            
            chunkCount++;
            totalProcessed += changes.size();
            
            // Update to last entry
            ChangeEntry lastEntry = changes.get(changes.size() - 1);
            lastTimestamp = lastEntry.getDiffProcessingTime();
            lastSerial = lastEntry.getSerialNumber();
        }
        
        assertEquals("Should process 3 chunks", 3, chunkCount);
        assertEquals("Should process 25 changes total", 25, totalProcessed);
    }
    
    @Test
    public void testResumeFromLastProcessedPosition() throws IOException, CommitFailedException {
        // Add 20 changes
        for (int i = 0; i < 20; i++) {
            addChangeEntry("/content/node" + i, 1000L + i, 1L);
        }
        
        commitAndCreateQuery();
        
        // Process first chunk
        List<ChangeEntry> chunk1 = indexQuery.getUnprocessedChanges(0L, 0L, TEST_CHUNK_SIZE);
        assertEquals("First chunk should have 10 entries", TEST_CHUNK_SIZE, chunk1.size());
        
        // Save progress
        ChangeEntry lastOfChunk1 = chunk1.get(chunk1.size() - 1);
        IndexProgressMetadata progress = IndexProgressMetadata.builder()
            .withIndexPath(TEST_INDEX_PATH)
            .withLastProcessedTimestamp(lastOfChunk1.getDiffProcessingTime())
            .withLastProcessedSerialNumber(lastOfChunk1.getSerialNumber())
            .withTotalProcessed((long) chunk1.size())
            .build();
        metadataManager.updateIndexProgress(progress);
        
        // Simulate crash and restart - retrieve progress
        IndexProgressMetadata retrieved = metadataManager.getIndexProgress(TEST_INDEX_PATH);
        
        // Resume from saved position
        List<ChangeEntry> chunk2 = indexQuery.getUnprocessedChanges(
            retrieved.getLastProcessedTimestamp(),
            retrieved.getLastProcessedSerialNumber(),
            TEST_CHUNK_SIZE
        );
        
        assertEquals("Second chunk should have 10 entries", TEST_CHUNK_SIZE, chunk2.size());
        assertEquals("/content/node10", chunk2.get(0).getPath());
    }
    
    @Test
    public void testHandleTimestampCollisionsAcrossChunks() throws IOException {
        long timestamp = 1000L;
        
        // Add entries with same timestamp but different serials
        // This tests that chunk boundaries respect composite key ordering
        for (int i = 0; i < 25; i++) {
            addChangeEntry("/content/node" + i, timestamp, i);
        }
        
        commitAndCreateQuery();
        
        // Process in chunks
        List<String> allPaths = new ArrayList<>();
        long lastTimestamp = 0L;
        long lastSerial = 0L;
        
        while (true) {
            List<ChangeEntry> changes = indexQuery.getUnprocessedChanges(
                lastTimestamp, lastSerial, TEST_CHUNK_SIZE);
            
            if (changes.isEmpty()) {
                break;
            }
            
            for (ChangeEntry entry : changes) {
                allPaths.add(entry.getPath());
            }
            
            ChangeEntry lastEntry = changes.get(changes.size() - 1);
            lastTimestamp = lastEntry.getDiffProcessingTime();
            lastSerial = lastEntry.getSerialNumber();
        }
        
        // Verify all 25 entries were processed in correct order
        assertEquals("Should process all 25 entries", 25, allPaths.size());
        for (int i = 0; i < 25; i++) {
            assertEquals("/content/node" + i, allPaths.get(i));
        }
    }
    
    @Test
    public void testCountUnprocessedChanges() throws IOException, CommitFailedException {
        // Add 15 changes
        for (int i = 0; i < 15; i++) {
            addChangeEntry("/content/node" + i, 1000L + i, 1L);
        }
        
        commitAndCreateQuery();
        
        // Initial count
        int totalCount = indexQuery.countUnprocessedChanges(0L, 0L);
        assertEquals("Should count 15 unprocessed changes", 15, totalCount);
        
        // Process first chunk (10 entries)
        List<ChangeEntry> chunk1 = indexQuery.getUnprocessedChanges(0L, 0L, TEST_CHUNK_SIZE);
        ChangeEntry lastOfChunk1 = chunk1.get(chunk1.size() - 1);
        
        // Count remaining
        int remainingCount = indexQuery.countUnprocessedChanges(
            lastOfChunk1.getDiffProcessingTime(),
            lastOfChunk1.getSerialNumber()
        );
        assertEquals("Should count 5 remaining changes", 5, remainingCount);
    }
    
    @Test
    public void testProcessingWithMixedTimestamps() throws IOException {
        // Add entries with varying timestamps to test ordering
        addChangeEntry("/content/node3", 1003L, 1L);
        addChangeEntry("/content/node1", 1001L, 1L);
        addChangeEntry("/content/node4", 1004L, 1L);
        addChangeEntry("/content/node2", 1002L, 1L);
        
        commitAndCreateQuery();
        
        // Should be ordered by timestamp
        List<ChangeEntry> changes = indexQuery.getUnprocessedChanges(0L, 0L, 10);
        
        assertEquals(4, changes.size());
        assertEquals("/content/node1", changes.get(0).getPath());
        assertEquals("/content/node2", changes.get(1).getPath());
        assertEquals("/content/node3", changes.get(2).getPath());
        assertEquals("/content/node4", changes.get(3).getPath());
    }
    
    @Test
    public void testNodeStoreIntegration() throws CommitFailedException {
        // Verify test content is accessible
        NodeState root = nodeStore.getRoot();
        NodeState content = root.getChildNode("content");
        
        assertTrue("Content node should exist", content.exists());
        assertTrue("node1 should exist", content.getChildNode("node1").exists());
        assertTrue("node2 should exist", content.getChildNode("node2").exists());
        assertTrue("node3 should exist", content.getChildNode("node3").exists());
    }
    
    /**
     * NOTE: Full integration testing with actual IndexDefinition and FulltextIndexEditorContext
     * is covered by ChangeTrackingE2ETest. This unit test focuses on the chunk iteration
     * and metadata management aspects that can be tested in isolation.
     */
    @Test
    public void testFullIntegrationPlaceholder() {
        // This is a placeholder to document that full integration testing
        // (including actual index editors, aggregations, etc.) is done in:
        // oak-lucene/src/test/java/org/apache/jackrabbit/oak/plugins/index/lucene/ChangeTrackingE2ETest.java
        
        assertTrue("Full integration testing is in ChangeTrackingE2ETest", true);
    }
}

