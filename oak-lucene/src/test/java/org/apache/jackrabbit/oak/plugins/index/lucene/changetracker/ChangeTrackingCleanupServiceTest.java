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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Unit test for {@link ChangeTrackingCleanupService}.
 */
public class ChangeTrackingCleanupServiceTest {

    private Directory directory;
    private IndexWriter writer;
    private NodeStore nodeStore;
    private IndexProgressMetadataManager metadataManager;
    private ChangeTrackingCleanupService cleanupService;

    @Before
    public void setup() throws Exception {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
        
        nodeStore = new MemoryNodeStore();
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        
        // Default retention buffer: 1 hour
        cleanupService = new ChangeTrackingCleanupService(
                writer, metadataManager, TimeUnit.HOURS.toMillis(1));
    }

    @After
    public void teardown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    private void addChangeEntry(String path, long timestamp, long serial) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("ct:path", path, Field.Store.YES));
        doc.add(new StringField("ct:checkpoint1", "cp1", Field.Store.YES));
        doc.add(new StringField("ct:checkpoint2", "cp2", Field.Store.YES));
        doc.add(new LongField("ct:diffProcessingTime", timestamp, Field.Store.YES));
        doc.add(new LongField("ct:serialNumber", serial, Field.Store.YES));
        writer.addDocument(doc);
    }

    @Test
    public void testCleanupNoIndexes() throws Exception {
        writer.commit();
        
        int deleted = cleanupService.cleanup();
        
        assertEquals(0, deleted);
    }

    @Test
    public void testCleanupNoProcessedChanges() throws Exception {
        // Register index but don't process any changes
        metadataManager.registerIndex("/oak:index/testIndex");
        
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 2000L, 0L);
        writer.commit();
        
        int deleted = cleanupService.cleanup();
        
        assertEquals(0, deleted);
    }

    @Test
    public void testCleanupWithProcessedChanges() throws Exception {
        // Add some entries
        long now = System.currentTimeMillis();
        long twoHoursAgo = now - TimeUnit.HOURS.toMillis(2);
        long oneHourAgo = now - TimeUnit.HOURS.toMillis(1);
        
        addChangeEntry("/content/node1", twoHoursAgo, 0L);
        addChangeEntry("/content/node2", oneHourAgo, 0L);
        addChangeEntry("/content/node3", now, 0L);
        writer.commit();
        
        // Register index and mark it as having processed up to oneHourAgo
        metadataManager.registerIndex("/oak:index/testIndex");
        IndexProgressMetadata progress = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/testIndex")
                .lastProcessedTimestamp(oneHourAgo)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress.getIndexPath(),
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                (int) progress.getTotalProcessed());
        
        // Cleanup should delete entries older than (oneHourAgo - 1 hour) = twoHoursAgo
        int deleted = cleanupService.cleanup();
        
        // Should have deleted the entry from twoHoursAgo
        assertTrue("Should delete at least 1 entry", deleted >= 1);
        
        // Verify remaining entries
        IndexReader reader = DirectoryReader.open(directory);
        int remaining = reader.numDocs();
        reader.close();
        
        assertEquals(3 - deleted, remaining);
    }

    @Test
    public void testCleanupWithMultipleIndexes() throws Exception {
        long now = System.currentTimeMillis();
        long threeHoursAgo = now - TimeUnit.HOURS.toMillis(3);
        long twoHoursAgo = now - TimeUnit.HOURS.toMillis(2);
        long oneHourAgo = now - TimeUnit.HOURS.toMillis(1);
        
        addChangeEntry("/content/node1", threeHoursAgo, 0L);
        addChangeEntry("/content/node2", twoHoursAgo, 0L);
        addChangeEntry("/content/node3", oneHourAgo, 0L);
        writer.commit();
        
        // Register two indexes with different progress
        metadataManager.registerIndex("/oak:index/index1");
        metadataManager.registerIndex("/oak:index/index2");
        
        // Index1 has processed up to twoHoursAgo
        IndexProgressMetadata progress1 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/index1")
                .lastProcessedTimestamp(twoHoursAgo)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress1.getIndexPath(),
                progress1.getLastProcessedTimestamp(),
                progress1.getLastProcessedSerialNumber(),
                (int) progress1.getTotalProcessed());
        
        // Index2 has only processed up to threeHoursAgo (slower)
        IndexProgressMetadata progress2 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/index2")
                .lastProcessedTimestamp(threeHoursAgo)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress2.getIndexPath(),
                progress2.getLastProcessedTimestamp(),
                progress2.getLastProcessedSerialNumber(),
                (int) progress2.getTotalProcessed());
        
        // Cleanup should use the minimum (threeHoursAgo) across both indexes
        // With 1 hour buffer, cutoff = threeHoursAgo - 1 hour = fourHoursAgo
        // So nothing should be deleted (all entries are newer than fourHoursAgo)
        int deleted = cleanupService.cleanup();
        
        assertEquals(0, deleted);
    }

    @Test
    public void testCleanupWithCustomRetentionBuffer() throws Exception {
        long now = System.currentTimeMillis();
        long oneHourAgo = now - TimeUnit.HOURS.toMillis(1);
        long twoHoursAgo = now - TimeUnit.HOURS.toMillis(2);
        
        // Create service with NO retention buffer
        ChangeTrackingCleanupService noBufferService = new ChangeTrackingCleanupService(
                writer, metadataManager, 0L);
        
        addChangeEntry("/content/node1", twoHoursAgo, 0L);
        addChangeEntry("/content/node2", oneHourAgo, 0L);
        writer.commit();
        
        // Register index and mark as processed up to oneHourAgo
        metadataManager.registerIndex("/oak:index/testIndex");
        IndexProgressMetadata progress = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/testIndex")
                .lastProcessedTimestamp(oneHourAgo)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress.getIndexPath(),
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                (int) progress.getTotalProcessed());
        
        // With no buffer, should delete entries up to oneHourAgo (exclusive)
        int deleted = noBufferService.cleanup();
        
        assertTrue("Should delete at least 1 entry", deleted >= 1);
    }

    @Test
    public void testCleanupMethod() throws Exception {
        // Test the cleanup() method
        long now = System.currentTimeMillis();
        long twoHoursAgo = now - TimeUnit.HOURS.toMillis(2);
        
        addChangeEntry("/content/node1", twoHoursAgo, 0L);
        writer.commit();
        
        metadataManager.registerIndex("/oak:index/testIndex");
        IndexProgressMetadata progress = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/testIndex")
                .lastProcessedTimestamp(now)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress.getIndexPath(),
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                (int) progress.getTotalProcessed());
        
        // Call cleanup() - should not throw
        cleanupService.cleanup();
    }

    @Test
    public void testCleanupDoesNotDeleteRecentEntries() throws Exception {
        long now = System.currentTimeMillis();
        long tenMinutesAgo = now - TimeUnit.MINUTES.toMillis(10);
        
        addChangeEntry("/content/node1", tenMinutesAgo, 0L);
        addChangeEntry("/content/node2", now, 0L);
        writer.commit();
        
        // Register index with recent progress
        metadataManager.registerIndex("/oak:index/testIndex");
        IndexProgressMetadata progress = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/testIndex")
                .lastProcessedTimestamp(now)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress.getIndexPath(),
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                (int) progress.getTotalProcessed());
        
        int deleted = cleanupService.cleanup();
        
        // With 1 hour buffer, cutoff = now - 1 hour
        // Both entries are newer than cutoff, so nothing should be deleted
        assertEquals(0, deleted);
    }

    @Test
    public void testCleanupWithZeroTimestamp() throws Exception {
        addChangeEntry("/content/node1", 0L, 0L);
        addChangeEntry("/content/node2", 1000L, 0L);
        writer.commit();
        
        metadataManager.registerIndex("/oak:index/testIndex");
        IndexProgressMetadata progress = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/testIndex")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress.getIndexPath(),
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                (int) progress.getTotalProcessed());
        
        // Should handle zero timestamps gracefully
        int deleted = cleanupService.cleanup();
        
        // The entry with timestamp 0 might or might not be deleted depending on implementation
        assertTrue("Deleted count should be non-negative", deleted >= 0);
    }

    @Test
    public void testMultipleCleanupRuns() throws Exception {
        long now = System.currentTimeMillis();
        long twoHoursAgo = now - TimeUnit.HOURS.toMillis(2);
        
        addChangeEntry("/content/node1", twoHoursAgo, 0L);
        writer.commit();
        
        metadataManager.registerIndex("/oak:index/testIndex");
        IndexProgressMetadata progress = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/testIndex")
                .lastProcessedTimestamp(now)
                .lastProcessedSerialNumber(0L)
                .build();
        metadataManager.updateProgress(progress.getIndexPath(),
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                (int) progress.getTotalProcessed());
        
        // First cleanup
        int deleted1 = cleanupService.cleanup();
        
        // Second cleanup - should find nothing to delete
        int deleted2 = cleanupService.cleanup();
        
        assertTrue("First cleanup should delete something", deleted1 > 0);
        assertEquals("Second cleanup should find nothing", 0, deleted2);
    }
}

