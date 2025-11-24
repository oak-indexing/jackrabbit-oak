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

import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
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
import java.util.List;

import static org.junit.Assert.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingConstants.*;

/**
 * Unit tests for ChangeTrackingIndexQuery.
 * 
 * Tests cover:
 * - Querying unprocessed changes
 * - Composite key ordering (timestamp, serialNumber)
 * - Handling timestamp collisions
 * - Empty results
 * - Pagination (chunk size limits)
 * - Oldest/newest change queries
 * - Count queries
 */
public class ChangeTrackingIndexQueryTest {
    
    private Directory directory;
    private IndexWriter writer;
    private IndexReader reader;
    private ChangeTrackingIndexQuery query;
    
    @Before
    public void setup() throws IOException {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
    }
    
    @After
    public void teardown() throws IOException {
        if (query != null) {
            query.close();
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
    
    private void addChangeEntry(String path, String cp1, String cp2, 
                                long timestamp, long serial) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(FIELD_PATH, path, Field.Store.YES));
        doc.add(new StringField(FIELD_CHECKPOINT1, cp1, Field.Store.YES));
        doc.add(new StringField(FIELD_CHECKPOINT2, cp2, Field.Store.YES));
        doc.add(new LongField(FIELD_DIFF_PROCESSING_TIME, timestamp, Field.Store.YES));
        doc.add(new LongField(FIELD_SERIAL_NUMBER, serial, Field.Store.YES));
        writer.addDocument(doc);
    }
    
    private void commitAndOpenQuery() throws IOException {
        writer.commit();
        reader = DirectoryReader.open(directory);
        query = new ChangeTrackingIndexQuery(reader);
    }
    
    @Test
    public void testGetUnprocessedChanges() throws IOException {
        // Add entries with different timestamps
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        addChangeEntry("/node2", "cp1", "cp2", 2000L, 1L);
        addChangeEntry("/node3", "cp1", "cp2", 3000L, 1L);
        
        commitAndOpenQuery();
        
        // Query for changes after timestamp 1500
        List<ChangeEntry> changes = query.getUnprocessedChanges(1500L, 0L, 10);
        
        assertEquals("Should return 2 changes", 2, changes.size());
        assertEquals("/node2", changes.get(0).getPath());
        assertEquals("/node3", changes.get(1).getPath());
    }
    
    @Test
    public void testCompositeKeyOrdering() throws IOException {
        // Add entries with same timestamp but different serial numbers
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        addChangeEntry("/node2", "cp1", "cp2", 1000L, 2L);
        addChangeEntry("/node3", "cp1", "cp2", 1000L, 3L);
        
        commitAndOpenQuery();
        
        // Query for changes after (1000, 1)
        List<ChangeEntry> changes = query.getUnprocessedChanges(1000L, 1L, 10);
        
        assertEquals("Should return 2 changes", 2, changes.size());
        assertEquals("/node2", changes.get(0).getPath());
        assertEquals(2L, changes.get(0).getSerialNumber());
        assertEquals("/node3", changes.get(1).getPath());
        assertEquals(3L, changes.get(1).getSerialNumber());
    }
    
    @Test
    public void testTimestampCollisionHandling() throws IOException {
        // Multiple entries with exact same timestamp
        long timestamp = 1000L;
        addChangeEntry("/node1", "cp1", "cp2", timestamp, 1L);
        addChangeEntry("/node2", "cp1", "cp2", timestamp, 2L);
        addChangeEntry("/node3", "cp1", "cp2", timestamp, 3L);
        addChangeEntry("/node4", "cp1", "cp2", timestamp + 1, 1L);
        
        commitAndOpenQuery();
        
        // Query after (1000, 2) should return node3 and node4
        List<ChangeEntry> changes = query.getUnprocessedChanges(timestamp, 2L, 10);
        
        assertEquals("Should return 2 changes", 2, changes.size());
        assertEquals("/node3", changes.get(0).getPath());
        assertEquals(timestamp, changes.get(0).getDiffProcessingTime());
        assertEquals(3L, changes.get(0).getSerialNumber());
        assertEquals("/node4", changes.get(1).getPath());
        assertEquals(timestamp + 1, changes.get(1).getDiffProcessingTime());
    }
    
    @Test
    public void testEmptyResults() throws IOException {
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        
        commitAndOpenQuery();
        
        // Query for changes after a timestamp that doesn't exist
        List<ChangeEntry> changes = query.getUnprocessedChanges(9999L, 0L, 10);
        
        assertTrue("Should return empty list", changes.isEmpty());
    }
    
    @Test
    public void testChunkSizeLimit() throws IOException {
        // Add 10 entries
        for (int i = 0; i < 10; i++) {
            addChangeEntry("/node" + i, "cp1", "cp2", 1000L + i, 1L);
        }
        
        commitAndOpenQuery();
        
        // Query with limit of 5
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 5);
        
        assertEquals("Should return exactly 5 changes", 5, changes.size());
        
        // Verify ordering (should be oldest first)
        assertEquals("/node0", changes.get(0).getPath());
        assertEquals("/node4", changes.get(4).getPath());
    }
    
    @Test
    public void testOrderingByTimestampThenSerial() throws IOException {
        // Add entries in non-sequential order
        addChangeEntry("/node3", "cp1", "cp2", 2000L, 2L);
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        addChangeEntry("/node4", "cp1", "cp2", 2000L, 3L);
        addChangeEntry("/node2", "cp1", "cp2", 2000L, 1L);
        
        commitAndOpenQuery();
        
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 10);
        
        assertEquals(4, changes.size());
        
        // Verify correct ordering
        assertEquals("/node1", changes.get(0).getPath());
        assertEquals(1000L, changes.get(0).getDiffProcessingTime());
        
        assertEquals("/node2", changes.get(1).getPath());
        assertEquals(2000L, changes.get(1).getDiffProcessingTime());
        assertEquals(1L, changes.get(1).getSerialNumber());
        
        assertEquals("/node3", changes.get(2).getPath());
        assertEquals(2000L, changes.get(2).getDiffProcessingTime());
        assertEquals(2L, changes.get(2).getSerialNumber());
        
        assertEquals("/node4", changes.get(3).getPath());
        assertEquals(2000L, changes.get(3).getDiffProcessingTime());
        assertEquals(3L, changes.get(3).getSerialNumber());
    }
    
    @Test
    public void testGetOldestChange() throws IOException {
        addChangeEntry("/node2", "cp1", "cp2", 2000L, 1L);
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        addChangeEntry("/node3", "cp1", "cp2", 3000L, 1L);
        
        commitAndOpenQuery();
        
        ChangeEntry oldest = query.getOldestChange();
        
        assertNotNull("Should return oldest change", oldest);
        assertEquals("/node1", oldest.getPath());
        assertEquals(1000L, oldest.getDiffProcessingTime());
    }
    
    @Test
    public void testGetNewestChange() throws IOException {
        addChangeEntry("/node2", "cp1", "cp2", 2000L, 1L);
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        addChangeEntry("/node3", "cp1", "cp2", 3000L, 1L);
        
        commitAndOpenQuery();
        
        ChangeEntry newest = query.getNewestChange();
        
        assertNotNull("Should return newest change", newest);
        assertEquals("/node3", newest.getPath());
        assertEquals(3000L, newest.getDiffProcessingTime());
    }
    
    @Test
    public void testGetOldestChangeEmptyIndex() throws IOException {
        commitAndOpenQuery();
        
        ChangeEntry oldest = query.getOldestChange();
        
        assertNull("Should return null for empty index", oldest);
    }
    
    @Test
    public void testCountUnprocessedChanges() throws IOException {
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        addChangeEntry("/node2", "cp1", "cp2", 2000L, 1L);
        addChangeEntry("/node3", "cp1", "cp2", 3000L, 1L);
        addChangeEntry("/node4", "cp1", "cp2", 4000L, 1L);
        
        commitAndOpenQuery();
        
        int count = query.countUnprocessedChanges(1500L, 0L);
        
        assertEquals("Should count 3 unprocessed changes", 3, count);
    }
    
    @Test
    public void testCountZeroUnprocessed() throws IOException {
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        
        commitAndOpenQuery();
        
        int count = query.countUnprocessedChanges(9999L, 0L);
        
        assertEquals("Should count 0 unprocessed changes", 0, count);
    }
    
    @Test
    public void testChangeEntryParsing() throws IOException {
        String testPath = "/content/dam/asset-12345";
        String checkpoint1 = "checkpoint-before";
        String checkpoint2 = "checkpoint-after";
        long timestamp = 1701234567890L;
        long serial = 42L;
        
        addChangeEntry(testPath, checkpoint1, checkpoint2, timestamp, serial);
        
        commitAndOpenQuery();
        
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 10);
        
        assertEquals(1, changes.size());
        
        ChangeEntry entry = changes.get(0);
        assertEquals(testPath, entry.getPath());
        assertEquals(checkpoint1, entry.getCheckpoint1());
        assertEquals(checkpoint2, entry.getCheckpoint2());
        assertEquals(timestamp, entry.getDiffProcessingTime());
        assertEquals(serial, entry.getSerialNumber());
    }
    
    @Test
    public void testPaginationScenario() throws IOException {
        // Add 25 entries
        for (int i = 0; i < 25; i++) {
            addChangeEntry("/node" + i, "cp1", "cp2", 1000L + i, 1L);
        }
        
        commitAndOpenQuery();
        
        // First page (0-9)
        List<ChangeEntry> page1 = query.getUnprocessedChanges(0L, 0L, 10);
        assertEquals(10, page1.size());
        assertEquals("/node0", page1.get(0).getPath());
        
        // Second page (10-19)
        ChangeEntry lastOfPage1 = page1.get(9);
        List<ChangeEntry> page2 = query.getUnprocessedChanges(
            lastOfPage1.getDiffProcessingTime(),
            lastOfPage1.getSerialNumber(),
            10
        );
        assertEquals(10, page2.size());
        assertEquals("/node10", page2.get(0).getPath());
        
        // Third page (20-24)
        ChangeEntry lastOfPage2 = page2.get(9);
        List<ChangeEntry> page3 = query.getUnprocessedChanges(
            lastOfPage2.getDiffProcessingTime(),
            lastOfPage2.getSerialNumber(),
            10
        );
        assertEquals(5, page3.size());
        assertEquals("/node20", page3.get(0).getPath());
        assertEquals("/node24", page3.get(4).getPath());
    }
    
    @Test
    public void testAutoCloseableImplementation() throws IOException {
        addChangeEntry("/node1", "cp1", "cp2", 1000L, 1L);
        commitAndOpenQuery();
        
        // Test that close() works
        query.close();
        
        // Verify reader is closed
        assertEquals("Reader ref count should be 0 after close", 0, reader.getRefCount());
    }
}

