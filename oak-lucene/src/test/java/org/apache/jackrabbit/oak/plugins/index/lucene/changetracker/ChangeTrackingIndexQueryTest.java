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

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit test for {@link ChangeTrackingIndexQuery}.
 */
public class ChangeTrackingIndexQueryTest {

    private Directory directory;
    private IndexWriter writer;
    private ChangeTrackingIndexQuery query;

    @Before
    public void setup() throws Exception {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
    }

    @After
    public void teardown() throws Exception {
        if (query != null) {
            query.close();
        }
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
        doc.add(new StringField("ct:checkpoint1", "cp1-" + serial, Field.Store.YES));
        doc.add(new StringField("ct:checkpoint2", "cp2-" + serial, Field.Store.YES));
        doc.add(new LongField("ct:diffProcessingTime", timestamp, Field.Store.YES));
        doc.add(new LongField("ct:serialNumber", serial, Field.Store.YES));
        writer.addDocument(doc);
    }

    private void initQuery() throws Exception {
        writer.commit();
        IndexReader reader = DirectoryReader.open(directory);
        query = new ChangeTrackingIndexQuery(reader);
    }

    @Test
    public void testGetUnprocessedChanges() throws Exception {
        // Add some test data
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 1000L, 1L);
        addChangeEntry("/content/node3", 2000L, 0L);
        
        initQuery();
        
        // Query for changes after (0, 0)
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 10);
        
        assertEquals(3, changes.size());
        assertEquals("/content/node1", changes.get(0).getPath());
        assertEquals(1000L, changes.get(0).getDiffProcessingTime());
        assertEquals(0L, changes.get(0).getSerialNumber());
    }

    @Test
    public void testGetUnprocessedChangesWithLimit() throws Exception {
        // Add 5 entries
        for (int i = 0; i < 5; i++) {
            addChangeEntry("/content/node" + i, 1000L, i);
        }
        
        initQuery();
        
        // Query with limit of 3
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 3);
        
        assertEquals(3, changes.size());
    }

    @Test
    public void testGetUnprocessedChangesAfterTimestamp() throws Exception {
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 2000L, 0L);
        addChangeEntry("/content/node3", 3000L, 0L);
        
        initQuery();
        
        // Query for changes after timestamp 1000
        List<ChangeEntry> changes = query.getUnprocessedChanges(1000L, 0L, 10);
        
        assertEquals(2, changes.size());
        assertEquals("/content/node2", changes.get(0).getPath());
        assertEquals(2000L, changes.get(0).getDiffProcessingTime());
    }

    @Test
    public void testGetUnprocessedChangesAfterSerialNumber() throws Exception {
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 1000L, 1L);
        addChangeEntry("/content/node3", 1000L, 2L);
        
        initQuery();
        
        // Query for changes after (1000, 1) - should get only node3
        List<ChangeEntry> changes = query.getUnprocessedChanges(1000L, 1L, 10);
        
        assertEquals(1, changes.size());
        assertEquals("/content/node3", changes.get(0).getPath());
        assertEquals(2L, changes.get(0).getSerialNumber());
    }

    @Test
    public void testCountUnprocessedChanges() throws Exception {
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 2000L, 0L);
        addChangeEntry("/content/node3", 3000L, 0L);
        
        initQuery();
        
        assertEquals(3, query.countUnprocessedChanges(0L, 0L));
        assertEquals(2, query.countUnprocessedChanges(1000L, 0L));
        assertEquals(1, query.countUnprocessedChanges(2000L, 0L));
        assertEquals(0, query.countUnprocessedChanges(3000L, 0L));
    }

    @Test
    public void testGetOldestChange() throws Exception {
        addChangeEntry("/content/node3", 3000L, 0L);
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 2000L, 0L);
        
        initQuery();
        
        ChangeEntry oldest = query.getOldestChange();
        
        assertNotNull(oldest);
        assertEquals("/content/node1", oldest.getPath());
        assertEquals(1000L, oldest.getDiffProcessingTime());
    }

    @Test
    public void testGetNewestChange() throws Exception {
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 2000L, 0L);
        addChangeEntry("/content/node3", 3000L, 0L);
        
        initQuery();
        
        ChangeEntry newest = query.getNewestChange();
        
        assertNotNull(newest);
        assertEquals("/content/node3", newest.getPath());
        assertEquals(3000L, newest.getDiffProcessingTime());
    }

    @Test
    public void testGetOldestChangeEmpty() throws Exception {
        initQuery();
        
        ChangeEntry oldest = query.getOldestChange();
        assertNull(oldest);
    }

    @Test
    public void testGetNewestChangeEmpty() throws Exception {
        initQuery();
        
        ChangeEntry newest = query.getNewestChange();
        assertNull(newest);
    }

    @Test
    public void testSortingByTimestampAndSerial() throws Exception {
        // Add entries in random order
        addChangeEntry("/content/node3", 2000L, 1L);
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node4", 2000L, 2L);
        addChangeEntry("/content/node2", 2000L, 0L);
        
        initQuery();
        
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 10);
        
        assertEquals(4, changes.size());
        
        // Should be sorted by (timestamp, serial)
        assertEquals("/content/node1", changes.get(0).getPath());
        assertEquals(1000L, changes.get(0).getDiffProcessingTime());
        assertEquals(0L, changes.get(0).getSerialNumber());
        
        assertEquals("/content/node2", changes.get(1).getPath());
        assertEquals(2000L, changes.get(1).getDiffProcessingTime());
        assertEquals(0L, changes.get(1).getSerialNumber());
        
        assertEquals("/content/node3", changes.get(2).getPath());
        assertEquals(2000L, changes.get(2).getDiffProcessingTime());
        assertEquals(1L, changes.get(2).getSerialNumber());
        
        assertEquals("/content/node4", changes.get(3).getPath());
        assertEquals(2000L, changes.get(3).getDiffProcessingTime());
        assertEquals(2L, changes.get(3).getSerialNumber());
    }

    @Test
    public void testCompositeKeyQuery() throws Exception {
        // Add multiple entries with same timestamp
        addChangeEntry("/content/node1", 1000L, 0L);
        addChangeEntry("/content/node2", 1000L, 1L);
        addChangeEntry("/content/node3", 1000L, 2L);
        addChangeEntry("/content/node4", 1000L, 3L);
        
        initQuery();
        
        // Query after (1000, 1) - should get only entries with serial > 1
        List<ChangeEntry> changes = query.getUnprocessedChanges(1000L, 1L, 10);
        
        assertEquals(2, changes.size());
        assertEquals(2L, changes.get(0).getSerialNumber());
        assertEquals(3L, changes.get(1).getSerialNumber());
    }

    @Test
    public void testCheckpointTracking() throws Exception {
        addChangeEntry("/content/node1", 1000L, 0L);
        
        initQuery();
        
        List<ChangeEntry> changes = query.getUnprocessedChanges(0L, 0L, 1);
        
        assertEquals(1, changes.size());
        ChangeEntry entry = changes.get(0);
        
        assertEquals("cp1-0", entry.getCheckpoint1());
        assertEquals("cp2-0", entry.getCheckpoint2());
    }

    @Test
    public void testEmptyResults() throws Exception {
        addChangeEntry("/content/node1", 1000L, 0L);
        
        initQuery();
        
        // Query for changes after the last entry
        List<ChangeEntry> changes = query.getUnprocessedChanges(1000L, 0L, 10);
        
        assertTrue(changes.isEmpty());
    }

    @Test
    public void testPagination() throws Exception {
        // Add 10 entries
        for (int i = 0; i < 10; i++) {
            addChangeEntry("/content/node" + i, 1000L, i);
        }
        
        initQuery();
        
        // Get first page
        List<ChangeEntry> page1 = query.getUnprocessedChanges(0L, 0L, 3);
        assertEquals(3, page1.size());
        
        // Get second page
        ChangeEntry lastFromPage1 = page1.get(page1.size() - 1);
        List<ChangeEntry> page2 = query.getUnprocessedChanges(
                lastFromPage1.getDiffProcessingTime(),
                lastFromPage1.getSerialNumber(),
                3);
        assertEquals(3, page2.size());
        
        // Verify no overlap
        assertNotEquals(page1.get(2).getPath(), page2.get(0).getPath());
    }
}

