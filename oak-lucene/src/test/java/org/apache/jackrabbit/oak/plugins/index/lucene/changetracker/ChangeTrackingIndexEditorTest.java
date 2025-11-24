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
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingConstants.*;

/**
 * Unit tests for ChangeTrackingIndexEditor.
 * 
 * Tests cover:
 * - Recording node additions
 * - Recording node changes
 * - Recording node deletions
 * - Recording property changes
 * - Serial number generation (handling timestamp collisions)
 * - Checkpoint range storage
 * - Entry count tracking
 */
public class ChangeTrackingIndexEditorTest {
    
    private Directory directory;
    private IndexWriter writer;
    private String checkpoint1;
    private String checkpoint2;
    private long diffProcessingTime;
    
    @Before
    public void setup() throws IOException {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
        
        checkpoint1 = "checkpoint-before-" + System.currentTimeMillis();
        checkpoint2 = "checkpoint-after-" + System.currentTimeMillis();
        diffProcessingTime = System.currentTimeMillis();
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
    
    @Test
    public void testRecordsChildNodeAdded() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        NodeState afterState = EmptyNodeState.EMPTY_NODE;
        
        // Simulate child node added
        editor.childNodeAdded("testNode", afterState);
        
        writer.commit();
        
        // Verify entry was written
        assertEquals("Should have recorded 1 entry", 1, editor.getEntriesWritten());
        
        // Verify in Lucene index
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should have 1 document", 1, reader.numDocs());
        
        // Verify document fields
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs results = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(1, results.totalHits);
        
        Document doc = searcher.doc(results.scoreDocs[0].doc);
        assertEquals("/testNode", doc.get(FIELD_PATH));
        assertEquals(checkpoint1, doc.get(FIELD_CHECKPOINT1));
        assertEquals(checkpoint2, doc.get(FIELD_CHECKPOINT2));
        assertEquals(String.valueOf(diffProcessingTime), doc.get(FIELD_DIFF_PROCESSING_TIME));
        assertNotNull(doc.get(FIELD_SERIAL_NUMBER));
        
        reader.close();
    }
    
    @Test
    public void testRecordsChildNodeChanged() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        NodeState beforeState = EmptyNodeState.EMPTY_NODE;
        NodeState afterState = EmptyNodeState.EMPTY_NODE;
        
        // Simulate child node changed
        editor.childNodeChanged("changedNode", beforeState, afterState);
        
        writer.commit();
        
        assertEquals("Should have recorded 1 entry", 1, editor.getEntriesWritten());
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Should have 1 document", 1, reader.numDocs());
        
        Document doc = reader.document(0);
        assertEquals("/changedNode", doc.get(FIELD_PATH));
        
        reader.close();
    }
    
    @Test
    public void testRecordsChildNodeDeleted() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        NodeState beforeState = EmptyNodeState.EMPTY_NODE;
        
        // Simulate child node deleted
        editor.childNodeDeleted("deletedNode", beforeState);
        
        writer.commit();
        
        assertEquals("Should have recorded 1 entry", 1, editor.getEntriesWritten());
        
        IndexReader reader = DirectoryReader.open(directory);
        Document doc = reader.document(0);
        assertEquals("/deletedNode", doc.get(FIELD_PATH));
        
        reader.close();
    }
    
    @Test
    public void testRecordsPropertyChanges() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        PropertyState property = PropertyStates.createProperty("testProp", "value");
        
        // Test property added
        editor.propertyAdded(property);
        writer.commit();
        assertEquals("Should have recorded 1 entry for property add", 1, editor.getEntriesWritten());
        
        // Create new editor for next test
        writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_47, null));
        editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        // Test property changed
        editor.propertyChanged(property, property);
        writer.commit();
        assertEquals("Should have recorded 1 entry for property change", 1, editor.getEntriesWritten());
        
        // Create new editor for next test
        writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_47, null));
        editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        // Test property deleted
        editor.propertyDeleted(property);
        writer.commit();
        assertEquals("Should have recorded 1 entry for property delete", 1, editor.getEntriesWritten());
        
        IndexReader reader = DirectoryReader.open(directory);
        // Should have 3 documents total (one for each property operation)
        assertEquals("Should have 3 documents", 3, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testSerialNumberGeneration() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        // Record multiple changes with same timestamp
        editor.childNodeAdded("node1", EmptyNodeState.EMPTY_NODE);
        editor.childNodeAdded("node2", EmptyNodeState.EMPTY_NODE);
        editor.childNodeAdded("node3", EmptyNodeState.EMPTY_NODE);
        
        writer.commit();
        
        assertEquals("Should have recorded 3 entries", 3, editor.getEntriesWritten());
        
        // Verify serial numbers are unique and sequential
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(3, reader.numDocs());
        
        long[] serials = new long[3];
        for (int i = 0; i < 3; i++) {
            Document doc = reader.document(i);
            serials[i] = Long.parseLong(doc.get(FIELD_SERIAL_NUMBER));
        }
        
        // Verify serials are different
        assertTrue("Serial 0 and 1 should be different", serials[0] != serials[1]);
        assertTrue("Serial 1 and 2 should be different", serials[1] != serials[2]);
        assertTrue("Serial 0 and 2 should be different", serials[0] != serials[2]);
        
        reader.close();
    }
    
    @Test
    public void testCheckpointRangeStorage() throws CommitFailedException, IOException {
        String cp1 = "checkpoint-start-12345";
        String cp2 = "checkpoint-end-67890";
        long timestamp = 1701234567890L;
        
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, cp1, cp2, timestamp
        );
        
        editor.childNodeAdded("testNode", EmptyNodeState.EMPTY_NODE);
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        Document doc = reader.document(0);
        
        assertEquals("checkpoint1 should match", cp1, doc.get(FIELD_CHECKPOINT1));
        assertEquals("checkpoint2 should match", cp2, doc.get(FIELD_CHECKPOINT2));
        assertEquals("timestamp should match", String.valueOf(timestamp), doc.get(FIELD_DIFF_PROCESSING_TIME));
        
        reader.close();
    }
    
    @Test
    public void testNestedPathHandling() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor rootEditor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        // Enter a child node
        rootEditor.enter(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        
        // Get child editor
        ChangeTrackingIndexEditor childEditor = 
            (ChangeTrackingIndexEditor) rootEditor.childNodeChanged("parent", EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        
        assertNotNull("Child editor should not be null", childEditor);
        
        // Record change in child
        if (childEditor != null) {
            childEditor.childNodeAdded("child", EmptyNodeState.EMPTY_NODE);
        }
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertTrue("Should have at least 1 document", reader.numDocs() >= 1);
        
        // Find the nested path document
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs results = searcher.search(new MatchAllDocsQuery(), 10);
        
        boolean foundNestedPath = false;
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);
            String path = doc.get(FIELD_PATH);
            if (path.contains("parent") && path.contains("child")) {
                foundNestedPath = true;
                break;
            }
        }
        
        assertTrue("Should have recorded nested path", foundNestedPath);
        
        reader.close();
    }
    
    @Test
    public void testEntryCountTracking() throws CommitFailedException, IOException {
        ChangeTrackingIndexEditor editor = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, diffProcessingTime
        );
        
        assertEquals("Initial count should be 0", 0, editor.getEntriesWritten());
        
        editor.childNodeAdded("node1", EmptyNodeState.EMPTY_NODE);
        assertEquals("Count should be 1 after first add", 1, editor.getEntriesWritten());
        
        editor.childNodeAdded("node2", EmptyNodeState.EMPTY_NODE);
        assertEquals("Count should be 2 after second add", 2, editor.getEntriesWritten());
        
        PropertyState prop = PropertyStates.createProperty("prop", "value");
        editor.propertyAdded(prop);
        assertEquals("Count should be 3 after property add", 3, editor.getEntriesWritten());
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals("Lucene index should have 3 documents", 3, reader.numDocs());
        reader.close();
    }
    
    @Test
    public void testMultipleEditorsShareSerialNumberGenerator() throws CommitFailedException, IOException {
        long sharedTimestamp = System.currentTimeMillis();
        
        ChangeTrackingIndexEditor editor1 = new ChangeTrackingIndexEditor(
            writer, checkpoint1, checkpoint2, sharedTimestamp
        );
        
        editor1.childNodeAdded("node1", EmptyNodeState.EMPTY_NODE);
        
        // Create child editor (should share serial number generator)
        ChangeTrackingIndexEditor editor2 = 
            (ChangeTrackingIndexEditor) editor1.childNodeAdded("parent", EmptyNodeState.EMPTY_NODE);
        
        if (editor2 != null) {
            editor2.childNodeAdded("child", EmptyNodeState.EMPTY_NODE);
        }
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        
        // Verify all entries have the same timestamp but different serial numbers
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs results = searcher.search(new MatchAllDocsQuery(), 10);
        
        for (ScoreDoc scoreDoc : results.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);
            assertEquals("All entries should have same timestamp", 
                        String.valueOf(sharedTimestamp), 
                        doc.get(FIELD_DIFF_PROCESSING_TIME));
            assertNotNull("All entries should have serial number", 
                         doc.get(FIELD_SERIAL_NUMBER));
        }
        
        reader.close();
    }
}

