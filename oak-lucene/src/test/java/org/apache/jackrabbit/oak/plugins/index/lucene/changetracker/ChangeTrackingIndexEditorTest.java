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
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.lucene.document.Document;
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

import static org.junit.Assert.*;

/**
 * Unit test for {@link ChangeTrackingIndexEditor}.
 */
public class ChangeTrackingIndexEditorTest {

    private Directory directory;
    private IndexWriter writer;
    private ChangeTrackingIndexEditor editor;

    @Before
    public void setup() throws Exception {
        directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47, null);
        writer = new IndexWriter(directory, config);
        
        long timestamp = System.currentTimeMillis();
        editor = new ChangeTrackingIndexEditor(writer, timestamp);
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

    @Test
    public void testPropertyAdded() throws Exception {
        editor.enter(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        editor.propertyAdded(PropertyStates.createProperty("test", "value"));
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.numDocs());
        
        Document doc = reader.document(0);
        assertEquals("/", doc.get("ct:path"));
        assertNotNull(doc.get("ct:diffProcessingTime"));
        assertNotNull(doc.get("ct:serialNumber"));
        
        reader.close();
    }

    @Test
    public void testPropertyChanged() throws Exception {
        editor.enter(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        editor.propertyChanged(
                PropertyStates.createProperty("test", "old"),
                PropertyStates.createProperty("test", "new"));
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.numDocs());
        reader.close();
    }

    @Test
    public void testPropertyDeleted() throws Exception {
        editor.enter(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        editor.propertyDeleted(PropertyStates.createProperty("test", "value"));
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.numDocs());
        reader.close();
    }

    @Test
    public void testChildNodeAdded() throws Exception {
        editor.childNodeAdded("child", EmptyNodeState.EMPTY_NODE);
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.numDocs());
        
        Document doc = reader.document(0);
        assertEquals("/child", doc.get("ct:path"));
        
        reader.close();
    }

    @Test
    public void testChildNodeChanged() throws Exception {
        editor.childNodeChanged("child", EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.numDocs());
        
        Document doc = reader.document(0);
        assertEquals("/child", doc.get("ct:path"));
        
        reader.close();
    }

    @Test
    public void testChildNodeDeleted() throws Exception {
        editor.childNodeDeleted("child", EmptyNodeState.EMPTY_NODE);
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(1, reader.numDocs());
        
        Document doc = reader.document(0);
        assertEquals("/child", doc.get("ct:path"));
        
        reader.close();
    }

    @Test
    public void testMultipleChanges() throws Exception {
        // Add multiple changes
        editor.propertyAdded(PropertyStates.createProperty("prop1", "value1"));
        editor.childNodeAdded("child1", EmptyNodeState.EMPTY_NODE);
        editor.childNodeChanged("child2", EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(3, reader.numDocs());
        reader.close();
    }

    @Test
    public void testSerialNumbers() throws Exception {
        // Add multiple changes - should get unique serial numbers
        editor.propertyAdded(PropertyStates.createProperty("prop1", "value1"));
        editor.propertyAdded(PropertyStates.createProperty("prop2", "value2"));
        editor.propertyAdded(PropertyStates.createProperty("prop3", "value3"));
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(3, reader.numDocs());
        
        // Check that serial numbers are sequential
        Document doc0 = reader.document(0);
        Document doc1 = reader.document(1);
        Document doc2 = reader.document(2);
        
        long serial0 = Long.parseLong(doc0.get("ct:serialNumber"));
        long serial1 = Long.parseLong(doc1.get("ct:serialNumber"));
        long serial2 = Long.parseLong(doc2.get("ct:serialNumber"));
        
        assertEquals(0L, serial0);
        assertEquals(1L, serial1);
        assertEquals(2L, serial2);
        
        reader.close();
    }

    @Test
    public void testNestedPaths() throws Exception {
        // Navigate down and add child
        editor.enter(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        ChangeTrackingIndexEditor childEditor = 
                (ChangeTrackingIndexEditor) editor.childNodeAdded("level1", EmptyNodeState.EMPTY_NODE);
        
        assertNotNull(childEditor);
        
        childEditor.childNodeAdded("level2", EmptyNodeState.EMPTY_NODE);
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        assertEquals(2, reader.numDocs());
        
        boolean foundLevel1 = false;
        boolean foundLevel2 = false;
        
        for (int i = 0; i < reader.numDocs(); i++) {
            Document doc = reader.document(i);
            String path = doc.get("ct:path");
            if ("/level1".equals(path)) {
                foundLevel1 = true;
            }
            if ("/level1/level2".equals(path)) {
                foundLevel2 = true;
            }
        }
        
        assertTrue("Should find /level1", foundLevel1);
        assertTrue("Should find /level1/level2", foundLevel2);
        
        reader.close();
    }

    @Test
    public void testTimestampTracking() throws Exception {
        editor.enter(EmptyNodeState.EMPTY_NODE, EmptyNodeState.EMPTY_NODE);
        editor.propertyAdded(PropertyStates.createProperty("test", "value"));
        
        writer.commit();
        
        IndexReader reader = DirectoryReader.open(directory);
        Document doc = reader.document(0);
        
        // Verify timestamp is recorded (not null and > 0)
        String timestampStr = doc.get("ct:diffProcessingTime");
        assertNotNull("diffProcessingTime should be recorded", timestampStr);
        long timestamp = Long.parseLong(timestampStr);
        assertTrue("diffProcessingTime should be > 0", timestamp > 0);
        
        reader.close();
    }

    @Test
    public void testEntriesWrittenCount() throws Exception {
        assertEquals(0, editor.getEntriesWritten());
        
        editor.propertyAdded(PropertyStates.createProperty("prop1", "value1"));
        assertEquals(1, editor.getEntriesWritten());
        
        editor.childNodeAdded("child1", EmptyNodeState.EMPTY_NODE);
        assertEquals(2, editor.getEntriesWritten());
        
        editor.propertyChanged(
                PropertyStates.createProperty("prop2", "old"),
                PropertyStates.createProperty("prop2", "new"));
        assertEquals(3, editor.getEntriesWritten());
    }
}

