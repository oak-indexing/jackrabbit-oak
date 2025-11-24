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
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Editor that writes change tracking entries to a dedicated Lucene index.
 * 
 * <p>This editor is invoked during the async diff process and records the path
 * of every changed node along with the checkpoint range and timestamp.
 * 
 * <p>The change tracking index enables chunked processing of repository changes
 * without needing to re-diff the entire tree for each index update cycle.
 * 
 * <p><strong>Key Features:</strong>
 * <ul>
 *   <li>Lightweight - only tracks paths, not full node state</li>
 *   <li>Serial number generation for unique ordering</li>
 *   <li>Checkpoint range tracking for analysis</li>
 *   <li>Fast writes using Lucene</li>
 * </ul>
 */
public class ChangeTrackingIndexEditor implements Editor {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingIndexEditor.class);
    
    // Field names in the Lucene index
    private static final String FIELD_PATH = "ct:path";
    private static final String FIELD_CHECKPOINT1 = "ct:checkpoint1";
    private static final String FIELD_CHECKPOINT2 = "ct:checkpoint2";
    private static final String FIELD_DIFF_PROCESSING_TIME = "ct:diffProcessingTime";
    private static final String FIELD_SERIAL_NUMBER = "ct:serialNumber";
    
    private final IndexWriter indexWriter;
    private final String currentPath;
    private final String checkpoint1;
    private final String checkpoint2;
    private final long diffProcessingTime;
    
    // Serial number management (shared across all editors in this diff run)
    private final SerialNumberGenerator serialNumberGenerator;
    
    private long entriesWritten = 0;
    
    /**
     * Creates the root change tracking editor for a diff run.
     * 
     * @param indexWriter the Lucene index writer for the change tracking index
     * @param checkpoint1 the first checkpoint in this diff range
     * @param checkpoint2 the last checkpoint in this diff range
     * @param diffProcessingTime the millisecond timestamp for this diff run
     */
    public ChangeTrackingIndexEditor(@NotNull IndexWriter indexWriter,
                                      @NotNull String checkpoint1,
                                      @NotNull String checkpoint2,
                                      long diffProcessingTime) {
        this(indexWriter, "/", checkpoint1, checkpoint2, diffProcessingTime, 
             new SerialNumberGenerator(diffProcessingTime));
    }
    
    /**
     * Internal constructor for creating child editors.
     */
    private ChangeTrackingIndexEditor(@NotNull IndexWriter indexWriter,
                                       @NotNull String currentPath,
                                       @NotNull String checkpoint1,
                                       @NotNull String checkpoint2,
                                       long diffProcessingTime,
                                       @NotNull SerialNumberGenerator serialNumberGenerator) {
        this.indexWriter = indexWriter;
        this.currentPath = currentPath;
        this.checkpoint1 = checkpoint1;
        this.checkpoint2 = checkpoint2;
        this.diffProcessingTime = diffProcessingTime;
        this.serialNumberGenerator = serialNumberGenerator;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        // Nothing to do on enter
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        // Commit happens externally
    }
    
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // Property change indicates node changed - record it
        recordChange();
    }
    
    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        // Property change indicates node changed - record it
        recordChange();
    }
    
    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // Property deletion indicates node changed - record it
        recordChange();
    }
    
    @Override
    @Nullable
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        // Child added - record this path and traverse children
        recordChange();
        return childEditor(name);
    }
    
    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // Child changed - record this path and traverse children
        recordChange();
        return childEditor(name);
    }
    
    @Override
    @Nullable
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // Child deleted - record this path (no need to traverse - node is gone)
        String childPath = buildChildPath(name);
        recordChangeAtPath(childPath);
        return null; // Don't traverse deleted nodes
    }
    
    /**
     * Records a change at the current path.
     */
    private void recordChange() throws CommitFailedException {
        recordChangeAtPath(currentPath);
    }
    
    /**
     * Records a change at the specified path.
     * 
     * @param path the absolute path of the changed node
     */
    private void recordChangeAtPath(String path) throws CommitFailedException {
        try {
            long serialNumber = serialNumberGenerator.next();
            
            Document doc = new Document();
            
            // ct:path - for lookup and deduplication
            doc.add(new StringField(FIELD_PATH, path, Field.Store.YES));
            
            // ct:checkpoint1 - first checkpoint in diff range (for analysis)
            doc.add(new StringField(FIELD_CHECKPOINT1, checkpoint1, Field.Store.YES));
            
            // ct:checkpoint2 - last checkpoint in diff range (for analysis)
            doc.add(new StringField(FIELD_CHECKPOINT2, checkpoint2, Field.Store.YES));
            
            // ct:diffProcessingTime - for ordering and queries (Lucene 4.7 uses LongField)
            doc.add(new LongField(FIELD_DIFF_PROCESSING_TIME, diffProcessingTime, Field.Store.YES));
            
            // ct:serialNumber - for unique ordering within same timestamp (Lucene 4.7 uses LongField)
            doc.add(new LongField(FIELD_SERIAL_NUMBER, serialNumber, Field.Store.YES));
            
            indexWriter.addDocument(doc);
            entriesWritten++;
            
            if (entriesWritten % 10000 == 0) {
                LOG.info("Change tracking: recorded {} changes", entriesWritten);
            }
            
        } catch (IOException e) {
            throw new CommitFailedException(
                CommitFailedException.STATE, 1,
                "Failed to write change tracking entry for path: " + path, e);
        }
    }
    
    /**
     * Creates a child editor for traversing a child node.
     */
    private Editor childEditor(String name) {
        String childPath = buildChildPath(name);
        return new ChangeTrackingIndexEditor(
            indexWriter, childPath, checkpoint1, checkpoint2, 
            diffProcessingTime, serialNumberGenerator);
    }
    
    /**
     * Builds the full path for a child node.
     */
    private String buildChildPath(String name) {
        if ("/".equals(currentPath)) {
            return "/" + name;
        } else {
            return currentPath + "/" + name;
        }
    }
    
    /**
     * @return the number of change entries written by this editor
     */
    public long getEntriesWritten() {
        return entriesWritten;
    }
    
    /**
     * Generates unique serial numbers for changes within the same timestamp.
     * Thread-safe for use across multiple editors in a single diff run.
     */
    private static class SerialNumberGenerator {
        private final long timestamp;
        private long counter = 0;
        
        SerialNumberGenerator(long timestamp) {
            this.timestamp = timestamp;
        }
        
        synchronized long next() {
            return counter++;
        }
        
        long getCount() {
            return counter;
        }
    }
}

