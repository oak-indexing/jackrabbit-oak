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
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Editor that writes change tracking entries to a dedicated Lucene index.
 * 
 * <p>This editor is invoked during the async diff process and records the path
 * of every changed node along with timestamp and serial number for ordering.
 * 
 * <p>The change tracking index enables chunked processing of repository changes
 * without needing to re-diff the entire tree for each index update cycle.
 * 
 * <p><strong>Key Features:</strong>
 * <ul>
 *   <li>Lightweight - only tracks paths, not full node state or checkpoints</li>
 *   <li>Serial number generation for unique ordering</li>
 *   <li>Timestamp-based ordering and retention</li>
 *   <li>Fast writes using Lucene</li>
 * </ul>
 * 
 * <p><strong>Design Note:</strong> We do NOT store checkpoint IDs because:
 * <ul>
 *   <li>Change entries don't contain node content, just paths</li>
 *   <li>Checkpoints are managed at the processing level, not entry level</li>
 *   <li>Storing checkpoints would complicate cleanup and add unnecessary data</li>
 * </ul>
 */
public class ChangeTrackingIndexEditor implements Editor {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingIndexEditor.class);
    
    // Field names in the Lucene index
    private static final String FIELD_PATH = "ct:path";
    private static final String FIELD_DIFF_PROCESSING_TIME = "ct:diffProcessingTime";
    private static final String FIELD_SERIAL_NUMBER = "ct:serialNumber";
    
    private final AsyncChangeTrackingWriter writer;
    private final String currentPath;
    private final long diffProcessingTime;
    
    // Serial number management (shared across all editors in this diff run)
    private final SerialNumberGenerator serialNumberGenerator;
    
    private long entriesWritten = 0;
    
    /**
     * Flag to debounce multiple writes for the same node (e.g. multiple property changes).
     */
    private boolean changeRecorded = false;
    
    /**
     * Creates the root change tracking editor for a diff run.
     * 
     * @param indexWriter the Lucene index writer for the change tracking index
     * @param diffProcessingTime the millisecond timestamp for this diff run
     */
    public ChangeTrackingIndexEditor(@NotNull IndexWriter indexWriter,
                                      long diffProcessingTime) {
        this(new AsyncChangeTrackingWriter(indexWriter), "/", diffProcessingTime, 
             new SerialNumberGenerator(diffProcessingTime));
    }
    
    /**
     * Internal constructor for creating child editors.
     */
    private ChangeTrackingIndexEditor(@NotNull AsyncChangeTrackingWriter writer,
                                       @NotNull String currentPath,
                                       long diffProcessingTime,
                                       @NotNull SerialNumberGenerator serialNumberGenerator) {
        this.writer = writer;
        this.currentPath = currentPath;
        this.diffProcessingTime = diffProcessingTime;
        this.serialNumberGenerator = serialNumberGenerator;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        // Nothing to do on enter
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        // If this is the root editor, flush the async writer
        if ("/".equals(currentPath)) {
            writer.flush();
        }
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
        // Child added - don't record PARENT path. Return child editor to traverse and record child path.
        return childEditor(name);
    }
    
    @Override
    @Nullable
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // Child changed - don't record PARENT path. Return child editor to traverse and record child path.
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
        // Debounce: Only record once per node visit
        if (changeRecorded) {
            return;
        }
        recordChangeAtPath(currentPath);
        changeRecorded = true;
    }
    
    /**
     * Records a change at the specified path.
     * 
     * @param path the absolute path of the changed node
     */
    private void recordChangeAtPath(String path) throws CommitFailedException {
        long serialNumber = serialNumberGenerator.next();
        
        Document doc = new Document();
        
        // ct:path - the changed path
        doc.add(new StringField(FIELD_PATH, path, Field.Store.YES));
        
        // ct:diffProcessingTime - for ordering and retention (Lucene 4.7 uses LongField)
        doc.add(new LongField(FIELD_DIFF_PROCESSING_TIME, diffProcessingTime, Field.Store.YES));
        
        // ct:serialNumber - for unique ordering within same timestamp (Lucene 4.7 uses LongField)
        doc.add(new LongField(FIELD_SERIAL_NUMBER, serialNumber, Field.Store.YES));
        
        // Use async writer to avoid blocking traversal
        writer.add(doc, new Term(FIELD_PATH, path));
        entriesWritten++;
        
        if (entriesWritten % 10000 == 0) {
            LOG.info("Change tracking: recorded {} changes", entriesWritten);
        }
    }
    
    /**
     * Creates a child editor for traversing a child node.
     */
    private Editor childEditor(String name) {
        String childPath = buildChildPath(name);
        return new ChangeTrackingIndexEditor(
            writer, childPath, 
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
     * Async writer that buffers Lucene updates and writes them in a background thread.
     * Decouples traversal CPU cost from Lucene I/O/locking.
     */
    private static class AsyncChangeTrackingWriter {
        private final IndexWriter indexWriter;
        private final BlockingQueue<WriteOp> queue = new LinkedBlockingQueue<>(5000);
        private final Thread worker;
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        
        private static class WriteOp {
            final Document doc;
            final Term term;
            WriteOp(Document doc, Term term) { this.doc = doc; this.term = term; }
        }
        
        private static final WriteOp STOP_OP = new WriteOp(null, null);

        AsyncChangeTrackingWriter(IndexWriter indexWriter) {
            this.indexWriter = indexWriter;
            this.worker = new Thread(this::runWorker, "ChangeTracking-AsyncWriter");
            this.worker.setDaemon(true);
            this.worker.start();
        }

        void add(Document doc, Term term) throws CommitFailedException {
            checkError();
            try {
                queue.put(new WriteOp(doc, term));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CommitFailedException(CommitFailedException.STATE, 1, "Interrupted while queuing change", e);
            }
        }

        void flush() throws CommitFailedException {
            try {
                // Signal stop
                queue.put(STOP_OP);
                // Wait for worker to finish
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CommitFailedException(CommitFailedException.STATE, 1, "Interrupted while waiting for async writer", e);
            }
            checkError();
        }
        
        private void checkError() throws CommitFailedException {
            Throwable t = error.get();
            if (t != null) {
                throw new CommitFailedException(CommitFailedException.STATE, 1, "Async writer failed", t);
            }
        }
        
        private void runWorker() {
            try {
                while (true) {
                    WriteOp op = queue.take();
                    if (op == STOP_OP) {
                        break;
                    }
                    indexWriter.updateDocument(op.term, op.doc);
                }
            } catch (Throwable t) {
                error.set(t);
                // Drain queue to prevent producer blocking
                queue.clear(); 
            }
        }
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
