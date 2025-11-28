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

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;

import java.io.IOException;

/**
 * Simple wrapper that adapts a Lucene IndexWriter to the LuceneIndexWriter interface.
 * Used by ChangeTrackingAsyncIndexUpdate for direct document writing.
 * 
 * <p>This is similar to NRTIndexWriter but designed for change tracking use case
 * where we need to properly update/delete documents based on path.
 */
class SimpleIndexWriterWrapper implements LuceneIndexWriter {
    
    private final IndexWriter writer;
    private boolean indexUpdated = false;
    
    public SimpleIndexWriterWrapper(IndexWriter writer) {
        this.writer = writer;
    }
    
    @Override
    public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
        // Create a Term for the path field to identify which document to update
        Term pathTerm = new Term(FieldNames.PATH, path);
        writer.updateDocument(pathTerm, doc);
        indexUpdated = true;
    }
    
    @Override
    public void deleteDocuments(String path) throws IOException {
        // Delete the exact path
        Term pathTerm = new Term(FieldNames.PATH, path);
        writer.deleteDocuments(pathTerm);
        
        // Also delete all child paths
        writer.deleteDocuments(new PrefixQuery(new Term(FieldNames.PATH, path + "/")));
        indexUpdated = true;
    }
    
    @Override
    public boolean close(long timestamp) throws IOException {
        // Don't close the underlying writer - let the caller manage it
        // Just return whether we modified the index
        return indexUpdated;
    }
}

