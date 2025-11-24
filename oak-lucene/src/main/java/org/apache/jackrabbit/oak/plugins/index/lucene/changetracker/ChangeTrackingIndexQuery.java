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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Queries the change tracking Lucene index to retrieve unprocessed changes.
 * 
 * <p>This component uses composite key queries to efficiently find changes
 * that haven't been processed yet by a specific index:
 * 
 * <pre>
 * WHERE (ct:diffProcessingTime, ct:serialNumber) > (lastProcessedTimestamp, lastProcessedSerialNumber)
 * ORDER BY ct:diffProcessingTime ASC, ct:serialNumber ASC
 * LIMIT chunkSize
 * </pre>
 * 
 * <p>The query ensures:
 * <ul>
 *   <li>No changes are missed (precise continuation point)</li>
 *   <li>Deterministic ordering (composite key)</li>
 *   <li>Efficient range scans (Lucene NumericRangeQuery)</li>
 * </ul>
 */
public class ChangeTrackingIndexQuery implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingIndexQuery.class);
    
    private static final String FIELD_PATH = "ct:path";
    private static final String FIELD_CHECKPOINT1 = "ct:checkpoint1";
    private static final String FIELD_CHECKPOINT2 = "ct:checkpoint2";
    private static final String FIELD_DIFF_PROCESSING_TIME = "ct:diffProcessingTime";
    private static final String FIELD_SERIAL_NUMBER = "ct:serialNumber";
    
    private final IndexReader indexReader;
    private final IndexSearcher indexSearcher;
    
    /**
     * Creates a query component for the change tracking index.
     * 
     * @param indexReader the Lucene index reader
     */
    public ChangeTrackingIndexQuery(@NotNull IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexSearcher = new IndexSearcher(indexReader);
    }
    
    /**
     * Retrieves unprocessed changes after the given position.
     * 
     * @param afterTimestamp the last processed timestamp (exclusive)
     * @param afterSerialNumber the last processed serial number (exclusive)
     * @param limit the maximum number of entries to return
     * @return list of change entries, ordered by (timestamp, serialNumber)
     * @throws IOException if the query fails
     */
    @NotNull
    public List<ChangeEntry> getUnprocessedChanges(long afterTimestamp,
                                                     long afterSerialNumber,
                                                     int limit) throws IOException {
        Query query = buildCompositeQuery(afterTimestamp, afterSerialNumber);
        
        // Sort by timestamp, then serial number
        Sort sort = new Sort(
            new SortField(FIELD_DIFF_PROCESSING_TIME, SortField.Type.LONG),
            new SortField(FIELD_SERIAL_NUMBER, SortField.Type.LONG)
        );
        
        // In Lucene 4.7, search() returns TopDocs (not TopFieldDocs)
        TopDocs topDocs = indexSearcher.search(query, limit, sort);
        
        List<ChangeEntry> results = new ArrayList<>(topDocs.scoreDocs.length);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = indexSearcher.doc(scoreDoc.doc);
            ChangeEntry entry = parseDocument(doc);
            if (entry != null) {
                results.add(entry);
            }
        }
        
        LOG.debug("Found {} unprocessed changes after timestamp={}, serial={}",
                results.size(), afterTimestamp, afterSerialNumber);
        
        return results;
    }
    
    /**
     * Counts the number of unprocessed changes.
     * 
     * @param afterTimestamp the last processed timestamp (exclusive)
     * @param afterSerialNumber the last processed serial number (exclusive)
     * @return the count of unprocessed entries
     * @throws IOException if the query fails
     */
    public int countUnprocessedChanges(long afterTimestamp, long afterSerialNumber) throws IOException {
        Query query = buildCompositeQuery(afterTimestamp, afterSerialNumber);
        TotalHitCountCollector collector = new TotalHitCountCollector();
        indexSearcher.search(query, collector);
        return collector.getTotalHits();
    }
    
    /**
     * Gets the oldest unprocessed change (lowest timestamp/serial).
     * 
     * @return the oldest change entry, or null if none exist
     * @throws IOException if the query fails
     */
    @NotNull
    public ChangeEntry getOldestChange() throws IOException {
        // In Lucene 4.7, use MatchAllDocsQuery from org.apache.lucene.search
        Query query = new org.apache.lucene.search.MatchAllDocsQuery();
        
        Sort sort = new Sort(
            new SortField(FIELD_DIFF_PROCESSING_TIME, SortField.Type.LONG),
            new SortField(FIELD_SERIAL_NUMBER, SortField.Type.LONG)
        );
        
        // In Lucene 4.7, search() returns TopDocs (not TopFieldDocs)
        TopDocs topDocs = indexSearcher.search(query, 1, sort);
        
        if (topDocs.scoreDocs.length == 0) {
            return null;
        }
        
        Document doc = indexSearcher.doc(topDocs.scoreDocs[0].doc);
        return parseDocument(doc);
    }
    
    /**
     * Gets the newest change (highest timestamp/serial).
     * 
     * @return the newest change entry, or null if none exist
     * @throws IOException if the query fails
     */
    @NotNull
    public ChangeEntry getNewestChange() throws IOException {
        // In Lucene 4.7, use MatchAllDocsQuery from org.apache.lucene.search
        Query query = new org.apache.lucene.search.MatchAllDocsQuery();
        
        Sort sort = new Sort(
            new SortField(FIELD_DIFF_PROCESSING_TIME, SortField.Type.LONG, true), // descending
            new SortField(FIELD_SERIAL_NUMBER, SortField.Type.LONG, true) // descending
        );
        
        // In Lucene 4.7, search() returns TopDocs (not TopFieldDocs)
        TopDocs topDocs = indexSearcher.search(query, 1, sort);
        
        if (topDocs.scoreDocs.length == 0) {
            return null;
        }
        
        Document doc = indexSearcher.doc(topDocs.scoreDocs[0].doc);
        return parseDocument(doc);
    }
    
    /**
     * Builds a composite key query: (timestamp, serial) > (after, afterSerial).
     * 
     * <p>Query Logic:
     * <ul>
     *   <li>Find all entries with timestamp > afterTimestamp</li>
     *   <li>OR entries with timestamp == afterTimestamp AND serialNumber > afterSerialNumber</li>
     * </ul>
     */
    private Query buildCompositeQuery(long afterTimestamp, long afterSerialNumber) {
        BooleanQuery outerQuery = new BooleanQuery();
        
        // Case 1: timestamp > afterTimestamp (any serial number is fine)
        // Lucene 4.7: Use NumericRangeQuery instead of LongPoint
        Query timestampGreaterQuery = NumericRangeQuery.newLongRange(
            FIELD_DIFF_PROCESSING_TIME,
            Math.addExact(afterTimestamp, 1),  // exclusive (min)
            Long.MAX_VALUE,                     // max
            true,                               // minInclusive
            true                                // maxInclusive
        );
        outerQuery.add(timestampGreaterQuery, BooleanClause.Occur.SHOULD);
        
        // Case 2: timestamp == afterTimestamp AND serialNumber > afterSerialNumber
        BooleanQuery sameTimestampQuery = new BooleanQuery();
        
        // Exact match on timestamp (range with same min/max)
        Query timestampEqualQuery = NumericRangeQuery.newLongRange(
            FIELD_DIFF_PROCESSING_TIME,
            afterTimestamp,  // min
            afterTimestamp,  // max
            true,            // minInclusive
            true             // maxInclusive
        );
        sameTimestampQuery.add(timestampEqualQuery, BooleanClause.Occur.MUST);
        
        Query serialGreaterQuery = NumericRangeQuery.newLongRange(
            FIELD_SERIAL_NUMBER,
            Math.addExact(afterSerialNumber, 1),  // exclusive (min)
            Long.MAX_VALUE,                        // max
            true,                                  // minInclusive
            true                                   // maxInclusive
        );
        sameTimestampQuery.add(serialGreaterQuery, BooleanClause.Occur.MUST);
        
        outerQuery.add(sameTimestampQuery, BooleanClause.Occur.SHOULD);
        
        return outerQuery;
    }
    
    /**
     * Parses a Lucene document into a ChangeEntry.
     */
    private ChangeEntry parseDocument(Document doc) {
        try {
            String path = doc.get(FIELD_PATH);
            String checkpoint1 = doc.get(FIELD_CHECKPOINT1);
            String checkpoint2 = doc.get(FIELD_CHECKPOINT2);
            
            // Get stored fields for numeric values
            Number timestampNum = doc.getField(FIELD_DIFF_PROCESSING_TIME).numericValue();
            Number serialNum = doc.getField(FIELD_SERIAL_NUMBER).numericValue();
            
            if (path == null || checkpoint1 == null || checkpoint2 == null ||
                timestampNum == null || serialNum == null) {
                LOG.warn("Incomplete document found in change tracking index");
                return null;
            }
            
            return new ChangeEntry(
                path,
                checkpoint1,
                checkpoint2,
                timestampNum.longValue(),
                serialNum.longValue()
            );
        } catch (Exception e) {
            LOG.error("Failed to parse change tracking document", e);
            return null;
        }
    }
    
    /**
     * Closes the index reader.
     */
    public void close() throws IOException {
        if (indexReader != null) {
            indexReader.close();
        }
    }
}

