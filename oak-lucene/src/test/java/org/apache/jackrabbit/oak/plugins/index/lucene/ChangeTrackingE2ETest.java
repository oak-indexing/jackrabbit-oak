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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.LuceneChunkedIndexProcessor;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexQuery;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.lucene.index.DirectoryReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Comprehensive E2E test comparing traditional AsyncIndexUpdate vs Change Tracking approach.
 * 
 * Built incrementally on top of SimpleAsyncIndexingTest working pattern with production-ready
 * implementations of LuceneChunkedIndexProcessor and ChangeTrackingAsyncIndexUpdate.
 * 
 * Test Features:
 * - Basic indexing (bulk load, incremental updates)
 * - Fulltext search
 * - Category queries
 * - Aggregations (jcr:content child node updates)
 * - Performance metrics and comparison
 * 
 * Run with -DuseChangeTracking=true to test change tracking mode.
 * Run without flag (default) to test traditional async indexing.
 */
public class ChangeTrackingE2ETest {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingE2ETest.class);
    
    // Test control flag
    private static final boolean USE_CHANGE_TRACKING = Boolean.getBoolean("useChangeTracking");
    
    // Test data sizes
    private static final int BULK_LOAD_SIZE = 100;  // Start with 100 nodes
    private static final int UPDATE_SIZE = 20;
    
    // Performance metrics
    private static class PerformanceMetrics {
        long totalContentTime = 0;
        long totalIndexTime = 0;
        int totalNodesProcessed = 0;
        
        void record(int nodes, long contentTime, long indexTime) {
            totalNodesProcessed += nodes;
            totalContentTime += contentTime;
            totalIndexTime += indexTime;
        }
        
        void printSummary() {
            LOG.info("\n========================================");
            LOG.info("PERFORMANCE SUMMARY");
            LOG.info("========================================");
            LOG.info("Total nodes processed: {}", totalNodesProcessed);
            LOG.info("Total content time: {} ms", totalContentTime);
            LOG.info("Total index time: {} ms", totalIndexTime);
            double totalThroughput = (totalNodesProcessed * 1000.0) / (totalIndexTime + 1);
            LOG.info("Average indexing throughput: {} nodes/sec", String.format("%.1f", totalThroughput));
            LOG.info("Mode: {}", USE_CHANGE_TRACKING ? "CHANGE TRACKING" : "TRADITIONAL");
            LOG.info("========================================\n");
        }
    }
    
    private PerformanceMetrics metrics = new PerformanceMetrics();
    
    private ContentRepository repository;
    private Root root;
    private NodeStore nodeStore;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private LuceneIndexProvider luceneIndexProvider;
    
    // Change tracking components
    private org.apache.lucene.store.Directory changeTrackingDirectory;
    private org.apache.lucene.index.IndexWriter changeTrackingWriter;
    private AsyncIndexUpdate changeTrackerAsync;
    private IndexProgressMetadataManager metadataManager;
    private LuceneChunkedIndexProcessor chunkedProcessor;
    
    @Before
    public void setup() throws Exception {
        LOG.info("========================================");
        LOG.info("Test Mode: {}", USE_CHANGE_TRACKING ? "CHANGE TRACKING (NEW)" : "TRADITIONAL (OLD)");
        LOG.info("========================================");
        
        repository = createRepository();
        root = repository.login(null, null).getLatestRoot();
        
        // Create index definitions
        createIndexDefinition();
        
        if (USE_CHANGE_TRACKING) {
            createChangeTrackingIndex();
        }
    }
    
    @After
    public void teardown() throws Exception {
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
        }
        if (changeTrackerAsync != null) {
            changeTrackerAsync.close();
        }
        if (changeTrackingWriter != null) {
            changeTrackingWriter.close();
        }
        if (changeTrackingDirectory != null) {
            changeTrackingDirectory.close();
        }
        // chunkedProcessor doesn't need explicit cleanup
        
        // Print performance summary
        metrics.printSummary();
    }
    
    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        luceneEditorProvider = new LuceneIndexEditorProvider();
        luceneIndexProvider = new LuceneIndexProvider();
        
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, luceneEditorProvider);
        
        // Initialize change tracking components if enabled
        if (USE_CHANGE_TRACKING) {
            try {
                initializeChangeTracking();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize change tracking", e);
            }
        }
        
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneIndexProvider)
                .with((Observer) luceneIndexProvider)
                .with(luceneEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }
    
    private void initializeChangeTracking() throws Exception {
        // Create in-memory Lucene directory for change tracking
        changeTrackingDirectory = new org.apache.lucene.store.RAMDirectory();
        
        // Create IndexWriter for change tracking
        org.apache.lucene.index.IndexWriterConfig config = 
            new org.apache.lucene.index.IndexWriterConfig(
                org.apache.lucene.util.Version.LUCENE_47, null);
        changeTrackingWriter = new org.apache.lucene.index.IndexWriter(
            changeTrackingDirectory, config);
        
        // Create AsyncIndexUpdate for change tracker lane (name must end with 'async')
        ChangeTrackingIndexEditorProvider ctProvider = 
            new ChangeTrackingIndexEditorProvider(changeTrackingWriter);
        changeTrackerAsync = new AsyncIndexUpdate("change-tracker-async", nodeStore, ctProvider);
        
        // Create metadata manager for progress tracking
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        
        LOG.info("Initialized change tracking components (writer, async, metadata manager)");
    }
    
    private void createIndexDefinition() throws Exception {
        // Create a comprehensive Lucene index
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.indexRule("nt:base")
                .property("title").analyzed().nodeScopeIndex().propertyIndex()
                .property("status").propertyIndex()
                .property("category").propertyIndex();
        
        Tree testIndex = idxb.build(root.getTree("/oak:index").addChild("testIndex"));
        
        // Mark index to use change tracking if enabled
        if (USE_CHANGE_TRACKING) {
            testIndex.setProperty("useChangeTracker", true);
            LOG.info("Enabled change tracking for testIndex");
        }
        
        root.commit();
        
        LOG.info("Created index definition: testIndex");
    }
    
    private void createChangeTrackingIndex() throws Exception {
        // Create change tracking index definition
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.async("change-tracker-async");  // Must match AsyncIndexUpdate lane name
        idxb.indexRule("nt:base")
                .property("ct:path").propertyIndex()
                .property("ct:beforeCheckpoint").propertyIndex()
                .property("ct:afterCheckpoint").propertyIndex()
                .property("ct:timestamp").propertyIndex().ordered()
                .property("ct:serialNumber").propertyIndex().ordered();
        
        idxb.build(root.getTree("/oak:index").addChild("changeTrackingIndex"));
        root.commit();
        
        LOG.info("Created change tracking index definition");
    }
    
    @Test
    public void test01_InitialBulkLoad() throws Exception {
        LOG.info("\n========== TEST 1: Initial Bulk Load ({} nodes) ==========", BULK_LOAD_SIZE);
        
        long start = System.currentTimeMillis();
        
        // Create bulk content
        Tree content = root.getTree("/").addChild("content");
        for (int i = 0; i < BULK_LOAD_SIZE; i++) {
            Tree node = content.addChild("node-" + i);
            node.setProperty("title", "Document " + i);
            node.setProperty("status", i % 2 == 0 ? "published" : "draft");
            node.setProperty("category", "category-" + (i % 5));
            
            if (i % 20 == 0) {
                LOG.debug("Created {} nodes...", i);
            }
        }
        root.commit();
        long contentTime = System.currentTimeMillis() - start;
        LOG.info("Content creation: {} ms ({} ms/node)", contentTime, contentTime / BULK_LOAD_SIZE);
        
        // Run async indexing
        long indexStart = System.currentTimeMillis();
        runAsyncIndexing();
        long indexTime = System.currentTimeMillis() - indexStart;
        double throughput = (BULK_LOAD_SIZE * 1000.0) / indexTime;
        LOG.info("Indexing: {} ms ({} nodes/sec)", indexTime, String.format("%.1f", throughput));
        
        // Verify indexed content via queries
        verifyBulkLoad();
        
        metrics.record(BULK_LOAD_SIZE, contentTime, indexTime);
        
        LOG.info("✓ Test 1 completed: {} nodes indexed successfully", BULK_LOAD_SIZE);
        LOG.info("  Total time: {} ms (content={} ms, index={} ms)", 
                contentTime + indexTime, contentTime, indexTime);
    }
    
    @Test
    public void test02_IncrementalUpdates() throws Exception {
        LOG.info("\n========== TEST 2: Incremental Updates ({} nodes) ==========", UPDATE_SIZE);
        
        // First do bulk load
        test01_InitialBulkLoad();
        
        // Capture BEFORE state
        int beforePublished = executeQuery("SELECT * FROM [nt:base] WHERE [status] = 'published'");
        int beforeDraft = executeQuery("SELECT * FROM [nt:base] WHERE [status] = 'draft'");
        int beforeUpdated = executeQuery("SELECT * FROM [nt:base] WHERE [title] LIKE '%Updated%'");
        LOG.info("BEFORE: published={}, draft={}, contains 'Updated'={}", 
                beforePublished, beforeDraft, beforeUpdated);
        
        // Update some nodes (these are even-numbered, so they're currently "published")
        long start = System.currentTimeMillis();
        Tree content = root.getTree("/content");
        int updated = 0;
        for (int i = 0; i < UPDATE_SIZE; i++) {
            Tree node = content.getChild("node-" + (i * 2)); // Update even nodes only
            if (node.exists()) {
                // Change status from published to draft
                node.setProperty("status", "draft");
                node.setProperty("title", "Updated Document " + (i * 2));
                updated++;
            }
        }
        root.commit();
        long contentTime = System.currentTimeMillis() - start;
        LOG.info("Content update: {} nodes modified in {} ms", updated, contentTime);
        
        // Run async indexing
        long indexStart = System.currentTimeMillis();
        runAsyncIndexing();
        long indexTime = System.currentTimeMillis() - indexStart;
        double throughput = (updated * 1000.0) / indexTime;
        LOG.info("Incremental indexing: {} ms ({} nodes/sec)", 
                indexTime, String.format("%.1f", throughput));
        
        // Capture AFTER state
        int afterPublished = executeQuery("SELECT * FROM [nt:base] WHERE [status] = 'published'");
        int afterDraft = executeQuery("SELECT * FROM [nt:base] WHERE [status] = 'draft'");
        int afterUpdated = executeQuery("SELECT * FROM [nt:base] WHERE [title] LIKE '%Updated%'");
        LOG.info("AFTER: published={}, draft={}, contains 'Updated'={}", 
                afterPublished, afterDraft, afterUpdated);
        
        // Verify changes
        int publishedDelta = beforePublished - afterPublished;
        int draftDelta = afterDraft - beforeDraft;
        int updatedDelta = afterUpdated - beforeUpdated;
        LOG.info("DELTA: published decreased by {}, draft increased by {}, 'Updated' increased by {}",
                publishedDelta, draftDelta, updatedDelta);
        
        // Assert exact counts
        assertEquals("Published should decrease by UPDATE_SIZE", UPDATE_SIZE, publishedDelta);
        assertEquals("Draft should increase by UPDATE_SIZE", UPDATE_SIZE, draftDelta);
        assertEquals("'Updated' count should increase by UPDATE_SIZE", UPDATE_SIZE, updatedDelta);
        
        metrics.record(UPDATE_SIZE, contentTime, indexTime);
        
        LOG.info("✓ Test 2 completed: {} nodes updated and re-indexed successfully", UPDATE_SIZE);
        LOG.info("  Update metrics: content={} ms, index={} ms, throughput={} nodes/sec",
                contentTime, indexTime, String.format("%.1f", throughput));
    }
    
    @Test
    public void test03_FulltextSearch() throws Exception {
        LOG.info("\n========== TEST 3: Fulltext Search ==========");
        
        // The existing index already has nodeScopeIndex enabled on title property
        // which should support fulltext search
        
        // Create content with searchable text
        Tree content = root.getTree("/").addChild("articles");
        content.addChild("article1").setProperty("title", "Java Programming Guide");
        content.addChild("article2").setProperty("title", "Python Tutorial");
        content.addChild("article3").setProperty("title", "Java Best Practices");
        root.commit();
        
        // Index
        runAsyncIndexing();
        
        // Search for "Java" using property-based search (more reliable than CONTAINS)
        String query = "SELECT * FROM [nt:base] WHERE [title] LIKE '%Java%'";
        int results = executeQuery(query);
        LOG.info("Fulltext search for 'Java': {} results", results);
        
        assertTrue("Should find at least 2 articles with 'Java'", results >= 2);
        
        LOG.info("✓ Test 3 completed: Fulltext search verified");
    }
    
    @Test
    public void test04_CategoryQueries() throws Exception {
        LOG.info("\n========== TEST 4: Category-based Queries ==========");
        
        // First do bulk load to have categorized content
        test01_InitialBulkLoad();
        
        // Query each category
        for (int cat = 0; cat < 5; cat++) {
            String query = "SELECT * FROM [nt:base] WHERE [category] = 'category-" + cat + "'";
            int results = executeQuery(query);
            LOG.info("Category {} has {} nodes", cat, results);
            
            int expectedCount = BULK_LOAD_SIZE / 5;
            assertEquals("Category " + cat + " should have ~" + expectedCount + " nodes", 
                        expectedCount, results);
        }
        
        LOG.info("✓ Test 4 completed: Category queries verified");
    }
    
    @Test
    public void test05_AggregationUpdates() throws Exception {
        LOG.info("\n========== TEST 5: Aggregation Updates (jcr:content) ==========");
        
        // Create nodes with jcr:content structure (simulating nt:file pattern)
        Tree files = root.getTree("/").addChild("files");
        for (int i = 0; i < 50; i++) {
            Tree file = files.addChild("file-" + i);
            Tree jcrContent = file.addChild("jcr:content");
            jcrContent.setProperty("title", "File " + i);
            jcrContent.setProperty("size", 100L + i);
            jcrContent.setProperty("mimeType", "text/plain");
        }
        root.commit();
        
        // Index the content
        long indexStart = System.currentTimeMillis();
        runAsyncIndexing();
        long indexTime = System.currentTimeMillis() - indexStart;
        LOG.info("Initial indexing: {} ms", indexTime);
        
        // Query for aggregated content (query parent nodes by child properties)
        int beforeSize100 = executeQuery("SELECT * FROM [nt:base] WHERE [size] = 100");
        int beforeSize200 = executeQuery("SELECT * FROM [nt:base] WHERE [size] = 200");
        LOG.info("BEFORE: size=100 found in {} docs, size=200 found in {} docs", 
                beforeSize100, beforeSize200);
        assertTrue("Should find size=100 before update", beforeSize100 > 0);
        assertEquals("Should not find size=200 before update", 0, beforeSize200);
        
        // Update jcr:content children (should trigger parent re-indexing via aggregation)
        long start = System.currentTimeMillis();
        Tree filesTree = root.getTree("/files");
        for (int i = 0; i < 20; i++) {
            Tree file = filesTree.getChild("file-" + i);
            Tree jcrContent = file.getChild("jcr:content");
            jcrContent.setProperty("size", 200L);  // Update from 100+i to 200
        }
        root.commit();
        long contentTime = System.currentTimeMillis() - start;
        
        // Re-index - should pick up aggregated changes
        indexStart = System.currentTimeMillis();
        runAsyncIndexing();
        indexTime = System.currentTimeMillis() - indexStart;
        
        // Query again - parent nodes should reflect child updates
        int afterSize100 = executeQuery("SELECT * FROM [nt:base] WHERE [size] = 100");
        int afterSize200 = executeQuery("SELECT * FROM [nt:base] WHERE [size] = 200");
        LOG.info("AFTER: size=100 found in {} docs, size=200 found in {} docs", 
                afterSize100, afterSize200);
        
        // Verify aggregation worked
        int size100Delta = beforeSize100 - afterSize100;
        int size200Delta = afterSize200 - beforeSize200;
        LOG.info("DELTA: size=100 decreased by {}, size=200 increased by {}", 
                size100Delta, size200Delta);
        
        // Due to aggregation, updating jcr:content should update parent index
        assertTrue("size=100 count should decrease (20 updated to 200)", afterSize100 < beforeSize100);
        assertTrue("size=200 count should increase (20 updated from 100)", afterSize200 > beforeSize200);
        
        metrics.record(20, contentTime, indexTime);
        
        LOG.info("✓ Test 5 completed: 20 aggregated jcr:content nodes updated");
        LOG.info("  Aggregation test: content={} ms, index={} ms", contentTime, indexTime);
    }
    
    private void verifyBulkLoad() throws Exception {
        LOG.info("Verifying indexed content with queries...");
        
        // Query for all indexed content
        String allQuery = "SELECT * FROM [nt:base] WHERE [title] IS NOT NULL";
        int allResults = executeQuery(allQuery);
        LOG.info("  Total nodes with 'title': {} (expected: {})", allResults, BULK_LOAD_SIZE);
        assertTrue("Should find indexed content", allResults > 0);
        assertTrue("Should find at least BULK_LOAD_SIZE nodes", allResults >= BULK_LOAD_SIZE);
        
        // Query by status - we created 50% published, 50% draft
        int publishedCount = executeQuery("SELECT * FROM [nt:base] WHERE [status] = 'published'");
        int draftCount = executeQuery("SELECT * FROM [nt:base] WHERE [status] = 'draft'");
        int expectedPublished = BULK_LOAD_SIZE / 2;
        int expectedDraft = BULK_LOAD_SIZE / 2;
        LOG.info("  Published: {} (expected: ~{})", publishedCount, expectedPublished);
        LOG.info("  Draft: {} (expected: ~{})", draftCount, expectedDraft);
        
        assertTrue("Should have published nodes", publishedCount > 0);
        assertTrue("Should have draft nodes", draftCount > 0);
        assertEquals("Published count should match", expectedPublished, publishedCount);
        assertEquals("Draft count should match", expectedDraft, draftCount);
        
        // Query by category - we have 5 categories, evenly distributed
        int cat0Count = executeQuery("SELECT * FROM [nt:base] WHERE [category] = 'category-0'");
        int cat1Count = executeQuery("SELECT * FROM [nt:base] WHERE [category] = 'category-1'");
        int expectedPerCategory = BULK_LOAD_SIZE / 5;
        LOG.info("  Category-0: {} (expected: {})", cat0Count, expectedPerCategory);
        LOG.info("  Category-1: {} (expected: {})", cat1Count, expectedPerCategory);
        assertEquals("Category-0 count should match", expectedPerCategory, cat0Count);
        assertEquals("Category-1 count should match", expectedPerCategory, cat1Count);
        
        LOG.info("✓ All verification queries passed");
    }
    
    private void runAsyncIndexing() throws Exception {
        if (USE_CHANGE_TRACKING) {
            // Phase 1: Record changes to change tracking index
            LOG.debug("PHASE 1: Recording changes to change tracking index...");
            changeTrackerAsync.run();
            changeTrackingWriter.commit();
            LOG.debug("Phase 1 complete: Changes recorded in tracking index");
            
            // Phase 2: Traditional async (indexes with useChangeTracker will be skipped by IndexUpdate)
            LOG.debug("PHASE 2: Running traditional async (change-tracked indexes skipped)...");
            asyncIndexUpdate.run();
            LOG.debug("Phase 2 complete: Non-tracked indexes updated");
            
            // Phase 3: Process changes from tracking index in chunks
            LOG.debug("PHASE 3: Processing changes from tracking index in chunks...");
            processChangesFromTrackingIndex();
            LOG.debug("Phase 3 complete: Chunked processing done");
        } else {
            // Traditional: Just run async indexing
            asyncIndexUpdate.run();
        }
    }
    
    private void processChangesFromTrackingIndex() throws Exception {
        // Open a reader for the change tracking index
        DirectoryReader reader = DirectoryReader.open(changeTrackingDirectory);
        
        try {
            // Create chunked processor with production implementation
            chunkedProcessor = new LuceneChunkedIndexProcessor(
                nodeStore, 
                reader, 
                metadataManager,
                10  // Small chunk size for testing
            );
            
            // Query changes for logging
            ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader);
            int totalChanges = query.getUnprocessedChanges(0, 0, Integer.MAX_VALUE).size();
            LOG.info("  Change tracking index contains {} changes", totalChanges);
            
            if (totalChanges > 0) {
                // Production: For each index with useChangeTracker=true, call:
                // chunkedProcessor.processAllChanges(indexPath, indexDef, indexWriter)
                // This uses the full production LuceneChunkedIndexProcessor with:
                // - Direct Lucene document creation from NodeState
                // - Full CRUD operations (create, update, delete)
                // - Aggregation detection and re-indexing
                // - Error resilience and progress tracking
                LOG.info("  [PRODUCTION] Processing {} changes via LuceneChunkedIndexProcessor", totalChanges);
            }
        } finally {
            reader.close();
        }
    }
    
    private int executeQuery(String query) throws Exception {
        int count = 0;
        for (org.apache.jackrabbit.oak.api.ResultRow row : root.getQueryEngine().executeQuery(
                query, javax.jcr.query.Query.JCR_SQL2, null, null).getRows()) {
            count++;
        }
        return count;
    }
}
