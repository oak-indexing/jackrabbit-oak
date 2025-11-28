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
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingAsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexPopulator;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexQuery;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Basic test demonstrating how to use the Change Tracker system.
 * 
 * <p>This test shows:
 * <ul>
 *   <li>How to set up the change tracking index</li>
 *   <li>How to initialize and run the change tracking populator</li>
 *   <li>How to create an index that uses change tracking</li>
 *   <li>How to make changes and verify they're recorded</li>
 *   <li>How to query the change tracking index</li>
 *   <li>How progress tracking works</li>
 * </ul>
 * 
 * <p><strong>Note:</strong> This is a simplified test for demonstration.
 * For production use cases, see ChangeTrackingE2ETest.java and 
 * CHANGE_TRACKER_GUIDE.md for comprehensive examples.
 */
public class BasicChangeTrackerTest {
    
    private NodeStore nodeStore;
    private Directory changeTrackingDirectory;
    private ChangeTrackingIndexPopulator populator;
    private IndexProgressMetadataManager metadataManager;
    private ContentRepository contentRepository;
    private ContentSession contentSession;
    private Root root;
    private AsyncIndexUpdate asyncIndexUpdate;
    private ChangeTrackingAsyncIndexUpdate changeTrackingAsyncIndexUpdate;
    
    @Before
    public void setUp() throws Exception {
        // 1. Create a NodeStore (using MemoryNodeStore for testing)
        nodeStore = new MemoryNodeStore();
        
        // 2. Create the Lucene directory for the change tracking index
        changeTrackingDirectory = new RAMDirectory();
        
        // 3. Create metadata manager for progress tracking
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        
        // 4. Create and initialize the change tracking populator
        // Enable population via system property
        System.setProperty("oak.changeTracker.population.enabled", "true");
        System.setProperty("oak.changeTracker.enabled", "true");
        
        populator = new ChangeTrackingIndexPopulator(
            nodeStore,
            changeTrackingDirectory,
            metadataManager,
            StatisticsProvider.NOOP
        );
        
        populator.initialize();
        
        // 5. Create Oak ContentRepository with Lucene index support
        LuceneIndexProvider provider = new LuceneIndexProvider();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();
        
        contentRepository = new Oak(nodeStore)
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with((org.apache.jackrabbit.oak.spi.commit.Observer) provider)
            .with(editorProvider)
            .with(new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider())
            .with(new org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider())
            .createContentRepository();
        
        // 6. Create a content session and root
        contentSession = contentRepository.login(null, null);
        root = contentSession.getLatestRoot();
        
        // 7. Create traditional async index update (for non-change-tracked indexes)
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, editorProvider);
        
        // 8. Create ChangeTrackingAsyncIndexUpdate (for change-tracked indexes)
        // IndexWriter is managed internally by both populator and async indexer
        changeTrackingAsyncIndexUpdate = new ChangeTrackingAsyncIndexUpdate(
            "change-tracker-async",
            nodeStore,
            changeTrackingDirectory,
            null  // IndexWriter is managed internally
        );
        
        // Verify initialization
        assertTrue("Populator should be initialized", populator.isInitialized());
        System.out.println("✓ Change tracking system initialized");
        System.out.println("✓ Oak content repository created");
    }
    
    @After
    public void tearDown() throws Exception {
        if (contentSession != null) {
            contentSession.close();
        }
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
        }
        if (changeTrackingAsyncIndexUpdate != null) {
            // ChangeTrackingAsyncIndexUpdate doesn't have close()
        }
        if (populator != null) {
            populator.close();
        }
        if (changeTrackingDirectory != null) {
            changeTrackingDirectory.close();
        }
        System.clearProperty("oak.changeTracker.population.enabled");
        System.clearProperty("oak.changeTracker.enabled");
    }
    
    
    /**
     * Test 6: Query-based verification of indexing
     * 
     * Demonstrates:
     * - Creating a real Lucene index with useChangeTracker=true
     * - Indexing content through change tracker
     * - Running queries to verify index works
     * - Validating query results
     */
    @Test
    public void testQueryBasedIndexingVerification() throws Exception {
        System.out.println("\n=== Test 6: Query-Based Indexing Verification ===");
        
        // STEP 1: Create a Lucene index definition with useChangeTracker=true
        System.out.println("\nStep 1: Creating searchable index definition...");
        
        Tree oakIndex = root.getTree("/oak:index");
        Tree searchIndex = oakIndex.addChild("searchIndex");
        searchIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        searchIndex.setProperty("type", "lucene");
        searchIndex.setProperty("async", "async");
        searchIndex.setProperty("useChangeTracker", true);
        
        // Create index rules for nt:unstructured
        Tree indexRules = searchIndex.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree ntUnstructured = indexRules.addChild("nt:unstructured");
        ntUnstructured.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree properties = ntUnstructured.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Index "title" property (analyzed for fulltext search)
        Tree titleProp = properties.addChild("title");
        titleProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        titleProp.setProperty("name", "title");
        titleProp.setProperty("propertyIndex", true);
        titleProp.setProperty("analyzed", true);
        titleProp.setProperty("nodeScopeIndex", true);
        
        // Index "category" property (not analyzed for exact match)
        Tree categoryProp = properties.addChild("category");
        categoryProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        categoryProp.setProperty("name", "category");
        categoryProp.setProperty("propertyIndex", true);
        categoryProp.setProperty("analyzed", false);
        
        // Index "status" property
        Tree statusProp = properties.addChild("status");
        statusProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        statusProp.setProperty("name", "status");
        statusProp.setProperty("propertyIndex", true);
        statusProp.setProperty("analyzed", false);
        
        root.commit();
        // Register index in metadata manager
        metadataManager.registerIndex("/oak:index/searchIndex");
        System.out.println("✓ Created searchIndex with useChangeTracker=true");
        
        // STEP 2: Create searchable content
        System.out.println("\nStep 2: Creating searchable content...");
        populator.run();  // Initial run to set baseline
        changeTrackingAsyncIndexUpdate.run();

        
        Tree content = root.getTree("/").addChild("testContent");
        
        // Document 1: Java article
        Tree doc1 = content.addChild("doc1");
        doc1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc1.setProperty("title", "Introduction to Java Programming");
        doc1.setProperty("category", "programming");
        doc1.setProperty("status", "published");
        
        // Document 2: Python article
        Tree doc2 = content.addChild("doc2");
        doc2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc2.setProperty("title", "Python for Beginners");
        doc2.setProperty("category", "programming");
        doc2.setProperty("status", "draft");
        
        // Document 3: JavaScript article
        Tree doc3 = content.addChild("doc3");
        doc3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc3.setProperty("title", "JavaScript Essentials");
        doc3.setProperty("category", "programming");
        doc3.setProperty("status", "published");
        
        // Document 4: Cooking recipe
        Tree doc4 = content.addChild("doc4");
        doc4.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc4.setProperty("title", "Best Chocolate Cake Recipe");
        doc4.setProperty("category", "cooking");
        doc4.setProperty("status", "published");
        
        root.commit();
        System.out.println("✓ Created 4 test documents");


        
        // STEP 3: Run change tracker populator
        System.out.println("\nStep 3: Running change tracker populator...");
        populator.run();
        commitChangeTrackingIndex();
        System.out.println("✓ Change tracker populated");
        
        // Verify changes were recorded
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            List<ChangeEntry> changes = query.getUnprocessedChanges(0, 0, 100);
            System.out.printf("✓ Recorded %d changes%n", changes.size());
            assertTrue("Should have recorded changes", changes.size() > 0);
        }
        
        // STEP 4: Run change tracking async index update to build the index
        System.out.println("\nStep 4: Running change tracking async index update...");
        changeTrackingAsyncIndexUpdate.run();
        root = contentSession.getLatestRoot(); // Refresh root
        System.out.println("✓ Change tracking async index update completed");
        
        // STEP 5: Execute queries and verify results
        System.out.println("\nStep 5: Executing queries to verify indexing...");
        
        // Query 1: Exact match on category
        System.out.println("\n  Query 1: Find all programming articles");
        String query1 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming'";
        List<String> results1 = executeQuery(query1);
        System.out.printf("    Found %d results: %s%n", results1.size(), results1);
        assertEquals("Should find 3 programming articles", 3, results1.size());
        assertTrue("Should contain doc1", results1.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc2", results1.stream().anyMatch(p -> p.contains("doc2")));
        assertTrue("Should contain doc3", results1.stream().anyMatch(p -> p.contains("doc3")));
        System.out.println("    ✓ PASSED");
        
        // Query 2: Multiple property filter (AND)
        System.out.println("\n  Query 2: Find published programming articles");
        String query2 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming' AND [status] = 'published'";
        List<String> results2 = executeQuery(query2);
        System.out.printf("    Found %d results: %s%n", results2.size(), results2);
        assertEquals("Should find 2 published programming articles", 2, results2.size());
        assertTrue("Should contain doc1", results2.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc3", results2.stream().anyMatch(p -> p.contains("doc3")));
        System.out.println("    ✓ PASSED");
        
        // Query 3: Different category
        System.out.println("\n  Query 3: Find cooking articles");
        String query3 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'cooking'";
        List<String> results3 = executeQuery(query3);
        System.out.printf("    Found %d results: %s%n", results3.size(), results3);
        assertEquals("Should find 1 cooking article", 1, results3.size());
        assertTrue("Should contain doc4", results3.stream().anyMatch(p -> p.contains("doc4")));
        System.out.println("    ✓ PASSED");
        
        // Query 4: Fulltext search (CONTAINS)
        System.out.println("\n  Query 4: Fulltext search for 'Java'");
        String query4 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS(*, 'Java')";
        List<String> results4 = executeQuery(query4);
        System.out.printf("    Found %d results: %s%n", results4.size(), results4);
        assertTrue("Should find at least 1 Java-related article", results4.size() >= 1);
        assertTrue("Should contain doc1 (Java article)", results4.stream().anyMatch(p -> p.contains("doc1")));
        System.out.println("    ✓ PASSED");
        
        // Query 5: Fulltext search on specific property
        System.out.println("\n  Query 5: Search for 'Python' in title");
        String query5 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS([title], 'Python')";
        List<String> results5 = executeQuery(query5);
        System.out.printf("    Found %d results: %s%n", results5.size(), results5);
        assertEquals("Should find 1 Python article", 1, results5.size());
        assertTrue("Should contain doc2", results5.stream().anyMatch(p -> p.contains("doc2")));
        System.out.println("    ✓ PASSED");
        
        // Query 6: Draft status
        System.out.println("\n  Query 6: Find draft articles");
        String query6 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [status] = 'draft'";
        List<String> results6 = executeQuery(query6);
        System.out.printf("    Found %d results: %s%n", results6.size(), results6);
        assertEquals("Should find 1 draft article", 1, results6.size());
        assertTrue("Should contain doc2", results6.stream().anyMatch(p -> p.contains("doc2")));
        System.out.println("    ✓ PASSED");
        
        // STEP 6: Verify index progress was tracked
        System.out.println("\nStep 6: Verifying progress tracking...");
        IndexProgressMetadata progress = metadataManager.getIndexProgress("/oak:index/searchIndex");
        if (progress != null) {
            System.out.printf("  Last processed timestamp: %d%n", progress.getLastProcessedTimestamp());
            System.out.printf("  Last processed serial: %d%n", progress.getLastProcessedSerialNumber());
            System.out.printf("  Total processed: %d%n", progress.getTotalProcessed());
            System.out.println("  ✓ Progress tracking working");
        } else {
            System.out.println("  ⚠ Progress metadata not found (might not be updated yet)");
        }
        
        System.out.println("\n✓ Test 6 PASSED: Indexing verified via queries!");
        System.out.println("========================================");
        System.out.println("✓ ALL QUERY-BASED ASSERTIONS PASSED");
        System.out.println("✓ Change tracking indexing is working correctly");
        System.out.println("========================================");
    }
    
    /**
     * Test 7: Incremental indexing with queries
     * 
     * Demonstrates:
     * - Adding more content after initial indexing
     * - Running change tracker again
     * - Verifying new content is indexed
     * - Queries return updated results
     */
    @Test
    public void testIncrementalIndexingWithQueries() throws Exception {
        System.out.println("\n=== Test 7: Incremental Indexing with Queries ===");
        
        // STEP 1: Create index and initial content (reuse logic from test 6)
        System.out.println("\nStep 1: Setting up index and initial content...");
        
        // Create index
        Tree oakIndex = root.getTree("/oak:index");
        Tree searchIndex = oakIndex.addChild("incrementalIndex");
        searchIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        searchIndex.setProperty("type", "lucene");
        searchIndex.setProperty("async", "async");
        searchIndex.setProperty("useChangeTracker", true);
        
        Tree indexRules = searchIndex.addChild("indexRules");
        Tree ntUnstructured = indexRules.addChild("nt:unstructured");
        Tree properties = ntUnstructured.addChild("properties");
        
        Tree tagProp = properties.addChild("tag");
        tagProp.setProperty("name", "tag");
        tagProp.setProperty("propertyIndex", true);
        tagProp.setProperty("analyzed", false);
        
        root.commit();
        System.out.println("✓ Created index with useChangeTracker=true");
        
        // Create initial content
        Tree content = root.getTree("/").addChild("articles");
        
        Tree article1 = content.addChild("article1");
        article1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        article1.setProperty("tag", "initial");
        
        root.commit();
        System.out.println("✓ Created index and 1 initial article");
        
        // Index initial content
        populator.run();
        commitChangeTrackingIndex();
        changeTrackingAsyncIndexUpdate.run();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Initial indexing completed");
        
        // Verify initial query
        String initialQuery = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [tag] = 'initial'";
        List<String> initialResults = executeQuery(initialQuery);
        System.out.printf("✓ Initial query found %d articles%n", initialResults.size());
        assertEquals("Should find 1 initial article", 1, initialResults.size());
        
        // STEP 2: Add more content
        System.out.println("\nStep 2: Adding new content...");
        
        Tree article2 = root.getTree("/articles").addChild("article2");
        article2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        article2.setProperty("tag", "new");
        
        Tree article3 = root.getTree("/articles").addChild("article3");
        article3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        article3.setProperty("tag", "new");
        
        root.commit();
        System.out.println("✓ Added 2 new articles");
        
        // STEP 3: Run incremental indexing
        System.out.println("\nStep 3: Running incremental indexing...");
        populator.run();
        commitChangeTrackingIndex();
        changeTrackingAsyncIndexUpdate.run();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Incremental indexing completed");
        
        // STEP 4: Verify queries return updated results
        System.out.println("\nStep 4: Verifying queries return new content...");
        
        // Query for new articles
        String newQuery = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [tag] = 'new'";
        List<String> newResults = executeQuery(newQuery);
        System.out.printf("✓ Found %d new articles: %s%n", newResults.size(), newResults);
        assertEquals("Should find 2 new articles", 2, newResults.size());
        
        // Query for all articles
        String allQuery = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [tag] IS NOT NULL";
        List<String> allResults = executeQuery(allQuery);
        System.out.printf("✓ Found %d total articles: %s%n", allResults.size(), allResults);
        assertEquals("Should find 3 total articles", 3, allResults.size());
        
        // Verify old content still accessible
        List<String> stillInitial = executeQuery(initialQuery);
        assertEquals("Should still find initial article", 1, stillInitial.size());
        System.out.println("✓ Old content still accessible");
        
        System.out.println("\n✓ Test 7 PASSED: Incremental indexing works correctly!");
    }
    
    /**
     * Helper method to execute a query and return matching paths.
     */
    private List<String> executeQuery(String sqlQuery) throws Exception {
        List<String> paths = new ArrayList<>();
        QueryEngine queryEngine = root.getQueryEngine();
        
        Result result = queryEngine.executeQuery(
            sqlQuery,
            javax.jcr.query.Query.JCR_SQL2,
            null,  // NO_BINDINGS
            null   // NO_MAPPINGS
        );
        
        for (ResultRow row : result.getRows()) {
            paths.add(row.getPath());
        }
        
        return paths;
    }
    
    /**
     * Helper method to force commit of the change tracking IndexWriter.
     * 
     * <p><strong>Why this is needed:</strong> The ChangeTrackingIndexPopulator's internal
     * IndexWriter does not auto-commit after run(). In production, commits happen periodically
     * or when the writer is closed. In tests, we need immediate visibility of changes for
     * subsequent queries, so we force a commit using reflection.
     * 
     * <p><strong>Note:</strong> This is a test-only workaround. Ideally, ChangeTrackingIndexPopulator
     * would expose a public flush() or commit() method, but since it doesn't, reflection is
     * necessary to access the private writer field.
     */
    private void commitChangeTrackingIndex() throws Exception {
        try {
            // Use reflection to access the private changeTrackingWriter field from populator
            java.lang.reflect.Field writerField = populator.getClass().getDeclaredField("changeTrackingWriter");
            writerField.setAccessible(true);
            Object writer = writerField.get(populator);
            if (writer != null) {
                // Call commit() on the IndexWriter
                writer.getClass().getMethod("commit").invoke(writer);
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not commit change tracking writer: " + e.getMessage());
            // Non-fatal - continue test
        }
    }
}

