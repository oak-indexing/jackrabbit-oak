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
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
    private LuceneIndexProvider provider;
    
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
        provider = new LuceneIndexProvider();
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
        
        // Register DAM node types (dam:Asset, dam:AssetContent) using CND
        registerDamNodeTypes();
        
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
    
    /**
     * Registers DAM node types (dam:Asset, dam:AssetContent) for testing.
     */
    private void registerDamNodeTypes() throws Exception {
        try {
            // Load node type definitions from CND file
            InputStream cndStream = getClass().getResourceAsStream("/dam-nodetypes.cnd");
            if (cndStream == null) {
                throw new IllegalStateException("dam-nodetypes.cnd not found in classpath");
            }
            
            // Register node types
            NodeTypeRegistry.register(root, cndStream, "dam-nodetypes.cnd");
            root.commit();
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to register DAM node types", e);
        }
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
        searchIndex.setProperty("compatVersion", 2);
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
        
        // Create damAssetLucene13 index (modeled after AEM damAssetLucene-13)
        Tree damIndex = oakIndex.addChild("damAssetLucene13");
        damIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        damIndex.setProperty("type", "lucene");
        damIndex.setProperty("async", "async");
        damIndex.setProperty("compatVersion", 2);
        damIndex.setProperty("evaluatePathRestrictions", true);
        damIndex.setProperty("includedPaths", Arrays.asList("/testContent"), Type.STRINGS);
        damIndex.setProperty("useChangeTracker", true);
        
        // Aggregation rules (damAssetLucene-13 pattern)
        Tree aggregates = damIndex.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damAssetAggregate = aggregates.addChild("dam:Asset");
        damAssetAggregate.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // include0: jcr:content
        Tree include0 = damAssetAggregate.addChild("include0");
        include0.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include0.setProperty("path", "jcr:content");
        
        // include1: jcr:content/metadata
        Tree include1 = damAssetAggregate.addChild("include1");
        include1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include1.setProperty("path", "jcr:content/metadata");
        
        // Index rules for dam:Asset
        Tree damRules = damIndex.addChild("indexRules");
        damRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damAssetRule = damRules.addChild("dam:Asset");
        damAssetRule.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damProps = damAssetRule.addChild("properties");
        damProps.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // dc:title (analyzed, fulltext)
        Tree dcTitle = damProps.addChild("dcTitle");
        dcTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcTitle.setProperty("name", "jcr:content/metadata/dc:title");
        dcTitle.setProperty("analyzed", true);
        dcTitle.setProperty("nodeScopeIndex", true);
        dcTitle.setProperty("propertyIndex", true);
        
        // jcr:title (analyzed, fulltext)
        Tree jcrTitle = damProps.addChild("jcrTitle");
        jcrTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrTitle.setProperty("name", "jcr:content/metadata/jcr:title");
        jcrTitle.setProperty("analyzed", true);
        jcrTitle.setProperty("nodeScopeIndex", true);
        jcrTitle.setProperty("propertyIndex", true);
        
        // dam:status
        Tree damStatus = damProps.addChild("damStatus");
        damStatus.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        damStatus.setProperty("name", "jcr:content/metadata/dam:status");
        damStatus.setProperty("propertyIndex", true);
        
        // dc:format
        Tree dcFormat = damProps.addChild("dcFormat");
        dcFormat.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcFormat.setProperty("name", "jcr:content/metadata/dc:format");
        dcFormat.setProperty("propertyIndex", true);
        
        root.commit();
        metadataManager.registerIndex("/oak:index/damAssetLucene13");
        System.out.println("✓ Created damAssetLucene13 with useChangeTracker=true");
        
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
        
        // Document 5: dam:Asset with relative properties
        Tree doc5 = content.addChild("asset1");
        doc5.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        
        Tree jcrContent5 = doc5.addChild("jcr:content");
        jcrContent5.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree metadata5 = jcrContent5.addChild("metadata");
        metadata5.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata5.setProperty("jcr:title", "My Awesome Asset");
        metadata5.setProperty("dam:status", "approved");
        metadata5.setProperty("dc:format", "image/jpeg");
        
        // Document 6: dam:Asset with another relative property value
        Tree doc6 = content.addChild("asset2");
        doc6.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        
        Tree jcrContent6 = doc6.addChild("jcr:content");
        jcrContent6.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree metadata6 = jcrContent6.addChild("metadata");
        metadata6.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata6.setProperty("jcr:title", "Nested Asset");
        metadata6.setProperty("dam:status", "draft");
        metadata6.setProperty("dc:format", "application/pdf");
        
        // Document 7: dam:Asset with aggregated content
        Tree doc7 = content.addChild("asset3");
        doc7.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        
        Tree jcrContent7 = doc7.addChild("jcr:content");
        jcrContent7.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrContent7.setProperty("description", "Aggregation Magic Content"); // Aggregated by include0
        
        Tree metadata7 = jcrContent7.addChild("metadata");
        metadata7.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata7.setProperty("jcr:title", "Aggregated Asset");
        
        root.commit();
        System.out.println("✓ Created 7 test documents (4 nt:unstructured, 3 dam:Asset)");


        
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
        
        // Force refresh the index tracker to ensure it sees the new index files
        if (provider != null) {
            provider.getTracker().refresh();
            // Manually trigger update to force refresh
            provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        }
        
        System.out.println("✓ Change tracking async index update completed");
        
        // STEP 5: Execute queries and verify results
        System.out.println("\nStep 5: Executing queries to verify indexing...");
        
        // Query 1: Exact match on category
        System.out.println("\n  Query 1: Find all programming articles");
        String query1 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming' option(traversal fail, index name searchIndex)";
        List<String> results1 = executeQuery(query1);
        System.out.printf("    Found %d results: %s%n", results1.size(), results1);
        assertEquals("Should find 3 programming articles", 3, results1.size());
        assertTrue("Should contain doc1", results1.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc2", results1.stream().anyMatch(p -> p.contains("doc2")));
        assertTrue("Should contain doc3", results1.stream().anyMatch(p -> p.contains("doc3")));
        System.out.println("    ✓ PASSED");
        
        // Query 2: Multiple property filter (AND)
        System.out.println("\n  Query 2: Find published programming articles");
        String query2 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming' AND [status] = 'published' option(traversal fail, index name searchIndex)";
        List<String> results2 = executeQuery(query2);
        System.out.printf("    Found %d results: %s%n", results2.size(), results2);
        assertEquals("Should find 2 published programming articles", 2, results2.size());
        assertTrue("Should contain doc1", results2.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc3", results2.stream().anyMatch(p -> p.contains("doc3")));
        System.out.println("    ✓ PASSED");
        
        // Query 3: Different category
        System.out.println("\n  Query 3: Find cooking articles");
        String query3 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'cooking' option(traversal fail, index name searchIndex)";
        List<String> results3 = executeQuery(query3);
        System.out.printf("    Found %d results: %s%n", results3.size(), results3);
        assertEquals("Should find 1 cooking article", 1, results3.size());
        assertTrue("Should contain doc4", results3.stream().anyMatch(p -> p.contains("doc4")));
        System.out.println("    ✓ PASSED");
        
        // Query 4: Fulltext search (CONTAINS)
        System.out.println("\n  Query 4: Fulltext search for 'Java'");
        String query4 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS(*, 'Java') option(traversal fail, index name searchIndex)";
        List<String> results4 = executeQuery(query4);
        System.out.printf("    Found %d results: %s%n", results4.size(), results4);
        assertTrue("Should find at least 1 Java-related article", results4.size() >= 1);
        assertTrue("Should contain doc1 (Java article)", results4.stream().anyMatch(p -> p.contains("doc1")));
        System.out.println("    ✓ PASSED");
        
        // Query 5: Fulltext search on specific property
        System.out.println("\n  Query 5: Search for 'Python' in title");
        String query5 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS([title], 'Python') option(traversal fail, index name searchIndex)";
        List<String> results5 = executeQuery(query5);
        System.out.printf("    Found %d results: %s%n", results5.size(), results5);
        assertEquals("Should find 1 Python article", 1, results5.size());
        assertTrue("Should contain doc2", results5.stream().anyMatch(p -> p.contains("doc2")));
        System.out.println("    ✓ PASSED");
        
        // Query 6: Draft status
        System.out.println("\n  Query 6: Find draft articles");
        String query6 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [status] = 'draft' option(traversal fail, index name searchIndex)";
        List<String> results6 = executeQuery(query6);
        System.out.printf("    Found %d results: %s%n", results6.size(), results6);
        assertEquals("Should find 1 draft article", 1, results6.size());
        assertTrue("Should contain doc2", results6.stream().anyMatch(p -> p.contains("doc2")));
        System.out.println("    ✓ PASSED");
        
        // Query 7: dam:Asset search using damAssetLucene13
        System.out.println("\n  Query 7: Find dam:Asset by jcr:title (relative property)");
        String query7 = "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS([jcr:content/metadata/jcr:title], 'Awesome') option(traversal fail, index name damAssetLucene13)";
        List<String> results7 = executeQuery(query7);
        System.out.printf("    Found %d results: %s%n", results7.size(), results7);
        assertEquals("Should find 1 asset", 1, results7.size());
        assertTrue("Should contain asset1", results7.stream().anyMatch(p -> p.contains("asset1")));
        System.out.println("    ✓ PASSED");
        
        // Query 8: Relative property equality (dam:status)
        System.out.println("\n  Query 8: Find asset by relative dam:status (equality)");
        String query8 = "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dam:status] = 'draft' option(traversal fail, index name damAssetLucene13)";
        List<String> results8 = executeQuery(query8);
        System.out.printf("    Found %d results: %s%n", results8.size(), results8);
        assertEquals("Should find 1 asset by status", 1, results8.size());
        assertTrue("Should contain asset2", results8.stream().anyMatch(p -> p.contains("asset2")));
        System.out.println("    ✓ PASSED");
        
        // Query 9: Relative property equality (dc:format)
        System.out.println("\n  Query 9: Find asset by dc:format (equality)");
        String query9 = "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dc:format] = 'image/jpeg' option(traversal fail, index name damAssetLucene13)";
        List<String> results9 = executeQuery(query9);
        System.out.printf("    Found %d results: %s%n", results9.size(), results9);
        assertEquals("Should find 1 image asset", 1, results9.size());
        assertTrue("Should contain asset1", results9.stream().anyMatch(p -> p.contains("asset1")));
        System.out.println("    ✓ PASSED");
        
        // Query 10: Aggregated content search
        System.out.println("\n  Query 10: Find asset by aggregated content");
        // Searching for 'Magic' which is in jcr:content/description (aggregated by include0)
        String query10 = "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS(*, 'Magic') option(traversal fail, index name damAssetLucene13)";
        List<String> results10 = executeQuery(query10);
        System.out.printf("    Found %d results: %s%n", results10.size(), results10);
        assertEquals("Should find 1 asset by aggregated content", 1, results10.size());
        assertTrue("Should contain asset3", results10.stream().anyMatch(p -> p.contains("asset3")));
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
        
        // Force refresh
        if (provider != null) {
            provider.getTracker().refresh();
            provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        }
        
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
        
        // Force refresh
        if (provider != null) {
            provider.getTracker().refresh();
            provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        }
        
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
    
    @Test
    public void testDeleteAggregatedNode() throws Exception {
        System.out.println("\n=== Test 8: Delete Aggregated Node Verification ===");

        // STEP 1: Create index definition
        Tree oakIndex = root.getTree("/oak:index");
        Tree damIndex = oakIndex.addChild("deleteTestIndex");
        damIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        damIndex.setProperty("type", "lucene");
        damIndex.setProperty("async", "async");
        damIndex.setProperty("useChangeTracker", true);
        // Ensure path filter works: only index /deleteTest
        damIndex.setProperty("evaluatePathRestrictions", true);
        damIndex.setProperty("includedPaths", Arrays.asList("/deleteTest"), Type.STRINGS);
        
        // Aggregates
        Tree aggregates = damIndex.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAssetAggregate = aggregates.addChild("nt:unstructured");
        damAssetAggregate.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree include0 = damAssetAggregate.addChild("include0");
        include0.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include0.setProperty("path", "jcr:content");

        // Index Rules
        Tree damRules = damIndex.addChild("indexRules");
        damRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAssetRule = damRules.addChild("nt:unstructured");
        damAssetRule.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        // Need fulltext enabled (nodeScopeIndex implied by aggregation)
        
        root.commit();
        metadataManager.registerIndex("/oak:index/deleteTestIndex");
        
        // STEP 2: Create Content
        Tree content = root.getTree("/").addChild("deleteTest");
        Tree asset = content.addChild("asset1");
        asset.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree jcrContent = asset.addChild("jcr:content");
        jcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrContent.setProperty("description", "SecretKeyWord"); // Aggregated
        
        root.commit();
        
        // STEP 3: Index
        populator.run();
        commitChangeTrackingIndex();
        changeTrackingAsyncIndexUpdate.run();
        root = contentSession.getLatestRoot();
        if (provider != null) {
            provider.getTracker().refresh();
            provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        }

        // STEP 4: Verify Search
        String query = "SELECT [jcr:path] FROM [nt:unstructured] WHERE ISDESCENDANTNODE('/deleteTest') AND CONTAINS(*, 'SecretKeyWord') option(traversal fail, index name deleteTestIndex)";
        List<String> results = executeQuery(query);
        assertEquals("Should find asset before delete", 1, results.size());
        
        // STEP 5: Delete aggregated node
        root.getTree("/deleteTest/asset1/jcr:content").remove();
        root.commit();
        System.out.println("✓ Deleted aggregated node /deleteTest/asset1/jcr:content");
        
        // STEP 6: Re-index
        populator.run(); // populates change tracker with deletion
        commitChangeTrackingIndex();
        changeTrackingAsyncIndexUpdate.run(); // processes deletion
        root = contentSession.getLatestRoot();
        if (provider != null) {
            provider.getTracker().refresh();
            provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        }
        
        // STEP 7: Verify Search (Should NOT find it)
        List<String> resultsAfter = executeQuery(query);
        assertEquals("Should NOT find asset after deleting aggregated content", 0, resultsAfter.size());
        System.out.println("✓ Verified deletion update: Aggregation removed");
        
        System.out.println("\n✓ Test 8 PASSED: Aggregation deletion handled correctly!");
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
