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
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexPopulator;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingAsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexQuery;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.index.DirectoryReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple test demonstrating CONTAINS queries on relative properties.
 * 
 * <p><strong>Test Modes:</strong>
 * <ul>
 *   <li><strong>Traditional Mode</strong> (default): Uses standard AsyncIndexUpdate</li>
 *   <li><strong>Change Tracking Mode</strong> (-DuseChangeTracking=true): Uses 3-indexer architecture</li>
 * </ul>
 * 
 * <p><strong>Usage Examples:</strong>
 * <pre>
 * # Traditional mode (default)
 * mvn test -Dtest=SimpleContainsRelativePropertyTest
 * 
 * # Change tracking mode
 * mvn test -Dtest=SimpleContainsRelativePropertyTest -DuseChangeTracking=true
 * </pre>
 */
public class SimpleContainsRelativePropertyTest {
    
    // Test control flag
    private static final boolean USE_CHANGE_TRACKING = Boolean.getBoolean("useChangeTracking");
    
    private ContentRepository repository;
    private Root root;
    private NodeStore nodeStore;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private LuceneIndexProvider luceneIndexProvider;
    
    // Change tracking components (Three-Indexer Architecture)
    private org.apache.lucene.store.Directory changeTrackingDirectory;
    private org.apache.lucene.index.IndexWriter changeTrackingWriter;
    private IndexProgressMetadataManager metadataManager;
    private ChangeTrackingIndexPopulator changeTrackingPopulator;       // 1. Populates change tracking index
    private AsyncIndexUpdate traditionalAsyncIndexer;                   // 2. Processes non-CT indexes
    private ChangeTrackingAsyncIndexUpdate changeTrackingAsyncIndexer;  // 3. Processes CT indexes
    
    @Before
    public void setup() throws Exception {
        System.out.println("\n========================================");
        System.out.println("Test Configuration:");
        System.out.println("  Mode: " + (USE_CHANGE_TRACKING ? "CHANGE TRACKING (3 indexers)" : "TRADITIONAL (1 indexer)"));
        System.out.println("========================================\n");
        
        repository = createRepository();
        root = repository.login(null, null).getLatestRoot();
        
        // Ensure /oak:index exists
        if (!root.getTree("/oak:index").exists()) {
            root.getTree("/").addChild("oak:index");
            root.commit();
        }
        
        // Register DAM node types (dam:Asset, dam:AssetContent)
        registerDamNodeTypes();
        
        System.out.println("Note: All queries use 'option(traversal fail)' to ensure index usage\n");
    }
    
    @After
    public void teardown() throws Exception {
        // Close traditional async indexer
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
        }
        
        // Close change tracking components
        if (changeTrackingPopulator != null) {
            changeTrackingPopulator.close();
        }
        if (changeTrackingWriter != null) {
            changeTrackingWriter.close();
        }
        if (changeTrackingDirectory != null) {
            changeTrackingDirectory.close();
        }
    }
    
    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        luceneEditorProvider = new LuceneIndexEditorProvider();
        luceneIndexProvider = new LuceneIndexProvider();
        
        // Create composite editor provider with all the editor providers we need
        org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider compositeEditorProvider = 
            org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose(
                java.util.Arrays.asList(
                    luceneEditorProvider,
                    new PropertyIndexEditorProvider(),
                    new NodeCounterEditorProvider()
                )
            );
        
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compositeEditorProvider);
        
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
                .with(new NodeCounterEditorProvider())
                .createContentRepository();
    }
    
    /**
     * Initializes the three-indexer change tracking architecture:
     * 1. ChangeTrackingIndexPopulator - Populates the change tracking index
     * 2. Traditional AsyncIndexUpdate - Processes indexes WITHOUT useChangeTracker
     * 3. ChangeTrackingAsyncIndexUpdate - Processes indexes WITH useChangeTracker
     */
    private void initializeChangeTracking() throws Exception {
        System.out.println("Initializing three-indexer change tracking architecture...");
        
        // Create in-memory Lucene directory for change tracking index
        changeTrackingDirectory = new org.apache.lucene.store.RAMDirectory();
        
        // Create metadata manager for progress tracking and coordination
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        
        // 1. Create Change Tracking Populator (records changes to tracking index)
        changeTrackingPopulator = new ChangeTrackingIndexPopulator(
            nodeStore,
            changeTrackingDirectory,
            metadataManager,
            StatisticsProvider.NOOP
        );
        changeTrackingPopulator.initialize();
        System.out.println("  [1/3] ChangeTrackingIndexPopulator initialized");
        
        // 2. Traditional AsyncIndexUpdate already created in createRepository()
        traditionalAsyncIndexer = asyncIndexUpdate;
        System.out.println("  [2/3] Traditional AsyncIndexUpdate ready");
        
        // 3. Create Change Tracking AsyncIndexUpdate (processes CT indexes)
        changeTrackingAsyncIndexer = new ChangeTrackingAsyncIndexUpdate(
            "change-tracker-async",
            nodeStore,
            changeTrackingDirectory,
            changeTrackingWriter
        );
        System.out.println("  [3/3] ChangeTrackingAsyncIndexUpdate initialized");
        
        System.out.println("✓ Three-indexer architecture ready\n");
    }
    
    /**
     * Registers DAM node types (dam:Asset, dam:AssetContent) for testing.
     * 
     * This loads the node type definitions from dam-nodetypes.cnd and registers them
     * in the repository. This is required before creating dam:Asset nodes.
     * 
     * @throws Exception if node type registration fails
     */
    private void registerDamNodeTypes() throws Exception {
        System.out.println("Registering DAM node types...");
        
        try {
            // Load node type definitions from CND file
            InputStream cndStream = getClass().getResourceAsStream("/dam-nodetypes.cnd");
            if (cndStream == null) {
                throw new IllegalStateException("dam-nodetypes.cnd not found in classpath");
            }
            
            // Register node types
            NodeTypeRegistry.register(root, cndStream, "dam-nodetypes.cnd");
            root.commit();
            
            System.out.println("✓ DAM node types registered (dam:Asset, dam:AssetContent)");
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to register DAM node types: " + e.getMessage());
            throw e;
        }
    }
    
    /**
     * Creates a Lucene index definition for testing CONTAINS queries on relative properties.
     * 
     * Index includes:
     * - jcr:content/metadata/jcr:title (analyzed, nodeScopeIndex) - for fulltext search
     * - jcr:content/metadata/dc:title (analyzed, nodeScopeIndex) - for fulltext search
     * - jcr:content/metadata/status (propertyIndex) - for exact match queries
     * 
     * Can be configured to index either nt:base or dam:Asset nodes.
     * 
     * @param indexName The name of the index to create
     * @throws Exception if index creation fails
     */
    private void createDamAssetIndex(String indexName) throws Exception {
        System.out.println("Creating index definition...");
        
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        
        // Index rule for dam:Asset (or nt:base as fallback)
        LuceneIndexDefinitionBuilder.IndexRule rule = idxb.indexRule("dam:Asset");
        
        // Direct property on dam:Asset node itself (non-relative)
        rule.property("assetType")
            .propertyIndex();
        
        // Relative property with fulltext analysis for CONTAINS queries
        rule.property("jcr:content/metadata/jcr:title")
            .analyzed()
            .nodeScopeIndex()
            .propertyIndex();
        
        // Another relative property with fulltext analysis
        rule.property("jcr:content/metadata/dc:title")
            .analyzed()
            .nodeScopeIndex()
            .propertyIndex();
        
        // Relative property for exact match (no analysis)
        rule.property("jcr:content/metadata/status")
            .propertyIndex();
        
        // Build and commit index
        Tree indexTree = idxb.build(root.getTree("/oak:index").addChild(indexName));
        
        // Mark index to use change tracking if enabled
        if (USE_CHANGE_TRACKING) {
            indexTree.setProperty("useChangeTracker", true);
            System.out.println("  - useChangeTracker: true");
        }
        
        root.commit();
        
        System.out.println("✓ Index definition created: " + indexName);
        System.out.println("  - Node type: dam:Asset");
        System.out.println("  - Direct property: assetType (exact match)");
        System.out.println("  - Relative property: jcr:content/metadata/jcr:title (analyzed, nodeScopeIndex)");
        System.out.println("  - Relative property: jcr:content/metadata/dc:title (analyzed, nodeScopeIndex)");
        System.out.println("  - Relative property: jcr:content/metadata/status (exact match)");
    }
    
    /**
     * Setup shared test data - creates index and test content.
     * This is called once before all test methods run.
     */
    private void setupTestData() throws Exception {
        System.out.println("\n========== Setup: Creating Index and Test Content ==========\n");
        
        // ========================================
        // Step 1: Create Index Definition
        // ========================================
        System.out.println("Step 1: Creating index definition...");
        createDamAssetIndex("damAssetLucene");
        
        // ========================================
        // Step 2: Create Test Content
        // ========================================
        System.out.println("\nStep 2: Creating test content...");
        
        // Asset 1: Java content, published, image type
        Tree asset1 = root.getTree("/").addChild("asset1");
        asset1.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        asset1.setProperty("assetType", "image");  // Direct property on dam:Asset
        Tree jcrContent1 = asset1.addChild("jcr:content");
        jcrContent1.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata1 = jcrContent1.addChild("metadata");
        metadata1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata1.setProperty("jcr:title", "Java Programming Guide");
        metadata1.setProperty("dc:title", "Comprehensive Java Tutorial");
        metadata1.setProperty("status", "published");
        
        // Asset 2: Python content, draft, document type
        Tree asset2 = root.getTree("/").addChild("asset2");
        asset2.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        asset2.setProperty("assetType", "document");  // Direct property on dam:Asset
        Tree jcrContent2 = asset2.addChild("jcr:content");
        jcrContent2.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata2 = jcrContent2.addChild("metadata");
        metadata2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata2.setProperty("jcr:title", "Python Tutorial");
        metadata2.setProperty("dc:title", "Python for Beginners");
        metadata2.setProperty("status", "draft");
        
        // Asset 3: Java content, published, video type
        Tree asset3 = root.getTree("/").addChild("asset3");
        asset3.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        asset3.setProperty("assetType", "video");  // Direct property on dam:Asset
        Tree jcrContent3 = asset3.addChild("jcr:content");
        jcrContent3.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata3 = jcrContent3.addChild("metadata");
        metadata3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata3.setProperty("jcr:title", "Java Best Practices");
        metadata3.setProperty("dc:title", "Advanced Java Techniques");
        metadata3.setProperty("status", "published");
        
        // Asset 4: JavaScript content, published, image type
        Tree asset4 = root.getTree("/").addChild("asset4");
        asset4.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        asset4.setProperty("assetType", "image");  // Direct property on dam:Asset
        Tree jcrContent4 = asset4.addChild("jcr:content");
        jcrContent4.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata4 = jcrContent4.addChild("metadata");
        metadata4.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata4.setProperty("jcr:title", "JavaScript Essentials");
        metadata4.setProperty("dc:title", "Modern JavaScript Guide");
        metadata4.setProperty("status", "published");
        
        root.commit();
        
        System.out.println("✓ Created 4 test dam:Asset nodes:");
        System.out.println("  - asset1: assetType=image, Java content, published");
        System.out.println("  - asset2: assetType=document, Python content, draft");
        System.out.println("  - asset3: assetType=video, Java content, published");
        System.out.println("  - asset4: assetType=image, JavaScript content, published");
        
        // ========================================
        // Step 3: Run Async Indexing
        // ========================================
        System.out.println("\nStep 3: Running async indexing...");
        
        runAsyncIndexing();
        
        System.out.println("✓ Async indexing completed successfully");
        System.out.println("\n========================================\n");
    }
    
    /**
     * Test 1: CONTAINS queries on specific relative properties.
     * 
     * Verifies that CONTAINS queries work on relative properties like:
     * - jcr:content/metadata/jcr:title
     * - jcr:content/metadata/dc:title
     * 
     * Expected behavior:
     * - Parent nodes (dam:Asset) are returned, not the child nodes where properties exist
     * - Fulltext analysis correctly tokenizes words (Java != JavaScript)
     */
    @Test
    public void test01_ContainsOnRelativeProperties() throws Exception {
        setupTestData();
        
        System.out.println("========== TEST 1: CONTAINS on Relative Properties ==========\n");
        
        // Query 1: CONTAINS on jcr:title
        System.out.println("Query 1: CONTAINS([jcr:content/metadata/jcr:title], 'Java')");
        String query1 = "select [jcr:path] from [dam:Asset] where CONTAINS([jcr:content/metadata/jcr:title], 'Java') option(traversal fail)";
        List<String> results1 = executeQuery(query1);
        System.out.println("  Results: " + results1);
        System.out.println("  Expected: [/asset1, /asset3] (contain 'Java' as a word)");
        assertEquals("Should find 2 assets with 'Java'", 2, results1.size());
        assertTrue("Should contain asset1", results1.contains("/asset1"));
        assertTrue("Should contain asset3", results1.contains("/asset3"));
        assertFalse("Should NOT contain asset2 (Python)", results1.contains("/asset2"));
        assertFalse("Should NOT contain asset4 (JavaScript is a different word)", results1.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 2: CONTAINS on dc:title
        System.out.println("Query 2: CONTAINS([jcr:content/metadata/dc:title], 'Java')");
        String query2 = "select [jcr:path] from [dam:Asset] where CONTAINS([jcr:content/metadata/dc:title], 'Java') option(traversal fail)";
        List<String> results2 = executeQuery(query2);
        System.out.println("  Results: " + results2);
        System.out.println("  Expected: [/asset1, /asset3] (contain 'Java' as a word)");
        assertEquals("Should find 2 assets with 'Java'", 2, results2.size());
        assertTrue("Should contain asset1", results2.contains("/asset1"));
        assertTrue("Should contain asset3", results2.contains("/asset3"));
        assertFalse("Should NOT contain asset2", results2.contains("/asset2"));
        assertFalse("Should NOT contain asset4 (JavaScript is a different word)", results2.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        System.out.println("========================================");
        System.out.println("✓ TEST 1 PASSED!");
        System.out.println("Key Finding: CONTAINS queries correctly return parent nodes for relative properties");
        System.out.println("========================================\n");
    }
    
    /**
     * Test 2: Node-scoped CONTAINS queries (CONTAINS with *).
     * 
     * Verifies that node-scoped CONTAINS queries work correctly:
     * - CONTAINS(*, 'term') searches all nodeScopeIndex properties
     * - Includes both direct properties and relative properties with nodeScopeIndex=true
     * 
     * Expected behavior:
     * - Searches across all analyzed properties in the node and its descendants
     * - Returns parent nodes (dam:Asset) that match the search term
     */
    @Test
    public void test02_NodeScopedContains() throws Exception {
        setupTestData();
        
        System.out.println("========== TEST 2: Node-Scoped CONTAINS (*) ==========\n");
        
        // Query: Node-scoped CONTAINS
        System.out.println("Query: CONTAINS(*, 'Python')");
        String query = "select [jcr:path] from [dam:Asset] where CONTAINS(*, 'Python') option(traversal fail)";
        List<String> results = executeQuery(query);
        System.out.println("  Results: " + results);
        System.out.println("  Expected: [/asset2] (only asset2 has Python in any property)");
        assertEquals("Should find 1 asset with 'Python'", 1, results.size());
        assertTrue("Should contain asset2", results.contains("/asset2"));
        assertFalse("Should NOT contain asset1", results.contains("/asset1"));
        assertFalse("Should NOT contain asset3", results.contains("/asset3"));
        assertFalse("Should NOT contain asset4", results.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        System.out.println("========================================");
        System.out.println("✓ TEST 2 PASSED!");
        System.out.println("Key Finding: CONTAINS(*, 'term') searches all nodeScopeIndex properties");
        System.out.println("========================================\n");
    }
    
    /**
     * Test 3: Equality queries on relative properties.
     * 
     * Verifies that equality queries work on relative properties:
     * - Simple equality: [relative/path] = 'value'
     * - Combined with CONTAINS: CONTAINS(...) AND [relative/path] = 'value'
     * 
     * Expected behavior:
     * - Equality queries on relative properties return parent nodes
     * - Can be combined with CONTAINS queries for complex filtering
     */
    @Test
    public void test03_EqualityOnRelativeProperties() throws Exception {
        setupTestData();
        
        System.out.println("========== TEST 3: Equality on Relative Properties ==========\n");
        
        // Query 1: Simple equality on status
        System.out.println("Query 1: [jcr:content/metadata/status] = 'published'");
        String query1 = "select [jcr:path] from [dam:Asset] where [jcr:content/metadata/status] = 'published' option(traversal fail)";
        List<String> results1 = executeQuery(query1);
        System.out.println("  Results: " + results1);
        System.out.println("  Expected: [/asset1, /asset3, /asset4] (all published)");
        assertEquals("Should find 3 published assets", 3, results1.size());
        assertTrue("Should contain asset1", results1.contains("/asset1"));
        assertTrue("Should contain asset3", results1.contains("/asset3"));
        assertTrue("Should contain asset4", results1.contains("/asset4"));
        assertFalse("Should NOT contain asset2 (draft)", results1.contains("/asset2"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 2: Combined CONTAINS + equality filter
        System.out.println("Query 2: CONTAINS([jcr:content/metadata/jcr:title], 'Java') AND status = 'published'");
        String query2 = "select [jcr:path] from [dam:Asset] where " +
                       "CONTAINS([jcr:content/metadata/jcr:title], 'Java') " +
                       "AND [jcr:content/metadata/status] = 'published' option(traversal fail)";
        List<String> results2 = executeQuery(query2);
        System.out.println("  Results: " + results2);
        System.out.println("  Expected: [/asset1, /asset3] (Java + published)");
        assertEquals("Should find 2 published Java assets", 2, results2.size());
        assertTrue("Should contain asset1", results2.contains("/asset1"));
        assertTrue("Should contain asset3", results2.contains("/asset3"));
        assertFalse("Should NOT contain asset2 (draft)", results2.contains("/asset2"));
        assertFalse("Should NOT contain asset4 (JavaScript, not Java)", results2.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        System.out.println("========================================");
        System.out.println("✓ TEST 3 PASSED!");
        System.out.println("Key Finding: Equality queries and combined filters work correctly on relative properties");
        System.out.println("========================================\n");
    }
    
    /**
     * Test 4: Equality queries on direct properties (non-relative).
     * 
     * Verifies that equality queries work on direct properties defined on dam:Asset itself:
     * - Direct property: assetType (on dam:Asset node)
     * - Contrast with relative properties: jcr:content/metadata/status
     * 
     * Expected behavior:
     * - Direct properties can be queried with simple equality: [propertyName] = 'value'
     * - No path traversal needed (unlike relative properties)
     * - Can be combined with other filters
     */
    @Test
    public void test04_EqualityOnDirectProperty() throws Exception {
        setupTestData();
        
        System.out.println("========== TEST 4: Equality on Direct Property (Non-Relative) ==========\n");
        
        // Query 1: Equality on direct property "assetType"
        System.out.println("Query 1: [assetType] = 'image'");
        String query1 = "select [jcr:path] from [dam:Asset] where [assetType] = 'image' option(traversal fail)";
        List<String> results1 = executeQuery(query1);
        System.out.println("  Results: " + results1);
        System.out.println("  Expected: [/asset1, /asset4] (both are image type)");
        assertEquals("Should find 2 image assets", 2, results1.size());
        assertTrue("Should contain asset1", results1.contains("/asset1"));
        assertTrue("Should contain asset4", results1.contains("/asset4"));
        assertFalse("Should NOT contain asset2 (document)", results1.contains("/asset2"));
        assertFalse("Should NOT contain asset3 (video)", results1.contains("/asset3"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 2: Equality on different assetType value
        System.out.println("Query 2: [assetType] = 'document'");
        String query2 = "select [jcr:path] from [dam:Asset] where [assetType] = 'document' option(traversal fail)";
        List<String> results2 = executeQuery(query2);
        System.out.println("  Results: " + results2);
        System.out.println("  Expected: [/asset2] (only asset2 is document type)");
        assertEquals("Should find 1 document asset", 1, results2.size());
        assertTrue("Should contain asset2", results2.contains("/asset2"));
        System.out.println("  ✓ PASSED\n");
        
        System.out.println("========================================");
        System.out.println("✓ TEST 4 PASSED!");
        System.out.println("Key Finding: Direct properties (assetType) can be queried without path traversal");
        System.out.println("Comparison: Direct property [assetType] vs Relative property [jcr:content/metadata/status]");
        System.out.println("========================================\n");
    }
    
    /**
     * Test 5: Combined filters (direct properties + relative properties).
     * 
     * Verifies that complex combined queries work correctly:
     * - Direct property + Relative property: [assetType] = 'x' AND [jcr:content/metadata/status] = 'y'
     * - Direct property + CONTAINS on relative property
     * 
     * Expected behavior:
     * - All filter conditions must be satisfied
     * - Demonstrates real-world query patterns combining multiple criteria
     * - Index must efficiently handle combined predicates
     */
    @Test
    public void test05_CombinedFilters() throws Exception {
        setupTestData();
        
        System.out.println("========== TEST 5: Combined Filters (Direct + Relative) ==========\n");
        
        // Query 1: Combined direct + relative property filters
        System.out.println("Query 1: [assetType] = 'image' AND [jcr:content/metadata/status] = 'published'");
        String query1 = "select [jcr:path] from [dam:Asset] where " +
                       "[assetType] = 'image' AND [jcr:content/metadata/status] = 'published' option(traversal fail)";
        List<String> results1 = executeQuery(query1);
        System.out.println("  Results: " + results1);
        System.out.println("  Expected: [/asset1, /asset4] (both image AND published)");
        assertEquals("Should find 2 published image assets", 2, results1.size());
        assertTrue("Should contain asset1", results1.contains("/asset1"));
        assertTrue("Should contain asset4", results1.contains("/asset4"));
        assertFalse("Should NOT contain asset2 (document type)", results1.contains("/asset2"));
        assertFalse("Should NOT contain asset3 (video type)", results1.contains("/asset3"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 2: Combined CONTAINS + direct property filter
        System.out.println("Query 2: [assetType] = 'image' AND CONTAINS([jcr:content/metadata/jcr:title], 'Java')");
        String query2 = "select [jcr:path] from [dam:Asset] where " +
                       "[assetType] = 'image' AND CONTAINS([jcr:content/metadata/jcr:title], 'Java') option(traversal fail)";
        List<String> results2 = executeQuery(query2);
        System.out.println("  Results: " + results2);
        System.out.println("  Expected: [/asset1] (image type with 'Java' in title)");
        assertEquals("Should find 1 image asset with Java", 1, results2.size());
        assertTrue("Should contain asset1", results2.contains("/asset1"));
        assertFalse("Should NOT contain asset4 (has JavaScript, not Java)", results2.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        System.out.println("========================================");
        System.out.println("✓ TEST 5 PASSED!");
        System.out.println("Key Finding: Complex combined filters work efficiently with proper index usage");
        System.out.println("Demonstrates: Direct property + Relative property + CONTAINS queries combined");
        System.out.println("========================================\n");
    }
    
    /**
     * Runs indexing using either traditional or change tracking mode.
     * 
     * <p>Traditional Mode: Single AsyncIndexUpdate
     * <p>Change Tracking Mode: Three-Indexer Flow
     * <ol>
     *   <li>Populator: changeTrackingPopulator.run() - Records changes to tracking index</li>
     *   <li>Traditional: traditionalAsyncIndexer.run() - Processes non-CT indexes</li>
     *   <li>Change Tracking: changeTrackingAsyncIndexer.run() - Processes CT indexes</li>
     * </ol>
     */
    private void runAsyncIndexing() throws Exception {
        if (USE_CHANGE_TRACKING) {
            System.out.println("========================================");
            System.out.println("THREE-INDEXER CHANGE TRACKING MODE");
            System.out.println("========================================");
            
            // Phase 1: Populate change tracking index
            System.out.println("PHASE 1: Running ChangeTrackingIndexPopulator...");
            long phase1Start = System.currentTimeMillis();
            changeTrackingPopulator.run();
            long phase1Time = System.currentTimeMillis() - phase1Start;
            System.out.println("Phase 1 complete: " + phase1Time + " ms");
            
            // Query to see how many changes were recorded
            DirectoryReader reader = DirectoryReader.open(changeTrackingDirectory);
            try {
                @SuppressWarnings("resource") // query is not a resource, reader is closed below
                ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader);
                int totalChanges = query.getUnprocessedChanges(0, 0, Integer.MAX_VALUE).size();
                System.out.println("  Change tracking index: " + totalChanges + " entries");
            } finally {
                reader.close();
            }
            
            // Phase 2: Process traditional indexes (none in this test, but would run)
            System.out.println("PHASE 2: Running Traditional AsyncIndexUpdate...");
            long phase2Start = System.currentTimeMillis();
            traditionalAsyncIndexer.run();
            long phase2Time = System.currentTimeMillis() - phase2Start;
            System.out.println("Phase 2 complete: " + phase2Time + " ms");
            
            // Phase 3: Process change-tracked indexes
            System.out.println("PHASE 3: Running ChangeTrackingAsyncIndexUpdate...");
            long phase3Start = System.currentTimeMillis();
            changeTrackingAsyncIndexer.run();
            long phase3Time = System.currentTimeMillis() - phase3Start;
            System.out.println("Phase 3 complete: " + phase3Time + " ms");
            
            // Summary
            long totalTime = phase1Time + phase2Time + phase3Time;
            System.out.println("========================================");
            System.out.println("ALL THREE INDEXERS COMPLETE");
            System.out.println("Total time: " + totalTime + " ms");
            System.out.println("  Phase 1 (Change Tracker Populate): " + phase1Time + " ms");
            System.out.println("  Phase 2 (Traditional Indexer):      " + phase2Time + " ms");
            System.out.println("  Phase 3 (Change Tracked Indexer):   " + phase3Time + " ms");
            System.out.println("========================================");
        } else {
            System.out.println("========================================");
            System.out.println("TRADITIONAL MODE");
            System.out.println("========================================");
            
            // Traditional: Just run async indexing
            long start = System.currentTimeMillis();
            asyncIndexUpdate.run();
            long time = System.currentTimeMillis() - start;
            
            if (asyncIndexUpdate.isFailing()) {
                System.err.println("ERROR: Async indexing failed!");
                System.err.println("Index lane: " + asyncIndexUpdate.getIndexStats());
                fail("Async indexing should not be failing");
            }
            
            System.out.println("Traditional AsyncIndexUpdate complete: " + time + " ms");
            System.out.println("========================================");
        }
    }
    
    /**
     * Helper method to execute a query and return results as a list of paths.
     * With traversal disabled, queries will fail if they cannot use an index.
     */
    private List<String> executeQuery(String query) throws Exception {
        Result result = root.getQueryEngine().executeQuery(
            query, 
            "JCR-SQL2", 
            null, 
            null
        );
        
        List<String> paths = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            paths.add(row.getPath());
        }
        return paths;
    }
}

