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
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
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
 * Run with: mvn test -Dtest=SimpleContainsRelativePropertyTest
 */
public class SimpleContainsRelativePropertyTest {
    
    private ContentRepository repository;
    private Root root;
    private NodeStore nodeStore;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private LuceneIndexProvider luceneIndexProvider;
    
    @Before
    public void setup() throws Exception {
        repository = createRepository();
        root = repository.login(null, null).getLatestRoot();
        
        // Ensure /oak:index exists
        if (!root.getTree("/oak:index").exists()) {
            root.getTree("/").addChild("oak:index");
            root.commit();
        }
        
        // Register DAM node types (dam:Asset, dam:AssetContent)
        registerDamNodeTypes();
    }
    
    @After
    public void teardown() {
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
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
        idxb.build(root.getTree("/oak:index").addChild(indexName));
        root.commit();
        
        System.out.println("✓ Index definition created: " + indexName);
        System.out.println("  - Node type: dam:Asset");
        System.out.println("  - jcr:content/metadata/jcr:title (analyzed, nodeScopeIndex)");
        System.out.println("  - jcr:content/metadata/dc:title (analyzed, nodeScopeIndex)");
        System.out.println("  - jcr:content/metadata/status (exact match)");
    }
    
    /**
     * Comprehensive test for CONTAINS queries on relative properties.
     * 
     * Tests:
     * 1. CONTAINS query on relative property jcr:content/metadata/jcr:title
     * 2. CONTAINS query on relative property jcr:content/metadata/dc:title  
     * 3. Node-scoped CONTAINS (CONTAINS(*, 'term'))
     * 4. Equality query on relative property jcr:content/metadata/status
     * 5. Combined CONTAINS + equality filter
     * 
     * Verifies:
     * - Parent nodes are returned (not child nodes where properties exist)
     * - Multiple assets with different property values are correctly filtered
     * - Both analyzed (fulltext) and non-analyzed (exact match) properties work
     */
    @Test
    public void testContainsQueriesOnRelativeProperties() throws Exception {
        System.out.println("\n========== Simple CONTAINS on Relative Properties Test ==========\n");
        
        // ========================================
        // Step 1: Create Index Definition
        // ========================================
        System.out.println("Step 1: Creating index definition...");
        createDamAssetIndex("damAssetLucene");
        
        // ========================================
        // Step 2: Create Test Content
        // ========================================
        System.out.println("\nStep 2: Creating test content...");
        
        // Asset 1: Java content, published
        Tree asset1 = root.getTree("/").addChild("asset1");
        asset1.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent1 = asset1.addChild("jcr:content");
        jcrContent1.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata1 = jcrContent1.addChild("metadata");
        metadata1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata1.setProperty("jcr:title", "Java Programming Guide");
        metadata1.setProperty("dc:title", "Comprehensive Java Tutorial");
        metadata1.setProperty("status", "published");
        
        // Asset 2: Python content, draft
        Tree asset2 = root.getTree("/").addChild("asset2");
        asset2.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent2 = asset2.addChild("jcr:content");
        jcrContent2.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata2 = jcrContent2.addChild("metadata");
        metadata2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata2.setProperty("jcr:title", "Python Tutorial");
        metadata2.setProperty("dc:title", "Python for Beginners");
        metadata2.setProperty("status", "draft");
        
        // Asset 3: Java content, published
        Tree asset3 = root.getTree("/").addChild("asset3");
        asset3.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent3 = asset3.addChild("jcr:content");
        jcrContent3.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata3 = jcrContent3.addChild("metadata");
        metadata3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata3.setProperty("jcr:title", "Java Best Practices");
        metadata3.setProperty("dc:title", "Advanced Java Techniques");
        metadata3.setProperty("status", "published");
        
        // Asset 4: JavaScript content, published (contains "Java" substring)
        Tree asset4 = root.getTree("/").addChild("asset4");
        asset4.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent4 = asset4.addChild("jcr:content");
        jcrContent4.setProperty("jcr:primaryType", "dam:AssetContent", Type.NAME);
        Tree metadata4 = jcrContent4.addChild("metadata");
        metadata4.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata4.setProperty("jcr:title", "JavaScript Essentials");
        metadata4.setProperty("dc:title", "Modern JavaScript Guide");
        metadata4.setProperty("status", "published");
        
        root.commit();
        
        System.out.println("✓ Created 4 test dam:Asset nodes:");
        System.out.println("  - asset1: dam:Asset with Java content, published");
        System.out.println("  - asset2: dam:Asset with Python content, draft");
        System.out.println("  - asset3: dam:Asset with Java content, published");
        System.out.println("  - asset4: dam:Asset with JavaScript content, published");
        
        // ========================================
        // Step 3: Run Async Indexing
        // ========================================
        System.out.println("\nStep 3: Running async indexing...");
        
        asyncIndexUpdate.run();
        
        if (asyncIndexUpdate.isFailing()) {
            System.err.println("ERROR: Async indexing failed!");
            System.err.println("Index lane: " + asyncIndexUpdate.getIndexStats());
            fail("Async indexing should not be failing");
        }
        
        System.out.println("✓ Async indexing completed successfully");
        
        // ========================================
        // Step 4: Execute Queries
        // ========================================
        System.out.println("\nStep 4: Executing queries...\n");
        
        // Query 1: CONTAINS on jcr:title
        System.out.println("Query 1: CONTAINS([jcr:content/metadata/jcr:title], 'Java')");
        String query1 = "select [jcr:path] from [dam:Asset] where CONTAINS([jcr:content/metadata/jcr:title], 'Java')";
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
        String query2 = "select [jcr:path] from [dam:Asset] where CONTAINS([jcr:content/metadata/dc:title], 'Java')";
        List<String> results2 = executeQuery(query2);
        System.out.println("  Results: " + results2);
        System.out.println("  Expected: [/asset1, /asset3] (contain 'Java' as a word)");
        assertEquals("Should find 2 assets with 'Java'", 2, results2.size());
        assertTrue("Should contain asset1", results2.contains("/asset1"));
        assertTrue("Should contain asset3", results2.contains("/asset3"));
        assertFalse("Should NOT contain asset2", results2.contains("/asset2"));
        assertFalse("Should NOT contain asset4 (JavaScript is a different word)", results2.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 3: Node-scoped CONTAINS
        System.out.println("Query 3: CONTAINS(*, 'Python')");
        String query3 = "select [jcr:path] from [dam:Asset] where CONTAINS(*, 'Python')";
        List<String> results3 = executeQuery(query3);
        System.out.println("  Results: " + results3);
        System.out.println("  Expected: [/asset2] (only asset2 has Python)");
        assertEquals("Should find 1 asset", 1, results3.size());
        assertTrue("Should contain asset2", results3.contains("/asset2"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 4: Equality on status
        System.out.println("Query 4: [jcr:content/metadata/status] = 'published'");
        String query4 = "select [jcr:path] from [dam:Asset] where [jcr:content/metadata/status] = 'published'";
        List<String> results4 = executeQuery(query4);
        System.out.println("  Results: " + results4);
        System.out.println("  Expected: [/asset1, /asset3, /asset4] (all published)");
        assertEquals("Should find 3 published assets", 3, results4.size());
        assertTrue("Should contain asset1", results4.contains("/asset1"));
        assertTrue("Should contain asset3", results4.contains("/asset3"));
        assertTrue("Should contain asset4", results4.contains("/asset4"));
        assertFalse("Should NOT contain asset2 (draft)", results4.contains("/asset2"));
        System.out.println("  ✓ PASSED\n");
        
        // Query 5: Combined CONTAINS + equality filter
        System.out.println("Query 5: CONTAINS([jcr:content/metadata/jcr:title], 'Java') AND status = 'published'");
        String query5 = "select [jcr:path] from [dam:Asset] where " +
                       "CONTAINS([jcr:content/metadata/jcr:title], 'Java') " +
                       "AND [jcr:content/metadata/status] = 'published'";
        List<String> results5 = executeQuery(query5);
        System.out.println("  Results: " + results5);
        System.out.println("  Expected: [/asset1, /asset3] (Java + published)");
        assertEquals("Should find 2 published Java assets", 2, results5.size());
        assertTrue("Should contain asset1", results5.contains("/asset1"));
        assertTrue("Should contain asset3", results5.contains("/asset3"));
        assertFalse("Should NOT contain asset2 (draft)", results5.contains("/asset2"));
        assertFalse("Should NOT contain asset4 (JavaScript, not Java)", results5.contains("/asset4"));
        System.out.println("  ✓ PASSED\n");
        
        // ========================================
        // Summary
        // ========================================
        System.out.println("========================================");
        System.out.println("✓ ALL TESTS PASSED!");
        System.out.println("========================================");
        System.out.println("\nKey Findings:");
        System.out.println("1. CONTAINS queries work correctly on relative properties");
        System.out.println("2. Parent nodes are returned (not child nodes)");
        System.out.println("3. Multiple relative properties can be indexed and queried");
        System.out.println("4. Node-scoped CONTAINS (CONTAINS(*, 'term')) works");
        System.out.println("5. Equality queries work on relative properties");
        System.out.println("6. Combined CONTAINS + equality filters work correctly");
        System.out.println();
    }
    
    /**
     * Helper method to execute a query and return results as a list of paths.
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

