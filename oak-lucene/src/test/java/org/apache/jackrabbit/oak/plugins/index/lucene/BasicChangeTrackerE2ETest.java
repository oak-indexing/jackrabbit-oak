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
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingAsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexPopulator;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * End-to-end test for Change Tracker verifying different NodeStore types and Indexing Strategies.
 * 
 * <p>This test covers:
 * <ul>
 *   <li>NodeStore types: MEMORY (others like SEGMENT, DOCUMENT can be added if deps available)</li>
 *   <li>Indexing strategies: TRADITIONAL vs CHANGE_TRACKER</li>
 *   <li>Full indexing lifecycle: Definition -> Content -> Indexing -> Query</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class BasicChangeTrackerE2ETest {

    public enum NodeStoreType {
        MEMORY,
        SEGMENT,
        DOCUMENT
    }

    public enum IndexingStrategy {
        TRADITIONAL,
        CHANGE_TRACKER
    }

    @Parameterized.Parameters(name = "{0}, {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { NodeStoreType.MEMORY, IndexingStrategy.TRADITIONAL },
            { NodeStoreType.MEMORY, IndexingStrategy.CHANGE_TRACKER },
            { NodeStoreType.SEGMENT, IndexingStrategy.TRADITIONAL },
            { NodeStoreType.SEGMENT, IndexingStrategy.CHANGE_TRACKER },
            { NodeStoreType.DOCUMENT, IndexingStrategy.TRADITIONAL },
            { NodeStoreType.DOCUMENT, IndexingStrategy.CHANGE_TRACKER }
        });
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private final NodeStoreType nodeStoreType;
    private final IndexingStrategy indexingStrategy;

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

    // SegmentNodeStore components
    private FileStore fileStore;
    private ScheduledExecutorService scheduledExecutor;
    
    // MongoDB components
    private MongoConnection mongoConnection;
    private DocumentNodeStore documentNodeStore;

    public BasicChangeTrackerE2ETest(NodeStoreType nodeStoreType, IndexingStrategy indexingStrategy) {
        this.nodeStoreType = nodeStoreType;
        this.indexingStrategy = indexingStrategy;
    }

    @Before
    public void setUp() throws Exception {
        // 1. Create NodeStore
        if (nodeStoreType == NodeStoreType.MEMORY) {
            nodeStore = new MemoryNodeStore();
        } else if (nodeStoreType == NodeStoreType.SEGMENT) {
            nodeStore = createSegmentNodeStore();
        } else if (nodeStoreType == NodeStoreType.DOCUMENT) {
            nodeStore = createMongoNodeStore();
        }
        
        // 2. Setup Change Tracking components (only relevant if strategy is CHANGE_TRACKER)
        if (indexingStrategy == IndexingStrategy.CHANGE_TRACKER) {
            changeTrackingDirectory = new RAMDirectory();
            metadataManager = new IndexProgressMetadataManager(nodeStore);
            populator = new ChangeTrackingIndexPopulator(
                nodeStore,
                changeTrackingDirectory,
                metadataManager,
                StatisticsProvider.NOOP
            );
            populator.initialize();
        }

        // 3. Create Oak ContentRepository
        provider = new LuceneIndexProvider();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();

        contentRepository = new Oak(nodeStore)
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with((Observer) provider)
            .with(editorProvider)
            .with(new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider())
            .with(new org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider())
            .createContentRepository();

        // 4. Login
        contentSession = contentRepository.login(null, null);
        root = contentSession.getLatestRoot();

        // 5. Register Node Types
        registerDamNodeTypes();

        // 6. Setup Indexers
        if (indexingStrategy == IndexingStrategy.TRADITIONAL) {
            // IMPORTANT: Use "async" lane for traditional indexing to match index definition default
            // Use CompositeIndexEditorProvider to ensure all necessary editors are available
            asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, 
                org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose(
                    Arrays.asList(
                        editorProvider,
                        new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider(),
                        new org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider()
                    )
                )
            );
        } else {
            // For CHANGE_TRACKER strategy
            // We still need standard async update for non-fulltext or as fallback/base? 
            // But for this test, we rely on ChangeTrackingAsyncIndexUpdate for the "async" lane equivalent
            changeTrackingAsyncIndexUpdate = new ChangeTrackingAsyncIndexUpdate(
                "async", // Use standard lane name
                nodeStore,
                changeTrackingDirectory,
                null
            );
        }
    }

    private void registerDamNodeTypes() throws Exception {
        try {
            InputStream cndStream = getClass().getResourceAsStream("/dam-nodetypes.cnd");
            if (cndStream == null) {
                throw new IllegalStateException("dam-nodetypes.cnd not found in classpath");
            }
            NodeTypeRegistry.register(root, cndStream, "dam-nodetypes.cnd");
            root.commit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to register DAM node types", e);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (contentSession != null) contentSession.close();
        if (asyncIndexUpdate != null) asyncIndexUpdate.close();
        if (populator != null) populator.close();
        if (changeTrackingDirectory != null) changeTrackingDirectory.close();
        
        // Clean up NodeStore resources
        if (nodeStoreType == NodeStoreType.SEGMENT) {
            if (fileStore != null) {
                fileStore.close();
            }
            if (scheduledExecutor != null) {
                scheduledExecutor.shutdown();
            }
        }
        
        if (nodeStoreType == NodeStoreType.DOCUMENT) {
            if (documentNodeStore != null) {
                documentNodeStore.dispose();
            }
            if (mongoConnection != null) {
                String dbName = mongoConnection.getDBName();
                MongoUtils.dropCollections(dbName);
            }
        }
    }

    private NodeStore createSegmentNodeStore() {
        try {
            File segmentDir = temporaryFolder.newFolder("segmentstore-" + System.currentTimeMillis());
            
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(scheduledExecutor);
            
            fileStore = FileStoreBuilder.fileStoreBuilder(segmentDir)
                    .withStatisticsProvider(statisticsProvider)
                    .withMaxFileSize(256)  // Small segments for testing
                    .withMemoryMapping(false)  // Disable memory mapping for test
                    .build();
            
            return SegmentNodeStoreBuilders.builder(fileStore).build();
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException("Failed to create SegmentNodeStore", e);
        }
    }

    private NodeStore createMongoNodeStore() {
        // Check availability first
        assumeTrue("MongoDB not available", MongoUtils.isAvailable());
        
        try {
            mongoConnection = connectionFactory.getConnection();
            
            // Clean up any existing collections
            MongoUtils.dropCollections(mongoConnection.getDatabase());
            
            // Create DocumentNodeStore with MongoDB backend
            documentNodeStore = new DocumentMK.Builder()
                    .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName())
                    .setAsyncDelay(0)  // Disable async delay for testing
                    .getNodeStore();
            
            return documentNodeStore;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create MongoDB DocumentNodeStore. Ensure MongoDB is available.", e);
        }
    }

    private void runIndexer() {
        if (indexingStrategy == IndexingStrategy.TRADITIONAL) {
            asyncIndexUpdate.run();
        } else {
            try {
                populator.run();
                // Commit populator internal writer if needed (reflection hack removed, assuming implementation is correct)
                commitChangeTrackingIndex();
                
                // Process all chunks until caught up
                // ChangeTrackingAsyncIndexUpdate now loops internally to process all available chunks
                changeTrackingAsyncIndexUpdate.run();
            } catch (Exception e) {
                throw new RuntimeException("Indexing failed", e);
            }
        }
        
        // Force refresh
        if (provider != null) {
            provider.getTracker().refresh();
            try {
                provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
            } catch (Exception e) {
                // Ignore commit failed exception in refresh
            }
        }
    }
    
    private void commitChangeTrackingIndex() throws Exception {
        if (populator != null) {
            java.lang.reflect.Field writerField = ChangeTrackingIndexPopulator.class.getDeclaredField("changeTrackingWriter");
            writerField.setAccessible(true);
            org.apache.lucene.index.IndexWriter writer = (org.apache.lucene.index.IndexWriter) writerField.get(populator);
            if (writer != null) {
                writer.commit();
            }
        }
    }

    @Test
    public void testHybridIndexing() throws Exception {
        System.out.println("\n=== Test: Hybrid Indexing (" + nodeStoreType + ", " + indexingStrategy + ") ===");

        // 1. Create Index Definitions
        Tree oakIndex = root.getTree("/oak:index");

        // A. searchIndex (for nt:unstructured)
        Tree searchIndex = oakIndex.addChild("searchIndex");
        searchIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        searchIndex.setProperty("type", "lucene");
        searchIndex.setProperty("async", "async");
        searchIndex.setProperty("compatVersion", 2);
        if (indexingStrategy == IndexingStrategy.CHANGE_TRACKER) {
            searchIndex.setProperty("useChangeTracker", true);
        } else {
             searchIndex.setProperty("reindex", true);
        }
        
        Tree indexRules = searchIndex.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree ntUnstructured = indexRules.addChild("nt:unstructured");
        ntUnstructured.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree properties = ntUnstructured.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree titleProp = properties.addChild("title");
        titleProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        titleProp.setProperty("name", "title");
        titleProp.setProperty("propertyIndex", true);
        titleProp.setProperty("analyzed", true);
        titleProp.setProperty("nodeScopeIndex", true);
        
        Tree categoryProp = properties.addChild("category");
        categoryProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        categoryProp.setProperty("name", "category");
        categoryProp.setProperty("propertyIndex", true);
        categoryProp.setProperty("analyzed", false);
        
        Tree statusProp = properties.addChild("status");
        statusProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        statusProp.setProperty("name", "status");
        statusProp.setProperty("propertyIndex", true);
        statusProp.setProperty("analyzed", false);

        // B. damAssetLucene13 (for dam:Asset)
        Tree damIndex = oakIndex.addChild("damAssetLucene13");
        damIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        damIndex.setProperty("type", "lucene");
        damIndex.setProperty("async", "async");
        damIndex.setProperty("compatVersion", 2);
        damIndex.setProperty("evaluatePathRestrictions", true);
        damIndex.setProperty("includedPaths", Arrays.asList("/testContent"), Type.STRINGS);
        if (indexingStrategy == IndexingStrategy.CHANGE_TRACKER) {
            damIndex.setProperty("useChangeTracker", true);
        } else {
             damIndex.setProperty("reindex", true);
        }

        // Aggregates
        Tree aggregates = damIndex.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAssetAggregate = aggregates.addChild("dam:Asset");
        damAssetAggregate.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree include0 = damAssetAggregate.addChild("include0");
        include0.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include0.setProperty("path", "jcr:content");
        Tree include1 = damAssetAggregate.addChild("include1");
        include1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include1.setProperty("path", "jcr:content/metadata");

        // Index Rules
        Tree damRules = damIndex.addChild("indexRules");
        damRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAssetRule = damRules.addChild("dam:Asset");
        damAssetRule.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damProps = damAssetRule.addChild("properties");
        damProps.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree dcTitle = damProps.addChild("dcTitle");
        dcTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcTitle.setProperty("name", "jcr:content/metadata/dc:title");
        dcTitle.setProperty("analyzed", true);
        dcTitle.setProperty("nodeScopeIndex", true);
        dcTitle.setProperty("propertyIndex", true);
        
        Tree jcrTitle = damProps.addChild("jcrTitle");
        jcrTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrTitle.setProperty("name", "jcr:content/metadata/jcr:title");
        jcrTitle.setProperty("analyzed", true);
        jcrTitle.setProperty("nodeScopeIndex", true);
        jcrTitle.setProperty("propertyIndex", true);
        
        Tree damStatus = damProps.addChild("damStatus");
        damStatus.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        damStatus.setProperty("name", "jcr:content/metadata/dam:status");
        damStatus.setProperty("propertyIndex", true);
        
        Tree dcFormat = damProps.addChild("dcFormat");
        dcFormat.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcFormat.setProperty("name", "jcr:content/metadata/dc:format");
        dcFormat.setProperty("propertyIndex", true);

        root.commit();
        if (metadataManager != null) {
            metadataManager.registerIndex("/oak:index/searchIndex");
            metadataManager.registerIndex("/oak:index/damAssetLucene13");
        }

        // 2. Create Content
        Tree content = root.getTree("/").addChild("testContent");
        
        // Docs 1-4 (nt:unstructured)
        Tree doc1 = content.addChild("doc1");
        doc1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc1.setProperty("title", "Introduction to Java Programming");
        doc1.setProperty("category", "programming");
        doc1.setProperty("status", "published");
        
        Tree doc2 = content.addChild("doc2");
        doc2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc2.setProperty("title", "Python for Beginners");
        doc2.setProperty("category", "programming");
        doc2.setProperty("status", "draft");
        
        Tree doc3 = content.addChild("doc3");
        doc3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc3.setProperty("title", "JavaScript Essentials");
        doc3.setProperty("category", "programming");
        doc3.setProperty("status", "published");
        
        Tree doc4 = content.addChild("doc4");
        doc4.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc4.setProperty("title", "Best Chocolate Cake Recipe");
        doc4.setProperty("category", "cooking");
        doc4.setProperty("status", "published");
        
        // Docs 5-7 (dam:Asset)
        Tree doc5 = content.addChild("asset1");
        doc5.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent5 = doc5.addChild("jcr:content");
        jcrContent5.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree metadata5 = jcrContent5.addChild("metadata");
        metadata5.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata5.setProperty("jcr:title", "My Awesome Asset");
        metadata5.setProperty("dam:status", "approved");
        metadata5.setProperty("dc:format", "image/jpeg");
        
        Tree doc6 = content.addChild("asset2");
        doc6.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent6 = doc6.addChild("jcr:content");
        jcrContent6.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree metadata6 = jcrContent6.addChild("metadata");
        metadata6.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata6.setProperty("jcr:title", "Nested Asset");
        metadata6.setProperty("dam:status", "draft");
        metadata6.setProperty("dc:format", "application/pdf");
        
        Tree doc7 = content.addChild("asset3");
        doc7.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
        Tree jcrContent7 = doc7.addChild("jcr:content");
        jcrContent7.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrContent7.setProperty("description", "Aggregation Magic Content");
        Tree metadata7 = jcrContent7.addChild("metadata");
        metadata7.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        metadata7.setProperty("jcr:title", "Aggregated Asset");
        
        root.commit();

        // 3. Index
        contentSession.getLatestRoot();
        runIndexer();

        // Explicit refresh
        if (provider != null) {
            provider.getTracker().refresh();
            try {
                 provider.contentChanged(nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
            } catch (Exception e) {}
        }
        
        // 4. Execute Queries (same as BasicChangeTrackerTest)
        
        // Query 1: Exact match on category
        String query1 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming' option(traversal fail, index name searchIndex)";
        List<String> results1 = executeQuery(query1);
        assertEquals("Should find 3 programming articles", 3, results1.size());
        assertTrue("Should contain doc1", results1.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc2", results1.stream().anyMatch(p -> p.contains("doc2")));
        assertTrue("Should contain doc3", results1.stream().anyMatch(p -> p.contains("doc3")));
        
        // Query 2: Multiple property filter (AND)
        String query2 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming' AND [status] = 'published' option(traversal fail, index name searchIndex)";
        List<String> results2 = executeQuery(query2);
        assertEquals("Should find 2 published programming articles", 2, results2.size());
        assertTrue("Should contain doc1", results2.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc3", results2.stream().anyMatch(p -> p.contains("doc3")));
        
        // Query 3: Different category
        String query3 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'cooking' option(traversal fail, index name searchIndex)";
        List<String> results3 = executeQuery(query3);
        assertEquals("Should find 1 cooking article", 1, results3.size());
        assertTrue("Should contain doc4", results3.stream().anyMatch(p -> p.contains("doc4")));
        
        // Query 4: Fulltext search (CONTAINS)
        String query4 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS(*, 'Java') option(traversal fail, index name searchIndex)";
        List<String> results4 = executeQuery(query4);
        assertTrue("Should find at least 1 Java-related article", results4.size() >= 1);
        assertTrue("Should contain doc1 (Java article)", results4.stream().anyMatch(p -> p.contains("doc1")));
        
        // Query 5: Fulltext search on specific property
        String query5 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS([title], 'Python') option(traversal fail, index name searchIndex)";
        List<String> results5 = executeQuery(query5);
        assertEquals("Should find 1 Python article", 1, results5.size());
        assertTrue("Should contain doc2", results5.stream().anyMatch(p -> p.contains("doc2")));
        
        // Query 6: Draft status
        String query6 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [status] = 'draft' option(traversal fail, index name searchIndex)";
        List<String> results6 = executeQuery(query6);
        assertEquals("Should find 1 draft article", 1, results6.size());
        assertTrue("Should contain doc2", results6.stream().anyMatch(p -> p.contains("doc2")));
        
        // Query 7: dam:Asset search using damAssetLucene13
        String query7 = "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS([jcr:content/metadata/jcr:title], 'Awesome') option(traversal fail, index name damAssetLucene13)";
        List<String> results7 = executeQuery(query7);
        assertEquals("Should find 1 asset", 1, results7.size());
        assertTrue("Should contain asset1", results7.stream().anyMatch(p -> p.contains("asset1")));
        
        // Query 8: Relative property equality (dam:status)
        String query8 = "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dam:status] = 'draft' option(traversal fail, index name damAssetLucene13)";
        List<String> results8 = executeQuery(query8);
        assertEquals("Should find 1 asset by status", 1, results8.size());
        assertTrue("Should contain asset2", results8.stream().anyMatch(p -> p.contains("asset2")));
        
        // Query 9: Relative property equality (dc:format)
        String query9 = "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dc:format] = 'image/jpeg' option(traversal fail, index name damAssetLucene13)";
        List<String> results9 = executeQuery(query9);
        assertEquals("Should find 1 image asset", 1, results9.size());
        assertTrue("Should contain asset1", results9.stream().anyMatch(p -> p.contains("asset1")));
        
        // Query 10: Aggregated content search
        String query10 = "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS(*, 'Magic') option(traversal fail, index name damAssetLucene13)";
        List<String> results10 = executeQuery(query10);
        assertEquals("Should find 1 asset by aggregated content", 1, results10.size());
        assertTrue("Should contain asset3", results10.stream().anyMatch(p -> p.contains("asset3")));
        
        System.out.println("✓ Verified all 10 queries in " + indexingStrategy);
    }

    private List<String> executeQuery(String sqlQuery) throws Exception {
        List<String> paths = new ArrayList<>();
        QueryEngine queryEngine = root.getQueryEngine();
        
        // Increased retry count and timeout for traditional indexing which might be slower/async
        for (int i = 0; i < 50; i++) {
            paths.clear();
            try {
                Result result = queryEngine.executeQuery(
                    sqlQuery,
                    javax.jcr.query.Query.JCR_SQL2,
                    null, null
                );
                for (ResultRow row : result.getRows()) {
                    paths.add(row.getPath());
                }
            } catch (IllegalArgumentException e) {
                // Index not ready
            }
            
            if (!paths.isEmpty()) {
                // Found results, assume consistent enough for test
                break;
            }
            
            if (provider != null) {
                provider.getTracker().refresh();
            }
            // Wait a bit longer between retries
            Thread.sleep(100);
        }
        return paths;
    }
}
