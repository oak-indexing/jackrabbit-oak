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
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import java.lang.management.ThreadMXBean;

import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class BasicChangeTrackerPerfTest {

    public enum NodeStoreType {
        MEMORY,
        SEGMENT,
        DOCUMENT
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { NodeStoreType.MEMORY },
            { NodeStoreType.SEGMENT },
            { NodeStoreType.DOCUMENT }
        });
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private final NodeStoreType nodeStoreType;
    
    // Configurable parameters
    private static final int NODE_COUNT = 1000;
    private static final int BATCH_SIZE = 100;

    public BasicChangeTrackerPerfTest(NodeStoreType nodeStoreType) {
        this.nodeStoreType = nodeStoreType;
    }

    @Test
    public void comparePerformance() throws Exception {
        System.out.println(String.format("\n=== Performance Comparison: %s (Nodes: %d) ===", nodeStoreType, NODE_COUNT));
        
        // Run Traditional
        Result traditional = runTest(false);
        System.out.println("\n--- Traditional Strategy ---");
        System.out.println(traditional);

        // Run Change Tracker
        Result changeTracker = runTest(true);
        System.out.println("\n--- Change Tracker Strategy ---");
        System.out.println(changeTracker);

        // Comparison
        System.out.println("\n--- Comparison ---");
        double speedup = (double) traditional.totalTimeMs / changeTracker.totalTimeMs;
        System.out.printf("Speedup (Traditional / ChangeTracker): %.2fx%n", speedup);
        System.out.printf("Traditional Throughput: %.2f nodes/sec%n", traditional.throughput);
        System.out.printf("ChangeTracker Throughput: %.2f nodes/sec%n", changeTracker.throughput);
    }

    private Result runTest(boolean useChangeTracker) throws Exception {
        PerfContext ctx = new PerfContext();
        setupContext(ctx, useChangeTracker);

        try {
            // 1. Create Content
            long startContent = System.currentTimeMillis();
            Tree content = ctx.root.getTree("/").addChild("content");
            for (int i = 0; i < NODE_COUNT; i++) {
                Tree item = content.addChild("asset-" + i);
                item.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
                
                Tree jcrContent = item.addChild("jcr:content");
                jcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                
                Tree metadata = jcrContent.addChild("metadata");
                metadata.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                
                // Set properties defined in damAssetLucene13
                metadata.setProperty("jcr:title", "Asset Title " + i);
                metadata.setProperty("dc:title", "Dublin Core Title " + i);
                metadata.setProperty("dc:creator", "User " + (i % 50));
                metadata.setProperty("dam:status", (i % 2 == 0) ? "approved" : "draft");
                metadata.setProperty("dc:format", (i % 3 == 0) ? "image/jpeg" : (i % 3 == 1) ? "application/pdf" : "video/mp4");
                
                // New properties from full index
                metadata.setProperty("cq:tags", Arrays.asList("tag1", "tag" + (i % 10)), Type.STRINGS);
                metadata.setProperty("dam:size", (long) (i * 1024));
                metadata.setProperty("dam:sha1", "hash" + i);
                metadata.setProperty("jcr:lastModified", ISO8601.format(Calendar.getInstance()));
                
                // Create aggregated child nodes (even if empty, they are part of the structure)
                Tree renditions = jcrContent.addChild("renditions");
                renditions.setProperty("jcr:primaryType", "nt:folder", Type.NAME); // Often nt:folder or nt:unstructured
                Tree original = renditions.addChild("original");
                original.setProperty("jcr:primaryType", "nt:file", Type.NAME);
                Tree originalContent = original.addChild("jcr:content");
                originalContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
                originalContent.setProperty("jcr:data", "binary-placeholder".getBytes());
                originalContent.setProperty("jcr:mimeType", (i % 3 == 0) ? "image/jpeg" : "application/octet-stream");
                
                // Text extraction rendition
                Tree txtRendition = renditions.addChild("cqdam.text.txt");
                txtRendition.setProperty("jcr:primaryType", "nt:file", Type.NAME);
                Tree txtContent = txtRendition.addChild("jcr:content");
                txtContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
                txtContent.setProperty("jcr:data", ("Extracted text content for asset " + i).getBytes());
                
                Tree comments = jcrContent.addChild("comments");
                comments.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                Tree comment1 = comments.addChild("comment1");
                comment1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                comment1.setProperty("text", "This is a comment on asset " + i);
                
                Tree usages = jcrContent.addChild("usages");
                usages.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                usages.setProperty("usedBy", Arrays.asList("/content/page1", "/content/page2"), Type.STRINGS);
                
                // Add master data
                Tree data = jcrContent.addChild("data");
                data.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                Tree master = data.addChild("master");
                master.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                master.setProperty("modelPath", "/conf/my-model");
                
                // Add subassets structure
                Tree subassets = item.addChild("subassets");
                subassets.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                Tree subasset1 = subassets.addChild("sub1");
                subasset1.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
                Tree subJcrContent = subasset1.addChild("jcr:content");
                subJcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                Tree subRenditions = subJcrContent.addChild("renditions");
                subRenditions.setProperty("jcr:primaryType", "nt:folder", Type.NAME);
                Tree subOriginal = subRenditions.addChild("original");
                subOriginal.setProperty("jcr:primaryType", "nt:file", Type.NAME);
                Tree subOriginalContent = subOriginal.addChild("jcr:content");
                subOriginalContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
                subOriginalContent.setProperty("jcr:data", "subasset-binary".getBytes());
                
                if (i % BATCH_SIZE == 0) {
                    ctx.root.commit();
                }
            }
            ctx.root.commit();
            long contentTime = System.currentTimeMillis() - startContent;

            // 2. Indexing
            // Re-login to ensure fresh session for indexing if needed (though usually not required for AsyncIndexUpdate)
            
            java.lang.management.ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            if (threadBean.isThreadCpuTimeSupported()) {
                threadBean.setThreadCpuTimeEnabled(true);
            }
            threadBean.resetPeakThreadCount();

            long startIndexing = System.currentTimeMillis();
            long phase1Time = 0;
            long phase3Time = 0;
            long startMem = getUsedMemory();
            long startGcCount = getGcCount();
            long startGcTime = getGcTime();
            long startCpuTime = getProcessCpuTime();

            if (useChangeTracker) {
                // Phase 1: Populator
                long p1Start = System.currentTimeMillis();
                ctx.populator.run();
                // Commit internal writer
                java.lang.reflect.Field writerField = ChangeTrackingIndexPopulator.class.getDeclaredField("changeTrackingWriter");
                writerField.setAccessible(true);
                org.apache.lucene.index.IndexWriter writer = (org.apache.lucene.index.IndexWriter) writerField.get(ctx.populator);
                if (writer != null) writer.commit();
                phase1Time = System.currentTimeMillis() - p1Start;

                // Phase 3: Change Tracker Async
                long p3Start = System.currentTimeMillis();
                ctx.changeTrackingAsyncIndexUpdate.run();
                phase3Time = System.currentTimeMillis() - p3Start;
            } else {
                ctx.asyncIndexUpdate.run();
            }
            
            long totalIndexTime = System.currentTimeMillis() - startIndexing;
            
            // DEBUG: Check index state
            ctx.root.refresh();
            Tree idx = ctx.root.getTree("/oak:index/damAssetLucene13");
            System.out.println("DEBUG: Index Exists: " + idx.exists());
            System.out.println("DEBUG: Index Has :data: " + idx.hasChild(":data"));
            System.out.println("DEBUG: Index Reindex Count: " + idx.getProperty("reindexCount"));
            System.out.println("DEBUG: Content Root Children: " + ctx.root.getTree("/content").getChildrenCount(100));
            
            long endMem = getUsedMemory();
            long endGcCount = getGcCount();
            long endGcTime = getGcTime();
            long endCpuTime = getProcessCpuTime();
            long directBufferMem = getDirectBufferMemory();
            long diskUsage = getDiskUsage(ctx);

            // 3. Verification Queries
            long queryStart = System.currentTimeMillis();
            int q1Count = 0;
            int q2Count = 0;
            int q3Count = 0;
            int q4Count = 0;
            int q5Count = 0;
            String q6Facets = "";

            // Robust retry loop for index visibility (similar to E2E test)
            boolean consistent = false;
            for (int i = 0; i < 50; i++) {
                try {
                    // Refresh index tracker to ensure query sees the index
                    ctx.root.refresh();
                    ctx.provider.getTracker().refresh();
                    // Trigger manual content change to force tracker update if needed
                    try {
                        ctx.provider.contentChanged(ctx.nodeStore.getRoot(), org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
                    } catch (Exception e) {
                        // ignore
                    }

                    q1Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dam:status] = 'approved' option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    q2Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS([jcr:content/metadata/jcr:title], 'Title') option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    q3Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dam:size] > 0 option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    q4Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/cq:tags] = 'tag1' option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    q5Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS(*, 'Extracted text') option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    
                    q6Facets = executeFacetQuery(ctx, "SELECT [jcr:path], [rep:facet(jcr:content/metadata/dam:status)] FROM [dam:Asset] WHERE [jcr:content/metadata/dam:status] IS NOT NULL option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");

                    // If we get here without exception and get results (expected > 0), check consistency
                    if (q1Count > 0) {
                        consistent = true;
                        break;
                    }
                    Thread.sleep(100);
                } catch (Exception e) {
                    // Swallow IllegalArgumentException (traversal fail) and others during retry
                    if (i == 49) throw new RuntimeException("Queries failed after retries", e);
                    Thread.sleep(100);
                }
            }
            
            if (!consistent) {
                throw new RuntimeException("Queries returned 0 results after retries");
            }
            
            long queryTime = System.currentTimeMillis() - queryStart;

            System.out.println("Query 1 (status='approved'): " + q1Count + " (Expected: 500)");
            System.out.println("Query 2 (contains 'Title'): " + q2Count + " (Expected: 1000)");
            System.out.println("Query 3 (size > 0): " + q3Count + " (Expected: 1000)");
            System.out.println("Query 4 (tags='tag1'): " + q4Count + " (Expected: 1000)");
            System.out.println("Query 5 (aggregation 'Extracted text'): " + q5Count + " (Expected: 1000)");
            System.out.println("Query 6 (Facets): " + q6Facets);

            Result result = new Result();
            result.totalTimeMs = totalIndexTime;
            result.contentCreationTimeMs = contentTime;
            result.phase1TimeMs = phase1Time;
            result.phase3TimeMs = phase3Time;
            result.throughput = (double) NODE_COUNT / (totalIndexTime / 1000.0);
            result.memoryUsedBytes = endMem - startMem; // Approximate delta
            result.gcCount = endGcCount - startGcCount;
            result.gcTimeMs = endGcTime - startGcTime;
            result.processCpuTimeMs = (endCpuTime != -1 && startCpuTime != -1) ? (endCpuTime - startCpuTime) : -1;
            result.directBufferMemoryBytes = directBufferMem;
            result.diskUsageBytes = diskUsage;
            result.maxHeapUsedBytes = getMaxHeapUsed();
            result.maxNonHeapUsedBytes = getMaxNonHeapUsed();
            result.peakThreadCount = threadBean.getPeakThreadCount();
            result.queryTimeMs = queryTime;
            result.queryResult1 = q1Count;
            result.queryResult2 = q2Count;
            result.queryResult3 = q3Count;
            result.queryResult4 = q4Count;
            result.queryResult5 = q5Count;
            result.facetResult = q6Facets;
            
            // NodeStore IO stats could be fetched here if available (e.g. FileStore stats)

            return result;

        } finally {
            teardownContext(ctx);
        }
    }

    private static long getUsedMemory() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private class PerfContext {
        NodeStore nodeStore;
        Directory changeTrackingDirectory;
        ChangeTrackingIndexPopulator populator;
        IndexProgressMetadataManager metadataManager;
        ContentRepository contentRepository;
        ContentSession contentSession;
        Root root;
        AsyncIndexUpdate asyncIndexUpdate;
        ChangeTrackingAsyncIndexUpdate changeTrackingAsyncIndexUpdate;
        LuceneIndexProvider provider;
        LuceneIndexEditorProvider editorProvider;
        
        FileStore fileStore;
        File storeDir;
        ScheduledExecutorService scheduledExecutor;
        MongoConnection mongoConnection;
        DocumentNodeStore documentNodeStore;
    }

    private void setupContext(PerfContext ctx, boolean useChangeTracker) throws Exception {
        // NodeStore
        if (nodeStoreType == NodeStoreType.MEMORY) {
            ctx.nodeStore = new MemoryNodeStore();
        } else if (nodeStoreType == NodeStoreType.SEGMENT) {
            File segmentDir = temporaryFolder.newFolder("segment-" + System.nanoTime());
            ctx.storeDir = segmentDir;
            ctx.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(ctx.scheduledExecutor);
            ctx.fileStore = FileStoreBuilder.fileStoreBuilder(segmentDir)
                    .withStatisticsProvider(statisticsProvider)
                    .withMaxFileSize(256)
                    .withMemoryMapping(false)
                    .build();
            ctx.nodeStore = SegmentNodeStoreBuilders.builder(ctx.fileStore).build();
        } else if (nodeStoreType == NodeStoreType.DOCUMENT) {
            assumeTrue("MongoDB not available", MongoUtils.isAvailable());
            ctx.mongoConnection = connectionFactory.getConnection();
            MongoUtils.dropCollections(ctx.mongoConnection.getDatabase());
            ctx.documentNodeStore = new DocumentMK.Builder()
                    .setMongoDB(ctx.mongoConnection.getMongoClient(), ctx.mongoConnection.getDBName())
                    .setAsyncDelay(0)
                    .getNodeStore();
            ctx.nodeStore = ctx.documentNodeStore;
        }

        // CT Components
        if (useChangeTracker) {
            ctx.changeTrackingDirectory = new RAMDirectory();
            ctx.metadataManager = new IndexProgressMetadataManager(ctx.nodeStore);
            ctx.populator = new ChangeTrackingIndexPopulator(
                ctx.nodeStore, ctx.changeTrackingDirectory, ctx.metadataManager, StatisticsProvider.NOOP
            );
            ctx.populator.initialize();
        }

        // Repository
        ctx.provider = new LuceneIndexProvider();
        ctx.editorProvider = new LuceneIndexEditorProvider();
        
        ctx.contentRepository = new Oak(ctx.nodeStore)
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) ctx.provider)
            .with((Observer) ctx.provider)
            .with(ctx.editorProvider)
            .with(new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider())
            .createContentRepository();

        ctx.contentSession = ctx.contentRepository.login(null, null);
        ctx.root = ctx.contentSession.getLatestRoot();
        
        // Register DAM node types
        registerDamNodeTypes(ctx.root);

        // Index Definition: damAssetLucene13
        Tree oakIndex = ctx.root.getTree("/oak:index");
        Tree index = oakIndex.addChild("damAssetLucene13");
        index.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        index.setProperty("type", "lucene");
        index.setProperty("async", "async");
        index.setProperty("compatVersion", 2);
        index.setProperty("evaluatePathRestrictions", true);
        index.setProperty("includedPaths", Arrays.asList("/content"), Type.STRINGS);
        if (useChangeTracker) {
            // CRITICAL: Do NOT set reindex=true for Change Tracker.
            // Failure Analysis:
            // The ChangeTrackingIndexPopulator handles the initial population ("reindexing")
            // by traversing the repository. Setting reindex=true causes the query engine
            // to treat the index as "unavailable" (waiting for standard reindex) while
            // the change tracker is trying to update it, leading to Traversal exceptions.
            index.setProperty("useChangeTracker", true);
        } else {
            index.setProperty("reindex", true);
        }

        // Aggregates (12 includes)
        Tree aggregates = index.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damAssetAgg = aggregates.addChild("dam:Asset");
        damAssetAgg.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        damAssetAgg.addChild("include0").setProperty("path", "jcr:content");
        damAssetAgg.addChild("include1").setProperty("path", "jcr:content/metadata");
        damAssetAgg.addChild("include2").setProperty("path", "jcr:content/metadata/*");
        damAssetAgg.addChild("include3").setProperty("path", "jcr:content/renditions");
        damAssetAgg.addChild("include4").setProperty("path", "jcr:content/renditions/original");
        damAssetAgg.addChild("include5").setProperty("path", "jcr:content/renditions/original/jcr:content");
        damAssetAgg.addChild("include6").setProperty("path", "jcr:content/comments");
        damAssetAgg.addChild("include7").setProperty("path", "jcr:content/comments/*");
        damAssetAgg.addChild("include8").setProperty("path", "jcr:content/data/master");
        damAssetAgg.addChild("include9").setProperty("path", "jcr:content/usages");
        damAssetAgg.addChild("include10").setProperty("path", "jcr:content/renditions/cqdam.text.txt/jcr:content");
        damAssetAgg.addChild("include11").setProperty("path", "subassets/*/jcr:content/renditions/original/jcr:content");

        // Facets configuration
        Tree facets = index.addChild("facets");
        facets.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        facets.setProperty("topChildren", 100);
        facets.setProperty("secure", "statistical");

        // Index Rules
        Tree indexRules = index.addChild("indexRules");
        Tree damAsset = indexRules.addChild("dam:Asset");
        damAsset.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree properties = damAsset.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Properties
        Tree dcTitle = properties.addChild("dcTitle");
        dcTitle.setProperty("name", "jcr:content/metadata/dc:title");
        dcTitle.setProperty("analyzed", true);
        dcTitle.setProperty("nodeScopeIndex", true);
        dcTitle.setProperty("propertyIndex", true);
        dcTitle.setProperty("useInSpellcheck", true);
        
        Tree jcrTitle = properties.addChild("jcrTitle");
        jcrTitle.setProperty("name", "jcr:content/metadata/jcr:title");
        jcrTitle.setProperty("analyzed", true);
        jcrTitle.setProperty("nodeScopeIndex", true);
        jcrTitle.setProperty("propertyIndex", true);
        jcrTitle.setProperty("useInSpellcheck", true);
        
        Tree dcCreator = properties.addChild("dcCreator");
        dcCreator.setProperty("name", "jcr:content/metadata/dc:creator");
        dcCreator.setProperty("propertyIndex", true);

        Tree damStatus = properties.addChild("damStatus");
        damStatus.setProperty("name", "jcr:content/metadata/dam:status");
        damStatus.setProperty("propertyIndex", true);
        damStatus.setProperty("facets", true);
        
        Tree dcFormat = properties.addChild("dcFormat");
        dcFormat.setProperty("name", "jcr:content/metadata/dc:format");
        dcFormat.setProperty("propertyIndex", true);
        
        Tree cqTags = properties.addChild("cqTags");
        cqTags.setProperty("name", "jcr:content/metadata/cq:tags");
        cqTags.setProperty("nodeScopeIndex", true);
        cqTags.setProperty("propertyIndex", true);
        cqTags.setProperty("analyzed", true);
        cqTags.setProperty("useInSuggest", true);
        cqTags.setProperty("facets", true);
        
        Tree damSize = properties.addChild("damSize");
        damSize.setProperty("name", "jcr:content/metadata/dam:size");
        damSize.setProperty("propertyIndex", true);
        damSize.setProperty("type", "Long");
        damSize.setProperty("ordered", true);
        
        Tree jcrLastModified = properties.addChild("jcrLastModified");
        jcrLastModified.setProperty("name", "jcr:content/metadata/jcr:lastModified");
        jcrLastModified.setProperty("propertyIndex", true);
        jcrLastModified.setProperty("type", "Date");
        jcrLastModified.setProperty("ordered", true);
        
        Tree damSha1 = properties.addChild("damSha1");
        damSha1.setProperty("name", "jcr:content/metadata/dam:sha1");
        damSha1.setProperty("propertyIndex", true);
        
        ctx.root.commit();
        
        // Register index in metadata if needed
        if (useChangeTracker) {
             // CRITICAL: Register the index with the metadata manager.
             // Failure Analysis:
             // If this is skipped, ChangeTrackingAsyncIndexUpdate may not be aware of this index 
             // or wont be able to track its checkpoint state properly.
             // This leads to a situation where the populator runs (diffs are calculated), 
             // but the async indexer doesn't process the changes for this specific index 
             // or fails to commit the index update, resulting in 0 query results.
             // In E2E tests, this manifested as "Queries returned 0 results after retries".
             ctx.metadataManager.registerIndex("/oak:index/damAssetLucene13");
        }

        // Indexers
        if (useChangeTracker) {
            ctx.changeTrackingAsyncIndexUpdate = new ChangeTrackingAsyncIndexUpdate(
                "async", ctx.nodeStore, ctx.changeTrackingDirectory, null
            );
        } else {
            ctx.asyncIndexUpdate = new AsyncIndexUpdate("async", ctx.nodeStore, 
                org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose(
                    Arrays.asList(
                        ctx.editorProvider,
                        new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider(),
                        new org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider()
                    )
                )
            );
        }
    }

    private void teardownContext(PerfContext ctx) throws Exception {
        if (ctx.contentSession != null) ctx.contentSession.close();
        if (ctx.asyncIndexUpdate != null) ctx.asyncIndexUpdate.close();
        if (ctx.populator != null) ctx.populator.close();
        if (ctx.changeTrackingDirectory != null) ctx.changeTrackingDirectory.close();
        
        if (ctx.fileStore != null) ctx.fileStore.close();
        if (ctx.scheduledExecutor != null) ctx.scheduledExecutor.shutdown();
        if (ctx.documentNodeStore != null) ctx.documentNodeStore.dispose();
        // Mongo connection closed by rule if used? No, connectionFactory rule manages resources but connection itself might stay open.
        // The assumption is Rule cleans up.
    }

    private static long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) {
                sum += count;
            }
        }
        return sum;
    }

    private static long getGcTime() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = b.getCollectionTime();
            if (time != -1) {
                sum += time;
            }
        }
        return sum;
    }

    private void registerDamNodeTypes(Root root) {
        try {
            InputStream stream = getClass().getResourceAsStream("/dam-nodetypes.cnd");
            if (stream == null) {
                stream = getClass().getClassLoader().getResourceAsStream("dam-nodetypes.cnd");
            }
            NodeTypeRegistry.register(root, stream, "dam-nodetypes.cnd");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static long getMaxHeapUsed() {
        long sum = 0;
        for (java.lang.management.MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == java.lang.management.MemoryType.HEAP) {
                sum += pool.getPeakUsage().getUsed();
            }
        }
        return sum;
    }

    private static long getMaxNonHeapUsed() {
        long sum = 0;
        for (java.lang.management.MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == java.lang.management.MemoryType.NON_HEAP) {
                sum += pool.getPeakUsage().getUsed();
            }
        }
        return sum;
    }

    private int executeQuery(PerfContext ctx, String statement, String language) {
        try {
            org.apache.jackrabbit.oak.api.Result result = ctx.root.getQueryEngine().executeQuery(
                statement, language, 
                java.util.Collections.emptyMap(),
                org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS
            );
            int count = 0;
            for (org.apache.jackrabbit.oak.api.ResultRow row : result.getRows()) {
                // Iterate to consume iterator
                row.getPath();
                count++;
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String executeFacetQuery(PerfContext ctx, String statement, String language) {
        try {
            org.apache.jackrabbit.oak.api.Result result = ctx.root.getQueryEngine().executeQuery(
                statement, language, 
                java.util.Collections.emptyMap(),
                org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS
            );
            StringBuilder sb = new StringBuilder();
            for (org.apache.jackrabbit.oak.api.ResultRow row : result.getRows()) {
                String facet = row.getValue("rep:facet(jcr:content/metadata/dam:status)").toString();
                if (facet != null && !facet.isEmpty() && !facet.equals("null")) {
                    sb.append(facet).append("; ");
                }
            }
            return sb.toString();
        } catch (Exception e) {
            // Don't throw, just return empty string to avoid failing the whole test if facets not ready
            return "";
        }
    }

    private static class Result {
        long totalTimeMs;
        long contentCreationTimeMs;
        long phase1TimeMs;
        long phase3TimeMs;
        double throughput;
        long memoryUsedBytes;
        long gcCount;
        long gcTimeMs;
        long maxHeapUsedBytes;
        long maxNonHeapUsedBytes;
        int peakThreadCount;
        long processCpuTimeMs;
        long directBufferMemoryBytes;
        long diskUsageBytes;
        long queryTimeMs;
        int queryResult1;
        int queryResult2;
        int queryResult3;
        int queryResult4;
        int queryResult5;
        String facetResult;

        @Override
        public String toString() {
            return String.format("Total Time: %d ms%n" +
                               "Content Creation: %d ms%n" +
                               "Throughput: %.2f nodes/sec%n" +
                               "Memory Delta: %d KB%n" + 
                               "Max Heap Used: %d MB%n" +
                               "Max Non-Heap Used: %d MB%n" +
                               "Peak Threads: %d%n" +
                               "Process CPU Time: %d ms%n" +
                               "Direct Buffer Memory: %d KB%n" +
                               "Disk Usage: %d KB%n" +
                               "GC Count: %d%n" +
                               "GC Time: %d ms%n" +
                               "Phase 1 (Populate): %d ms%n" +
                               "Phase 3 (Index): %d ms%n" +
                               "Query Time: %d ms%n" +
                               "Query 1 Results: %d%n" +
                               "Query 2 Results: %d%n" +
                               "Query 3 Results: %d%n" +
                               "Query 4 Results: %d%n" +
                               "Query 5 Results: %d%n" +
                               "Facet Result: %s",
                               totalTimeMs, contentCreationTimeMs, throughput, memoryUsedBytes / 1024, 
                               maxHeapUsedBytes / (1024 * 1024), maxNonHeapUsedBytes / (1024 * 1024), peakThreadCount,
                               processCpuTimeMs, directBufferMemoryBytes / 1024, diskUsageBytes / 1024,
                               gcCount, gcTimeMs,
                               phase1TimeMs, phase3TimeMs,
                               queryTimeMs, queryResult1, queryResult2, queryResult3, queryResult4, queryResult5, facetResult);
        }
    }

    private static long getProcessCpuTime() {
        java.lang.management.OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) osBean).getProcessCpuTime() / 1_000_000;
        }
        return -1;
    }

    private static long getDirectBufferMemory() {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if (pool.getName().equals("direct")) {
                return pool.getMemoryUsed();
            }
        }
        return -1;
    }

    private static long getDiskUsage(PerfContext ctx) {
        if (ctx.storeDir != null) {
            try {
                try (java.util.stream.Stream<Path> walk = Files.walk(ctx.storeDir.toPath())) {
                    return walk.filter(p -> p.toFile().isFile())
                            .mapToLong(p -> p.toFile().length())
                            .sum();
                }
            } catch (IOException e) {
                System.err.println("Error calculating disk usage: " + e.getMessage());
            }
        }
        return 0;
    }
}

