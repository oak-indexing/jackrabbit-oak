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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
                metadata.setProperty("dam:status", (i % 2 == 0) ? "approved" : "draft");
                metadata.setProperty("dc:format", (i % 3 == 0) ? "image/jpeg" : (i % 3 == 1) ? "application/pdf" : "video/mp4");
                
                if (i % BATCH_SIZE == 0) {
                    ctx.root.commit();
                }
            }
            ctx.root.commit();
            long contentTime = System.currentTimeMillis() - startContent;

            // 2. Indexing
            // Re-login to ensure fresh session for indexing if needed (though usually not required for AsyncIndexUpdate)
            
            long startIndexing = System.currentTimeMillis();
            long phase1Time = 0;
            long phase3Time = 0;
            long startMem = getUsedMemory();

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
            long endMem = getUsedMemory();

            Result result = new Result();
            result.totalTimeMs = totalIndexTime;
            result.contentCreationTimeMs = contentTime;
            result.phase1TimeMs = phase1Time;
            result.phase3TimeMs = phase3Time;
            result.throughput = (double) NODE_COUNT / (totalIndexTime / 1000.0);
            result.memoryUsedBytes = endMem - startMem; // Approximate delta
            
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
            index.setProperty("useChangeTracker", true);
        }

        // Aggregates
        Tree aggregates = index.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damAssetAgg = aggregates.addChild("dam:Asset");
        damAssetAgg.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree include0 = damAssetAgg.addChild("include0");
        include0.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include0.setProperty("path", "jcr:content");
        
        Tree include1 = damAssetAgg.addChild("include1");
        include1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include1.setProperty("path", "jcr:content/metadata");

        // Index Rules
        Tree indexRules = index.addChild("indexRules");
        Tree damAsset = indexRules.addChild("dam:Asset");
        damAsset.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree properties = damAsset.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // dc:title
        Tree dcTitle = properties.addChild("dcTitle");
        dcTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcTitle.setProperty("name", "jcr:content/metadata/dc:title");
        dcTitle.setProperty("analyzed", true);
        dcTitle.setProperty("nodeScopeIndex", true);
        dcTitle.setProperty("propertyIndex", true);
        
        // jcr:title
        Tree jcrTitle = properties.addChild("jcrTitle");
        jcrTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrTitle.setProperty("name", "jcr:content/metadata/jcr:title");
        jcrTitle.setProperty("analyzed", true);
        jcrTitle.setProperty("nodeScopeIndex", true);
        jcrTitle.setProperty("propertyIndex", true);
        
        // dam:status
        Tree damStatus = properties.addChild("damStatus");
        damStatus.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        damStatus.setProperty("name", "jcr:content/metadata/dam:status");
        damStatus.setProperty("propertyIndex", true);
        
        // dc:format
        Tree dcFormat = properties.addChild("dcFormat");
        dcFormat.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcFormat.setProperty("name", "jcr:content/metadata/dc:format");
        dcFormat.setProperty("propertyIndex", true);
        
        ctx.root.commit();
        
        // Register index in metadata if needed
        if (useChangeTracker) {
             // The populator automatically handles this via initialize() or run() logic implicitly
             // but ensuring it's registered helps
             // In BasicChangeTrackerE2ETest we saw commit issues, so relying on populator.initialize() done above
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

    private static class Result {
        long totalTimeMs;
        long contentCreationTimeMs;
        long phase1TimeMs;
        long phase3TimeMs;
        double throughput;
        long memoryUsedBytes;

        @Override
        public String toString() {
            return String.format("Total Time: %d ms%n" +
                               "Content Creation: %d ms%n" +
                               "Throughput: %.2f nodes/sec%n" +
                               "Memory Delta: %d KB%n" + 
                               "Phase 1 (Populate): %d ms%n" +
                               "Phase 3 (Index): %d ms",
                               totalTimeMs, contentCreationTimeMs, throughput, memoryUsedBytes / 1024, phase1TimeMs, phase3TimeMs);
        }
    }
}

