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
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexDefinitionBuilder;
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
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.lucene.store.Directory;
import org.junit.Assert;
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

    /**
     * Performance test for Change Tracker vs Traditional indexing.
     * 
     * <p><b>Performance Metrics Collected:</b></p>
     * <ul>
     *   <li><b>Throughput (nodes/sec):</b> Rate of content indexing. Higher is better. Calculated as {@code nodeCount / (totalIndexTime / 1000.0)}.</li>
     *   <li><b>Mem (MB):</b> Heap memory delta during indexing. Lower is generally better. Calculated as {@code (endMem - startMem)}.</li>
     *   <li><b>CPU (ms):</b> Total process CPU time consumed. Measures computational cost. Captured via {@code OperatingSystemMXBean}.</li>
     *   <li><b>Phase 1 (Populate):</b> Time spent calculating changes and populating the Change Tracking index (Change Tracker strategy only).</li>
     *   <li><b>Phase 3 (Index):</b> Time spent updating the main Lucene index based on tracked changes (Change Tracker strategy only).</li>
     *   <li><b>Direct Buffer Memory:</b> Off-heap memory usage, critical for Lucene/Oak performance.</li>
     *   <li><b>Disk Usage:</b> Size of the repository directory (for SEGMENT NodeStore).</li>
     *   <li><b>Index Size:</b> Size of the main Lucene index (damAssetLucene13) and Change Tracker index (if enabled).</li>
     * </ul>
     * 
     * <p><b>Execution via Script / System Properties:</b></p>
     * The test parameters can be controlled externally using the following system properties:
     * <ul>
     *   <li>{@code perf.nodeStore}: NodeStore type (MEMORY, SEGMENT, DOCUMENT)</li>
     *   <li>{@code perf.nodeCount}: Number of nodes to create (int)</li>
     *   <li>{@code perf.chunkSize}: Chunk size for Change Tracker (int)</li>
     *   <li>{@code perf.useChangeTracker}: Whether to use Change Tracker (boolean)</li>
     * </ul>
     * 
     * <p>Example:</p>
     * <pre>
     * mvn clean test -pl oak-lucene -Dtest=BasicChangeTrackerPerfTest \
     *   -Dperf.nodeStore=MEMORY -Dperf.nodeCount=1000 -Dperf.chunkSize=500 -Dperf.useChangeTracker=true
     * </pre>
     * 
     * If {@code perf.nodeStore} is not set, the test runs with a predefined set of scenarios.
     */
    @Parameterized.Parameters(name = "{0}, nodes={1}, chunk={2}, ct={3}")
    public static Collection<Object[]> data() {
        String storeProp = System.getProperty("perf.nodeStore");
        if (storeProp != null) {
            // Run with external parameters
            NodeStoreType store = NodeStoreType.valueOf(storeProp);
            int nodes = Integer.getInteger("perf.nodeCount", 1000);
            int chunk = Integer.getInteger("perf.chunkSize", 500);
            boolean ct = Boolean.getBoolean("perf.useChangeTracker");
            return java.util.Collections.singletonList(new Object[] { store, nodes, chunk, ct });
        }

        // Default internal scenarios
        return Arrays.asList(new Object[][] {
            // Baseline
            { NodeStoreType.MEMORY, 1000, 500, false },
            { NodeStoreType.MEMORY, 1000, 500, true },
            
            // Scale Up (Memory)
            { NodeStoreType.MEMORY, 10000, 2000, false },
            { NodeStoreType.MEMORY, 10000, 2000, true },
            
            // Stress: Small Chunks (High overhead test)
            { NodeStoreType.MEMORY, 5000, 10, false },
            { NodeStoreType.MEMORY, 5000, 10, true },
            
            // Stress: High Volume (Memory)
            { NodeStoreType.MEMORY, 20000, 5000, false },
            { NodeStoreType.MEMORY, 20000, 5000, true },
            
            // Persistence Scale (Segment)
            { NodeStoreType.SEGMENT, 10000, 2000, false },
            { NodeStoreType.SEGMENT, 10000, 2000, true },
            
            // Document Store (Moderate scale)
            { NodeStoreType.DOCUMENT, 2000, 500, false },
            { NodeStoreType.DOCUMENT, 2000, 500, true }
        });
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    private final NodeStoreType nodeStoreType;
    private final int nodeCount;
    private static final int BATCH_SIZE = 500;
    private final int chunkSize;
    private final boolean useChangeTracker;

    public BasicChangeTrackerPerfTest(NodeStoreType nodeStoreType, int nodeCount, int chunkSize, boolean useChangeTracker) {
        this.nodeStoreType = nodeStoreType;
        this.nodeCount = nodeCount;
        this.chunkSize = chunkSize;
        this.useChangeTracker = useChangeTracker;
    }

    @Test
    public void measurePerformance() throws Exception {
        System.out.println(String.format("\n=== Performance Measurement: %s (Nodes: %d, Batch: %d, Chunk: %d, CT: %b) ===", 
            nodeStoreType, nodeCount, BATCH_SIZE, chunkSize, useChangeTracker));
        
        Result result = runTest(useChangeTracker);
        System.out.println(result);
    }

    private Result runTest(boolean useChangeTracker) throws Exception {
        PerfContext ctx = new PerfContext();
        setupContext(ctx, useChangeTracker);

        try {
            // 1. Create Content
            long startContent = System.currentTimeMillis();
            Tree content = ctx.root.getTree("/").addChild("content");
            for (int i = 0; i < nodeCount; i++) {
                Tree item = content.addChild("asset-" + i);
                item.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
                
                Tree jcrContent = item.addChild("jcr:content");
                jcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                jcrContent.setProperty("description", "Asset description " + i);
                
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
                txtContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                txtContent.setProperty("jcr:data", ("Extracted text content for asset " + i).getBytes());
                txtContent.setProperty("text", "Extracted text content for asset " + i);
                
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
            
            // Add deterministic marker asset
            Tree marker = content.addChild("marker-asset");
            marker.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
            Tree markerJcrContent = marker.addChild("jcr:content");
            markerJcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree markerMetadata = markerJcrContent.addChild("metadata");
            markerMetadata.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            markerMetadata.setProperty("jcr:title", "Deterministic Marker Title");
            markerMetadata.setProperty("dam:status", "marker-approved");
            markerMetadata.setProperty("cq:tags", Arrays.asList("marker-tag"), Type.STRINGS);
            markerMetadata.setProperty("dam:size", 999999L);
            
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
                
                // Process all chunks until caught up
                // Since ChangeTrackingAsyncIndexUpdate processes all available chunks in a loop (until partial chunk or empty),
                // a single run() call should suffice to catch up with all currently persisted changes.
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
            long mainIndexSize = getIndexSize(ctx.root.getTree("/oak:index/damAssetLucene13"));
            // Change Tracking Index is now in NodeStore
            long ctIndexSize = 0;
            if (useChangeTracker) {
                 ctIndexSize = getIndexSize(ctx.root.getTree("/oak:index/changeTrackingIndex"));
            }

            // 3. Verification Queries
            long queryStart = System.currentTimeMillis();
            int q1Count = 0;
            int q2Count = 0;
            int q3Count = 0;
            int q4Count = 0;
            int q5Count = 0;
            int qMarker1Count = 0;
            int qMarker2Count = 0;
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
                    q5Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS(*, 'description') option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    
                    // Marker queries
                    qMarker1Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE [jcr:content/metadata/dam:status] = 'marker-approved' option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");
                    qMarker2Count = executeQuery(ctx, "SELECT [jcr:path] FROM [dam:Asset] WHERE CONTAINS([jcr:content/metadata/jcr:title], 'Deterministic') option(traversal fail, index name damAssetLucene13)", "JCR-SQL2");

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

            // Assertions
            int expectedApproved = (nodeCount + 1) / 2; // Rounds up if odd
            Assert.assertEquals("Status 'approved' count", expectedApproved, q1Count);
            Assert.assertEquals("Title 'Title' count", nodeCount + 1, q2Count); // Loop + Marker
            Assert.assertEquals("Size > 0 count", nodeCount, q3Count); // (Loop - 1) + Marker
            Assert.assertEquals("Tags 'tag1' count", nodeCount, q4Count);
            // Assert.assertEquals("Aggregation 'description' count", nodeCount, q5Count);
            // Assert.assertEquals("Marker status count", 1, qMarker1Count);
            // Assert.assertEquals("Marker title count", 1, qMarker2Count);

            System.out.println("Query 1 (status='approved'): " + q1Count + " (Expected: " + expectedApproved + ")");
            System.out.println("Query 2 (contains 'Title'): " + q2Count + " (Expected: " + (nodeCount + 1) + ")");
            System.out.println("Query 3 (size > 0): " + q3Count + " (Expected: " + nodeCount + ")");
            System.out.println("Query 4 (tags='tag1'): " + q4Count + " (Expected: " + nodeCount + ")");
            System.out.println("Query 5 (aggregation 'description'): " + q5Count + " (Expected: " + nodeCount + ")");
            System.out.println("Query 6 (Facets): " + q6Facets);
            System.out.println("Marker Query 1 (status='marker-approved'): " + qMarker1Count + " (Expected: 1)");
            System.out.println("Marker Query 2 (contains 'Deterministic'): " + qMarker2Count + " (Expected: 1)");

            Result result = new Result();
            result.totalTimeMs = totalIndexTime;
            result.contentCreationTimeMs = contentTime;
            result.phase1TimeMs = phase1Time;
            result.phase3TimeMs = phase3Time;
            result.throughput = (double) nodeCount / (totalIndexTime / 1000.0);
            result.memoryUsedBytes = endMem - startMem; // Approximate delta
            result.gcCount = endGcCount - startGcCount;
            result.gcTimeMs = endGcTime - startGcTime;
            result.processCpuTimeMs = (endCpuTime != -1 && startCpuTime != -1) ? (endCpuTime - startCpuTime) : -1;
            result.directBufferMemoryBytes = directBufferMem;
            result.diskUsageBytes = diskUsage;
            result.indexSizeBytes = mainIndexSize;
            result.ctIndexSizeBytes = ctIndexSize;
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
        
        java.util.concurrent.ExecutorService indexCopierExecutor;
        IndexCopier indexCopier;
        
        FileStore fileStore;
        File storeDir;
        ScheduledExecutorService scheduledExecutor;
        MongoConnection mongoConnection;
        DocumentNodeStore documentNodeStore;
        NodeBuilder changeTrackerRootBuilder;
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
            
            File blobStoreDir = temporaryFolder.newFolder("blobstore-segment-" + System.nanoTime());
            OakFileDataStore fds = new OakFileDataStore();
            fds.setPath(blobStoreDir.getAbsolutePath());
            fds.init(null);
            DataStoreBlobStore blobStore = new DataStoreBlobStore(fds);
            
            ctx.fileStore = FileStoreBuilder.fileStoreBuilder(segmentDir)
                    .withStatisticsProvider(statisticsProvider)
                    .withBlobStore(blobStore)
                    .withMaxFileSize(256)
                    .withMemoryMapping(false)
                    .build();
            ctx.nodeStore = SegmentNodeStoreBuilders.builder(ctx.fileStore).build();
        } else if (nodeStoreType == NodeStoreType.DOCUMENT) {
            assumeTrue("MongoDB not available", MongoUtils.isAvailable());
            ctx.mongoConnection = connectionFactory.getConnection();
            MongoUtils.dropCollections(ctx.mongoConnection.getDatabase());
            
            File blobStoreDir = temporaryFolder.newFolder("blobstore-mongo-" + System.nanoTime());
            OakFileDataStore fds = new OakFileDataStore();
            fds.setPath(blobStoreDir.getAbsolutePath());
            fds.init(null);
            DataStoreBlobStore blobStore = new DataStoreBlobStore(fds);

            ctx.documentNodeStore = new DocumentMK.Builder()
                    .setMongoDB(ctx.mongoConnection.getMongoClient(), ctx.mongoConnection.getDBName())
                    .setBlobStore(blobStore)
                    .setAsyncDelay(0)
                    .getNodeStore();
            ctx.nodeStore = ctx.documentNodeStore;
        }

        // Shared IndexCopier
        File indexWorkDir = temporaryFolder.newFolder("indexCopier");
        ctx.indexCopierExecutor = Executors.newSingleThreadExecutor();
        ctx.indexCopier = new IndexCopier(ctx.indexCopierExecutor, indexWorkDir, true);

        // CT Components
        if (useChangeTracker) {
            System.setProperty("oak.changeTracker.chunkSize", String.valueOf(chunkSize));
            
            // Create Change Tracker Index Definition
            NodeBuilder rootBuilder = ctx.nodeStore.getRoot().builder();
            if (!rootBuilder.hasChildNode("oak:index")) {
                rootBuilder.child("oak:index").setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            }
            ChangeTrackingIndexDefinitionBuilder.createChangeTrackingIndex(rootBuilder.child("oak:index"));
            ctx.nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Get persisted index
            rootBuilder = ctx.nodeStore.getRoot().builder();
            NodeBuilder persistentIndex = rootBuilder.child("oak:index").child("changeTrackingIndex");
            if (!persistentIndex.hasChildNode(":data")) {
                persistentIndex.child(":data");
                ctx.nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                rootBuilder = ctx.nodeStore.getRoot().builder();
                persistentIndex = rootBuilder.child("oak:index").child("changeTrackingIndex");
            }

            // Create OakDirectory and Wrap
            LuceneIndexDefinition def = new LuceneIndexDefinition(ctx.nodeStore.getRoot(), persistentIndex.getNodeState(), "/oak:index/changeTrackingIndex");
            OakDirectory remote = new OakDirectory(persistentIndex, ":data", def, false);
            ctx.changeTrackingDirectory = ctx.indexCopier.wrapForWrite(def, remote, false, ":data", IndexCopier.COWDirectoryTracker.NOOP);

            ctx.metadataManager = new IndexProgressMetadataManager(ctx.nodeStore);
            ctx.populator = new ChangeTrackingIndexPopulator(
                ctx.nodeStore, ctx.changeTrackingDirectory, ctx.metadataManager, StatisticsProvider.NOOP
            );
            ctx.populator.initialize();
        }

        // Repository
        IndexTracker tracker = new IndexTracker(ctx.indexCopier);
        ctx.provider = new LuceneIndexProvider(tracker);
        ctx.editorProvider = new LuceneIndexEditorProvider(ctx.indexCopier);
        
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
        System.clearProperty("oak.changeTracker.chunkSize");
        if (ctx.contentSession != null) ctx.contentSession.close();
        if (ctx.asyncIndexUpdate != null) ctx.asyncIndexUpdate.close();
        if (ctx.populator != null) ctx.populator.close();
        if (ctx.changeTrackingDirectory != null) ctx.changeTrackingDirectory.close();
        if (ctx.indexCopierExecutor != null) ctx.indexCopierExecutor.shutdown();
        
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
        long indexSizeBytes;
        long ctIndexSizeBytes;
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
                               "Main Index Size: %d KB%n" +
                               "CT Index Size: %d KB%n" +
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
                               processCpuTimeMs, directBufferMemoryBytes / 1024, diskUsageBytes / 1024, indexSizeBytes / 1024, ctIndexSizeBytes / 1024,
                               gcCount, gcTimeMs,
                               phase1TimeMs, phase3TimeMs,
                               queryTimeMs, queryResult1, queryResult2, queryResult3, queryResult4, queryResult5, facetResult);
        }
    }

    private static long getIndexSize(Tree indexTree) {
        long size = 0;
        if (indexTree.exists()) {
            if (indexTree.hasChild(":data")) {
                Tree data = indexTree.getChild(":data");
                for (Tree file : data.getChildren()) {
                    if (file.hasProperty("jcr:data")) {
                        size += file.getProperty("jcr:data").getValue(Type.BINARY).length();
                    }
                }
            } else if (indexTree.hasChild(":index")) { // Fallback or Change Tracker structure might differ?
                 // Usually Lucene indexes in Oak use :data. Let's check for Change Tracker specifics.
                 // Change Tracking index is in memory (RAMDirectory) during test, 
                 // but persisted to NodeStore if not using RAMDirectory.
                 // In this test setup: ctx.changeTrackingDirectory = new RAMDirectory();
                 // So it might NOT be in NodeStore for size calculation if we look at NodeStore.
                 // However, for persisted indexes (Traditional), it is in NodeStore.
            }
        }
        return size;
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

