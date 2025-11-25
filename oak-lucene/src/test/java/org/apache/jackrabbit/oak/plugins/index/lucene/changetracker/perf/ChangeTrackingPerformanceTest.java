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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.perf;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexPopulator;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingAsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexQuery;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.lucene.index.DirectoryReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.Reader;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertTrue;

/**
 * Comprehensive performance test to identify breaking points for change tracking implementation.
 * 
 * <p><strong>Test Scenarios:</strong>
 * <ol>
 *   <li><strong>Gradual Load Increase</strong> - Find breaking point for bulk ingestion (1K → 1M assets)</li>
 *   <li><strong>Incremental Updates</strong> - Test update performance and memory stability (10% → 50%)</li>
 *   <li><strong>Mixed Workload</strong> - Simulate production patterns (60% ingest, 30% update, 10% child)</li>
 * </ol>
 * 
 * <p><strong>NodeStore Types:</strong>
 * <ul>
 *   <li>MemoryNodeStore (default) - Baseline, no I/O overhead</li>
 *   <li>SegmentNodeStore (-DuseSegmentStore=true) - Disk I/O overhead</li>
 *   <li>MongoDB (-DuseMongoStore=true) - Network + disk overhead</li>
 * </ul>
 * 
 * <p><strong>Test Modes:</strong>
 * <ul>
 *   <li>Traditional (-DuseChangeTracking=false) - Standard AsyncIndexUpdate</li>
 *   <li>Change Tracking (-DuseChangeTracking=true) - Three-indexer architecture</li>
 * </ul>
 * 
 * <p><strong>Usage Examples:</strong>
 * <pre>
 * # Quick test with MemoryNodeStore (512MB heap)
 * mvn test -Dtest=ChangeTrackingPerformanceTest -Xmx512m
 * 
 * # Stress test with SegmentNodeStore (4GB heap)
 * mvn test -Dtest=ChangeTrackingPerformanceTest -DuseSegmentStore=true -Xmx4g
 * 
 * # Production test with MongoDB (8GB heap, change tracking)
 * mvn test -Dtest=ChangeTrackingPerformanceTest -DuseMongoStore=true -DuseChangeTracking=true -Xmx8g
 * </pre>
 * 
 * <p><strong>Breaking Point Detection:</strong>
 * <ul>
 *   <li>Memory: GC time > 30% = CRITICAL, OOM = FAILURE</li>
 *   <li>Timeout: MongoDB transaction > 55s = CRITICAL, >60s = FAILURE</li>
 *   <li>Performance: Throughput < 25% baseline = CRITICAL</li>
 * </ul>
 */
public class ChangeTrackingPerformanceTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(ChangeTrackingPerformanceTest.class);
    
    // ========================================
    // Test Configuration
    // ========================================
    
    private static final boolean USE_CHANGE_TRACKING = Boolean.getBoolean("useChangeTracking");
    private static final boolean USE_SEGMENT_STORE = Boolean.getBoolean("useSegmentStore");
    private static final boolean USE_MONGO_STORE = Boolean.getBoolean("useMongoStore");
    
    // Auto-detect heap size and calculate appropriate test sizes
    private static final long MAX_HEAP_MB = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    private static final int[] ASSET_COUNTS = calculateAssetCounts(MAX_HEAP_MB);
    
    // Asset structure: ~15 nodes per asset (covers all 12 aggregates)
    // asset + jcr:content + metadata + renditions + 3 files + 3 jcr:content + comments + comment1 + data + master + usages
    private static final int NODES_PER_ASSET = 15;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    
    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();
    
    // ========================================
    // Test Components
    // ========================================
    
    // NodeStore components
    private NodeStore nodeStore;
    private FileStore fileStore;
    private ScheduledExecutorService scheduledExecutor;
    private MongoConnection mongoConnection;
    private DocumentNodeStore documentNodeStore;
    
    // Repository components
    private ContentRepository repository;
    private Root root;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private LuceneIndexProvider luceneIndexProvider;
    
    // Change tracking components
    private org.apache.lucene.store.Directory changeTrackingDirectory;
    private org.apache.lucene.index.IndexWriter changeTrackingWriter;
    private IndexProgressMetadataManager metadataManager;
    private ChangeTrackingIndexPopulator changeTrackingPopulator;
    private AsyncIndexUpdate traditionalAsyncIndexer;
    private ChangeTrackingAsyncIndexUpdate changeTrackingAsyncIndexer;
    
    // Performance monitoring
    private PerformanceMonitor performanceMonitor;
    private TestReport testReport;
    
    // ========================================
    // Test Lifecycle
    // ========================================
    
    @BeforeClass
    public static void checkMongoAvailability() {
        if (USE_MONGO_STORE) {
            assumeTrue("MongoDB not available", MongoUtils.isAvailable());
        }
    }
    
    @Before
    public void setup() throws Exception {
        String nodeStoreType = USE_MONGO_STORE ? "MongoDB DocumentNodeStore" : 
                               USE_SEGMENT_STORE ? "SegmentNodeStore" : 
                               "MemoryNodeStore";
        
        LOG.info("========================================");
        LOG.info("PERFORMANCE TEST CONFIGURATION");
        LOG.info("========================================");
        LOG.info("  Mode:       {}", USE_CHANGE_TRACKING ? "CHANGE TRACKING" : "TRADITIONAL");
        LOG.info("  NodeStore:  {}", nodeStoreType);
        LOG.info("  Max Heap:   {} MB", MAX_HEAP_MB);
        LOG.info("  Test Sizes: {}", formatAssetCounts(ASSET_COUNTS));
        LOG.info("========================================\n");
        
        performanceMonitor = new PerformanceMonitor();
        testReport = new TestReport();
        
        repository = createRepository();
        root = repository.login(null, null).getLatestRoot();
        
        // Register DAM node types
        registerDamNodeTypes();
        
        // Create damAssetLucene index definition
        createDamAssetLuceneIndex();
    }
    
    @After
    public void teardown() throws Exception {
        // Close indexing components
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
        }
        if (changeTrackingPopulator != null) {
            changeTrackingPopulator.close();
        }
        if (changeTrackingWriter != null) {
            changeTrackingWriter.close();
        }
        if (changeTrackingDirectory != null) {
            changeTrackingDirectory.close();
        }
        
        // Close NodeStore resources
        if (USE_SEGMENT_STORE) {
            if (fileStore != null) {
                fileStore.close();
            }
            if (scheduledExecutor != null) {
                scheduledExecutor.shutdown();
            }
        }
        
        if (USE_MONGO_STORE) {
            if (documentNodeStore != null) {
                documentNodeStore.dispose();
            }
            if (mongoConnection != null) {
                MongoUtils.dropCollections(mongoConnection.getDBName());
            }
        }
        
        // Generate final report
        testReport.generateReport();
    }
    
    // ========================================
    // Test Scenarios
    // ========================================
    
    @Test
    public void scenario1_GradualLoadIncrease() throws Exception {
        LOG.info("\n");
        LOG.info("========================================");
        LOG.info("SCENARIO 1: GRADUAL LOAD INCREASE");
        LOG.info("========================================");
        LOG.info("Goal: Find breaking point for bulk ingestion");
        LOG.info("Test sizes: {}", formatAssetCounts(ASSET_COUNTS));
        LOG.info("========================================\n");
        
        for (int assetCount : ASSET_COUNTS) {
            LOG.info("\n--- Testing with {} assets ({} nodes) ---", assetCount, assetCount * NODES_PER_ASSET);
            
            performanceMonitor.startPhase("bulk_" + assetCount);
            
            try {
                // Create assets
                long contentStart = System.currentTimeMillis();
                DamAssetCreator.createAssets(root, assetCount, 0);
                root.commit();
                long contentTime = System.currentTimeMillis() - contentStart;
                
                performanceMonitor.recordContentTime(contentTime);
                LOG.info("Content creation: {} ms ({} ms/asset)", contentTime, contentTime / assetCount);
                
                // Run indexing
                IndexingTimings timings = runIndexing();
                performanceMonitor.recordIndexingTime(timings);
                
                // Check memory and performance
                MemoryStats memStats = performanceMonitor.captureMemoryStats();
                boolean isBreakingPoint = performanceMonitor.isBreakingPoint(memStats, timings, assetCount);
                
                // Record results
                testReport.recordPhase("Bulk " + assetCount, assetCount, contentTime, timings, memStats, isBreakingPoint);
                
                if (isBreakingPoint) {
                    LOG.warn("\n!!! BREAKING POINT DETECTED at {} assets !!!", assetCount);
                    LOG.warn("Memory: heap={}MB, GC={}%", memStats.heapUsedMB, memStats.gcTimePercent);
                    LOG.warn("Stopping gradual load test.");
                    break;
                }
                
                LOG.info("✓ Phase completed successfully");
                
            } catch (OutOfMemoryError e) {
                LOG.error("\n!!! OUT OF MEMORY at {} assets !!!", assetCount);
                testReport.recordFailure("Bulk " + assetCount, "OutOfMemoryError", new Exception(e));
                throw e;
            } catch (Exception e) {
                LOG.error("Error during bulk load of {} assets: {}", assetCount, e.getMessage(), e);
                testReport.recordFailure("Bulk " + assetCount, e.getClass().getSimpleName(), e);
                throw e;
            } finally {
                performanceMonitor.endPhase();
            }
        }
        
        LOG.info("\n========================================");
        LOG.info("SCENARIO 1 COMPLETE");
        LOG.info("========================================\n");
    }
    
    @Test
    public void scenario2_IncrementalUpdates() throws Exception {
        LOG.info("\n");
        LOG.info("========================================");
        LOG.info("SCENARIO 2: INCREMENTAL UPDATES");
        LOG.info("========================================");
        LOG.info("Goal: Test update performance and memory stability");
        LOG.info("========================================\n");
        
        // Find the maximum stable asset count from scenario 1
        int initialAssets = getMaxStableAssetCount();
        LOG.info("Initial bulk load: {} assets", initialAssets);
        
        // Create initial content
        performanceMonitor.startPhase("initial_load");
        DamAssetCreator.createAssets(root, initialAssets, 0);
        root.commit();
        IndexingTimings initialTimings = runIndexing();
        performanceMonitor.endPhase();
        
        LOG.info("Initial load complete. Starting incremental updates...\n");
        
        // Test increasing update percentages
        int[] updatePercentages = {10, 20, 30, 40, 50};
        
        for (int updatePercent : updatePercentages) {
            int updateCount = (initialAssets * updatePercent) / 100;
            
            LOG.info("\n--- Updating {} assets ({}%) ---", updateCount, updatePercent);
            
            performanceMonitor.startPhase("update_" + updatePercent + "pct");
            
            try {
                // Update assets (change metadata)
                long contentStart = System.currentTimeMillis();
                DamAssetCreator.updateAssetMetadata(root, updateCount);
                root.commit();
                long contentTime = System.currentTimeMillis() - contentStart;
                
                performanceMonitor.recordContentTime(contentTime);
                LOG.info("Content update: {} ms ({} ms/asset)", contentTime, contentTime / updateCount);
                
                // Run indexing
                IndexingTimings timings = runIndexing();
                performanceMonitor.recordIndexingTime(timings);
                
                // Check memory
                MemoryStats memStats = performanceMonitor.captureMemoryStats();
                boolean isBreakingPoint = performanceMonitor.isBreakingPoint(memStats, timings, updateCount);
                
                // Record results
                testReport.recordPhase("Update " + updatePercent + "%", updateCount, contentTime, timings, memStats, isBreakingPoint);
                
                if (isBreakingPoint) {
                    LOG.warn("\n!!! BREAKING POINT DETECTED at {}% updates !!!", updatePercent);
                    break;
                }
                
                LOG.info("✓ Update phase completed successfully");
                
            } catch (OutOfMemoryError e) {
                LOG.error("\n!!! OUT OF MEMORY during {}% update !!!", updatePercent);
                testReport.recordFailure("Update " + updatePercent + "%", "OutOfMemoryError", new Exception(e));
                throw e;
            } catch (Exception e) {
                LOG.error("Error during update: {}", e.getMessage(), e);
                testReport.recordFailure("Update " + updatePercent + "%", e.getClass().getSimpleName(), e);
                throw e;
            } finally {
                performanceMonitor.endPhase();
            }
        }
        
        LOG.info("\n========================================");
        LOG.info("SCENARIO 2 COMPLETE");
        LOG.info("========================================\n");
    }
    
    @Test
    public void scenario3_MixedWorkload() throws Exception {
        LOG.info("\n");
        LOG.info("========================================");
        LOG.info("SCENARIO 3: MIXED WORKLOAD");
        LOG.info("========================================");
        LOG.info("Goal: Simulate production patterns");
        LOG.info("Mix: 60% ingest, 30% metadata update, 10% child update");
        LOG.info("========================================\n");
        
        int baseAssets = getMaxStableAssetCount() / 2; // Use half of max stable
        int totalOperations = baseAssets;
        
        int ingestCount = (totalOperations * 60) / 100;
        int metadataUpdateCount = (totalOperations * 30) / 100;
        int childUpdateCount = (totalOperations * 10) / 100;
        
        LOG.info("Operations: {} ingest, {} metadata updates, {} child updates",
                ingestCount, metadataUpdateCount, childUpdateCount);
        
        performanceMonitor.startPhase("mixed_workload");
        
        try {
            long contentStart = System.currentTimeMillis();
            
            // 60% - New asset ingestion
            LOG.info("\n[1/3] Creating {} new assets...", ingestCount);
            DamAssetCreator.createAssets(root, ingestCount, 0);
            root.commit();
            
            // 30% - Metadata updates
            LOG.info("[2/3] Updating metadata for {} assets...", metadataUpdateCount);
            DamAssetCreator.updateAssetMetadata(root, metadataUpdateCount);
            root.commit();
            
            // 10% - Child node updates (renditions)
            LOG.info("[3/3] Updating renditions for {} assets...", childUpdateCount);
            DamAssetCreator.updateAssetRenditions(root, childUpdateCount);
            root.commit();
            
            long contentTime = System.currentTimeMillis() - contentStart;
            performanceMonitor.recordContentTime(contentTime);
            LOG.info("\nMixed content operations: {} ms", contentTime);
            
            // Run indexing
            IndexingTimings timings = runIndexing();
            performanceMonitor.recordIndexingTime(timings);
            
            // Check memory
            MemoryStats memStats = performanceMonitor.captureMemoryStats();
            boolean isBreakingPoint = performanceMonitor.isBreakingPoint(memStats, timings, totalOperations);
            
            // Record results
            testReport.recordPhase("Mixed Workload", totalOperations, contentTime, timings, memStats, isBreakingPoint);
            
            if (isBreakingPoint) {
                LOG.warn("\n!!! BREAKING POINT DETECTED in mixed workload !!!");
            } else {
                LOG.info("✓ Mixed workload completed successfully");
            }
            
        } catch (OutOfMemoryError e) {
            LOG.error("\n!!! OUT OF MEMORY during mixed workload !!!");
            testReport.recordFailure("Mixed Workload", "OutOfMemoryError", new Exception(e));
            throw e;
        } catch (Exception e) {
            LOG.error("Error during mixed workload: {}", e.getMessage(), e);
            testReport.recordFailure("Mixed Workload", e.getClass().getSimpleName(), e);
            throw e;
        } finally {
            performanceMonitor.endPhase();
        }
        
        LOG.info("\n========================================");
        LOG.info("SCENARIO 3 COMPLETE");
        LOG.info("========================================\n");
    }
    
    // ========================================
    // Repository Setup
    // ========================================
    
    protected ContentRepository createRepository() {
        if (USE_MONGO_STORE) {
            nodeStore = createMongoNodeStore();
        } else if (USE_SEGMENT_STORE) {
            nodeStore = createSegmentNodeStore();
        } else {
            nodeStore = new MemoryNodeStore();
        }
        
        luceneEditorProvider = new LuceneIndexEditorProvider();
        luceneIndexProvider = new LuceneIndexProvider();
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, luceneEditorProvider);
        
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
    
    private NodeStore createSegmentNodeStore() {
        try {
            File segmentDir = temporaryFolder.newFolder("segmentstore");
            LOG.info("Creating SegmentNodeStore at: {}", segmentDir.getAbsolutePath());
            
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(scheduledExecutor);
            
            fileStore = FileStoreBuilder.fileStoreBuilder(segmentDir)
                    .withStatisticsProvider(statisticsProvider)
                    .withMaxFileSize(256)
                    .withMemoryMapping(false)
                    .build();
            
            return SegmentNodeStoreBuilders.builder(fileStore).build();
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException("Failed to create SegmentNodeStore", e);
        }
    }
    
    private NodeStore createMongoNodeStore() {
        try {
            LOG.info("Creating MongoDB DocumentNodeStore...");
            
            mongoConnection = connectionFactory.getConnection();
            LOG.info("Connected to MongoDB: {}", mongoConnection.getDBName());
            
            MongoUtils.dropCollections(mongoConnection.getDatabase());
            
            documentNodeStore = new DocumentMK.Builder()
                    .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName())
                    .setAsyncDelay(0)
                    .getNodeStore();
            
            LOG.info("MongoDB DocumentNodeStore created successfully");
            
            return documentNodeStore;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create MongoDB DocumentNodeStore", e);
        }
    }
    
    private void initializeChangeTracking() throws Exception {
        LOG.info("Initializing change tracking...");
        
        changeTrackingDirectory = new org.apache.lucene.store.RAMDirectory();
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        
        changeTrackingPopulator = new ChangeTrackingIndexPopulator(
            nodeStore,
            changeTrackingDirectory,
            metadataManager,
            StatisticsProvider.NOOP
        );
        changeTrackingPopulator.initialize();
        
        traditionalAsyncIndexer = asyncIndexUpdate;
        
        changeTrackingAsyncIndexer = new ChangeTrackingAsyncIndexUpdate(
            "change-tracker-async",
            nodeStore,
            changeTrackingDirectory,
            changeTrackingWriter
        );
        
        LOG.info("Change tracking initialized");
    }
    
    // ========================================
    // Node Type Registration
    // ========================================
    
    private void registerDamNodeTypes() throws Exception {
        LOG.info("Registering DAM node types...");
        
        try {
            // Load node type definitions from CND file
            InputStream cndStream = getClass().getResourceAsStream("/dam-nodetypes.cnd");
            if (cndStream == null) {
                throw new IllegalStateException("dam-nodetypes.cnd not found in classpath");
            }
            
            // Register node types
            NodeTypeRegistry.register(root, cndStream, "dam-nodetypes.cnd");
            root.commit();
            
            LOG.info("DAM node types registered successfully (dam:Asset, dam:AssetContent)");
            
        } catch (Exception e) {
            LOG.error("Failed to register DAM node types: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    // ========================================
    // Index Definition
    // ========================================
    
    private void createDamAssetLuceneIndex() throws Exception {
        DamAssetIndexDefinitionBuilder builder = new DamAssetIndexDefinitionBuilder();
        Tree indexDef = builder.build(root);
        
        if (USE_CHANGE_TRACKING) {
            indexDef.setProperty("useChangeTracker", true);
            LOG.info("Enabled change tracking for damAssetLucene-13 index");
        }
        
        root.commit();
        LOG.info("Created damAssetLucene-13 index definition (12 aggregates: jcr:content, metadata, renditions, comments, usages, subassets)");
    }
    
    // ========================================
    // Indexing Execution
    // ========================================
    
    private IndexingTimings runIndexing() throws Exception {
        IndexingTimings timings = new IndexingTimings();
        
        if (USE_CHANGE_TRACKING) {
            // Phase 1: Populate change tracking index
            long phase1Start = System.currentTimeMillis();
            changeTrackingPopulator.run();
            timings.phase1Time = System.currentTimeMillis() - phase1Start;
            
            // Phase 2: Traditional indexes
            long phase2Start = System.currentTimeMillis();
            traditionalAsyncIndexer.run();
            timings.phase2Time = System.currentTimeMillis() - phase2Start;
            
            // Phase 3: Change-tracked indexes
            long phase3Start = System.currentTimeMillis();
            changeTrackingAsyncIndexer.run();
            timings.phase3Time = System.currentTimeMillis() - phase3Start;
            
        } else {
            // Traditional mode
            long start = System.currentTimeMillis();
            asyncIndexUpdate.run();
            timings.traditionalTime = System.currentTimeMillis() - start;
        }
        
        return timings;
    }
    
    // ========================================
    // Utility Methods
    // ========================================
    
    private static int[] calculateAssetCounts(long heapMB) {
        // Conservative estimates based on heap size
        // Each asset ~= 5KB in memory during indexing (traditional)
        // Change tracking reduces this by ~67%
        
        List<Integer> counts = new ArrayList<>();
        
        if (heapMB >= 512) counts.add(1000);
        if (heapMB >= 1024) counts.add(10000);
        if (heapMB >= 2048) counts.add(50000);
        if (heapMB >= 4096) counts.add(100000);
        if (heapMB >= 8192) counts.add(250000);
        if (heapMB >= 16384) counts.add(500000);
        if (heapMB >= 32768) counts.add(1000000);
        
        if (counts.isEmpty()) {
            counts.add(100); // Minimum test size
        }
        
        int[] result = new int[counts.size()];
        for (int i = 0; i < counts.size(); i++) {
            result[i] = counts.get(i);
        }
        return result;
    }
    
    private static String formatAssetCounts(int[] counts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < counts.length; i++) {
            if (i > 0) sb.append(", ");
            if (counts[i] >= 1000000) {
                sb.append(counts[i] / 1000000).append("M");
            } else if (counts[i] >= 1000) {
                sb.append(counts[i] / 1000).append("K");
            } else {
                sb.append(counts[i]);
            }
        }
        return sb.toString();
    }
    
    private int getMaxStableAssetCount() {
        // Return the largest asset count that didn't hit breaking point
        // For now, use 50% of the second-to-last test size
        if (ASSET_COUNTS.length >= 2) {
            return ASSET_COUNTS[ASSET_COUNTS.length - 2] / 2;
        }
        return ASSET_COUNTS[0];
    }
    
    // ========================================
    // Inner Classes
    // ========================================
    
    static class IndexingTimings {
        long phase1Time = 0;
        long phase2Time = 0;
        long phase3Time = 0;
        long traditionalTime = 0;
        
        long getTotalTime() {
            return USE_CHANGE_TRACKING ? (phase1Time + phase2Time + phase3Time) : traditionalTime;
        }
    }
    
    // Performance monitoring and reporting classes will be in separate files
    // (PerformanceMonitor, MemoryStats, TestReport)
}

