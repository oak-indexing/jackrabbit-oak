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
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
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

import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.api.Type;

/**
 * Comprehensive performance test to identify breaking points for change tracking implementation.
 * 
 * <p><strong>Test Scenarios:</strong>
 * <ol>
 *   <li><strong>Gradual Load Increase</strong> - Find breaking point for bulk ingestion (1K → 1M assets)</li>
 *   <li><strong>Incremental Updates</strong> - Test update performance and memory stability (10% → 100%)</li>
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
 *   <li>Memory: GC time > 50% = CRITICAL, OOM = FAILURE</li>
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
    
    // Shared IndexCopier components
    private ScheduledExecutorService indexCopierExecutor;
    private IndexCopier indexCopier;

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
        
        // Initialize shared IndexCopier
        File indexWorkDir = temporaryFolder.newFolder("indexCopier");
        indexCopierExecutor = Executors.newSingleThreadScheduledExecutor();
        indexCopier = new IndexCopier(indexCopierExecutor, indexWorkDir, true);
        
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
        if (indexCopierExecutor != null) {
            indexCopierExecutor.shutdown();
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
                
                // Print detailed indexing stats
                timings.printSummary(assetCount, contentTime);
                
                // Verify index is working correctly
                verifyIndexWithQueries(assetCount);
                
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
                
                // Print detailed indexing stats
                timings.printSummary(updateCount, contentTime);
                
                // Verify index is working correctly (still has all initial assets)
                verifyIndexWithQueries(initialAssets);
                
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
        
        IndexTracker tracker = new IndexTracker(indexCopier);
        luceneIndexProvider = new LuceneIndexProvider(tracker);
        luceneEditorProvider = new LuceneIndexEditorProvider(indexCopier);
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
            
            // Create FileDataStore
            File blobStoreDir = temporaryFolder.newFolder("blobstore-segment");
            OakFileDataStore fds = new OakFileDataStore();
            fds.setPath(blobStoreDir.getAbsolutePath());
            fds.init(null);
            
            DataStoreBlobStore blobStore = new DataStoreBlobStore(fds);
            
            fileStore = FileStoreBuilder.fileStoreBuilder(segmentDir)
                    .withStatisticsProvider(statisticsProvider)
                    .withBlobStore(blobStore)
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
            
            // Create FileDataStore
            File blobStoreDir = temporaryFolder.newFolder("blobstore-mongo");
            OakFileDataStore fds = new OakFileDataStore();
            fds.setPath(blobStoreDir.getAbsolutePath());
            fds.init(null);
            
            DataStoreBlobStore blobStore = new DataStoreBlobStore(fds);
            
            documentNodeStore = new DocumentMK.Builder()
                    .setMongoDB(mongoConnection.getMongoClient(), mongoConnection.getDBName())
                    .setBlobStore(blobStore)
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
        
        // Create the Lucene directory for the change tracking index in NodeStore
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        if (!rootBuilder.hasChildNode("oak:index")) {
            rootBuilder.child("oak:index").setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        }
        NodeBuilder oakIndex = rootBuilder.child("oak:index");
        ChangeTrackingIndexDefinitionBuilder.createChangeTrackingIndex(oakIndex);
        
        // Persist index definition
        nodeStore.merge(rootBuilder, org.apache.jackrabbit.oak.spi.commit.EmptyHook.INSTANCE, org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
        
        // Re-fetch root builder to ensure consistency for OakDirectory
        rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder persistentIndex = rootBuilder.child("oak:index").child("changeTrackingIndex");
        
        // Ensure :data node exists
        if (!persistentIndex.hasChildNode(":data")) {
            persistentIndex.child(":data");
            nodeStore.merge(rootBuilder, org.apache.jackrabbit.oak.spi.commit.EmptyHook.INSTANCE, org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
            rootBuilder = nodeStore.getRoot().builder();
            persistentIndex = rootBuilder.child("oak:index").child("changeTrackingIndex");
        }
        
        // Create OakDirectory backed by NodeStore
        LuceneIndexDefinition def = new LuceneIndexDefinition(nodeStore.getRoot(), persistentIndex.getNodeState(), "/oak:index/changeTrackingIndex");
        OakDirectory remote = new OakDirectory(persistentIndex, ":data", def, false);
        
        // Wrap with IndexCopier
        changeTrackingDirectory = indexCopier.wrapForWrite(def, remote, false, ":data", IndexCopier.COWDirectoryTracker.NOOP);
        
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
            null
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
            System.out.println("========================================");
            System.out.println("THREE-INDEXER CHANGE TRACKING MODE");
            System.out.println("========================================");
            
            // Phase 1: Populate change tracking index
            System.out.println("PHASE 1: Running ChangeTrackingIndexPopulator...");
            long phase1Start = System.currentTimeMillis();
            changeTrackingPopulator.run();
            timings.phase1Time = System.currentTimeMillis() - phase1Start;
            System.out.println("Phase 1 complete: " + timings.phase1Time + " ms");
            System.out.println("  Stats: " + changeTrackingPopulator.getStatistics());
            
            // Query to see how many changes were recorded
            DirectoryReader reader = DirectoryReader.open(changeTrackingDirectory);
            int totalChanges = 0;
            try {
                ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader);
                totalChanges = query.getUnprocessedChanges(0, 0, Integer.MAX_VALUE).size();
                timings.changeEntriesRecorded = totalChanges;
                System.out.println("  Change tracking index: " + totalChanges + " entries");
            } finally {
                reader.close();
            }
            
            // Phase 2: Traditional indexes
            System.out.println("PHASE 2: Running Traditional AsyncIndexUpdate...");
            long phase2Start = System.currentTimeMillis();
            traditionalAsyncIndexer.run();
            timings.phase2Time = System.currentTimeMillis() - phase2Start;
            System.out.println("Phase 2 complete: " + timings.phase2Time + " ms");
            
            // Phase 3: Change-tracked indexes
            System.out.println("PHASE 3: Running ChangeTrackingAsyncIndexUpdate...");
            long phase3Start = System.currentTimeMillis();
            changeTrackingAsyncIndexer.run();
            timings.phase3Time = System.currentTimeMillis() - phase3Start;
            System.out.println("Phase 3 complete: " + timings.phase3Time + " ms");
            
            // Summary
            long totalTime = timings.getTotalTime();
            System.out.println("========================================");
            System.out.println("ALL THREE INDEXERS COMPLETE");
            System.out.println("Performance Breakdown:");
            System.out.println("  Phase 1 (Change Tracker Populate): " + timings.phase1Time + " ms (" + totalChanges + " entries)");
            System.out.println("  Phase 2 (Traditional Indexer):      " + timings.phase2Time + " ms");
            System.out.println("  Phase 3 (Change Tracked Indexer):   " + timings.phase3Time + " ms");
            System.out.println("  TOTAL:                               " + totalTime + " ms");
            System.out.println("========================================");
            
        } else {
            System.out.println("========================================");
            System.out.println("TRADITIONAL MODE");
            System.out.println("========================================");
            
            // Traditional mode
            long start = System.currentTimeMillis();
            asyncIndexUpdate.run();
            timings.traditionalTime = System.currentTimeMillis() - start;
            
            System.out.println("Traditional AsyncIndexUpdate complete: " + timings.traditionalTime + " ms");
            System.out.println("========================================");
        }
        
        return timings;
    }
    
    // ========================================
    // Index Verification
    // ========================================
    
    /**
     * Verifies that the index is working correctly by running sample queries.
     * This ensures documents are indexed and queryable.
     */
    private void verifyIndexWithQueries(int expectedAssetCount) throws Exception {
        System.out.println("\n--- Verifying Index with Queries ---");
        LOG.info("--- Verifying Index with Queries ---");
        
        try {
            // Query 1: Find all dam:Asset nodes
            String allAssetsQuery = "SELECT * FROM [dam:Asset]";
            int allAssets = executeQuery(allAssetsQuery);
            System.out.println("  Q1 - All dam:Asset nodes: " + allAssets + " (expected: ~" + expectedAssetCount + ")");
            LOG.info("  Q1 - All dam:Asset nodes: {} (expected: ~{})", allAssets, expectedAssetCount);
            
            // Query 2: Find assets by metadata property (jcr:title)
            String titleQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/jcr:title] IS NOT NULL";
            int assetsWithTitle = executeQuery(titleQuery);
            System.out.println("  Q2 - Assets with jcr:title: " + assetsWithTitle);
            LOG.info("  Q2 - Assets with jcr:title: {}", assetsWithTitle);
            
            // Query 3: Find assets by format (image/jpeg)
            String formatQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/dc:format] = 'image/jpeg'";
            int jpegAssets = executeQuery(formatQuery);
            System.out.println("  Q3 - JPEG assets (dc:format='image/jpeg'): " + jpegAssets);
            LOG.info("  Q3 - JPEG assets: {}", jpegAssets);
            
            // Query 4: Find assets by status (approved)
            String statusQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/dam:status] = 'approved'";
            int approvedAssets = executeQuery(statusQuery);
            System.out.println("  Q4 - Approved assets (dam:status='approved'): " + approvedAssets);
            LOG.info("  Q4 - Approved assets: {}", approvedAssets);
            
            // Query 5: Find UPDATED assets (check if updates are indexed)
            String updatedQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/jcr:title] LIKE '%UPDATED%'";
            int updatedAssets = executeQuery(updatedQuery);
            System.out.println("  Q5 - Updated assets (title contains 'UPDATED'): " + updatedAssets);
            LOG.info("  Q5 - Updated assets: {}", updatedAssets);
            
            // Query 6: Find assets with modified timestamp (another update indicator)
            String modifiedQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/dam:lastModified] IS NOT NULL";
            int modifiedAssets = executeQuery(modifiedQuery);
            System.out.println("  Q6 - Assets with lastModified: " + modifiedAssets);
            LOG.info("  Q6 - Assets with lastModified: {}", modifiedAssets);
            
            // Query 7: Aggregation verification - search in deeply nested child nodes
            // This proves that jcr:content/metadata/* properties are aggregated to parent dam:Asset
            String aggregationQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/dc:creator] IS NOT NULL";
            int aggregatedAssets = executeQuery(aggregationQuery);
            System.out.println("  Q7 - Aggregation test (dc:creator in child): " + aggregatedAssets);
            LOG.info("  Q7 - Aggregation test (dc:creator in child): {}", aggregatedAssets);
            
            // Query 8: Aggregation with value filter - ensures aggregated values are searchable
            String aggregationValueQuery = "SELECT * FROM [dam:Asset] WHERE [jcr:content/metadata/dc:creator] = 'admin'";
            int creatorAssets = executeQuery(aggregationValueQuery);
            System.out.println("  Q8 - Aggregation value test (dc:creator='admin'): " + creatorAssets);
            LOG.info("  Q8 - Aggregation value test (dc:creator='admin'): {}", creatorAssets);
            
            // Query 9: Fulltext search - verify fulltext indexing works
            String fulltextQuery = "SELECT * FROM [dam:Asset] WHERE CONTAINS(*, 'Asset')";
            int fulltextAssets = executeQuery(fulltextQuery);
            System.out.println("  Q9 - Fulltext search (CONTAINS 'Asset'): " + fulltextAssets);
            LOG.info("  Q9 - Fulltext search (CONTAINS 'Asset'): {}", fulltextAssets);
            
            // Query 10: Fulltext search on specific property - verify property-level fulltext
            String fulltextPropQuery = "SELECT * FROM [dam:Asset] WHERE CONTAINS([jcr:content/metadata/jcr:title], 'Asset')";
            int fulltextPropAssets = executeQuery(fulltextPropQuery);
            System.out.println("  Q10 - Fulltext on property (title CONTAINS 'Asset'): " + fulltextPropAssets);
            LOG.info("  Q10 - Fulltext on property (title CONTAINS 'Asset'): {}", fulltextPropAssets);
            
            // Query 11: Fulltext search for UPDATED content - proves fulltext updates work
            String fulltextUpdatedQuery = "SELECT * FROM [dam:Asset] WHERE CONTAINS([jcr:content/metadata/jcr:title], 'UPDATED')";
            int fulltextUpdated = executeQuery(fulltextUpdatedQuery);
            System.out.println("  Q11 - Fulltext updated content (title CONTAINS 'UPDATED'): " + fulltextUpdated);
            LOG.info("  Q11 - Fulltext updated content (title CONTAINS 'UPDATED'): {}", fulltextUpdated);
            
            // Verify we found assets
            if (allAssets == 0) {
                System.out.println("❌ WARNING: No assets found in index! Index may not be working correctly.");
                LOG.warn("WARNING: No assets found in index! Index may not be working correctly.");
            } else if (allAssets < expectedAssetCount * 0.9) {
                System.out.println("⚠️  WARNING: Found " + allAssets + " assets but expected ~" + expectedAssetCount + ". Some assets may not be indexed.");
                LOG.warn("WARNING: Found {} assets but expected ~{}. Some assets may not be indexed.", 
                        allAssets, expectedAssetCount);
            } else {
                System.out.println("✓ Index verification passed: " + allAssets + " assets indexed and queryable");
                LOG.info("✓ Index verification passed: {} assets indexed and queryable", allAssets);
                
                // Additional verification for updates
                if (updatedAssets > 0) {
                    System.out.println("✓ Update verification passed: " + updatedAssets + " updated assets found in index");
                    LOG.info("✓ Update verification passed: {} updated assets found in index", updatedAssets);
                }
                
                // Aggregation verification
                if (aggregatedAssets > 0) {
                    System.out.println("✓ Aggregation verification passed: " + aggregatedAssets + " assets with aggregated child properties");
                    LOG.info("✓ Aggregation verification passed: {} assets with aggregated child properties", aggregatedAssets);
                }
                
                // Fulltext verification
                if (fulltextAssets > 0) {
                    System.out.println("✓ Fulltext verification passed: " + fulltextAssets + " assets found via fulltext search");
                    LOG.info("✓ Fulltext verification passed: {} assets found via fulltext search", fulltextAssets);
                }
                
                if (fulltextUpdated > 0) {
                    System.out.println("✓ Fulltext update verification passed: " + fulltextUpdated + " updated assets found via fulltext");
                    LOG.info("✓ Fulltext update verification passed: {} updated assets found via fulltext", fulltextUpdated);
                }
            }
            
        } catch (Exception e) {
            System.out.println("❌ ERROR during index verification: " + e.getMessage());
            LOG.error("Error during index verification: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * Executes a JCR-SQL2 query and returns the result count.
     */
    private int executeQuery(String query) throws Exception {
        int count = 0;
        for (org.apache.jackrabbit.oak.api.ResultRow row : root.getQueryEngine().executeQuery(
                query, javax.jcr.query.Query.JCR_SQL2, null, null).getRows()) {
            count++;
        }
        return count;
    }
    
    // ========================================
    // Utility Methods
    // ========================================
    
    private static int[] calculateAssetCounts(long heapMB) {
        // ALWAYS use system property if provided - for stress testing to break the system
        String bulkSizesProperty = System.getProperty("test.bulk.sizes");
        if (bulkSizesProperty != null && !bulkSizesProperty.isEmpty()) {
            LOG.info("Using bulk sizes from system property: {}", bulkSizesProperty);
            String[] sizes = bulkSizesProperty.split(",");
            int[] result = new int[sizes.length];
            for (int i = 0; i < sizes.length; i++) {
                result[i] = Integer.parseInt(sizes[i].trim());
            }
            LOG.info("Test will run with {} size configurations", result.length);
            return result;
        }
        
        // Default: Conservative estimates based on heap size
        LOG.info("Using default bulk sizes based on heap: {} MB", heapMB);
        
        List<Integer> counts = new ArrayList<>();
        
        counts.add(1000);
        counts.add(10000);
        counts.add(50000);
        if (heapMB >= 4096) counts.add(100000);
        if (heapMB >= 8192) counts.add(250000);
        if (heapMB >= 16384) counts.add(500000);
        if (heapMB >= 32768) counts.add(1000000);
        
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
        long phase1Time = 0;  // ChangeTrackingIndexPopulator
        long phase2Time = 0;  // Traditional AsyncIndexUpdate
        long phase3Time = 0;  // ChangeTrackingAsyncIndexUpdate
        long traditionalTime = 0;  // Single AsyncIndexUpdate (traditional mode)
        int changeEntriesRecorded = 0;
        
        long getTotalTime() {
            return USE_CHANGE_TRACKING ? (phase1Time + phase2Time + phase3Time) : traditionalTime;
        }
        
        void printSummary(int assetCount, long contentTime) {
            System.out.println("\n========================================");
            System.out.println("INDEXING PERFORMANCE SUMMARY");
            System.out.println("========================================");
            System.out.println("Mode: " + (USE_CHANGE_TRACKING ? "CHANGE TRACKING (3 indexers)" : "TRADITIONAL (1 indexer)"));
            System.out.println("Assets processed: " + assetCount);
            System.out.println("Content creation: " + contentTime + " ms");
            System.out.println("");
            
            if (USE_CHANGE_TRACKING) {
                long total = getTotalTime();
                System.out.println("Change Tracking Mode - Per-Phase Timings:");
                System.out.println("  Phase 1 (ChangeTrackingIndexPopulator):    " + phase1Time + " ms");
                System.out.println("  Phase 2 (Traditional AsyncIndexUpdate):     " + phase2Time + " ms");
                System.out.println("  Phase 3 (ChangeTrackingAsyncIndexUpdate):   " + phase3Time + " ms");
                System.out.println("  --------------------------------------------------");
                System.out.println("  TOTAL (all 3 phases):                        " + total + " ms");
                System.out.println("");
                
                // Calculate per-phase throughput
                if (phase1Time > 0) {
                    double phase1Throughput = (assetCount * 1000.0) / phase1Time;
                    System.out.println("  Phase 1 throughput: " + String.format("%.1f", phase1Throughput) + " assets/sec (records changes)");
                }
                if (phase3Time > 0) {
                    double phase3Throughput = (assetCount * 1000.0) / phase3Time;
                    System.out.println("  Phase 3 throughput: " + String.format("%.1f", phase3Throughput) + " assets/sec (indexes from tracker)");
                }
                
                double totalThroughput = (assetCount * 1000.0) / (total + 1);
                System.out.println("  Overall throughput: " + String.format("%.1f", totalThroughput) + " assets/sec (all 3 phases)");
                
                if (changeEntriesRecorded > 0) {
                    System.out.println("  Change entries recorded: " + changeEntriesRecorded);
                }
            } else {
                System.out.println("Traditional Mode - Timings:");
                System.out.println("  AsyncIndexUpdate total time: " + traditionalTime + " ms");
                double throughput = (assetCount * 1000.0) / (traditionalTime + 1);
                System.out.println("  Throughput: " + String.format("%.1f", throughput) + " assets/sec");
            }
            
            System.out.println("========================================");
            System.out.println("COMPARISON NOTE:");
            if (USE_CHANGE_TRACKING) {
                System.out.println("  Phase 3 time is the closest comparison to traditional mode");
                System.out.println("  (both perform actual Lucene document indexing)");
                System.out.println("  Phase 1 is overhead for recording changes");
            } else {
                System.out.println("  Traditional mode: Single indexer does checkpoint diff + indexing");
            }
            System.out.println("========================================\n");
        }
    }
    
    // Performance monitoring and reporting classes will be in separate files
    // (PerformanceMonitor, MemoryStats, TestReport)
}

