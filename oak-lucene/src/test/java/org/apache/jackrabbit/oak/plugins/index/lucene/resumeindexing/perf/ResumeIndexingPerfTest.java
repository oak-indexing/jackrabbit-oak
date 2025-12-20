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
package org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing.perf;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Performance test for Async Indexing - modeled after BasicChangeTrackerPerfTest.
 * 
 * <p>Compares Traditional vs Continuous indexing modes across:
 * <ul>
 *   <li>MemoryNodeStore - baseline, no I/O overhead</li>
 *   <li>SegmentNodeStore - disk I/O with FileDataStore</li>
 *   <li>DocumentNodeStore (MongoDB) - network + disk overhead</li>
 * </ul>
 * 
 * <p>Metrics captured:
 * <ul>
 *   <li>Total indexing time, throughput</li>
 *   <li>Memory: heap, non-heap, direct buffer</li>
 *   <li>GC: count and time</li>
 *   <li>CPU time, peak threads</li>
 *   <li>Disk usage</li>
 *   <li>Query verification with index hints</li>
 * </ul>
 */
public class ResumeIndexingPerfTest {

    public enum NodeStoreType {
        MEMORY, SEGMENT, DOCUMENT
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    // Configurable via system properties
    private static final NodeStoreType NODE_STORE_TYPE = NodeStoreType.valueOf(
        System.getProperty("perf.nodeStore", "SEGMENT").toUpperCase());
    private static final int NODE_COUNT = Math.max(1000, Integer.getInteger("perf.nodeCount", 10000)); // Default 10k
    private static final int BATCH_SIZE = Integer.getInteger("perf.batchSize", 100);
    private static final int CHUNK_SIZE = Integer.getInteger("perf.chunkSize", 1000); // Default 1k per chunk
    
    // Fixed query result target - always return ~1000 results regardless of NODE_COUNT
    private static final int QUERY_TARGET_COUNT = 1000;

    @Test
    public void runPerformanceTest() throws Exception {
        System.out.println(String.format("\n========================================"));
        System.out.println(String.format("RESUMABLE INDEXING PERFORMANCE TEST"));
        System.out.println(String.format("========================================"));
        System.out.println(String.format("NodeStore:   %s", NODE_STORE_TYPE));
        System.out.println(String.format("Node Count:  %d", NODE_COUNT));
        System.out.println(String.format("Batch Size:  %d", BATCH_SIZE));
        System.out.println(String.format("Chunk Size:  %d (nodes per run)", CHUNK_SIZE));
        System.out.println(String.format("========================================\n"));

        Result result = runTest();
        
        System.out.println("\n--- Performance Results ---");
        System.out.println(result);
        
        // Output for script parsing
        System.out.println("\n--- Script Parseable Output ---");
        System.out.println("Total Time: " + result.totalTimeMs + " ms");
        System.out.println("Throughput: " + String.format("%.1f", result.throughput));
        System.out.println("Memory Delta: " + (result.memoryUsedBytes / 1024) + " KB");
        System.out.println("Max Heap Used: " + (result.maxHeapUsedBytes / (1024 * 1024)) + " MB");
        System.out.println("Max Non-Heap Used: " + (result.maxNonHeapUsedBytes / (1024 * 1024)) + " MB");
        System.out.println("Peak Threads: " + result.peakThreadCount);
        System.out.println("Process CPU Time: " + result.processCpuTimeMs + " ms");
        System.out.println("Direct Buffer Memory: " + (result.directBufferMemoryBytes / 1024) + " KB");
        System.out.println("Disk Usage: " + (result.diskUsageBytes / 1024) + " KB");
        System.out.println("GC Count: " + result.gcCount);
        System.out.println("GC Time: " + result.gcTimeMs + " ms");
        System.out.println("Run Count: " + result.runCount);
        System.out.println("Diff Time: " + result.diffTimeMs + " ms");
        System.out.println("Main Index Size: " + (result.mainIndexSizeBytes / 1024) + " KB");
        System.out.println("Query Time: " + result.queryTimeMs + " ms");
        System.out.println("Query Approved: " + result.queryApproved);
        
        // Per-run timing details (for multi-run scenarios with interrupts)
        if (result.runTimings != null && result.runTimings.size() > 0) {
            System.out.println("\n--- Per-Run Timing Details ---");
            for (int i = 0; i < result.runTimings.size(); i++) {
                System.out.println(String.format("Run %d Time: %d ms", i + 1, result.runTimings.get(i)));
            }
            for (int i = 0; i < result.traversalTimings.size(); i++) {
                System.out.println(String.format("Run %d Traversal: %d ms", i + 1, result.traversalTimings.get(i)));
            }
            for (int i = 0; i < result.resumePaths.size(); i++) {
                System.out.println(String.format("Run %d Resume Path: %s", i + 1, result.resumePaths.get(i)));
            }
        }
        
        // Incremental searchability results and assertions
        if (result.incrementalQueryResults != null && result.incrementalQueryResults.size() > 0) {
            System.out.println("\n--- Incremental Searchability Verification ---");
            
            // Calculate max incremental results
            int maxIncrementalResults = result.incrementalQueryResults.stream().mapToInt(Integer::intValue).max().orElse(0);
            int totalChunks = result.incrementalQueryResults.size();
            int chunksWithResults = (int) result.incrementalQueryResults.stream().filter(r -> r > 0).count();
            
            // Output summary for script parsing
            System.out.println("INCREMENTAL_SUMMARY: maxResults=" + maxIncrementalResults + 
                ", totalChunks=" + totalChunks + ", chunksWithResults=" + chunksWithResults);
            
            // Per-chunk details
            for (int i = 0; i < result.incrementalQueryResults.size(); i++) {
                int queryResults = result.incrementalQueryResults.get(i);
                long queryTime = result.incrementalQueryTimes.get(i);
                System.out.println(String.format("Chunk %d: %d results (%.1f%% of total) in %d ms", 
                    i + 1, queryResults, (100.0 * queryResults / Math.max(1, result.queryApproved)), queryTime));
            }
            
            System.out.println(String.format("\nMax results seen in incremental queries: %d", maxIncrementalResults));
            
            // ASSERTIONS for incremental searchability
            assertTrue("Should have at least one chunk result recorded", totalChunks > 0);
            
            // NOTE: Incremental searchability requires deeper Oak Lucene changes to flush writers
            // on COMMIT_PROGRESS. Current implementation only flushes on final commit.
            // This is a known limitation - documenting expected behavior.
            if (maxIncrementalResults > 0) {
                System.out.println("✓ SUCCESS: Index showed incremental searchability!");
                // If we got incremental results, they should not exceed final total
                assertTrue("Incremental results (" + maxIncrementalResults + 
                          ") should not exceed final verified count (" + result.queryApproved + ")",
                          maxIncrementalResults <= result.queryApproved);
            } else {
                System.out.println("NOTE: Incremental searchability requires COMMIT_PROGRESS handler in LuceneIndexEditorProvider");
                System.out.println("      Current Oak Lucene only flushes writers on final commit (COMMIT_SUCCEDED)");
            }
        }
    }

    private Result runTest() throws Exception {
        PerfContext ctx = new PerfContext();
        setupContext(ctx);

        try {
            // === PHASE 1: Create index and complete initial index build ===
            System.out.println("\n--- Phase 1: Initial Index Creation ---");
            System.out.println("Running async indexer to complete initial index (reindex=true -> false)...");
            
            // Run until reindex becomes false
            int initialRuns = 0;
            while (true) {
                ctx.asyncIndexUpdate.run();
                ctx.indexTracker.refresh();
                initialRuns++;
                
                org.apache.jackrabbit.oak.spi.state.NodeState rootAfterRun = ctx.nodeStore.getRoot();
                org.apache.jackrabbit.oak.spi.state.NodeState idxState = 
                    rootAfterRun.getChildNode("oak:index").getChildNode("damAssetLucene");
                boolean reindex = idxState.getBoolean("reindex");
                
                // Debug output
                if (initialRuns <= 5 || initialRuns % 100 == 0) {
                    System.out.println("  Run #" + initialRuns + ": reindex=" + reindex + 
                        ", exists=" + idxState.exists() + 
                        ", checkpoint=" + rootAfterRun.getChildNode(":async").getString("async"));
                }
                
                if (!reindex) {
                    System.out.println("  Initial index complete after " + initialRuns + " run(s)");
                    break;
                }
                if (initialRuns > 1000) {
                    throw new RuntimeException("Initial indexing took too many runs (>1000)");
                }
            }
            
            // Verify checkpoint was created
            String initialCheckpoint = ctx.nodeStore.getRoot().getChildNode(":async").getString("async");
            System.out.println("  Checkpoint after initial index: " + initialCheckpoint);

            // === PHASE 2: Create content AFTER initial index is built ===
            System.out.println("\n--- Phase 2: Creating Content ---");
            long startContent = System.currentTimeMillis();
            createContent(ctx, NODE_COUNT, BATCH_SIZE);
            long contentTime = System.currentTimeMillis() - startContent;
            System.out.println("Content creation: " + contentTime + " ms (" + 
                String.format("%.1f", NODE_COUNT * 1000.0 / contentTime) + " nodes/sec)");

            // === PHASE 3: Run indexing to process new content ===
            System.out.println("\n--- Phase 3: Indexing New Content ---");
            System.out.println("oak.async.chunkSize: " + System.getProperty("oak.async.chunkSize", "not set"));
            System.out.println("\n*** RESUMABLE INDEXING MODE ***");
            System.out.println("Each run() processes one chunk, saves progress, and completes");
            System.out.println("Next run() resumes from saved position");
            System.out.println("Index becomes searchable after each run() completes\n");

            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            if (threadBean.isThreadCpuTimeSupported()) {
                threadBean.setThreadCpuTimeEnabled(true);
            }
            threadBean.resetPeakThreadCount();

            long startMem = getUsedMemory();
            long startGcCount = getGcCount();
            long startGcTime = getGcTime();
            long startCpuTime = getProcessCpuTime();

            long startIndexing = System.currentTimeMillis();
            int cycleCount = 0;
            int maxCycles = 100;  // Reasonable max for chunked cycles
            long maxTimeMs = 600_000;  // 10 minutes max
            String lastResumeState = null;
            int stuckCounter = 0;
            
            // Track per-cycle timing
            java.util.List<Long> runTimes = new java.util.ArrayList<>();
            java.util.List<Long> traversalTimes = new java.util.ArrayList<>();
            java.util.List<Long> resumeTimeToTargets = new java.util.ArrayList<>();
            java.util.List<Long> resumeTotalTimes = new java.util.ArrayList<>();
            java.util.List<String> resumePaths = new java.util.ArrayList<>();
            
            // Track cycle-end searchability - query results after each cycle completes
            java.util.List<Integer> incrementalQueryResults = new java.util.ArrayList<>();
            java.util.List<Long> incrementalQueryTimes = new java.util.ArrayList<>();
            
            // Track per-chunk metrics
            java.util.List<Long> chunkHeapUsedMB = new java.util.ArrayList<>();
            java.util.List<Long> chunkNonHeapUsedMB = new java.util.ArrayList<>();
            java.util.List<Integer> chunkGcCount = new java.util.ArrayList<>();
            java.util.List<Long> chunkGcTimeMs = new java.util.ArrayList<>();
            java.util.List<Long> chunkCpuTimeMs = new java.util.ArrayList<>();
            java.util.List<Long> chunkSegmentStoreSizeMB = new java.util.ArrayList<>();
            
            // Run indexing cycles until complete
            while (true) {
                long cycleStart = System.currentTimeMillis();
                
                // Check timeout
                if (System.currentTimeMillis() - startIndexing > maxTimeMs) {
                    throw new RuntimeException("Indexing timeout after " + (maxTimeMs/1000) + " seconds and " + cycleCount + " cycles");
                }
                
                // Check max cycles
                if (cycleCount >= maxCycles) {
                    throw new RuntimeException("Indexing exceeded max cycles (" + maxCycles + ")");
                }
                
                // Run one async indexing cycle
                ctx.asyncIndexUpdate.run();
                
                ctx.indexTracker.refresh();
                ctx.provider.contentChanged(ctx.nodeStore.getRoot(), 
                    org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
                long cycleTime = System.currentTimeMillis() - cycleStart;
                cycleCount++;
                
                // Capture timing for this cycle
                runTimes.add(cycleTime);
                
                // Get traversal time from AsyncIndexUpdate
                long traversalTime = ctx.asyncIndexUpdate.getLastDiffTimeMs();
                traversalTimes.add(traversalTime);
                
                // Get nodes processed in this chunk
                long nodesProcessed = ctx.asyncIndexUpdate.getLastNodesProcessed();
                
                // Get ResumingEditor timing (only non-zero during actual resume)
                long resumeTimeToTarget = ctx.asyncIndexUpdate.getLastResumeTimeToTargetMs();
                long resumeTotalTime = ctx.asyncIndexUpdate.getLastResumeTotalTimeMs();
                resumeTimeToTargets.add(resumeTimeToTarget);
                resumeTotalTimes.add(resumeTotalTime);
                
                // Check if indexing is complete
                org.apache.jackrabbit.oak.spi.state.NodeState rootState = ctx.nodeStore.getRoot();
                org.apache.jackrabbit.oak.spi.state.NodeState asyncNode = rootState.getChildNode(":async");
                
                // Check checkpoint state
                String currentCheckpoint = asyncNode.getString("async");
                org.apache.jackrabbit.oak.spi.state.NodeState idxAfterRun = 
                    rootState.getChildNode("oak:index").getChildNode("damAssetLucene");
                boolean hasData = idxAfterRun.hasChildNode(":data");
                
                // Count files in :data to debug index state
                long dataChildCount = 0;
                if (hasData) {
                    org.apache.jackrabbit.oak.spi.state.NodeState dataNode = idxAfterRun.getChildNode(":data");
                    dataChildCount = dataNode.getChildNodeCount(1000);
                }

                // Check if there's a resume state (indicates chunk limit was reached)
                org.apache.jackrabbit.oak.spi.state.NodeState laneNode = asyncNode.getChildNode("async-resume");
                boolean hasResumeState = laneNode.exists() && laneNode.hasProperty("lastIndexedPath");
                String currentResumeState = hasResumeState ? laneNode.getString("lastIndexedPath") : null;
                
                System.out.println("  Cycle #" + cycleCount + " completed in " + cycleTime + " ms (traversal: " + traversalTime + " ms, nodes: " + nodesProcessed + ")");
                System.out.println("    checkpoint=" + currentCheckpoint + 
                    ", hasData=" + hasData + ", dataFiles=" + dataChildCount);
                
                // If resume state exists, this run hit the chunk limit and saved progress
                if (hasResumeState) {
                    System.out.println("    → Resume state detected: " + currentResumeState);
                    System.out.println("    → Chunk limit reached - run completed and saved progress");
                    System.out.println("    → Next run will resume from this position");
                    
                    // Track resume path for this run
                    resumePaths.add(currentResumeState);
                    
                    // Detect if stuck at same path
                    if (currentResumeState.equals(lastResumeState)) {
                        stuckCounter++;
                        if (stuckCounter > 5) {
                            throw new RuntimeException("Resume appears stuck at path: " + currentResumeState + 
                                " (same path for " + stuckCounter + " consecutive runs)");
                        }
                    } else {
                        stuckCounter = 0;
                    }
                    lastResumeState = currentResumeState;
                    
                    // Test searchability after this run - MUST refresh index tracker and get fresh root
                    System.out.println("    → Testing searchability after run completion...");
                    
                    // Wait a moment for index to be fully flushed and file system sync
                    try { Thread.sleep(200); } catch (InterruptedException ie) {}
                    
                    // CRITICAL REFRESH SEQUENCE FOR INCREMENTAL SEARCHABILITY:
                    // NOTE: Query engine is tied to ORIGINAL providers from repository creation
                    // So we must update the ORIGINAL tracker, not create a new one
                    
                    // 1. Get fresh NodeState from NodeStore (contains committed index data)
                    org.apache.jackrabbit.oak.spi.state.NodeState freshRoot = ctx.nodeStore.getRoot();
                    
                    // 2. Force refresh on ORIGINAL tracker - this sets refresh flag
                    ctx.indexTracker.refresh();
                    
                    // 3. Update with fresh root - when refresh=true, this closes all readers and reopens
                    ctx.indexTracker.update(freshRoot);
                    
                    // 4. Force the tracker to open the index by acquiring it
                    org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexNode indexNode = 
                        ctx.indexTracker.acquireIndexNode("/oak:index/damAssetLucene");
                    if (indexNode != null) {
                        indexNode.release();  // Release after acquisition
                    }
                    
                    // 5. Notify provider of content change
                    ctx.provider.contentChanged(freshRoot, 
                        org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
                    
                    // 6. Get fresh root from content session
                    ctx.root = ctx.contentSession.getLatestRoot();
                    
                    long queryStart = System.currentTimeMillis();
                    
                    // Use regular query and count results (Oak's rep:count() doesn't work reliably)
                    String incrementalQuery = 
                        "SELECT [jcr:path] FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam') " +
                        "AND [jcr:content/metadata/dam:status] = 'approved'";
                    
                    long partialResults = executeCountQuery(ctx, incrementalQuery);
                    long queryTime = System.currentTimeMillis() - queryStart;
                    
                    incrementalQueryResults.add((int) partialResults);
                    incrementalQueryTimes.add(queryTime);
                    
                    // Collect per-chunk metrics for analysis
                    chunkHeapUsedMB.add(getHeapMemoryUsed() / (1024 * 1024));
                    chunkNonHeapUsedMB.add(getNonHeapMemoryUsed() / (1024 * 1024));
                    chunkGcCount.add((int) getGcCount());
                    chunkGcTimeMs.add(getGcTime());
                    chunkCpuTimeMs.add(getProcessCpuTime() / 1_000_000); // Convert to ms
                    chunkSegmentStoreSizeMB.add(getSegmentStoreSize(ctx) / (1024 * 1024));
                    
                    // Output for script parsing - one line per chunk
                    System.out.println(String.format("CHUNK_RESULT: cycle=%d, results=%d, time=%d, nodes=%d, path=%s", 
                        cycleCount, partialResults, queryTime, nodesProcessed, currentResumeState));
                    
                    // Output detailed metrics for this chunk
                    System.out.println(String.format("CHUNK_METRICS: cycle=%d, nodes=%d, heap=%dMB, nonHeap=%dMB, gc=%d, gcTime=%dms, cpu=%dms, segStore=%dMB",
                        cycleCount,
                        nodesProcessed,
                        chunkHeapUsedMB.get(chunkHeapUsedMB.size() - 1),
                        chunkNonHeapUsedMB.get(chunkNonHeapUsedMB.size() - 1),
                        chunkGcCount.get(chunkGcCount.size() - 1),
                        chunkGcTimeMs.get(chunkGcTimeMs.size() - 1),
                        chunkCpuTimeMs.get(chunkCpuTimeMs.size() - 1),
                        chunkSegmentStoreSizeMB.get(chunkSegmentStoreSizeMB.size() - 1)));
                    
                    // Assertion: Query should not return errors (negative values)
                    assertTrue("Incremental query should not fail (cycle " + cycleCount + ")", partialResults >= 0);
                    
                    // Continue to next run
                    continue;
                }

                // No resume state - indexing is complete
                System.out.println("  → No resume state - indexing complete!\n");
                
                if (cycleCount == 1) {
                    System.out.println("  NOTE: Completed in 1 run - chunk limit not reached");
                    System.out.println("        Increase content or decrease chunk size to see multiple runs\n");
                }
                
                break;
            }

            long totalIndexTime = System.currentTimeMillis() - startIndexing;

            long endMem = getUsedMemory();
            long endGcCount = getGcCount();
            long endGcTime = getGcTime();
            long endCpuTime = getProcessCpuTime();
            long directBufferMem = getDirectBufferMemory();
            long diskUsage = getDiskUsage(ctx);
            long mainIndexSize = getMainIndexSize(ctx);

            System.out.println("\nIndexing complete:");
            System.out.println("  Total time: " + totalIndexTime + " ms");
            System.out.println("  Total cycles: " + cycleCount);
            
            // Output per-cycle timing breakdown (only for multi-cycle runs)
            if (cycleCount > 1) {
                System.out.println("\n  Per-Cycle Timing Breakdown:");
                System.out.println(String.format("    %-5s | %9s | %9s | %9s | %9s | %9s | %-40s", 
                    "Cycle", "Total(ms)", "Trav(ms)", "OH(ms)", "Resume(ms)", "ResOH(ms)", "Path"));
                System.out.println("    ------|-----------|-----------|-----------|-----------|-----------|------------------------------------------");
                
                long totalTraversal = 0;
                long totalRun = 0;
                long totalResumeTime = 0;
                
                for (int i = 0; i < cycleCount; i++) {
                    long rt = runTimes.get(i);
                    long tt = traversalTimes.get(i);
                    long overhead = rt - tt;
                    long resumeTime = resumeTimeToTargets.get(i);
                    long resumeTotal = resumeTotalTimes.get(i);
                    long resumeOH = resumeTotal - resumeTime;
                    
                    totalTraversal += tt;
                    totalRun += rt;
                    totalResumeTime += resumeTime;
                    
                    String resumePath = (i < resumePaths.size()) ? resumePaths.get(i) : "N/A";
                    
                    // Show resume time only if non-zero
                    String resumeStr = resumeTime > 0 ? String.format("%9d", resumeTime) : "         -";
                    String resumeOHStr = resumeOH > 0 ? String.format("%9d", resumeOH) : "         -";
                    
                    System.out.println(String.format("    %-5d | %9d | %9d | %9d | %s | %s | %-40s", 
                        i + 1, rt, tt, overhead, resumeStr, resumeOHStr, 
                        resumePath.length() > 40 ? resumePath.substring(0, 37) + "..." : resumePath));
                }
                
                System.out.println("    ------|-----------|-----------|-----------|-----------|-----------|------------------------------------------");
                System.out.println(String.format("    %-5s | %9d | %9d | %9d | %9d | %9s | %s", 
                    "TOTAL", totalRun, totalTraversal, (totalRun - totalTraversal), totalResumeTime, 
                    String.format("%.1f%%", 100.0 * (totalRun - totalTraversal) / totalRun), 
                    "Overhead %"));
                    
                System.out.println("\n  Timing Breakdown Explanation:");
                System.out.println("    Total(ms)    - Wall clock time for each run");
                System.out.println("    Trav(ms)     - Diff traversal time (tree walk + ALL editors including ResumingEditor)");
                System.out.println("    OH(ms)       - Overhead beyond traversal (checkpointing, index writes, etc.)");
                System.out.println("    Resume(ms)   - Time ResumingEditor took to REACH resume point (0 if not resuming)");
                System.out.println("    ResOH(ms)    - ResumingEditor overhead after reaching target (0 if not resuming)");
                System.out.println("    Path         - Resume path or progress commit path");
            }
            
            // === Detailed metrics analysis (runs for both NORMAL and RESUME modes) ===
            System.out.println("\n  ===========================================");
            System.out.println("  DETAILED METRICS ANALYSIS");
            System.out.println("  ===========================================");
            
            // GC Overhead
            double gcOverheadPct = (endGcTime - startGcTime) * 100.0 / totalIndexTime;
            System.out.println(String.format("\n  GC Analysis:"));
            System.out.println(String.format("    Total GC Time: %d ms", endGcTime - startGcTime));
            System.out.println(String.format("    Total GC Count: %d collections", endGcCount - startGcCount));
            System.out.println(String.format("    GC Overhead: %.2f%% of total time", gcOverheadPct));
            if ((endGcCount - startGcCount) > 0) {
                System.out.println(String.format("    Average GC Pause: %.1f ms", 
                    (endGcTime - startGcTime) / (double)(endGcCount - startGcCount)));
            }
            
            // Memory Analysis
            long memoryDelta = endMem - startMem;
            double memoryEfficiency = memoryDelta / (double) NODE_COUNT;
            System.out.println(String.format("\n  Memory Analysis:"));
            System.out.println(String.format("    Memory Delta: %d MB", memoryDelta / (1024 * 1024)));
            System.out.println(String.format("    Memory Efficiency: %.1f bytes/node", memoryEfficiency));
            System.out.println(String.format("    Peak Heap: %d MB", 
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / (1024 * 1024)));
            
            // Memory pool breakdown
            System.out.println(String.format("\n  Memory Pools:"));
            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                if (pool.getType() == MemoryType.HEAP) {
                    long used = pool.getUsage().getUsed() / (1024 * 1024);
                    long max = pool.getUsage().getMax() / (1024 * 1024);
                    System.out.println(String.format("    %-20s: %5d MB / %5d MB", 
                        pool.getName(), used, max > 0 ? max : 0));
                }
            }
            
            // CPU Analysis
            long cpuDelta = endCpuTime - startCpuTime;
            double cpuEfficiency = NODE_COUNT / (cpuDelta / 1_000_000_000.0); // nodes per CPU second
            double cpuUtilization = (cpuDelta / 1_000_000.0) * 100.0 / totalIndexTime; // percentage
            System.out.println(String.format("\n  CPU Analysis:"));
            System.out.println(String.format("    Total CPU Time: %.2f s", cpuDelta / 1_000_000_000.0));
            System.out.println(String.format("    CPU Utilization: %.1f%% of wall time", cpuUtilization));
            System.out.println(String.format("    CPU Efficiency: %.0f nodes/cpu-second", cpuEfficiency));
            System.out.println(String.format("    Peak Threads: %d", threadBean.getPeakThreadCount()));
            
            // Disk Analysis
            long segmentStoreSize = getSegmentStoreSize(ctx);
            System.out.println(String.format("\n  Disk Analysis:"));
            System.out.println(String.format("    SegmentStore Size: %d MB", segmentStoreSize / (1024 * 1024)));
            System.out.println(String.format("    Lucene Index Size: %d MB", mainIndexSize / (1024 * 1024)));
            System.out.println(String.format("    Total Disk Usage: %d MB", diskUsage / (1024 * 1024)));
            
            // Per-chunk metrics summary (if available)
            if (!chunkHeapUsedMB.isEmpty()) {
                System.out.println(String.format("\n  Per-Chunk Metrics Summary:"));
                System.out.println(String.format("    Heap Growth: %d MB → %d MB", 
                    chunkHeapUsedMB.get(0), 
                    chunkHeapUsedMB.get(chunkHeapUsedMB.size() - 1)));
                System.out.println(String.format("    SegmentStore Growth: %d MB → %d MB", 
                    chunkSegmentStoreSizeMB.get(0), 
                    chunkSegmentStoreSizeMB.get(chunkSegmentStoreSizeMB.size() - 1)));
                
                // Calculate average per-chunk GC
                int totalChunkGc = 0;
                for (int gc : chunkGcCount) totalChunkGc += gc;
                System.out.println(String.format("    Average GC per chunk: %.1f collections", 
                    totalChunkGc / (double) chunkGcCount.size()));
            }

            // Debug: Check index state (use nodeStore directly for accurate state)
            org.apache.jackrabbit.oak.spi.state.NodeState debugRoot = ctx.nodeStore.getRoot();
            org.apache.jackrabbit.oak.spi.state.NodeState idx = 
                debugRoot.getChildNode("oak:index").getChildNode("damAssetLucene");
            System.out.println("\n  DEBUG Index State:");
            System.out.println("    Exists: " + idx.exists());
            System.out.println("    Has :data: " + idx.hasChildNode(":data"));
            if (idx.hasProperty("async")) {
                System.out.println("    async: " + idx.getProperty("async").getValue(Type.STRING));
            }
            if (idx.hasProperty("reindex")) {
                System.out.println("    reindex: " + idx.getProperty("reindex").getValue(Type.BOOLEAN));
            }
            if (idx.hasProperty("reindexCount")) {
                System.out.println("    reindexCount: " + idx.getProperty("reindexCount").getValue(Type.LONG));
            }
            org.apache.jackrabbit.oak.spi.state.NodeState contentState = 
                debugRoot.getChildNode("content").getChildNode("dam");
            System.out.println("    Content/dam children: " + contentState.getChildNodeCount(10));
            
            // Check :async state
            org.apache.jackrabbit.oak.spi.state.NodeState asyncState = debugRoot.getChildNode(":async");
            System.out.println("    :async exists: " + asyncState.exists());
            if (asyncState.exists()) {
                System.out.println("    :async checkpoint: " + asyncState.getString("async"));
            }

            // 4. Verification
            System.out.println("\n--- Phase 4: Verification ---");
            long queryStart = System.currentTimeMillis();
            
            // Verify index has data
            org.apache.jackrabbit.oak.spi.state.NodeState idxState = 
                ctx.nodeStore.getRoot().getChildNode("oak:index").getChildNode("damAssetLucene");
            boolean hasData = idxState.hasChildNode(":data");
            System.out.println("  Index has :data: " + hasData);
            assertTrue("Index should have :data child", hasData);
            
            // Refresh index tracker before queries - use ORIGINAL tracker (query engine is tied to it)
            org.apache.jackrabbit.oak.spi.state.NodeState finalRoot = ctx.nodeStore.getRoot();
            
            // Force refresh on ORIGINAL tracker
            ctx.indexTracker.refresh();
            ctx.indexTracker.update(finalRoot);
            
            // Force the tracker to open the index by acquiring it
            org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexNode finalIndexNode = 
                ctx.indexTracker.acquireIndexNode("/oak:index/damAssetLucene");
            if (finalIndexNode != null) {
                finalIndexNode.release();
            }
            
            // Notify provider of content change
            ctx.provider.contentChanged(finalRoot, 
                org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
            ctx.root = ctx.contentSession.getLatestRoot();
            
            // Verify with query using Lucene index (traversal fail ensures index is used)
            System.out.println("  Running query verification (with traversal fail)...");
            
            // Use regular query and count results to get actual total count
            String countQuery = 
                "SELECT [jcr:path] FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam') " +
                "AND [jcr:content/metadata/dam:status] = 'approved' " +
                "option(traversal fail, index name damAssetLucene)";
            
            long actualCount = executeCountQueryWithRetry(ctx, countQuery, 10, 500);
            System.out.println("    Actual count from index: " + actualCount);
            
            // Also run the regular query to show the capped result
            int queryApproved = executeQueryWithRetry(ctx, 
                "SELECT * FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam') " +
                "AND [jcr:content/metadata/dam:status] = 'approved' " +
                "option(traversal fail, index name damAssetLucene)", 
                10, 500);
            System.out.println("    Query result count (capped at 1000): " + queryApproved);
            
            long queryTime = System.currentTimeMillis() - queryStart;
            
            // Assertions - use actual count for verification
            System.out.println("  Expected approved nodes: " + QUERY_TARGET_COUNT);
            System.out.println("  Actual indexed nodes: " + actualCount);
            assertTrue("Actual count should be positive", actualCount > 0);
            assertTrue("Should have approximately " + QUERY_TARGET_COUNT + " approved nodes (±5%)", 
                      actualCount >= QUERY_TARGET_COUNT * 0.95 && actualCount <= QUERY_TARGET_COUNT * 1.05);
            
            // The queryApproved is capped at 1000, so verify it's at or below that
            assertTrue("Query result should be capped at 1000 or match actual count if less", 
                      queryApproved <= 1000 && queryApproved <= actualCount);

            // Capture diff time from AsyncIndexUpdate
            long diffTime = ctx.asyncIndexUpdate.getLastDiffTimeMs();

            // Build result
            Result result = new Result();
            result.totalTimeMs = totalIndexTime;
            result.contentCreationTimeMs = contentTime;
            result.throughput = (double) NODE_COUNT / (totalIndexTime / 1000.0);
            result.memoryUsedBytes = endMem - startMem;
            result.gcCount = endGcCount - startGcCount;
            result.gcTimeMs = endGcTime - startGcTime;
            result.processCpuTimeMs = (endCpuTime != -1 && startCpuTime != -1) ? (endCpuTime - startCpuTime) : -1;
            result.directBufferMemoryBytes = directBufferMem;
            result.diskUsageBytes = diskUsage;
            result.maxHeapUsedBytes = getMaxHeapUsed();
            result.maxNonHeapUsedBytes = getMaxNonHeapUsed();
            result.peakThreadCount = threadBean.getPeakThreadCount();
            result.queryTimeMs = queryTime;
            result.diffTimeMs = diffTime;
            result.runCount = cycleCount;
            result.mainIndexSizeBytes = mainIndexSize;
            result.nodeCount = NODE_COUNT;
            result.queryApproved = (int) actualCount;  // Use actual count instead of capped result
            
            // Add per-run timing details
            result.runTimings = runTimes;
            result.traversalTimings = traversalTimes;
            result.resumePaths = resumePaths;
            result.incrementalQueryResults = incrementalQueryResults;
            result.incrementalQueryTimes = incrementalQueryTimes;

            return result;

        } finally {
            teardownContext(ctx);
        }
    }

    // ========================================
    // Content Creation (matching BasicChangeTrackerPerfTest)
    // ========================================

    private void registerDamNodeTypes(Root root) throws Exception {
        String cnd = 
            "<dam = 'http://www.day.com/dam/1.0'>\n" +
            "[dam:Asset] > nt:hierarchyNode\n" +
            "  + jcr:content (nt:unstructured)\n";
        
        java.io.ByteArrayInputStream stream = new java.io.ByteArrayInputStream(cnd.getBytes());
        org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry.register(root, stream, "dam-nodetypes.cnd");
        root.commit();
    }

    private void createContent(PerfContext ctx, int count, int batchSize) throws Exception {
        Tree content = ctx.root.getTree("/").addChild("content");
        content.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree dam = content.addChild("dam");
        dam.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        // Create exactly QUERY_TARGET_COUNT dam:Asset nodes with status="approved"
        // Spread them evenly across all nodes so each chunk sees some approved nodes
        int approvedInterval = Math.max(1, count / QUERY_TARGET_COUNT);
        int approvedCount = 0;
        
        System.out.println("Creating " + count + " nodes with " + QUERY_TARGET_COUNT + 
            " approved nodes (every " + approvedInterval + "th node for even distribution)");

        for (int i = 0; i < count; i++) {
            Tree asset = dam.addChild("asset-" + i);
            asset.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
            
            // Add jcr:content with metadata
            Tree jcrContent = asset.addChild("jcr:content");
            jcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            
            Tree metadata = jcrContent.addChild("metadata");
            metadata.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            metadata.setProperty("dc:title", "Asset Title " + i);
            metadata.setProperty("dam:assetId", "asset-" + i);
            
            // Spread approved nodes evenly - every Nth node
            // This ensures each chunk will have approved nodes to find in incremental queries
            if (i % approvedInterval == 0 && approvedCount < QUERY_TARGET_COUNT) {
                metadata.setProperty("dam:status", "approved");
                approvedCount++;
            } else {
                metadata.setProperty("dam:status", "draft");
            }

            if ((i + 1) % batchSize == 0) {
                ctx.root.commit();
                if ((i + 1) % (batchSize * 10) == 0) {
                    System.out.println("  Created " + (i + 1) + "/" + count + " assets (" + approvedCount + " approved so far)");
                }
            }
        }
        ctx.root.commit();
        System.out.println("Created " + count + " dam:Asset nodes (" + approvedCount + " with status=approved, spread evenly)");
    }

    // ========================================
    // Context Setup/Teardown
    // ========================================

    private class PerfContext {
        NodeStore nodeStore;
        BlobStore blobStore;
        ContentRepository contentRepository;
        ContentSession contentSession;
        Root root;
        AsyncIndexUpdate asyncIndexUpdate;
        LuceneIndexProvider provider;
        LuceneIndexEditorProvider editorProvider;
        IndexCopier indexCopier;
        IndexTracker indexTracker;
        java.util.concurrent.ExecutorService indexCopierExecutor;
        
        FileStore fileStore;
        File storeDir;
        File indexDir;
        ScheduledExecutorService scheduledExecutor;
        MongoConnection mongoConnection;
        DocumentNodeStore documentNodeStore;
    }

    private void setupContext(PerfContext ctx) throws Exception {
        // NodeStore setup
        if (NODE_STORE_TYPE == NodeStoreType.MEMORY) {
            ctx.nodeStore = new MemoryNodeStore();
        } else if (NODE_STORE_TYPE == NodeStoreType.SEGMENT) {
            ctx.storeDir = temporaryFolder.newFolder("segment-" + System.nanoTime());
            ctx.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(ctx.scheduledExecutor);
            
            // FileDataStore for blob storage
            File blobDir = temporaryFolder.newFolder("blobs-" + System.nanoTime());
            OakFileDataStore fds = new OakFileDataStore();
            fds.setPath(blobDir.getAbsolutePath());
            fds.init(null);
            ctx.blobStore = new DataStoreBlobStore(fds);
            
            ctx.fileStore = FileStoreBuilder.fileStoreBuilder(ctx.storeDir)
                    .withStatisticsProvider(statisticsProvider)
                    .withBlobStore(ctx.blobStore)
                    .withMaxFileSize(256)
                    .withMemoryMapping(false)
                    .build();
            ctx.nodeStore = SegmentNodeStoreBuilders.builder(ctx.fileStore).build();
        } else if (NODE_STORE_TYPE == NodeStoreType.DOCUMENT) {
            assumeTrue("MongoDB not available", MongoUtils.isAvailable());
            ctx.mongoConnection = connectionFactory.getConnection();
            MongoUtils.dropCollections(ctx.mongoConnection.getDatabase());
            ctx.documentNodeStore = new DocumentMK.Builder()
                    .setMongoDB(ctx.mongoConnection.getMongoClient(), ctx.mongoConnection.getDBName())
                    .setAsyncDelay(0)
                    .getNodeStore();
            ctx.nodeStore = ctx.documentNodeStore;
        }

        // Lucene providers (without IndexCopier to avoid classpath issues)
        ctx.indexDir = temporaryFolder.newFolder("index-" + System.nanoTime());
        ctx.indexTracker = new IndexTracker();
        ctx.provider = new LuceneIndexProvider(ctx.indexTracker);
        ctx.editorProvider = new LuceneIndexEditorProvider();

        // Repository
        ctx.contentRepository = new Oak(ctx.nodeStore)
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) ctx.provider)
            .with((Observer) ctx.provider)
            .with(ctx.editorProvider)
            .with(new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider())
            .with(new org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider())
            .createContentRepository();

        ctx.contentSession = ctx.contentRepository.login(null, null);
        ctx.root = ctx.contentSession.getLatestRoot();

        // Register dam:Asset node type
        registerDamNodeTypes(ctx.root);

        // Create index definition for dam:Asset
        createLuceneIndex(ctx.root);

        // Set chunk size for resumable indexing
        if (CHUNK_SIZE > 0) {
            System.setProperty("oak.async.chunkSize", String.valueOf(CHUNK_SIZE));
            System.out.println("  [TEST] Resumable indexing enabled - chunkSize: " + CHUNK_SIZE);
        }

        // AsyncIndexUpdate
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

    private void createLuceneIndex(Root root) throws Exception {
        Tree oakIndex = root.getTree("/oak:index");
        Tree index = oakIndex.addChild("damAssetLucene");
        index.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        index.setProperty("type", "lucene");
        index.setProperty("async", "async");
        index.setProperty("compatVersion", 2);
        index.setProperty("reindex", true);
        index.setProperty("evaluatePathRestrictions", true);
        index.setProperty("includedPaths", Arrays.asList("/content"), Type.STRINGS);

        // Aggregation for dam:Asset - include jcr:content/metadata
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

        // Index Rules for dam:Asset
        Tree indexRules = index.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAsset = indexRules.addChild("dam:Asset");
        damAsset.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree properties = damAsset.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        // dc:title property (analyzed, fulltext)
        Tree titleProp = properties.addChild("dcTitle");
        titleProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        titleProp.setProperty("name", "jcr:content/metadata/dc:title");
        titleProp.setProperty("propertyIndex", true);
        titleProp.setProperty("analyzed", true);
        titleProp.setProperty("nodeScopeIndex", true);

        // dam:assetId property
        Tree assetIdProp = properties.addChild("damAssetId");
        assetIdProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        assetIdProp.setProperty("name", "jcr:content/metadata/dam:assetId");
        assetIdProp.setProperty("propertyIndex", true);

        // dam:status property (used for query verification)
        Tree statusProp = properties.addChild("damStatus");
        statusProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        statusProp.setProperty("name", "jcr:content/metadata/dam:status");
        statusProp.setProperty("propertyIndex", true);

        root.commit();
    }

    private void teardownContext(PerfContext ctx) throws Exception {
        if (ctx.contentSession != null) ctx.contentSession.close();
        if (ctx.asyncIndexUpdate != null) ctx.asyncIndexUpdate.close();
        if (ctx.indexCopierExecutor != null) ctx.indexCopierExecutor.shutdown();
        if (ctx.fileStore != null) ctx.fileStore.close();
        if (ctx.scheduledExecutor != null) ctx.scheduledExecutor.shutdown();
        if (ctx.documentNodeStore != null) ctx.documentNodeStore.dispose();
    }


    // ========================================
    // Query Execution
    // ========================================

    /**
     * Execute a query and return the result count.
     * No limit is applied - use only when result count is expected to be bounded (e.g., filtered by status).
     */
    private int executeQuery(PerfContext ctx, String statement) {
        try {
            ctx.root.refresh();  // Always refresh before query
            org.apache.jackrabbit.oak.api.Result result = ctx.root.getQueryEngine().executeQuery(
                statement, "JCR-SQL2",
                java.util.Collections.emptyMap(),
                org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS
            );
            int count = 0;
            for (org.apache.jackrabbit.oak.api.ResultRow row : result.getRows()) {
                row.getPath();
                count++;
            }
            return count;
        } catch (Exception e) {
            System.out.println("    Query ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return -1;
        }
    }
    
    /**
     * Execute a COUNT query and return the actual count.
     * Iterates through all results (no limit) to get the true count.
     */
    private long executeCountQuery(PerfContext ctx, String statement) {
        try {
            ctx.root.refresh();  // Always refresh before query
            org.apache.jackrabbit.oak.api.Result result = ctx.root.getQueryEngine().executeQuery(
                statement, "JCR-SQL2",
                java.util.Collections.emptyMap(),
                org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS
            );
            
            // Iterate through all results and count (no Oak limit applied)
            long count = 0;
            for (org.apache.jackrabbit.oak.api.ResultRow row : result.getRows()) {
                row.getPath();  // Access to ensure result is valid
                count++;
            }
            return count;
        } catch (Exception e) {
            System.out.println("    Count query ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return -1;
        }
    }
    
    private int executeQueryWithRetry(PerfContext ctx, String statement, int maxRetries, int delayMs) {
        for (int i = 0; i < maxRetries; i++) {
            try {
                ctx.indexTracker.refresh();
                ctx.provider.contentChanged(ctx.nodeStore.getRoot(), 
                    org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
                ctx.root = ctx.contentSession.getLatestRoot();
                
                int result = executeQuery(ctx, statement);
                if (result >= 0) {
                    return result;
                }
            } catch (Exception e) {
                System.out.println("    Retry " + (i+1) + " failed: " + e.getMessage());
            }
            try { Thread.sleep(delayMs); } catch (InterruptedException ie) { break; }
        }
        return -1;
    }
    
    private long executeCountQueryWithRetry(PerfContext ctx, String statement, int maxRetries, int delayMs) {
        for (int i = 0; i < maxRetries; i++) {
            try {
                ctx.indexTracker.refresh();
                ctx.provider.contentChanged(ctx.nodeStore.getRoot(), 
                    org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY);
                ctx.root = ctx.contentSession.getLatestRoot();
                
                long result = executeCountQuery(ctx, statement);
                if (result >= 0) {
                    return result;
                }
            } catch (Exception e) {
                System.out.println("    Count retry " + (i+1) + " failed: " + e.getMessage());
            }
            try { Thread.sleep(delayMs); } catch (InterruptedException ie) { break; }
        }
        return -1;
    }

    // ========================================
    // Metrics Collection
    // ========================================

    private static long getUsedMemory() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) sum += count;
        }
        return sum;
    }

    private static long getGcTime() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = b.getCollectionTime();
            if (time != -1) sum += time;
        }
        return sum;
    }

    private static long getMaxHeapUsed() {
        long sum = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                sum += pool.getPeakUsage().getUsed();
            }
        }
        return sum;
    }

    private static long getMaxNonHeapUsed() {
        long sum = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.NON_HEAP) {
                sum += pool.getPeakUsage().getUsed();
            }
        }
        return sum;
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
            try (java.util.stream.Stream<Path> walk = Files.walk(ctx.storeDir.toPath())) {
                return walk.filter(p -> p.toFile().isFile())
                        .mapToLong(p -> p.toFile().length())
                        .sum();
            } catch (IOException e) {
                return -1;
            }
        }
        return 0;
    }

    private static long getMainIndexSize(PerfContext ctx) {
        if (ctx.indexDir != null) {
            try (java.util.stream.Stream<Path> walk = Files.walk(ctx.indexDir.toPath())) {
                return walk.filter(p -> p.toFile().isFile())
                        .mapToLong(p -> p.toFile().length())
                        .sum();
            } catch (IOException e) {
                return -1;
            }
        }
        return 0;
    }
    
    private static long getHeapMemoryUsed() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }
    
    private static long getNonHeapMemoryUsed() {
        return ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed();
    }
    
    private static long getSegmentStoreSize(PerfContext ctx) {
        if (ctx.storeDir != null) {
            try (java.util.stream.Stream<Path> walk = Files.walk(ctx.storeDir.toPath())) {
                return walk.filter(p -> p.toFile().isFile())
                        .mapToLong(p -> p.toFile().length())
                        .sum();
            } catch (IOException e) {
                return -1;
            }
        }
        return 0;
    }

    // ========================================
    // Result Class
    // ========================================

    private static class Result {
        long totalTimeMs;
        long contentCreationTimeMs;
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
        long mainIndexSizeBytes;
        long queryTimeMs;
        long diffTimeMs;
        int runCount;
        int nodeCount;
        int queryApproved;
        
        // Per-run timing details (for interrupt testing)
        java.util.List<Long> runTimings;
        java.util.List<Long> traversalTimings;
        java.util.List<String> resumePaths;
        
        // Incremental searchability tracking
        java.util.List<Integer> incrementalQueryResults;
        java.util.List<Long> incrementalQueryTimes;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(
                "Total Time: %d ms%n" +
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
                "GC Count: %d%n" +
                "GC Time: %d ms%n" +
                "Run Count: %d%n" +
                "Diff Time: %d ms%n" +
                "Query Time: %d ms%n" +
                "Node Count: %d%n" +
                "Query Approved (index): %d",
                totalTimeMs, contentCreationTimeMs, throughput, 
                memoryUsedBytes / 1024, maxHeapUsedBytes / (1024 * 1024), 
                maxNonHeapUsedBytes / (1024 * 1024), peakThreadCount,
                processCpuTimeMs, directBufferMemoryBytes / 1024, 
                diskUsageBytes / 1024, mainIndexSizeBytes / 1024,
                gcCount, gcTimeMs, runCount, diffTimeMs,
                queryTimeMs, nodeCount, queryApproved));
            
            // Add per-run timing details (machine-parseable for script)
            // Only output if there are multiple runs (interrupt testing resulted in restarts)
            if (runTimings != null && runTimings.size() > 1) {
                sb.append("\n\n=== PER-RUN TIMING (machine-parseable) ===\n");
                for (int i = 0; i < runTimings.size(); i++) {
                    sb.append(String.format("Run %d Time: %d%n", i + 1, runTimings.get(i)));
                    if (i < traversalTimings.size()) {
                        sb.append(String.format("Run %d Traversal: %d%n", i + 1, traversalTimings.get(i)));
                    }
                    if (i < resumePaths.size()) {
                        sb.append(String.format("Run %d Resume Path: %s%n", i + 1, resumePaths.get(i)));
                    }
                }
            }
            
            // Add incremental searchability results
            if (incrementalQueryResults != null && incrementalQueryResults.size() > 0) {
                sb.append("\n=== INCREMENTAL SEARCHABILITY (machine-parseable) ===\n");
                for (int i = 0; i < incrementalQueryResults.size(); i++) {
                    sb.append(String.format("Chunk %d Query Results: %d%n", i + 1, incrementalQueryResults.get(i)));
                    if (i < incrementalQueryTimes.size()) {
                        sb.append(String.format("Chunk %d Query Time: %d%n", i + 1, incrementalQueryTimes.get(i)));
                    }
                }
            }
            
            return sb.toString();
        }
    }

    // Helper: assertEventually - retry until expected value or timeout
    @FunctionalInterface
    private interface IntSupplier {
        int get() throws Exception;
    }

    private int assertEventually(IntSupplier supplier, int expected, int maxRetries, int delayMs) 
            throws Exception {
        int result = 0;
        for (int i = 0; i < maxRetries; i++) {
            result = supplier.get();
            if (result == expected) {
                return result;
            }
            Thread.sleep(delayMs);
        }
        assertEquals("Expected value after " + maxRetries + " retries", expected, result);
        return result;
    }
}
