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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Generates comprehensive test reports for performance testing.
 * 
 * <p>Outputs:
 * <ul>
 *   <li>Console summary (via LOG and System.out)</li>
 *   <li>Markdown report (PERFORMANCE_TEST_RESULTS.md)</li>
 *   <li>CSV data (for graphing)</li>
 *   <li>Breaking points analysis</li>
 * </ul>
 */
public class TestReport {
    
    private static final Logger LOG = LoggerFactory.getLogger(TestReport.class);
    
    private final List<PhaseResult> results = new ArrayList<>();
    private final List<FailureRecord> failures = new ArrayList<>();
    private final long testStartTime = System.currentTimeMillis();
    
    private static final boolean USE_CHANGE_TRACKING = Boolean.getBoolean("useChangeTracking");
    private static final boolean USE_SEGMENT_STORE = Boolean.getBoolean("useSegmentStore");
    private static final boolean USE_MONGO_STORE = Boolean.getBoolean("useMongoStore");
    private static final long MAX_HEAP_MB = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    
    // ========================================
    // Recording Methods
    // ========================================
    
    public void recordPhase(String phaseName, int assetCount, long contentTime,
                           ChangeTrackingPerformanceTest.IndexingTimings timings,
                           MemoryStats memStats, boolean isBreakingPoint) {
        PhaseResult result = new PhaseResult();
        result.phaseName = phaseName;
        result.assetCount = assetCount;
        result.contentTime = contentTime;
        result.indexingTime = timings.getTotalTime();
        result.phase1Time = timings.phase1Time;
        result.phase2Time = timings.phase2Time;
        result.phase3Time = timings.phase3Time;
        result.traditionalTime = timings.traditionalTime;
        result.memStats = memStats;
        result.isBreakingPoint = isBreakingPoint;
        result.throughput = (result.indexingTime > 0) ? (assetCount * 1000.0 / result.indexingTime) : 0;
        
        results.add(result);
    }
    
    public void recordFailure(String phaseName, String errorType, Exception error) {
        FailureRecord failure = new FailureRecord();
        failure.phaseName = phaseName;
        failure.errorType = errorType;
        failure.errorMessage = error.getMessage();
        failure.timestamp = System.currentTimeMillis();
        
        failures.add(failure);
    }
    
    // ========================================
    // Report Generation
    // ========================================
    
    public void generateReport() {
        long testDuration = System.currentTimeMillis() - testStartTime;
        
        // Print console summary
        printConsoleSummary(testDuration);
        
        // Write markdown report
        writeMarkdownReport(testDuration);
        
        // Write CSV data
        writeCsvData();
        
        LOG.info("\n========================================");
        LOG.info("Reports generated:");
        LOG.info("  - Console output (above)");
        LOG.info("  - target/PERFORMANCE_TEST_RESULTS.md");
        LOG.info("  - target/performance_data.csv");
        LOG.info("========================================\n");
    }
    
    // ========================================
    // Console Summary
    // ========================================
    
    private void printConsoleSummary(long testDuration) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("\n");
        sb.append("========================================\n");
        sb.append("PERFORMANCE TEST SUMMARY\n");
        sb.append("========================================\n");
        sb.append(String.format("Test Configuration:\n"));
        sb.append(String.format("  Mode:       %s\n", USE_CHANGE_TRACKING ? "CHANGE TRACKING" : "TRADITIONAL"));
        sb.append(String.format("  NodeStore:  %s\n", getNodeStoreType()));
        sb.append(String.format("  Max Heap:   %d MB\n", MAX_HEAP_MB));
        sb.append(String.format("  Duration:   %d seconds\n", testDuration / 1000));
        sb.append("\n");
        
        sb.append("Phase Results:\n");
        sb.append(String.format("%-20s %10s %10s %10s %10s %10s %s\n",
                "Phase", "Assets", "Content", "Indexing", "Throughput", "GC%", "Status"));
        sb.append(String.format("%-20s %10s %10s %10s %10s %10s %s\n",
                "--------------------", "----------", "----------", "----------", "----------", "----------", "----------"));
        
        for (PhaseResult result : results) {
            String status = result.isBreakingPoint ? "BREAKING" : "OK";
            sb.append(String.format("%-20s %10d %8dms %8dms %8.1f/s %9.1f%% %s\n",
                    truncate(result.phaseName, 20),
                    result.assetCount,
                    result.contentTime,
                    result.indexingTime,
                    result.throughput,
                    result.memStats.gcTimePercent,
                    status));
        }
        
        sb.append("\n");
        
        if (!failures.isEmpty()) {
            sb.append("Failures:\n");
            for (FailureRecord failure : failures) {
                sb.append(String.format("  %s: %s - %s\n",
                        failure.phaseName, failure.errorType, failure.errorMessage));
            }
            sb.append("\n");
        }
        
        // Breaking point analysis
        PhaseResult lastSuccess = null;
        PhaseResult firstBreaking = null;
        
        for (PhaseResult result : results) {
            if (!result.isBreakingPoint) {
                lastSuccess = result;
            } else if (firstBreaking == null) {
                firstBreaking = result;
            }
        }
        
        if (lastSuccess != null && firstBreaking != null) {
            sb.append("Breaking Point Analysis:\n");
            sb.append(String.format("  Last Successful:  %d assets (%.1f assets/sec, GC %.1f%%)\n",
                    lastSuccess.assetCount, lastSuccess.throughput, lastSuccess.memStats.gcTimePercent));
            sb.append(String.format("  First Breaking:   %d assets (%.1f assets/sec, GC %.1f%%)\n",
                    firstBreaking.assetCount, firstBreaking.throughput, firstBreaking.memStats.gcTimePercent));
            sb.append(String.format("  Breaking Factor:  %.1fx increase\n",
                    (double) firstBreaking.assetCount / lastSuccess.assetCount));
            sb.append("\n");
        } else if (results.isEmpty() || !results.get(results.size() - 1).isBreakingPoint) {
            sb.append("Breaking Point Analysis:\n");
            sb.append("  No breaking point detected within test range\n");
            sb.append("  System handled all test loads successfully\n");
            sb.append("\n");
        }
        
        sb.append("========================================\n");
        
        String summary = sb.toString();
        LOG.info(summary);
        System.out.println(summary);
    }
    
    // ========================================
    // Markdown Report
    // ========================================
    
    private void writeMarkdownReport(long testDuration) {
        try {
            File reportFile = new File("target/PERFORMANCE_TEST_RESULTS.md");
            PrintWriter writer = new PrintWriter(new FileWriter(reportFile));
            
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            writer.println("# Performance Test Results");
            writer.println();
            writer.println("**Generated:** " + dateFormat.format(new Date()));
            writer.println();
            
            writer.println("## Test Configuration");
            writer.println();
            writer.println("| Parameter | Value |");
            writer.println("|-----------|-------|");
            writer.println("| Mode | " + (USE_CHANGE_TRACKING ? "Change Tracking" : "Traditional") + " |");
            writer.println("| NodeStore | " + getNodeStoreType() + " |");
            writer.println("| Max Heap | " + MAX_HEAP_MB + " MB |");
            writer.println("| Test Duration | " + (testDuration / 1000) + " seconds |");
            writer.println();
            
            writer.println("## Phase Results");
            writer.println();
            writer.println("| Phase | Assets | Content (ms) | Indexing (ms) | Throughput (assets/sec) | Heap (MB) | GC % | Status |");
            writer.println("|-------|--------|--------------|---------------|------------------------|-----------|------|--------|");
            
            for (PhaseResult result : results) {
                String status = result.isBreakingPoint ? "**BREAKING**" : "OK";
                writer.println(String.format("| %s | %d | %d | %d | %.1f | %d/%d | %.1f%% | %s |",
                        result.phaseName,
                        result.assetCount,
                        result.contentTime,
                        result.indexingTime,
                        result.throughput,
                        result.memStats.heapUsedMB,
                        result.memStats.heapMaxMB,
                        result.memStats.gcTimePercent,
                        status));
            }
            
            writer.println();
            
            if (USE_CHANGE_TRACKING) {
                writer.println("## Indexing Phase Breakdown (Change Tracking Mode)");
                writer.println();
                writer.println("| Phase | Assets | Phase 1 (ms) | Phase 2 (ms) | Phase 3 (ms) | Total (ms) |");
                writer.println("|-------|--------|--------------|--------------|--------------|------------|");
                
                for (PhaseResult result : results) {
                    writer.println(String.format("| %s | %d | %d | %d | %d | %d |",
                            result.phaseName,
                            result.assetCount,
                            result.phase1Time,
                            result.phase2Time,
                            result.phase3Time,
                            result.indexingTime));
                }
                
                writer.println();
            }
            
            if (!failures.isEmpty()) {
                writer.println("## Failures");
                writer.println();
                writer.println("| Phase | Error Type | Message |");
                writer.println("|-------|------------|---------|");
                
                for (FailureRecord failure : failures) {
                    writer.println(String.format("| %s | %s | %s |",
                            failure.phaseName,
                            failure.errorType,
                            failure.errorMessage));
                }
                
                writer.println();
            }
            
            writer.println("## Breaking Point Analysis");
            writer.println();
            
            PhaseResult lastSuccess = null;
            PhaseResult firstBreaking = null;
            
            for (PhaseResult result : results) {
                if (!result.isBreakingPoint) {
                    lastSuccess = result;
                } else if (firstBreaking == null) {
                    firstBreaking = result;
                }
            }
            
            if (lastSuccess != null && firstBreaking != null) {
                writer.println("**Last Successful Phase:**");
                writer.println("- Assets: " + lastSuccess.assetCount);
                writer.println("- Throughput: " + String.format("%.1f", lastSuccess.throughput) + " assets/sec");
                writer.println("- GC Time: " + String.format("%.1f", lastSuccess.memStats.gcTimePercent) + "%");
                writer.println("- Heap: " + lastSuccess.memStats.heapUsedMB + "/" + lastSuccess.memStats.heapMaxMB + " MB");
                writer.println();
                
                writer.println("**First Breaking Phase:**");
                writer.println("- Assets: " + firstBreaking.assetCount);
                writer.println("- Throughput: " + String.format("%.1f", firstBreaking.throughput) + " assets/sec");
                writer.println("- GC Time: " + String.format("%.1f", firstBreaking.memStats.gcTimePercent) + "%");
                writer.println("- Heap: " + firstBreaking.memStats.heapUsedMB + "/" + firstBreaking.memStats.heapMaxMB + " MB");
                writer.println();
                
                double factor = (double) firstBreaking.assetCount / lastSuccess.assetCount;
                writer.println("**Breaking Factor:** " + String.format("%.1f", factor) + "x increase");
            } else if (results.isEmpty() || !results.get(results.size() - 1).isBreakingPoint) {
                writer.println("No breaking point detected within test range.");
                writer.println();
                writer.println("System handled all test loads successfully.");
            }
            
            writer.close();
            LOG.info("Markdown report written to: {}", reportFile.getAbsolutePath());
            
        } catch (Exception e) {
            LOG.error("Failed to write markdown report: {}", e.getMessage(), e);
        }
    }
    
    // ========================================
    // CSV Data
    // ========================================
    
    private void writeCsvData() {
        try {
            File csvFile = new File("target/performance_data.csv");
            PrintWriter writer = new PrintWriter(new FileWriter(csvFile));
            
            // Header
            writer.println("Phase,AssetCount,ContentTime,IndexingTime,Phase1Time,Phase2Time,Phase3Time,TraditionalTime," +
                    "Throughput,HeapUsedMB,HeapMaxMB,GCTimePercent,IsBreakingPoint");
            
            // Data rows
            for (PhaseResult result : results) {
                writer.println(String.format("%s,%d,%d,%d,%d,%d,%d,%d,%.2f,%d,%d,%.2f,%s",
                        result.phaseName,
                        result.assetCount,
                        result.contentTime,
                        result.indexingTime,
                        result.phase1Time,
                        result.phase2Time,
                        result.phase3Time,
                        result.traditionalTime,
                        result.throughput,
                        result.memStats.heapUsedMB,
                        result.memStats.heapMaxMB,
                        result.memStats.gcTimePercent,
                        result.isBreakingPoint ? "TRUE" : "FALSE"));
            }
            
            writer.close();
            LOG.info("CSV data written to: {}", csvFile.getAbsolutePath());
            
        } catch (Exception e) {
            LOG.error("Failed to write CSV data: {}", e.getMessage(), e);
        }
    }
    
    // ========================================
    // Helper Methods
    // ========================================
    
    private String getNodeStoreType() {
        if (USE_MONGO_STORE) return "MongoDB DocumentNodeStore";
        if (USE_SEGMENT_STORE) return "SegmentNodeStore";
        return "MemoryNodeStore";
    }
    
    private String truncate(String str, int maxLength) {
        if (str.length() <= maxLength) return str;
        return str.substring(0, maxLength - 3) + "...";
    }
    
    // ========================================
    // Inner Classes
    // ========================================
    
    static class PhaseResult {
        String phaseName;
        int assetCount;
        long contentTime;
        long indexingTime;
        long phase1Time;
        long phase2Time;
        long phase3Time;
        long traditionalTime;
        double throughput;
        MemoryStats memStats;
        boolean isBreakingPoint;
    }
    
    static class FailureRecord {
        String phaseName;
        String errorType;
        String errorMessage;
        long timestamp;
    }
}

