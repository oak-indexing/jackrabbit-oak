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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;

/**
 * Monitors performance metrics during test execution.
 * 
 * <p>Tracks:
 * <ul>
 *   <li>Memory usage (heap, GC time)</li>
 *   <li>Content creation time</li>
 *   <li>Indexing time (per phase)</li>
 *   <li>Throughput</li>
 * </ul>
 * 
 * <p>Breaking point detection:
 * <ul>
 *   <li>GC time > 30% of total time = CRITICAL</li>
 *   <li>Throughput < 25% of baseline = CRITICAL</li>
 *   <li>OutOfMemoryError = FAILURE</li>
 * </ul>
 */
public class PerformanceMonitor {
    
    private static final Logger LOG = LoggerFactory.getLogger(PerformanceMonitor.class);
    
    // Breaking point thresholds
    private static final double GC_TIME_WARNING_THRESHOLD = 0.10;  // 10%
    private static final double GC_TIME_CRITICAL_THRESHOLD = 0.30; // 30%
    private static final double THROUGHPUT_CRITICAL_THRESHOLD = 0.25; // 25% of baseline
    
    private final MemoryMXBean memoryBean;
    private final List<GarbageCollectorMXBean> gcBeans;
    
    // Phase tracking
    private String currentPhase;
    private long phaseStartTime;
    private long phaseStartGcTime;
    private long phaseStartGcCount;
    
    // Metrics
    private long contentTime;
    private ChangeTrackingPerformanceTest.IndexingTimings indexingTimings;
    private Double baselineThroughput;
    
    public PerformanceMonitor() {
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    }
    
    // ========================================
    // Phase Management
    // ========================================
    
    public void startPhase(String phaseName) {
        this.currentPhase = phaseName;
        this.phaseStartTime = System.currentTimeMillis();
        this.phaseStartGcTime = getTotalGcTime();
        this.phaseStartGcCount = getTotalGcCount();
        
        LOG.debug("Phase started: {}", phaseName);
    }
    
    public void endPhase() {
        long phaseDuration = System.currentTimeMillis() - phaseStartTime;
        long phaseGcTime = getTotalGcTime() - phaseStartGcTime;
        long phaseGcCount = getTotalGcCount() - phaseStartGcCount;
        
        double gcPercent = (phaseDuration > 0) ? (phaseGcTime * 100.0 / phaseDuration) : 0;
        
        LOG.debug("Phase ended: {} (duration={}ms, GC={}ms [{}%], GC count={})",
                currentPhase, phaseDuration, phaseGcTime, String.format("%.1f", gcPercent), phaseGcCount);
        
        this.currentPhase = null;
    }
    
    // ========================================
    // Metric Recording
    // ========================================
    
    public void recordContentTime(long timeMs) {
        this.contentTime = timeMs;
    }
    
    public void recordIndexingTime(ChangeTrackingPerformanceTest.IndexingTimings timings) {
        this.indexingTimings = timings;
    }
    
    // ========================================
    // Memory Monitoring
    // ========================================
    
    public MemoryStats captureMemoryStats() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        
        long totalGcTime = getTotalGcTime();
        long totalGcCount = getTotalGcCount();
        
        // Calculate GC time percentage for current phase
        long phaseGcTime = totalGcTime - phaseStartGcTime;
        long phaseDuration = System.currentTimeMillis() - phaseStartTime;
        double gcTimePercent = (phaseDuration > 0) ? (phaseGcTime * 100.0 / phaseDuration) : 0;
        
        MemoryStats stats = new MemoryStats();
        stats.heapUsedMB = heapUsage.getUsed() / (1024 * 1024);
        stats.heapMaxMB = heapUsage.getMax() / (1024 * 1024);
        stats.heapCommittedMB = heapUsage.getCommitted() / (1024 * 1024);
        stats.heapUsedPercent = (stats.heapMaxMB > 0) ? (stats.heapUsedMB * 100.0 / stats.heapMaxMB) : 0;
        stats.gcTimeMs = totalGcTime;
        stats.gcCount = totalGcCount;
        stats.gcTimePercent = gcTimePercent;
        
        LOG.debug("Memory: heap={}/{}MB ({}%), GC={}ms [{}%], GC count={}",
                stats.heapUsedMB, stats.heapMaxMB, String.format("%.1f", stats.heapUsedPercent),
                stats.gcTimeMs, String.format("%.1f", stats.gcTimePercent), stats.gcCount);
        
        return stats;
    }
    
    // ========================================
    // Breaking Point Detection
    // ========================================
    
    public boolean isBreakingPoint(MemoryStats memStats, 
                                    ChangeTrackingPerformanceTest.IndexingTimings timings, 
                                    int assetCount) {
        boolean isBreaking = false;
        
        // Check 1: GC time percentage
        if (memStats.gcTimePercent > GC_TIME_CRITICAL_THRESHOLD * 100) {
            LOG.warn("BREAKING POINT: GC time {}% exceeds critical threshold {}%",
                    String.format("%.1f", memStats.gcTimePercent),
                    String.format("%.1f", GC_TIME_CRITICAL_THRESHOLD * 100));
            isBreaking = true;
        } else if (memStats.gcTimePercent > GC_TIME_WARNING_THRESHOLD * 100) {
            LOG.warn("WARNING: GC time {}% exceeds warning threshold {}%",
                    String.format("%.1f", memStats.gcTimePercent),
                    String.format("%.1f", GC_TIME_WARNING_THRESHOLD * 100));
        }
        
        // Check 2: Heap usage
        if (memStats.heapUsedPercent > 90) {
            LOG.warn("BREAKING POINT: Heap usage {}% is critically high",
                    String.format("%.1f", memStats.heapUsedPercent));
            isBreaking = true;
        } else if (memStats.heapUsedPercent > 80) {
            LOG.warn("WARNING: Heap usage {}% is high",
                    String.format("%.1f", memStats.heapUsedPercent));
        }
        
        // Check 3: Throughput degradation
        if (timings != null && assetCount > 0) {
            long totalTime = timings.getTotalTime();
            if (totalTime > 0) {
                double throughput = (assetCount * 1000.0) / totalTime;
                
                if (baselineThroughput == null) {
                    baselineThroughput = throughput;
                    LOG.debug("Baseline throughput: {} assets/sec", String.format("%.1f", throughput));
                } else {
                    double throughputRatio = throughput / baselineThroughput;
                    
                    if (throughputRatio < THROUGHPUT_CRITICAL_THRESHOLD) {
                        LOG.warn("BREAKING POINT: Throughput {} assets/sec is {}% of baseline (critical threshold: {}%)",
                                String.format("%.1f", throughput),
                                String.format("%.1f", throughputRatio * 100),
                                String.format("%.1f", THROUGHPUT_CRITICAL_THRESHOLD * 100));
                        isBreaking = true;
                    } else if (throughputRatio < 0.50) {
                        LOG.warn("WARNING: Throughput {} assets/sec is {}% of baseline",
                                String.format("%.1f", throughput),
                                String.format("%.1f", throughputRatio * 100));
                    }
                }
            }
        }
        
        return isBreaking;
    }
    
    // ========================================
    // Private Helper Methods
    // ========================================
    
    private long getTotalGcTime() {
        long total = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            long time = gcBean.getCollectionTime();
            if (time > 0) {
                total += time;
            }
        }
        return total;
    }
    
    private long getTotalGcCount() {
        long total = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            long count = gcBean.getCollectionCount();
            if (count > 0) {
                total += count;
            }
        }
        return total;
    }
}

/**
 * Memory statistics snapshot.
 */
class MemoryStats {
    long heapUsedMB;
    long heapMaxMB;
    long heapCommittedMB;
    double heapUsedPercent;
    long gcTimeMs;
    long gcCount;
    double gcTimePercent;
    
    @Override
    public String toString() {
        return String.format("Memory[heap=%d/%dMB (%.1f%%), GC=%dms (%.1f%%), count=%d]",
                heapUsedMB, heapMaxMB, heapUsedPercent, gcTimeMs, gcTimePercent, gcCount);
    }
}

