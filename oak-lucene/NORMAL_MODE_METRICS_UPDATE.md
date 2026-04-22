# Normal Mode Metrics Display Update

## Summary

Updated the performance test to display detailed metrics (GC, memory, CPU, disk) for **NORMAL mode** as well as RESUME mode. Previously, these metrics were only shown for multi-cycle (chunked) runs.

## Problem

The detailed metrics analysis section was inside an `if (cycleCount > 1)` condition block, which meant:
- ✅ **RESUME mode** (multiple chunks): Metrics displayed
- ❌ **NORMAL mode** (single cycle): Metrics NOT displayed

This made it difficult to compare the resource usage between NORMAL and RESUME modes.

## Solution

Moved the detailed metrics analysis outside the `if (cycleCount > 1)` block so it runs for all modes:

### Before
```java
if (cycleCount > 1) {
    // Per-cycle timing breakdown
    ...
    
    // Detailed metrics analysis - ONLY shown for multi-cycle
    System.out.println("DETAILED METRICS ANALYSIS");
    // GC Analysis
    // Memory Analysis
    // CPU Analysis
    // Disk Analysis
    ...
}
```

### After
```java
// Per-cycle timing breakdown (only for multi-cycle runs)
if (cycleCount > 1) {
    System.out.println("\n  Per-Cycle Timing Breakdown:");
    ...
}

// Detailed metrics analysis (runs for BOTH NORMAL and RESUME modes)
System.out.println("\n  DETAILED METRICS ANALYSIS");
System.out.println("  ===========================================");

// GC Analysis
double gcOverheadPct = (endGcTime - startGcTime) * 100.0 / totalIndexTime;
System.out.println("\n  GC Analysis:");
System.out.println(String.format("    Total GC Time: %d ms", endGcTime - startGcTime));
System.out.println(String.format("    Total GC Count: %d collections", endGcCount - startGcCount));
...

// Memory Analysis
System.out.println("\n  Memory Analysis:");
System.out.println(String.format("    Memory Delta: %d MB", memoryDelta / (1024 * 1024)));
...

// Memory pool breakdown
System.out.println("\n  Memory Pools:");
for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
    ...
}

// CPU Analysis
System.out.println("\n  CPU Analysis:");
System.out.println(String.format("    Total CPU Time: %.2f s", cpuDelta / 1_000_000_000.0));
...

// Disk Analysis
System.out.println("\n  Disk Analysis:");
System.out.println(String.format("    SegmentStore Size: %d MB", segmentStoreSize / (1024 * 1024)));
...
```

## What Gets Displayed Now

### For NORMAL Mode (1 cycle)
```
Indexing complete:
  Total time: 12345 ms
  Total cycles: 1

  ===========================================
  DETAILED METRICS ANALYSIS
  ===========================================

  GC Analysis:
    Total GC Time: 234 ms
    Total GC Count: 15 collections
    GC Overhead: 1.90% of total time
    Average GC Pause: 15.6 ms

  Memory Analysis:
    Memory Delta: 128 MB
    Memory Efficiency: 12800.0 bytes/node
    Peak Heap: 2048 MB

  Memory Pools:
    PS Eden Space        :   256 MB /  1024 MB
    PS Survivor Space    :    32 MB /   128 MB
    PS Old Gen           :   512 MB /  2048 MB

  CPU Analysis:
    Total CPU Time: 14.23 s
    CPU Utilization: 115.3% of wall time
    CPU Efficiency: 703 nodes/cpu-second
    Peak Threads: 12

  Disk Analysis:
    SegmentStore Size: 45 MB
    Lucene Index Size: 12 MB
    Total Disk Usage: 57 MB
```

### For RESUME Mode (multiple cycles)
Shows all the above metrics PLUS per-cycle timing breakdown:

```
Indexing complete:
  Total time: 15678 ms
  Total cycles: 5

  Per-Cycle Timing Breakdown:
    Cycle | Total(ms) | Trav(ms) | OH(ms) | Resume(ms) | ResOH(ms) | Path
    ------|-----------|----------|--------|------------|-----------|------
        1 |      3456 |     3200 |    256 |          - |         - | /content/dam/asset-2000
        2 |      3123 |     2900 |    223 |        150 |        50 | /content/dam/asset-4000
        ...

  ===========================================
  DETAILED METRICS ANALYSIS
  ===========================================
  
  GC Analysis:
    ...
  
  Memory Analysis:
    ...
  
  (same as NORMAL mode)
  
  Per-Chunk Metrics Summary:
    Heap Growth: 128 MB → 256 MB
    SegmentStore Growth: 25 MB → 125 MB
    Average GC per chunk: 3.2 collections
```

## Benefits

1. **Fair Comparison**: Can now compare resource usage between NORMAL and RESUME modes
2. **Complete Picture**: See GC, memory, CPU, disk metrics for all test runs
3. **Debugging**: Easier to identify resource issues in NORMAL mode
4. **Performance Analysis**: Understand if RESUME mode adds overhead in terms of GC/memory/CPU

## Files Modified

**Location:** `oak-lucene/src/test/java/org/apache/jackrabbit/oak/plugins/index/lucene/resumeindexing/perf/ResumeIndexingPerfTest.java`

**Changes:**
- Moved detailed metrics analysis outside `if (cycleCount > 1)` block
- Per-cycle timing breakdown remains inside the conditional (only for multi-cycle runs)
- All metrics sections now have proper indentation at the same level

## Metrics Displayed

### GC Analysis
- Total GC Time
- Total GC Count
- GC Overhead (% of total time)
- Average GC Pause

### Memory Analysis
- Memory Delta
- Memory Efficiency (bytes/node)
- Peak Heap
- Memory pool breakdown by name

### CPU Analysis
- Total CPU Time
- CPU Utilization (% of wall time)
- CPU Efficiency (nodes/cpu-second)
- Peak Threads

### Disk Analysis
- SegmentStore Size
- Lucene Index Size
- Total Disk Usage

### Per-Chunk Summary (RESUME mode only)
- Heap Growth
- SegmentStore Growth
- Average GC per chunk

## Testing

Run the performance test to see metrics for both modes:

```bash
cd oak-lucene
./compare_resume_perf.sh
```

Expected output:
- ✅ NORMAL mode shows "DETAILED METRICS ANALYSIS" section
- ✅ RESUME mode shows both per-cycle breakdown AND metrics analysis
- ✅ All modes show GC, memory, CPU, disk statistics
- ✅ Metrics are properly formatted and easy to compare

## Impact on Script Output

The `compare_resume_perf.sh` script already parses these metrics from the standard output:
- `GC Count:` - parsed for collated summary
- `GC Time:` - parsed for collated summary
- `Memory Delta:` - parsed for collated summary

Since these are already being output in the "Script Parseable Output" section (lines 138-139), the script will work correctly for both NORMAL and RESUME modes.

