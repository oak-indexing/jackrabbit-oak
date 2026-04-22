# Performance Metrics Enhancement Plan

## Current Metrics (Already Captured)
- ✅ Total time
- ✅ Throughput (nodes/sec)
- ✅ Memory used (delta)
- ✅ Max heap used
- ✅ Max non-heap used
- ✅ Peak thread count
- ✅ Process CPU time
- ✅ Direct buffer memory
- ✅ Disk usage
- ✅ Main index size
- ✅ GC count
- ✅ GC time
- ✅ Run count
- ✅ Diff time
- ✅ Query time
- ✅ PathTree size (full vs slim)

## Additional Metrics to Add

### 1. Memory Metrics (Per-Chunk Tracking)
- **Heap memory per chunk**: Track heap usage at start and end of each chunk
- **Memory growth rate**: MB/second during indexing
- **Memory efficiency**: Bytes per indexed node
- **GC pressure**: Collections per chunk
- **Survivor space usage**: Young gen promotion rate
- **Memory pools breakdown**: Eden, Survivor, Old Gen, Metaspace

**Priority**: HIGH - Critical for understanding memory behavior over time

### 2. GC Metrics (Detailed Analysis)
- **GC overhead percentage**: (GC time / Total time) * 100
- **GC frequency**: Collections per second
- **Average GC pause**: Total GC time / GC count
- **GC efficiency**: Nodes indexed per GC pause
- **Young vs Old Gen collections**: Separate counts
- **GC pause distribution**: Min, max, median, p95, p99

**Priority**: HIGH - Essential for performance tuning

### 3. Disk Metrics (Detailed Breakdown)
- **SegmentStore size**: Track growth over chunks
- **Lucene index segments**: Number and size
- **Checkpoint overhead**: Size of checkpoint data
- **Disk I/O bytes**: Read/write separately (if available via JMX)
- **File descriptor count**: Open files during indexing
- **Index compaction benefit**: Before/after segment merge

**Priority**: MEDIUM - Important for understanding storage impact

### 4. CPU Metrics (Utilization Analysis)
- **CPU usage percentage**: Process CPU / available CPU
- **User vs System time**: Breakdown of CPU usage
- **CPU efficiency**: Nodes per CPU second
- **Thread utilization**: Active threads vs available cores
- **Context switches**: If available

**Priority**: MEDIUM - Helps identify CPU bottlenecks

### 5. Oak-Specific Metrics (Per-Chunk)
- **Nodes traversed per chunk**: Already partially tracked
- **Nodes skipped (PathTree optimization)**: Already tracked
- **Documents added to Lucene**: Per chunk
- **Properties indexed**: Count per chunk
- **Binary size indexed**: Total bytes of binaries
- **Checkpoint count**: Number created/released

**Priority**: HIGH - Oak-specific insights

### 6. Performance Breakdown (Time Analysis)
- **Diff time per chunk**: Already partially tracked
- **Editor time per chunk**: Time spent in editors
- **Lucene write time**: Document additions
- **Lucene commit time**: Per chunk
- **Lucene merge time**: Per chunk (already tracked)
- **NodeStore commit time**: Per chunk
- **PathTree operations time**: Serialization/deserialization (already tracked)
- **Resume overhead**: Time to reach resume point

**Priority**: HIGH - Critical for bottleneck analysis

### 7. Comparison Metrics (Normal vs Resume)
- **Overhead of resumable indexing**: Time difference
- **Memory overhead of PathTree**: Memory difference
- **Throughput comparison**: Nodes/sec ratio
- **GC impact**: Additional collections due to PathTree
- **Disk overhead**: Additional storage for PathTree
- **Break-even point**: Chunk size where resume becomes beneficial

**Priority**: HIGH - Key for decision making

### 8. Incremental Searchability Metrics
- **Time to first searchable result**: Already tracked
- **Search result growth rate**: Results per second
- **Query latency per chunk**: Already tracked
- **Index reader refresh time**: Time to see new data

**Priority**: MEDIUM - Validates incremental benefit

## Implementation Priority

### Phase 1 (Immediate - High Value, Low Effort)
1. **GC overhead percentage** - Simple calculation
2. **Memory per chunk tracking** - Add snapshot per chunk
3. **CPU usage percentage** - Calculate from CPU time
4. **Heap/Non-heap breakdown** - Use MemoryPoolMXBean
5. **SegmentStore size tracking** - Directory size per chunk

### Phase 2 (Near-term - High Value, Medium Effort)
1. **GC pause distribution** - Track individual GC events
2. **Young vs Old Gen collections** - Use GarbageCollectorMXBean
3. **Lucene segment analysis** - Parse index directory
4. **Memory efficiency** - Bytes per node calculation
5. **CPU efficiency** - Nodes per CPU second

### Phase 3 (Future - Medium Value, Higher Effort)
1. **Disk I/O bytes** - May need JMX or OS-specific APIs
2. **File descriptor count** - OS-specific
3. **Binary size indexed** - Requires tracking in indexing logic
4. **Context switches** - OS-specific
5. **Cache hit/miss rates** - Requires Oak cache instrumentation

## Output Format Enhancements

### Console Output
```
=== MEMORY ANALYSIS ===
Heap: 512 MB → 2.1 GB (growth: 1.6 GB, rate: 45 MB/s)
Non-Heap: 128 MB → 145 MB (growth: 17 MB)
Memory Efficiency: 850 bytes/node
GC: 45 collections (15 young, 3 old), 1.2s total (2.3% overhead)
Average GC pause: 27ms

=== CPU ANALYSIS ===
CPU Time: 12.5s (42% of wall time)
CPU Efficiency: 1,600 nodes/cpu-second
Threads: 8 → 23 (peak)

=== DISK ANALYSIS ===
SegmentStore: 45 MB → 156 MB (growth: 111 MB)
Lucene Index: 23 MB (14 segments)
PathTree: 2.3 MB (SLIM format, 98% savings)
```

### CSV Export (for Excel/Analysis)
```csv
Chunk,Time_ms,Nodes,Heap_MB,NonHeap_MB,GC_Count,GC_Time_ms,CPU_ms,Disk_MB,Query_Results
1,1250,1500,512,128,2,45,523,45,150
2,1180,1500,768,132,1,22,498,67,300
...
```

### JSON Export (for Programmatic Analysis)
```json
{
  "scenario": "SEGMENT_20000_RESUME_SLIM",
  "summary": {
    "total_time_ms": 16400,
    "throughput": 1213.5,
    "chunks": 13
  },
  "memory": {
    "heap_max_mb": 2048,
    "heap_used_mb": 1256,
    "gc_overhead_pct": 2.3
  },
  "chunks": [
    {
      "chunk_id": 1,
      "time_ms": 1250,
      "nodes": 1500,
      "memory_mb": 512,
      "gc_count": 2
    }
  ]
}
```

## Recommended Immediate Actions

1. **Add GC overhead percentage** to summary output
2. **Track heap memory per chunk** to detect leaks
3. **Add SegmentStore size** to disk metrics
4. **Calculate CPU efficiency** (nodes/cpu-second)
5. **Add memory pool breakdown** (Eden, Old Gen, etc.)
6. **Export metrics to CSV** for Excel analysis
7. **Add comparison summary** (Normal vs Resume overhead)

## Success Criteria

- All Phase 1 metrics implemented and tested
- CSV export working for all scenarios
- Comparison metrics showing clear overhead analysis
- Script parses and displays new metrics correctly
- Documentation updated with metric descriptions

