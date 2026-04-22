# Nodes Processed Tracking and Even Distribution Update

## Summary

Added tracking for number of nodes processed per chunk and spread approved nodes evenly across all chunks to ensure incremental searchability shows results after every chunk.

## Changes Made

### 1. AsyncIndexUpdate.java - Added Nodes Processed Getter

**Location:** `oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/AsyncIndexUpdate.java`

**Change:** Added new method to expose nodes processed count:

```java
/**
 * Gets the number of nodes processed (traversed) in the last run.
 * This count excludes nodes that were fully processed and skipped via PathTree optimization.
 * 
 * @return number of nodes processed, 0 if no run has completed
 */
public long getLastNodesProcessed() {
    return indexStats.getNodesRead();
}
```

**Why:** Provides visibility into how many nodes were actually processed per chunk, excluding fully processed nodes that were skipped via PathTree optimization.

### 2. ResumeIndexingPerfTest.java - Track and Log Nodes

**Location:** `oak-lucene/src/test/java/org/apache/jackrabbit/oak/plugins/index/lucene/resumeindexing/perf/ResumeIndexingPerfTest.java`

**Changes:**

#### 2a. Get Nodes Processed Per Cycle
```java
// Get nodes processed in this chunk
long nodesProcessed = ctx.asyncIndexUpdate.getLastNodesProcessed();
```

#### 2b. Include in Cycle Output
```java
System.out.println("  Cycle #" + cycleCount + " completed in " + cycleTime + 
    " ms (traversal: " + traversalTime + " ms, nodes: " + nodesProcessed + ")");
```

#### 2c. Include in CHUNK_RESULT Output
```java
System.out.println(String.format("CHUNK_RESULT: cycle=%d, results=%d, time=%d, nodes=%d, path=%s", 
    cycleCount, partialResults, queryTime, nodesProcessed, currentResumeState));
```

#### 2d. Include in CHUNK_METRICS Output
```java
System.out.println(String.format("CHUNK_METRICS: cycle=%d, nodes=%d, heap=%dMB, nonHeap=%dMB, gc=%d, gcTime=%dms, cpu=%dms, segStore=%dMB",
    cycleCount, nodesProcessed, ...));
```

### 3. ResumeIndexingPerfTest.java - Even Distribution of Approved Nodes

**Location:** Same file, `createContent` method

**Change:** Added logging and comment to clarify even distribution:

```java
System.out.println("Creating " + count + " nodes with " + QUERY_TARGET_COUNT + 
    " approved nodes (every " + approvedInterval + "th node for even distribution)");

// Spread approved nodes evenly - every Nth node
// This ensures each chunk will have approved nodes to find in incremental queries
if (i % approvedInterval == 0 && approvedCount < QUERY_TARGET_COUNT) {
    metadata.setProperty("dam:status", "approved");
    approvedCount++;
}
```

**Why:** Ensures that incremental queries after each chunk return results, demonstrating that the index is searchable after every commit.

### 4. compare_resume_perf.sh - Display Nodes Processed

**Location:** `oak-lucene/compare_resume_perf.sh`

**Changes:**

#### 4a. Update Per-Chunk Search Results Table (Table 16)
Added "Nodes" column to show how many nodes were processed:

```bash
printf "    %-7s | %-8s | %-9s | %-9s | %s\n" "Chunk" "Nodes" "Results" "Time(ms)" "Resume Path"
echo "    $(printf '%.0s-' {1..80})"
grep "CHUNK_RESULT:" "$FILE" | while read line; do
    local nodes=$(echo $line | sed 's/.*nodes=\([0-9]*\).*/\1/')
    printf "    %-7d | %-8s | %-9s | %-9s | %s\n" "$cycle" "$nodes" "$results" "$ctime" "$path"
done
```

#### 4b. Update Per-Chunk Detailed Metrics Table (Table 17)
Added "Nodes" column as the second column:

```bash
echo "    Chunk | Nodes | Heap(MB) | NonHeap(MB) | GC Count | GC Time(ms) | CPU(ms) | SegStore(MB)"
echo "    ------|-------|----------|-------------|----------|-------------|---------|-------------"
grep "CHUNK_METRICS:" "$FILE" | while read line; do
    local nodes=$(echo $line | sed 's/.*nodes=\([0-9]*\).*/\1/')
    printf "    %5d | %5d | %8d | %11d | %8d | %11d | %7d | %11d\n" \
           "$cycle" "$nodes" "$heap" "$nonheap" "$gc" "$gctime" "$cpu" "$segstore"
done
```

## Benefits

### 1. Visibility Into Chunk Processing
- **Nodes per chunk** shows exactly how much work was done in each cycle
- Helps identify if chunks are processing the expected number of nodes
- Useful for tuning `oak.async.chunkSize` parameter

### 2. Verify Even Distribution
- Confirms that approved nodes are spread evenly across all chunks
- Each chunk should find roughly the same proportion of approved nodes
- Demonstrates incremental searchability is working correctly

### 3. Performance Analysis
- Compare nodes processed vs. time taken to calculate throughput per chunk
- Identify if later chunks process nodes faster due to PathTree optimization
- Correlate nodes processed with memory/GC/CPU metrics

### 4. Debugging Aid
- If a chunk shows 0 results but processed many nodes, indicates a problem
- If nodes processed is much lower than chunk size, shows PathTree skipping is working
- Helps verify chunk limits are being respected

## Example Output

### Console Output
```
Cycle #2 completed in 1523 ms (traversal: 1450 ms, nodes: 2000)
  → Chunk limit reached - run completed and saved progress
  → Testing searchability after run completion...
CHUNK_RESULT: cycle=2, results=200, time=45, nodes=2000, path=/content/dam/asset-2000
CHUNK_METRICS: cycle=2, nodes=2000, heap=128MB, nonHeap=45MB, gc=3, gcTime=12ms, cpu=1800ms, segStore=25MB
```

### Table 16: Per-Chunk Search Results
```
  [Table 16] Per-Chunk Search Results (Query results grow as indexing progresses)
  -------------------------
    Chunk   | Nodes    | Results   | Time(ms)  | Resume Path
    --------------------------------------------------------------------------------
        1   | 2000     | 200       | 42        | /content/dam/asset-2000
        2   | 2000     | 400       | 45        | /content/dam/asset-4000
        3   | 2000     | 600       | 48        | /content/dam/asset-6000
        4   | 2000     | 800       | 51        | /content/dam/asset-8000
        5   | 2000     | 1000      | 54        | /content/dam/asset-10000
```

### Table 17: Per-Chunk Detailed Metrics
```
  [Table 17] Per-Chunk Detailed Metrics (System resources per chunk)
  ---------------------------
    Chunk | Nodes | Heap(MB) | NonHeap(MB) | GC Count | GC Time(ms) | CPU(ms) | SegStore(MB)
    ------|-------|----------|-------------|----------|-------------|---------|-------------
        1 |  2000 |      128 |          45 |        3 |          12 |    1800 |          25
        2 |  2000 |      135 |          46 |        5 |          18 |    1750 |          50
        3 |  1500 |      140 |          47 |        7 |          22 |    1400 |          75
        4 |  1200 |      142 |          48 |        8 |          25 |    1100 |          95
        5 |  1000 |      145 |          48 |        9 |          28 |     950 |         115
```

**Note:** Later chunks show fewer nodes processed due to PathTree optimization skipping already-processed nodes.

## Testing

Run the performance test:

```bash
cd oak-lucene
./compare_resume_perf.sh
```

Look for:
1. ✅ Nodes count in cycle output
2. ✅ Nodes column in Tables 16 and 17
3. ✅ Results growing steadily (not all 0 until final chunk)
4. ✅ Later chunks showing fewer nodes as PathTree optimization kicks in

## Related Features

- **PathTree Optimization**: Skips fully processed nodes, reducing nodes count in later chunks
- **Incremental Searchability**: Approved nodes spread evenly ensures results after each chunk
- **Chunk Limits**: Nodes count helps verify `oak.async.chunkSize` is working correctly
- **Performance Metrics**: Nodes count correlates with GC, memory, CPU usage

