# Indexing Restart Detection - Log Analysis & Splunk Queries

## Overview
This document identifies logs and metrics to detect when indexing was retriggered due to pod deletion/restart, and tracks how many nodes were reindexed.

---

## Key Log Patterns to Detect Indexing Restart/Retrigger

### 1. **Resume State Detection Logs**

#### When Pod Restarts with Existing Resume State:
```
[async-index-update-{lane}] Found resume state - checkpoint: {checkpoint}, path: {path}
```
- **Location**: `AsyncIndexUpdate.java:762`
- **Indicates**: Indexing is resuming from a previous interrupted run
- **Fields**: 
  - `{lane}`: async indexing lane name (e.g., "async", "fulltext-async")
  - `{checkpoint}`: checkpoint ID being used
  - `{path}`: last indexed path before interruption

#### When Resume Checkpoint is Missing (Full Retrigger):
```
[async-index-update-{lane}] Resume checkpoint {checkpoint} no longer exists, starting fresh
```
- **Location**: `AsyncIndexUpdate.java:765`
- **Indicates**: Pod restart caused loss of resume checkpoint, full reindex triggered
- **Impact**: All nodes need to be reindexed from scratch

#### When Resume Checkpoint Cannot Be Retrieved:
```
[async-index-update-{lane}] Unable to retrieve resume checkpoint {checkpoint}, creating new checkpoint
```
- **Location**: `AsyncIndexUpdate.java:785`
- **Indicates**: Resume failed, creating new checkpoint and starting over

### 2. **Indexing Mode Logs**

#### Resumable Indexing Enabled:
```
[async-index-update-{lane}] Resumable indexing enabled - chunkSize: {size}
```
- **Location**: `AsyncIndexUpdate.java:263`
- **Indicates**: System configured for resume capability

#### Chunk-Based Indexing Started:
```
[async-index-update-{lane}] Chunk-based indexing enabled - chunkSize: {count}, chunkTimeMs: {time}
```
- **Location**: `AsyncIndexUpdate.java:1073`
- **Indicates**: Indexing will proceed in chunks (incremental commits)

#### Resuming from Specific Path:
```
[async-index-update-{lane}] Resuming from path: {path} with PathTree ({count} indexed nodes)
```
- **Location**: `AsyncIndexUpdate.java:1170`
- **Indicates**: Continuing indexing from a saved resume point
- **Fields**: 
  - `{path}`: resume path
  - `{count}`: number of nodes already indexed in PathTree

### 3. **Chunk Progress Logs (Node Counting)**

#### Chunk Limit Reached - By Count:
```
[async-index-update-{lane}] Chunk limit reached (COUNT) at path: {path} (processed {count} NEW nodes)
```
- **Location**: `AsyncIndexUpdate.java:559`
- **Indicates**: Chunk completed based on node count threshold
- **Fields**: `{count}`: number of nodes processed in this chunk

#### Chunk Limit Reached - By Time:
```
[async-index-update-{lane}] Chunk limit reached (TIME) at path: {path} (elapsed {time}ms, processed {count} nodes)
```
- **Location**: `AsyncIndexUpdate.java:574`
- **Indicates**: Chunk completed based on time threshold
- **Fields**: 
  - `{time}`: elapsed time in milliseconds
  - `{count}`: nodes processed

#### Chunk Commit:
```
[async-index-update-{lane}] Chunk limit reached at path: {path} - committing and saving resume state
```
- **Location**: `AsyncIndexUpdate.java:1268`
- **Indicates**: Committing chunk and saving resume state

#### Index Incrementally Searchable:
```
[async-index-update-{lane}] Index committed and incrementally searchable up to: {path}
```
- **Location**: `AsyncIndexUpdate.java:1292`
- **Indicates**: Chunk committed successfully, index is searchable up to this path

### 4. **Indexing Completion Logs**

#### Reindexing Started:
```
Reindexing will be performed for following indexes: {indexes}
```
- **Location**: `IndexUpdate.java:354`
- **Indicates**: Reindexing has been triggered for listed indexes

#### Reindexing Progress (Every 100K nodes):
```
{mode} Traversed #{count} {path} [{rate} nodes/s, {rate_hr} nodes/hr] {estimate}
```
- **Location**: `IndexingProgressReporter.java:97`
- **Fields**:
  - `{mode}`: "Reindexing" or "Incremental indexing"
  - `{count}`: total nodes traversed so far
  - `{path}`: current path being indexed
  - `{rate}`: traversal rate per second
  - `{rate_hr}`: traversal rate per hour
  - `{estimate}`: estimated time remaining (if available)

#### Reindexing Complete:
```
[async-index-update-{lane}] Reindexing completed for indexes: {indexes} in {duration} ({ms} ms)
```
- **Location**: `AsyncIndexUpdate.java:1455`
- **Indicates**: Full reindexing completed for listed indexes
- **Fields**: 
  - `{indexes}`: list of reindexed index paths with node counts (e.g., "/oak:index/lucene*(1234567)")
  - `{duration}`: human-readable duration
  - `{ms}`: duration in milliseconds

#### Async Index Run Complete:
```
[async-index-update-{lane}] AsyncIndex update run completed in {duration}. Indexed {updates} nodes, {nodesRead}
```
- **Location**: `AsyncIndexUpdate.java:1474-1478`
- **Indicates**: Regular async indexing cycle completed
- **Fields**:
  - `{updates}`: number of index updates
  - `{nodesRead}`: number of nodes read/traversed

### 5. **Resume State Management**

#### Resume State Saved:
```
[async-index-update-{lane}] Saved resume state to :async/{lane}-resume: path={path}, checkpoint={cp}, tree={stats}
```
- **Location**: `AsyncIndexUpdate.java:1354`
- **Indicates**: Resume state persisted to repository
- **Fields**:
  - `{path}`: last indexed path
  - `{cp}`: checkpoint ID
  - `{stats}`: PathTree statistics

#### Resume State Cleared:
```
[async-index-update-{lane}] Cleared resume state after successful completion
```
- **Location**: `AsyncIndexUpdate.java:1447`
- **Indicates**: Indexing completed successfully, resume state no longer needed

### 6. **Checkpoint Management**

#### Checkpoint Created:
```
[async-index-update-{lane}] Created checkpoint {id} with creation time {time}
```
- **Location**: `AsyncCheckpointCreator.java:109` (if using checkpoint creator)

#### Orphaned Checkpoint Removed:
```
[async-index-update-{lane}] Removed orphaned checkpoint '{id}' {info}
```
- **Location**: `AsyncIndexUpdate.java:1000`
- **Indicates**: Cleanup of old checkpoints from previous runs

---

## Splunk Query Examples

### Query 1: Detect Pod Restart with Resume
```splunk
index=oak source="*async-index-update*" 
  ("Found resume state" OR "Resume checkpoint" OR "Resuming from path")
| rex field=_raw "\[(?<lane>[^\]]+)\].*checkpoint: (?<checkpoint>[^,]+), path: (?<resume_path>[^\s]+)"
| rex field=_raw "Resuming from path: (?<resume_path>[^\s]+) with PathTree \((?<indexed_nodes>\d+) indexed nodes\)"
| table _time, lane, checkpoint, resume_path, indexed_nodes
| sort -_time
```
**Purpose**: Identifies when indexing resumed after pod restart

### Query 2: Detect Full Retrigger (Lost Resume State)
```splunk
index=oak source="*async-index-update*" 
  ("Resume checkpoint * no longer exists, starting fresh" OR "Unable to retrieve resume checkpoint")
| rex field=_raw "\[(?<lane>[^\]]+)\]"
| table _time, lane, _raw
| sort -_time
```
**Purpose**: Identifies when resume state was lost and full reindex triggered

### Query 3: Track Nodes Indexed Per Chunk
```splunk
index=oak source="*async-index-update*" 
  ("Chunk limit reached" OR "processed * NEW nodes" OR "processed * nodes")
| rex field=_raw "\[(?<lane>[^\]]+)\].*Chunk limit reached.*at path: (?<path>[^\s]+).*processed (?<nodes_count>\d+) (?:NEW )?nodes"
| rex field=_raw "Chunk limit reached \((?<limit_type>COUNT|TIME)\)"
| table _time, lane, limit_type, nodes_count, path
| sort -_time
```
**Purpose**: Tracks number of nodes processed per chunk

### Query 4: Track Total Nodes Reindexed
```splunk
index=oak source="*async-index-update*" 
  "Reindexing completed for indexes"
| rex field=_raw "\[(?<lane>[^\]]+)\].*Reindexing completed for indexes: \[(?<indexes>[^\]]+)\] in (?<duration>[^\(]+) \((?<ms>\d+) ms\)"
| rex field=indexes "(?<index_path>/[^*]+)\*\((?<nodes_indexed>\d+)\)"
| table _time, lane, index_path, nodes_indexed, duration, ms
| sort -_time
```
**Purpose**: Captures total nodes indexed when reindexing completes

### Query 5: Reindexing Progress (Every 100K Nodes)
```splunk
index=oak source="*async-index-update*" "Traversed #"
| rex field=_raw "(?<mode>Reindexing|Incremental indexing) Traversed #(?<node_count>\d+) (?<path>[^\[]+) \[(?<rate>[^\]]+)\]"
| table _time, mode, node_count, path, rate
| sort -_time
```
**Purpose**: Tracks indexing progress in real-time (100K node intervals)

### Query 6: Complete Indexing Session Timeline
```splunk
index=oak source="*async-index-update*" 
  ("Found resume state" OR "starting fresh" OR "Resuming from path" OR 
   "Chunk limit reached" OR "Reindexing completed" OR "AsyncIndex update run completed")
| rex field=_raw "\[(?<lane>[^\]]+)\]"
| rex field=_raw "processed (?<chunk_nodes>\d+) (?:NEW )?nodes"
| rex field=_raw "Indexed (?<total_updates>\d+) nodes"
| rex field=_raw "nodes: (?<nodes_read>\d+)"
| eval event_type=case(
    match(_raw, "Found resume state"), "Resume Detected",
    match(_raw, "starting fresh"), "Full Retrigger",
    match(_raw, "Resuming from path"), "Resume Started",
    match(_raw, "Chunk limit reached"), "Chunk Complete",
    match(_raw, "Reindexing completed"), "Reindex Complete",
    match(_raw, "AsyncIndex update run completed"), "Run Complete"
  )
| table _time, lane, event_type, chunk_nodes, total_updates, nodes_read, _raw
| sort lane, _time
```
**Purpose**: Complete timeline of indexing session from restart to completion

### Query 7: Calculate Total Nodes Reindexed After Pod Restart
```splunk
index=oak source="*async-index-update*" 
  (("Found resume state" OR "starting fresh") OR "Chunk limit reached" OR "Reindexing completed")
| rex field=_raw "\[(?<lane>[^\]]+)\]"
| rex field=_raw "Found resume state - checkpoint: (?<start_checkpoint>[^,]+), path: (?<start_path>[^\s]+)"
| rex field=_raw "starting fresh"
| rex field=_raw "processed (?<chunk_nodes>\d+) (?:NEW )?nodes"
| rex field=_raw "/[^*]+\*\((?<total_nodes>\d+)\)"
| streamstats current=f last(start_checkpoint) as session_checkpoint by lane
| where isnotnull(session_checkpoint) OR match(_raw, "starting fresh")
| stats sum(chunk_nodes) as total_chunk_nodes, max(total_nodes) as final_total by lane, session_checkpoint
| eval nodes_reindexed=coalesce(final_total, total_chunk_nodes)
| table lane, session_checkpoint, nodes_reindexed, total_chunk_nodes, final_total
| sort -nodes_reindexed
```
**Purpose**: Aggregates total nodes reindexed per session after pod restart

### Query 8: Detect Indexing Interruption (Pod Killed Mid-Chunk)
```splunk
index=oak source="*async-index-update*" 
  ("Chunk limit reached" OR "Saved resume state")
| rex field=_raw "\[(?<lane>[^\]]+)\].*path[=:]\s*(?<last_path>[^,\s]+)"
| rex field=_raw "processed (?<nodes>\d+)"
| transaction lane maxpause=5m endswith=("Saved resume state")
| where eventcount > 1
| table _time, lane, last_path, nodes, duration
| sort -_time
```
**Purpose**: Identifies interrupted indexing sessions (pod killed before completion)

---

## Key Metrics to Monitor

### 1. **Node Count Metrics**
- **`nodesRead`**: Total nodes traversed/read during indexing
- **`updates`**: Total index updates performed
- **`chunk_nodes`**: Nodes processed per chunk
- **`indexed_nodes`**: Nodes already indexed (from PathTree)

### 2. **Timing Metrics**
- **Chunk commit time**: Time to commit each chunk
- **Resume time**: Time to reach resume point
- **Total indexing duration**: Full reindexing time

### 3. **Resume State Indicators**
- **Resume checkpoint**: Checkpoint ID being used
- **Resume path**: Last indexed path
- **PathTree size**: Number of nodes in PathTree (already indexed)

---

## Common Scenarios

### Scenario 1: Pod Restart During Indexing (Resume Works)
```
1. [Found resume state - checkpoint: abc123, path: /content/dam/images/file1000]
2. [Resuming from path: /content/dam/images/file1000 with PathTree (523450 indexed nodes)]
3. [Chunk limit reached at path: /content/dam/images/file2000 (processed 50000 NEW nodes)]
4. [Saved resume state to :async/async-resume: path=/content/dam/images/file2000, ...]
5. [Reindexing completed for indexes: [/oak:index/lucene*(573450)] in 45 min]
```
**Total Nodes Reindexed**: 573,450 nodes
**Nodes Already Indexed**: 523,450 nodes (skipped via PathTree)
**Nodes Processed After Resume**: 50,000 nodes

### Scenario 2: Pod Restart with Lost Resume State (Full Retrigger)
```
1. [Resume checkpoint abc123 no longer exists, starting fresh]
2. [Reindexing will be performed for following indexes: [/oak:index/lucene]]
3. [Reindexing Traversed #100000 /content/dam/images/file1 [156.2 nodes/s, ...]]
4. [Reindexing Traversed #200000 /content/dam/images/file2 [158.7 nodes/s, ...]]
5. [Reindexing completed for indexes: [/oak:index/lucene*(1234567)] in 3 hr 15 min]
```
**Total Nodes Reindexed**: 1,234,567 nodes (full reindex from scratch)

### Scenario 3: Multiple Chunks After Pod Restart
```
1. [Found resume state - checkpoint: xyz789, path: /content/projects/p1]
2. [Chunk limit reached (COUNT) at path: /content/projects/p2 (processed 100000 NEW nodes)]
3. [Index committed and incrementally searchable up to: /content/projects/p2]
4. [Chunk limit reached (COUNT) at path: /content/projects/p3 (processed 100000 NEW nodes)]
5. [Index committed and incrementally searchable up to: /content/projects/p3]
6. [Reindexing completed for indexes: [/oak:index/lucene*(825000)] in 2 hr 10 min]
```
**Total Nodes Reindexed**: 825,000 nodes
**Chunk 1**: 100,000 nodes
**Chunk 2**: 100,000 nodes
**Additional nodes**: 625,000 nodes

---

## Important Notes

1. **Node Count Accuracy**: The node count in "Reindexing completed" message shows TOTAL nodes indexed, not just new nodes after resume.

2. **PathTree Optimization**: When resuming, nodes already indexed (stored in PathTree) are skipped. The log "processed X NEW nodes" reflects only newly processed nodes.

3. **Chunk Searchability**: Each chunk commit makes the index incrementally searchable, so even if pod restarts mid-indexing, partial results are available.

4. **Resume State Persistence**: Resume state is stored in `/:async/{lane}-resume` node with:
   - Last indexed path
   - Checkpoint ID
   - PathTree (serialized structure of indexed nodes)

5. **Checkpoint Lifecycle**: Checkpoints are cleaned up after indexing completes or after lease timeout expires.

---

## Debug Flags in Code

The following debug outputs are available (via System.out.println):
- `[DEBUG-MODE]`: Indexing mode information
- `[DEBUG-CHUNK]`: Chunk limit details
- `[DEBUG-RESUME]`: Resume operation details
- `[DEBUG-TIMING]`: Performance timing breakdowns
- `[DEBUG-PATHTREE-TRAVERSAL]`: PathTree traversal statistics

These can be enabled by looking for System.out.println statements in:
- `AsyncIndexUpdate.java`
- `PathTreeEditorDiff.java`
- `DirectResumeDiff.java`

---

## System Properties

Relevant system properties for resume indexing:
- `oak.async.resume`: Enable resume indexing (boolean)
- `oak.async.chunkTimeMs`: Time-based chunk limit in milliseconds (long)
- Chunk size: Configured via constructor parameter

---

## References

- **AsyncIndexUpdate.java**: Main async indexing controller
- **IndexUpdate.java**: Index update logic with reindex detection
- **IndexingProgressReporter.java**: Progress tracking and logging
- **PathTree.java**: Resume state tree structure
- **AsyncIndexStats**: JMX statistics and metrics

