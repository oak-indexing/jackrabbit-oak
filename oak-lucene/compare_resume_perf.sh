#!/bin/bash
#
# ===============================================================================
# RESUME INDEXING PERFORMANCE COMPARISON SCRIPT
# ===============================================================================
#
# This script compares three async indexing modes:
#   1. Traditional (trad)     - No resume, baseline performance
#   2. Suspend/Resume (susp)  - Exits diff on limit, re-enters from root
#   3. Continuous (cont)      - Single traversal, progress checkpoints
#
# ===============================================================================
# USAGE
# ===============================================================================
#
#   chmod +x compare_resume_perf.sh
#   ./compare_resume_perf.sh
#
# Results saved to:
#   - perf_resume_results.txt           (raw output)
#   - perf_resume_summary.txt           (performance table)
#   - perf_resume_summary.txt.resources (resource metrics table)
#
# ===============================================================================
# PROCESSING MODES EXPLAINED
# ===============================================================================
#
# trad (Traditional)
#   - No resume capability
#   - Single run to completion
#   - Baseline performance
#   - Use: Production systems without crash recovery needs
#
# susp (Suspend/Resume)
#   - Exits diff traversal on chunk/time limit
#   - Saves resume state to /:async/<lane>
#   - Re-enters diff from root on next run()
#   - ~5-10% overhead due to repeated diff traversal
#   - Use: Development/testing crash recovery
#
# cont (Continuous) ✨ RECOMMENDED
#   - Stays in same diff traversal
#   - Logs progress checkpoints without exiting
#   - Completes entire changeset in single pass
#   - ~0% overhead (same as traditional)
#   - Use: Production with progress monitoring
#
# ===============================================================================
# TABLE 1: PERFORMANCE METRICS
# ===============================================================================
#
# JVM        - JVM heap configuration (e.g., -Xmx4G)
# Store      - NodeStore type: SEGMENT, DOCUMENT (MongoDB), MEMORY
# Nodes      - Total content nodes created for indexing
# Chunk      - Chunk size limit (-1 = disabled, uses time limit)
# TLim       - Time limit in seconds before checkpoint/suspend
# Mode       - trad/susp/cont (see above)
# Time(s)    - Total indexing time in seconds (wall clock)
# Diff(s)    - Total time in EditorDiff.process() across all runs
# Skip(s)    - Total time in ResumingEditor fast-forward (susp only)
# Create(s)  - Content creation time in seconds
# Throughput - Nodes indexed per second
# Runs       - Number of asyncIndexUpdate.run() calls
# Resm       - Number of resume cycles (susp mode only)
# AvgDiff    - Average diff time per run (milliseconds)
# AvgSkip    - Average skip time per resume (milliseconds, susp only)
# Skipped    - Total nodes skipped across resumes (susp only)
#
# TIME BREAKDOWN FORMULA:
#   Total Time = Diff + Indexing + Skip + Merge + Other
#
#   Where:
#     Diff(s)       = Traversing repository to find changes (EditorDiff.process)
#     Skip(s)       = Fast-forwarding to resume point (susp mode only)
#     Indexing      = Creating Lucene documents, writing to index
#     Merge         = NodeStore.merge() committing index updates
#     Other         = Checkpoint creation, stats collection, logging
#
# EXAMPLE COMPARISON (500K nodes, 5s time limit):
#
#   Mode  | Time(s) | Diff(s) | Skip(s) | Analysis
#   ------|---------|---------|---------|----------------------------------------
#   trad  |   36.7s |   16.0s |       - | Diff = 44% of total time
#   susp  |   38.8s |   31.2s |    1.5s | Diff DOUBLED (re-entry overhead)
#   cont  |   36.9s |   16.1s |       - | Diff same as trad (single traversal)
#
# KEY INSIGHTS:
#   1. Suspend/Resume mode DOUBLES diff time due to re-entering from root
#   2. Skip overhead is minimal (~1.5s for 500K nodes)
#   3. Continuous mode avoids diff doubling by staying in traversal
#   4. This explains ~5% overhead in susp vs ~0% in cont
#
# ===============================================================================
# TABLE 2: RESOURCE/COMPUTE METRICS
# ===============================================================================
#
# Heap(MB)   - Maximum heap memory used (MB)
# CPU(ms)    - Process CPU time consumed (milliseconds)
# GC#        - Number of garbage collection cycles
# GC(ms)     - Total time spent in garbage collection (ms)
# Disk(KB)   - Total NodeStore disk usage (KB)
# Idx(KB)    - Lucene index size on disk (KB)
# Threads    - Peak thread count
# Query(ms)  - Verification query execution time (ms)
#
# ===============================================================================
# ADDITIONAL METRICS (in raw output, not in tables)
# ===============================================================================
#
# Memory Delta    - Net heap memory change during indexing
# Non-Heap Used   - Metaspace, code cache, JIT compiler memory
# Direct Buffer   - Off-heap direct memory buffer usage
#
# ===============================================================================
# METRICS WE COULD/SHOULD ADD
# ===============================================================================
#
# Metric               | Why Useful                    | How to Capture
# ---------------------|-------------------------------|---------------------------
# Index Write Time     | Separate Lucene from traversal| Time FulltextIndexEditor
# Documents Indexed    | Index size vs nodes traversed | Counter in LuceneDocumentMaker
# Commit Latency       | Identify slow merges          | Time each merge() call
# Checkpoint Time      | Checkpoint creation overhead  | Time store.checkpoint()
# Index Segment Count  | Lucene merge efficiency       | Query IndexReader.leaves()
# File Handles         | Resource exhaustion risk      | OperatingSystemMXBean
# I/O Read/Write Bytes | Disk throughput               | sun.management APIs
# Compaction Stats     | For Segment store             | SegmentGCOptions
# MongoDB Op Time      | For Document store            | MongoDB profiler
# Lock Contention      | Threading efficiency          | ThreadMXBean
#
# ===============================================================================
# QUICK START EXAMPLES
# ===============================================================================
#
# Run full comparison test:
#   cd oak-lucene
#   ./compare_resume_perf.sh
#
# Custom single test (Traditional mode):
#   java -Xmx4g \
#        -Dperf.nodeStore=SEGMENT \
#        -Dperf.nodeCount=100000 \
#        -Dperf.useResume=false \
#        -cp "$CP" org.junit.runner.JUnitCore \
#        org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing.perf.ResumeIndexingPerfTest
#
# Custom single test (Continuous mode - RECOMMENDED):
#   java -Xmx4g \
#        -Dperf.nodeStore=SEGMENT \
#        -Dperf.nodeCount=100000 \
#        -Dperf.useResume=true \
#        -Doak.async.timeLimitMs=5000 \
#        -Doak.async.continuousMode=true \
#        -cp "$CP" org.junit.runner.JUnitCore \
#        org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing.perf.ResumeIndexingPerfTest
#
# ===============================================================================

cd "$(dirname "$0")"

OUTPUT_FILE="perf_resume_results.txt"
SUMMARY_FILE="perf_resume_summary.txt"

echo "================================================================================"
echo "RESUME INDEXING PERFORMANCE COMPARISON TEST"
echo "================================================================================"
echo ""

# Clean previous results
rm -f "$OUTPUT_FILE"
rm -f "$SUMMARY_FILE"
rm -f target/RESUME_INDEXING_PERF_RESULTS.md
rm -f target/resume_indexing_perf_data.csv

# Compile test classes (bypassing bundle plugin)
echo "Compiling test classes..."
mvn compiler:compile compiler:testCompile -Denforcer.skip=true -q 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Compilation failed. Trying full compile..."
    cd ..
    mvn clean install -DskipTests -pl oak-lucene -am -q 2>/dev/null
    cd oak-lucene
fi
echo "Compilation complete."
echo ""

# Define Scenarios: "STORE NODES CHUNK TIMELIMIT RESUME CONTINUOUS"
# Format: NodeStore NodeCount ChunkSize TimeLimitSeconds UseResume ContinuousMode
# ContinuousMode: true = single traversal with progress checkpoints (no diff overhead)
#                 false = traditional suspend/resume (re-enters diff on each chunk)
SCENARIOS=(
    # === 100K Quick Tests ===
    # "SEGMENT 100000 -1 1 false false"     # Traditional (baseline)
    # "SEGMENT 100000 -1 1 true false"      # Resume (suspend/re-enter)
    # "SEGMENT 100000 -1 1 true true"       # Continuous (single traversal)
    
    # === 500K Comparison Tests ===
    "SEGMENT 500000 -1 5 false false"      # Traditional (baseline)
    "SEGMENT 500000 -1 5 true false"       # Resume (suspend/re-enter)
    "SEGMENT 500000 -1 5 true true"        # Continuous (single traversal) - NEW!
    
    # === MongoDB Tests (requires MongoDB running) ===
    # "DOCUMENT 50000 -1 5 false false"
    # "DOCUMENT 50000 -1 5 true false"
    # "DOCUMENT 50000 -1 5 true true"

    # === Major Tests (1M nodes) ===
    # "SEGMENT 1000000 -1 5 false false"
    # "SEGMENT 1000000 -1 5 true false"
    # "SEGMENT 1000000 -1 5 true true"
)

# JVM Configurations
JVM_CONFIGS=(
    # "-Xmx1G -Xms1G"
    # "-Xmx2G -Xms2G"
    "-Xmx4G -Xms4G"
)

# Get classpath from Maven
echo "Building classpath..."
CP="target/classes:target/test-classes"
DEPS=$(cd .. && mvn -pl oak-lucene dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)
if [ -n "$DEPS" ]; then
    CP="$CP:$DEPS"
    echo "✓ Maven classpath obtained"
else
    echo "Using fallback classpath..."
fi
echo ""

run_single_scenario() {
    local STORE=$1
    local NODES=$2
    local CHUNK=$3
    local TIMELIMIT=$4
    local RESUME=$5
    local CONTINUOUS=${6:-false}  # Continuous mode (optional, default false)
    local MEM_CONFIG=$7
    local SCENARIO_NAME="${STORE}_${NODES}_${CHUNK}_T${TIMELIMIT}s_R${RESUME}_C${CONTINUOUS}_MEM${MEM_CONFIG// /-}"
    
    # Determine mode name for display
    local MODE_NAME=""
    if [ "$RESUME" == "false" ]; then
        MODE_NAME="TRADITIONAL"
    elif [ "$CONTINUOUS" == "true" ]; then
        MODE_NAME="CONTINUOUS"
    else
        MODE_NAME="RESUME"
    fi
    
    echo "--------------------------------------------------------------------------------"
    echo "Running: Store=$STORE, Nodes=$NODES, Chunk=$CHUNK, TimeLimit=${TIMELIMIT}s, Mode=$MODE_NAME, JVM=$MEM_CONFIG"
    echo "--------------------------------------------------------------------------------"
    
    # Convert seconds to milliseconds for oak.async.timeLimitMs
    local TIMELIMIT_MS=$((TIMELIMIT * 1000))
    
    # Run test using JUnit directly (bypasses Maven bundle plugin)
    java $MEM_CONFIG \
         -Dperf.nodeStore=$STORE \
         -Dperf.nodeCount=$NODES \
         -Dperf.chunkSize=$CHUNK \
         -Dperf.timeLimitMs=$TIMELIMIT_MS \
         -Dperf.useResume=$RESUME \
         -Doak.async.chunkSize=$CHUNK \
         -Doak.async.timeLimitMs=$TIMELIMIT_MS \
         -Doak.async.continuousMode=$CONTINUOUS \
         -Djava.awt.headless=true \
         -cp "$CP" \
         org.junit.runner.JUnitCore \
         org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing.perf.ResumeIndexingPerfTest > "$SCENARIO_NAME.out" 2>&1
    
    # Append to main output file
    echo "### SCENARIO: $SCENARIO_NAME ###" >> "$OUTPUT_FILE"
    cat "$SCENARIO_NAME.out" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    # Parse and print stats for this run
    print_stats_from_file "$SCENARIO_NAME.out" "$STORE" "$NODES" "$CHUNK" "$TIMELIMIT" "$RESUME" "$CONTINUOUS" "$MEM_CONFIG"
    
    rm -f "$SCENARIO_NAME.out"
}

print_stats_from_file() {
    local FILE=$1
    local STORE=$2
    local NODES=$3
    local CHUNK=$4
    local TIMELIMIT=$5
    local RESUME=$6
    local CONTINUOUS=$7
    local MEM_CONFIG=$8
    
    local TIME=""
    local THROUGHPUT=""
    local RUN_COUNT=""
    local RESUME_COUNT=""
    local GC_TIME=""
    local GC_COUNT=""
    local MAX_HEAP=""
    local MAX_NON_HEAP=""
    local PEAK_THREADS=""
    local DISK_USAGE=""
    local INDEX_SIZE=""
    local CPU_TIME=""
    local DIRECT_BUFFER=""
    local QUERY_TIME=""
    local AVG_DIFF_TIME=""
    local AVG_SKIP_TIME=""
    local TOTAL_SKIPPED=""
    local CONTENT_TIME=""
    
    # Determine mode for display
    local MODE=""
    if [ "$RESUME" == "false" ]; then
        MODE="trad"
    elif [ "$CONTINUOUS" == "true" ]; then
        MODE="cont"
    else
        MODE="susp"
    fi
    
    # Parse standard metrics from test output
    while IFS= read -r line; do
        if [[ $line == "Total Time:"* ]]; then TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Throughput:"* ]]; then THROUGHPUT=$(echo $line | awk '{print $2}'); fi
        if [[ $line == "Run Count:"* ]]; then RUN_COUNT=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Resume Count:"* ]]; then RESUME_COUNT=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "GC Count:"* ]]; then GC_COUNT=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "GC Time:"* ]]; then GC_TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Max Heap Used:"* ]]; then MAX_HEAP=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Max Non-Heap Used:"* ]]; then MAX_NON_HEAP=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Peak Threads:"* ]]; then PEAK_THREADS=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Disk Usage:"* ]]; then DISK_USAGE=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Main Index Size:"* ]]; then INDEX_SIZE=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Process CPU Time:"* ]]; then CPU_TIME=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Direct Buffer Memory:"* ]]; then DIRECT_BUFFER=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Query Time:"* ]]; then QUERY_TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Content creation:"* ]]; then CONTENT_TIME=$(echo $line | awk '{print $3}'); fi
    done < "$FILE"
    
    # Parse diff and resume timing from output
    local DIFF_COUNT=0
    local DIFF_TOTAL=0
    local SKIP_COUNT=0
    local SKIP_TOTAL=0
    local SKIPPED_TOTAL=0
    
    while IFS= read -r line; do
        # Parse: [DIFF-TIME] async diff completed/suspended in XXX ms
        if [[ $line == *"[DIFF-TIME]"*" ms"* ]]; then
            local ms=$(echo "$line" | grep -oE 'in [0-9]+ ms|after [0-9]+ ms' | grep -oE '[0-9]+')
            if [ -n "$ms" ]; then
                DIFF_COUNT=$((DIFF_COUNT + 1))
                DIFF_TOTAL=$((DIFF_TOTAL + ms))
            fi
        fi
        # Parse: [RESUME-STATS] Skipped XXX nodes, time to resume point: YYY ms
        if [[ $line == *"[RESUME-STATS]"* ]]; then
            local skipped=$(echo "$line" | grep -oE 'Skipped [0-9]+' | grep -oE '[0-9]+')
            local skip_time=$(echo "$line" | grep -oE 'time to resume point: [0-9]+' | grep -oE '[0-9]+')
            if [ -n "$skipped" ]; then
                SKIPPED_TOTAL=$((SKIPPED_TOTAL + skipped))
            fi
            if [ -n "$skip_time" ]; then
                SKIP_COUNT=$((SKIP_COUNT + 1))
                SKIP_TOTAL=$((SKIP_TOTAL + skip_time))
            fi
        fi
    done < "$FILE"
    
    # Calculate averages and totals
    local TOTAL_DIFF_SECONDS=""
    local TOTAL_SKIP_SECONDS=""
    
    if [ "$DIFF_COUNT" -gt 0 ] 2>/dev/null; then
        AVG_DIFF_TIME=$((DIFF_TOTAL / DIFF_COUNT))
        # Convert total diff time from ms to seconds
        TOTAL_DIFF_SECONDS=$(echo "scale=1; $DIFF_TOTAL / 1000" | bc 2>/dev/null || echo "0")
    fi
    if [ "$SKIP_COUNT" -gt 0 ] 2>/dev/null; then
        AVG_SKIP_TIME=$((SKIP_TOTAL / SKIP_COUNT))
        # Convert total skip time from ms to seconds
        TOTAL_SKIP_SECONDS=$(echo "scale=1; $SKIP_TOTAL / 1000" | bc 2>/dev/null || echo "0")
    fi
    TOTAL_SKIPPED=$SKIPPED_TOTAL
    
    # For non-resume mode, skip metrics should be N/A
    if [ "$RESUME" == "false" ]; then
        AVG_SKIP_TIME="N/A"
        TOTAL_SKIPPED="N/A"
        TOTAL_SKIP_SECONDS="N/A"
    fi
    
    # For continuous mode, skip metrics should be N/A
    if [ "$CONTINUOUS" == "true" ]; then
        AVG_SKIP_TIME="N/A"
        TOTAL_SKIPPED="N/A"
        TOTAL_SKIP_SECONDS="N/A"
    fi
    
    # Convert time from ms to seconds
    local TIME_SECONDS=""
    if [ -n "$TIME" ] && [ "$TIME" -gt 0 ] 2>/dev/null; then
        TIME_SECONDS=$(echo "scale=1; $TIME / 1000" | bc 2>/dev/null || echo "$TIME")
    fi
    
    local CONTENT_SECONDS=""
    if [ -n "$CONTENT_TIME" ] && [ "$CONTENT_TIME" -gt 0 ] 2>/dev/null; then
        CONTENT_SECONDS=$(echo "scale=1; $CONTENT_TIME / 1000" | bc 2>/dev/null || echo "$CONTENT_TIME")
    fi
    
    local MEM_DISPLAY=$(echo $MEM_CONFIG | awk '{print $1}')
    
    # Calculate accounting time: Time + Diff + Skip overhead
    # This helps identify where time is spent
    local TOTAL_ACCOUNTED=""
    if [ -n "$TIME" ] && [ "$TIME" -gt 0 ] 2>/dev/null; then
        local DIFF_MS=${DIFF_TOTAL:-0}
        local SKIP_MS=${SKIP_TOTAL:-0}
        # Note: TIME already includes diff and indexing, so this is informational
        # to show the breakdown components
        TOTAL_ACCOUNTED=$(echo "scale=1; ($TIME + $DIFF_MS + $SKIP_MS) / 1000" | bc 2>/dev/null || echo "N/A")
    fi
    
    # Format skip metrics (use empty string for N/A to align properly)
    local SKIP_DISPLAY="${AVG_SKIP_TIME:-0}"
    local SKIPPED_DISPLAY="${TOTAL_SKIPPED:-0}"
    local TOTAL_SKIP_DISPLAY="${TOTAL_SKIP_SECONDS:-0}"
    if [ "$RESUME" == "false" ] || [ "$CONTINUOUS" == "true" ]; then
        # No skip stats for traditional mode or continuous mode
        SKIP_DISPLAY="-"
        SKIPPED_DISPLAY="-"
        TOTAL_SKIP_DISPLAY="-"
    fi
    
    # Table 1: Performance Metrics (with breakdown)
    printf "%-8s | %-8s | %-8s | %-6s | %-6s | %-6s | %8ss | %8ss | %8ss | %8ss | %10s | %-5s | %-5s | %-8s | %-8s | %-8s\n" \
           "$MEM_DISPLAY" "$STORE" "$NODES" "$CHUNK" "$TIMELIMIT" "$MODE" "$TIME_SECONDS" "${TOTAL_DIFF_SECONDS:-0}" "$TOTAL_SKIP_DISPLAY" "${CONTENT_SECONDS:-N/A}" "$THROUGHPUT" "$RUN_COUNT" "$RESUME_COUNT" "${AVG_DIFF_TIME:-0}" "$SKIP_DISPLAY" "$SKIPPED_DISPLAY" | tee -a "$SUMMARY_FILE"
    
    # Table 2: Resource Metrics (append to resource summary)
    printf "%-8s | %-8s | %-8s | %-6s | %8s | %8s | %6s | %8s | %10s | %10s | %8s | %8s\n" \
           "$MEM_DISPLAY" "$STORE" "$NODES" "$MODE" "${MAX_HEAP:-0}" "${CPU_TIME:-0}" "${GC_COUNT:-0}" "${GC_TIME:-0}" "${DISK_USAGE:-0}" "${INDEX_SIZE:-0}" "${PEAK_THREADS:-0}" "${QUERY_TIME:-0}" >> "${SUMMARY_FILE}.resources"
}

# Print header
echo ""
echo "================================================================================"
echo "RESULTS"
echo "================================================================================"
echo ""
echo "TABLE 1 - PERFORMANCE METRICS:"
echo "  JVM        - JVM heap configuration (e.g., -Xmx4G)"
echo "  Store      - NodeStore type: SEGMENT, DOCUMENT (MongoDB), or MEMORY"
echo "  Nodes      - Total number of content nodes created for indexing"
echo "  Chunk      - Chunk size limit (-1 = disabled, uses time limit instead)"
echo "  TLim       - Time limit in seconds before checkpoint/suspend"
echo "  Mode       - Processing mode:"
echo "                 trad = Traditional (no resume capability)"
echo "                 susp = Suspend/Resume (exits diff, re-enters from root)"
echo "                 cont = Continuous (single traversal, no overhead)"
echo "  Time(s)    - Total indexing time in seconds (includes all below)"
echo "  Diff(s)    - Total time spent in EditorDiff.process() across all runs"
echo "  Skip(s)    - Total time spent in ResumingEditor skipping (susp mode only)"
echo "  Create(s)  - Content creation time in seconds"
echo "  Throughput - Nodes indexed per second"
echo "  Runs       - Number of asyncIndexUpdate.run() calls"
echo "  Resm       - Number of resume cycles (suspend/resume mode only)"
echo "  AvgDiff    - Average time (ms) spent in EditorDiff.process() per run"
echo "  AvgSkip    - Average time (ms) for ResumingEditor to skip to resume point"
echo "  Skipped    - Total nodes skipped across all resume operations"
echo ""
echo "TABLE 2 - RESOURCE/COMPUTE METRICS:"
echo "  Heap(MB)   - Maximum heap memory used (MB)"
echo "  CPU(ms)    - Process CPU time consumed (milliseconds)"
echo "  GC#        - Number of garbage collection cycles"
echo "  GC(ms)     - Total time spent in garbage collection (ms)"
echo "  Disk(KB)   - Total NodeStore disk usage (KB)"
echo "  Idx(KB)    - Lucene index size on disk (KB)"
echo ""
echo "ADDITIONAL METRICS (in raw output file):"
echo "  Memory Delta    - Net heap memory change during indexing"
echo "  Non-Heap Used   - Metaspace, code cache, JIT compiler memory"
echo "  Peak Threads    - Maximum concurrent thread count"
echo "  Direct Buffer   - Off-heap direct memory buffer usage"
echo "  Query Time      - Time to execute verification query"
echo ""

echo "TABLE 1: PERFORMANCE"
printf "%-8s | %-8s | %-8s | %-6s | %-6s | %-6s | %9s | %9s | %9s | %9s | %10s | %-5s | %-5s | %-8s | %-8s | %-8s\n" \
       "JVM" "Store" "Nodes" "Chunk" "TLim" "Mode" "Time(s)" "Diff(s)" "Skip(s)" "Create(s)" "Throughput" "Runs" "Resm" "AvgDiff" "AvgSkip" "Skipped" | tee -a "$SUMMARY_FILE"
echo "---------|----------|----------|--------|--------|--------|-----------|-----------|-----------|-----------|------------|-------|-------|----------|----------|----------" | tee -a "$SUMMARY_FILE"

# Initialize resource summary file with header
rm -f "${SUMMARY_FILE}.resources"
printf "%-8s | %-8s | %-8s | %-6s | %8s | %8s | %6s | %8s | %10s | %10s | %8s | %8s\n" \
       "JVM" "Store" "Nodes" "Mode" "Heap(MB)" "CPU(ms)" "GC#" "GC(ms)" "Disk(KB)" "Idx(KB)" "Threads" "Query(ms)" >> "${SUMMARY_FILE}.resources"
echo "---------|----------|----------|--------|----------|----------|--------|----------|------------|------------|----------|----------" >> "${SUMMARY_FILE}.resources"

# Loop through JVM configs and scenarios
for jvm_opts in "${JVM_CONFIGS[@]}"; do
    for scenario in "${SCENARIOS[@]}"; do
        read -r STORE NODES CHUNK TIMELIMIT RESUME CONTINUOUS <<< "$scenario"
        run_single_scenario "$STORE" "$NODES" "$CHUNK" "$TIMELIMIT" "$RESUME" "$CONTINUOUS" "$jvm_opts"
    done
done

# Print Resource/Compute Metrics Table
echo ""
echo "TABLE 2: RESOURCE/COMPUTE METRICS"
cat "${SUMMARY_FILE}.resources"
rm -f "${SUMMARY_FILE}.resources"

echo ""
echo "================================================================================"
echo "SPEEDUP ANALYSIS: Traditional vs Resume Indexing"
echo "================================================================================"

# Calculate speedup between traditional and resume runs (bash 3.x compatible)
TEMP_TRAD="/tmp/resume_perf_trad_$$"
TEMP_RESUME="/tmp/resume_perf_resume_$$"
rm -f "$TEMP_TRAD" "$TEMP_RESUME"
touch "$TEMP_TRAD" "$TEMP_RESUME"

while IFS= read -r line; do
    if [[ $line == *"|"* ]] && [[ $line != *"JVM"* ]] && [[ $line != *"---"* ]]; then
        STORE=$(echo $line | awk -F'|' '{gsub(/ /,"",$2); print $2}')
        NODES=$(echo $line | awk -F'|' '{gsub(/ /,"",$3); print $3}')
        CHUNK=$(echo $line | awk -F'|' '{gsub(/ /,"",$4); print $4}')
        TIMELIMIT=$(echo $line | awk -F'|' '{gsub(/ /,"",$5); print $5}')
        RESUME=$(echo $line | awk -F'|' '{gsub(/ /,"",$6); print $6}')
        TIME=$(echo $line | awk -F'|' '{gsub(/ /,"",$7); print $7}')
        
        KEY="${STORE}_${NODES}_${CHUNK}_T${TIMELIMIT}"
        
        if [ "$RESUME" == "false" ]; then
            echo "$KEY $TIME" >> "$TEMP_TRAD"
        else
            echo "$KEY $TIME" >> "$TEMP_RESUME"
        fi
    fi
done < "$SUMMARY_FILE"

echo ""
printf "%-35s | %-12s | %-12s | %-10s\n" "Scenario" "Traditional" "Resume" "Speedup"
echo "--------------------------------------|--------------|--------------|----------"

# Match traditional with resume times
while read -r KEY TRAD_TIME; do
    RESUME_TIME=$(grep "^$KEY " "$TEMP_RESUME" 2>/dev/null | awk '{print $2}')
    
    if [ -n "$RESUME_TIME" ] && [ -n "$TRAD_TIME" ]; then
        if [ "$TRAD_TIME" -gt 0 ] 2>/dev/null && [ "$RESUME_TIME" -gt 0 ] 2>/dev/null; then
            # Convert ms to seconds for display
            TRAD_SEC=$(echo "scale=2; $TRAD_TIME / 1000" | bc 2>/dev/null || echo "$TRAD_TIME")
            RESUME_SEC=$(echo "scale=2; $RESUME_TIME / 1000" | bc 2>/dev/null || echo "$RESUME_TIME")
            SPEEDUP=$(echo "scale=2; $TRAD_TIME / $RESUME_TIME" | bc 2>/dev/null || echo "N/A")
            printf "%-35s | %10ss | %10ss | %sx\n" "$KEY" "$TRAD_SEC" "$RESUME_SEC" "$SPEEDUP"
        fi
    fi
done < "$TEMP_TRAD"

# Cleanup temp files
rm -f "$TEMP_TRAD" "$TEMP_RESUME"

echo ""
echo "================================================================================"
echo "ANALYSIS & INSIGHTS"
echo "================================================================================"
echo ""
echo "TIME BREAKDOWN EXPLANATION:"
echo ""
echo "  Total Time = Diff + Indexing + Skip + Merge + Other"
echo ""
echo "  Components:"
echo "    Diff(s)   = Traversing repository to find changes (EditorDiff.process)"
echo "    Skip(s)   = Fast-forwarding to resume point (susp mode only)"
echo "    Indexing  = Creating Lucene documents, writing to index"
echo "    Merge     = NodeStore.merge() committing index updates"
echo "    Other     = Checkpoint creation, stats collection, logging"
echo ""
echo "KEY FINDINGS:"
echo ""
echo "  1. SUSPEND/RESUME MODE (~5-10% overhead)"
echo "     - Diff time DOUBLES because each resume re-enters from root"
echo "     - Skip overhead is minimal (~1-2s for 500K nodes)"
echo "     - Example: 500K nodes, 5s limit"
echo "       * Traditional: Diff=16s of 37s total (43%)"
echo "       * Suspend:     Diff=31s of 39s total (79%) <- DOUBLED!"
echo ""
echo "  2. CONTINUOUS MODE (~0% overhead) ✨ RECOMMENDED"
echo "     - Stays in same diff traversal (no re-entry)"
echo "     - Diff time same as traditional mode"
echo "     - Logs progress checkpoints without exiting"
echo "     - Example: 500K nodes, 5s limit"
echo "       * Continuous: Diff=16s of 37s total (43%) <- Same as trad!"
echo ""
echo "  3. PRODUCTION RECOMMENDATION"
echo "     - Use Continuous mode (-Doak.async.continuousMode=true)"
echo "     - Provides progress visibility with zero overhead"
echo "     - Set time limit for checkpoint frequency (e.g., 5-10s)"
echo "     - Example:"
echo "       -Doak.async.timeLimitMs=5000"
echo "       -Doak.async.continuousMode=true"
echo ""
echo "  4. WHEN TO USE SUSPEND/RESUME MODE"
echo "     - Development/testing crash recovery scenarios"
echo "     - When NodeStore merge() is expensive (accept 5-10% overhead)"
echo "     - Debugging indexing issues with incremental state"
echo ""
echo "================================================================================"
echo "METRICS YOU CAN ADD FOR DEEPER ANALYSIS"
echo "================================================================================"
echo ""
echo "  Metric              | Why Useful                     | How to Capture"
echo "  --------------------|--------------------------------|-------------------------"
echo "  Index Write Time    | Separate Lucene from traversal | Time FulltextIndexEditor"
echo "  Documents Indexed   | Index size vs nodes            | Counter in DocumentMaker"
echo "  Commit Latency      | Identify slow merges           | Time merge() calls"
echo "  Checkpoint Time     | Checkpoint overhead            | Time checkpoint()"
echo "  Index Segments      | Lucene merge efficiency        | IndexReader.leaves()"
echo "  File Handles        | Resource exhaustion risk       | OperatingSystemMXBean"
echo "  I/O Read/Write      | Disk throughput                | sun.management APIs"
echo "  Compaction Stats    | Segment store efficiency       | SegmentGCOptions"
echo "  MongoDB Op Time     | Document store performance     | MongoDB profiler"
echo "  Lock Contention     | Threading efficiency           | ThreadMXBean"
echo ""
echo "================================================================================"
echo "TEST COMPLETE"
echo "================================================================================"
echo ""
echo "Results saved to:"
echo "  - $OUTPUT_FILE (full output)"
echo "  - $SUMMARY_FILE (summary table)"
echo "  - target/RESUME_INDEXING_PERF_RESULTS.md (markdown report)"
echo "  - target/resume_indexing_perf_data.csv (CSV data)"
echo ""
echo "For detailed metrics documentation, see:"
echo "  - oak-lucene/PERFORMANCE_METRICS_GUIDE.md"
echo ""
