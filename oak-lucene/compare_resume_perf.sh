#!/bin/bash
#
# ===============================================================================
# RESUMABLE INDEXING PERFORMANCE TEST SCRIPT
# ===============================================================================
#
# This script tests resumable indexing with chunk-size-based commits.
# Each AsyncIndexUpdate.run() processes one chunk, commits it, saves resume state,
# and exits. Next run() resumes from the saved position.
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
#
# ===============================================================================

cd "$(dirname "$0")"

OUTPUT_FILE="perf_resume_results.txt"
SUMMARY_FILE="perf_resume_summary.txt"

echo "================================================================================"
echo "RESUMABLE INDEXING PERFORMANCE TEST"
echo "================================================================================"
echo ""

# Clean previous results
rm -f "$OUTPUT_FILE"
rm -f "$SUMMARY_FILE"

# Compile test classes
echo "Compiling oak-core and oak-lucene..."
cd ..
mvn compiler:compile compiler:testCompile -pl oak-core,oak-lucene -q 2>/dev/null
cd oak-lucene
echo "Compilation complete."
echo ""

# Test Scenarios: "STORE NODES CHUNK_SIZE RESUME PT_TRAVERSAL"
# RESUME: "false" (traditional indexing) or "true" (resume/chunked indexing)
# PT_TRAVERSAL: "false" (standard EditorDiff) or "true" (use PathTree for traversal)
SCENARIOS=(
    # Normal mode - traditional indexing (no chunking, no resume)
    "SEGMENT 10000 0 false false"
    
    # Resume mode - chunk-based with PathTree traversal
    "SEGMENT 10000 2000 true true"
    
    # Optional: Add time-based chunking (chunkTimeMs)
    # Note: Enable by setting oak.async.chunkTimeMs in JVM_CONFIG
    
    # Larger tests - uncomment to run
    # "SEGMENT 50000 0 false false"
    # "SEGMENT 50000 10000 true true"
)

# JVM Configuration
JVM_CONFIG="-Xmx4G -Xms4G"

# Get classpath from Maven
echo "Building classpath..."
CP="../oak-core/target/classes:target/classes:target/test-classes"
DEPS=$(cd .. && mvn -pl oak-lucene dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)
if [ -n "$DEPS" ]; then
    CP="$CP:$DEPS"
    echo "✓ Maven classpath obtained"
fi
echo ""

run_single_scenario() {
    local STORE=$1
    local NODES=$2
    local CHUNK=$3
    local RESUME=$4
    local PATHTREE_TRAVERSAL=$5
    
    local MODE="NORMAL"
    if [ "$RESUME" = "true" ]; then
        MODE="RESUME"
    fi
    
    local TRAVERSAL_SUFFIX=""
    if [ "$PATHTREE_TRAVERSAL" = "true" ]; then
        TRAVERSAL_SUFFIX="_PTTRAVERSAL"
    fi
    local SCENARIO_NAME="${STORE}_${NODES}_${MODE}${TRAVERSAL_SUFFIX}"
    
    echo "--------------------------------------------------------------------------------"
    echo "Running: Store=$STORE, Nodes=$NODES, Mode=$MODE, ChunkSize=$CHUNK"
    echo "         Resume=$RESUME, PathTreeTraversal=$PATHTREE_TRAVERSAL"
    echo "--------------------------------------------------------------------------------"
    
    # Run test using JUnit directly
    java $JVM_CONFIG \
         -Dperf.nodeStore=$STORE \
         -Dperf.nodeCount=$NODES \
         -Dperf.chunkSize=$CHUNK \
         -Doak.async.chunkSize=$CHUNK \
         -Doak.async.resume=$RESUME \
         -Doak.async.usePathTreeTraversal=$PATHTREE_TRAVERSAL \
         -Djava.awt.headless=true \
         -cp "$CP" \
         org.junit.runner.JUnitCore \
         org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing.perf.ResumeIndexingPerfTest > "$SCENARIO_NAME.out" 2>&1
    
    # Check if test produced output
    if [ ! -s "$SCENARIO_NAME.out" ]; then
        echo "  ✗ ERROR: Test produced no output!"
        echo "### SCENARIO: $SCENARIO_NAME - FAILED (no output) ###" >> "$OUTPUT_FILE"
    else
        # Check for test failures/exceptions
        if grep -q "FAILURES\|Exception\|Error" "$SCENARIO_NAME.out" 2>/dev/null; then
            echo "  ⚠ Test may have encountered errors - check output file"
        fi
        
        # Append to main output file
        echo "### SCENARIO: $SCENARIO_NAME ###" >> "$OUTPUT_FILE"
        cat "$SCENARIO_NAME.out" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        
        # Parse and print stats
        print_stats_from_file "$SCENARIO_NAME.out" "$STORE" "$NODES" "$CHUNK" "$MODE" "$PATHTREE_TRAVERSAL"
    fi
    
    # Keep output file for debugging - don't remove
    echo "  Output saved to: $SCENARIO_NAME.out"
}

print_stats_from_file() {
    local FILE=$1
    local STORE=$2
    local NODES=$3
    local CHUNK=$4
    local MODE=$5
    local PATHTREE_TRAVERSAL=$6
    
    local TIME=""
    local THROUGHPUT=""
    local RUN_COUNT=""
    local QUERY_APPROVED=""
    local MAX_INCREMENTAL_RESULTS=""
    local TOTAL_CHUNKS=""
    local CHUNKS_WITH_RESULTS=""
    
    # Parse metrics from test output
    while IFS= read -r line; do
        if [[ $line == "Total Time:"* ]]; then TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Throughput:"* ]]; then THROUGHPUT=$(echo $line | awk '{print $2}'); fi
        if [[ $line == "Run Count:"* ]]; then RUN_COUNT=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Query Approved (index):"* ]]; then QUERY_APPROVED=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Max results seen in incremental queries:"* ]]; then MAX_INCREMENTAL_RESULTS=$(echo $line | awk '{print $7}'); fi
        if [[ $line == "INCREMENTAL_SUMMARY:"* ]]; then
            MAX_INCREMENTAL_RESULTS=$(echo $line | sed 's/.*maxResults=\([0-9]*\).*/\1/')
            TOTAL_CHUNKS=$(echo $line | sed 's/.*totalChunks=\([0-9]*\).*/\1/')
            CHUNKS_WITH_RESULTS=$(echo $line | sed 's/.*chunksWithResults=\([0-9]*\).*/\1/')
        fi
    done < "$FILE"
    
    # Convert time from ms to seconds
    local TIME_SECONDS="N/A"
    if [ -n "$TIME" ] && [ "$TIME" -gt 0 ] 2>/dev/null; then
        TIME_SECONDS=$(echo "scale=1; $TIME / 1000" | bc 2>/dev/null || echo "N/A")
    fi
    
    # Set defaults for missing metrics
    [ -z "$THROUGHPUT" ] && THROUGHPUT="N/A"
    [ -z "$RUN_COUNT" ] && RUN_COUNT="N/A"
    [ -z "$QUERY_APPROVED" ] && QUERY_APPROVED="N/A"
    [ -z "$MAX_INCREMENTAL_RESULTS" ] && MAX_INCREMENTAL_RESULTS="0"
    [ -z "$TOTAL_CHUNKS" ] && TOTAL_CHUNKS="N/A"
    [ -z "$CHUNKS_WITH_RESULTS" ] && CHUNKS_WITH_RESULTS="N/A"
    
    # Warn if critical metrics are missing
    if [ "$TIME_SECONDS" = "N/A" ] || [ "$THROUGHPUT" = "N/A" ]; then
        echo "  ⚠ WARNING: Test may have failed - missing critical metrics"
        echo "  Check $FILE for errors"
    fi
    
    # Print formatted output
    local PT_FLAG="N"
    if [ "$PATHTREE_TRAVERSAL" = "true" ]; then
        PT_FLAG="Y"
    fi
    printf "%-8s | %-8s | %-8s | %-8s | %-6s | %9s | %10s | %10s | %-5s\n" \
           "$STORE" "$NODES" "$MODE" "$CHUNK" "$PT_FLAG" "${TIME_SECONDS}s" "$THROUGHPUT" "$QUERY_APPROVED" "$RUN_COUNT" | tee -a "$SUMMARY_FILE"
    
    # Print incremental searchability results
    if [ "$MAX_INCREMENTAL_RESULTS" != "N/A" ] && [ "$MAX_INCREMENTAL_RESULTS" -gt 0 ] 2>/dev/null; then
        echo "  ✓ Incremental Searchability: WORKING"
        echo "    Max results during indexing: $MAX_INCREMENTAL_RESULTS / $QUERY_APPROVED"
        echo "    Chunks with search results: $CHUNKS_WITH_RESULTS / $TOTAL_CHUNKS"
    elif [ "$MAX_INCREMENTAL_RESULTS" = "0" ]; then
        echo "  ⚠ Incremental Searchability: NOT WORKING - Index only searchable after final commit"
    fi
    
    # ===============================================================
    # Parse and display DEBUG timing information
    # ===============================================================
    
    echo ""
    echo "  DETAILED TIMING BREAKDOWN:"
    echo "  --------------------------"
    
    # PathTree loading times
    if grep -q "\[DEBUG-PATHTREE\] Load time:" "$FILE" 2>/dev/null; then
        echo "  PathTree Loading:"
        grep "\[DEBUG-PATHTREE\] Load time:" "$FILE" | head -5 | while read line; do
            local loadTime=$(echo $line | sed 's/.*Load time: \([0-9]*\)ms.*/\1/')
            local nodes=$(echo $line | sed 's/.*nodes: \([0-9]*\).*/\1/')
            local indexed=$(echo $line | sed 's/.*indexed: \([0-9]*\).*/\1/')
            local fullyProcessed=$(echo $line | sed 's/.*fullyProcessed: \([0-9]*\).*/\1/')
            local size=$(echo $line | sed 's/.*size: \([0-9]*\).*/\1/')
            printf "    Load: %4d ms | Nodes: %6d | Indexed: %6d | FullyProc: %6d | Size: %8d bytes\n" \
                   "$loadTime" "$nodes" "$indexed" "$fullyProcessed" "$size"
        done
    fi
    
    # PathTree serialization times
    if grep -q "\[DEBUG-PATHTREE\] Serialize time:" "$FILE" 2>/dev/null; then
        echo "  PathTree Serialization:"
        grep "\[DEBUG-PATHTREE\] Serialize time:" "$FILE" | while read line; do
            local serTime=$(echo $line | sed 's/.*Serialize time: \([0-9]*\)ms.*/\1/')
            local nodes=$(echo $line | sed 's/.*nodes: \([0-9]*\).*/\1/')
            local indexed=$(echo $line | sed 's/.*indexed: \([0-9]*\).*/\1/')
            local fullyProcessed=$(echo $line | sed 's/.*fullyProcessed: \([0-9]*\).*/\1/')
            printf "    Serialize: %4d ms | Nodes: %6d | Indexed: %6d | FullyProc: %6d\n" \
                   "$serTime" "$nodes" "$indexed" "$fullyProcessed"
        done
    fi
    
    # PathTree pruning times
    if grep -q "\[DEBUG-PATHTREE\] Prune time:" "$FILE" 2>/dev/null; then
        echo "  PathTree Pruning:"
        grep "\[DEBUG-PATHTREE\] Prune time:" "$FILE" | while read line; do
            local pruneTime=$(echo $line | sed 's/.*Prune time: \([0-9]*\)ms.*/\1/')
            local pruned=$(echo $line | sed 's/.*pruned: \([0-9]*\).*/\1/')
            local before=$(echo $line | sed 's/.*before: \([0-9]*\).*/\1/')
            local after=$(echo $line | sed 's/.*after: \([0-9]*\).*/\1/')
            printf "    Prune: %4d ms | Pruned: %6d nodes | Before: %6d | After: %6d\n" \
                   "$pruneTime" "$pruned" "$before" "$after"
        done
    fi
    
    # Mode-specific timing (NORMAL or RESUME)
    if grep -q "\[DEBUG-MODE\]" "$FILE" 2>/dev/null; then
        echo ""
        echo "  Indexing Mode:"
        grep "\[DEBUG-MODE\]" "$FILE" | tail -1 | while read line; do
            echo "    $line"
        done
    fi
    
    # Diff time
    if grep -q "\[DEBUG-TIMING\].*Diff time:" "$FILE" 2>/dev/null; then
        echo "  Diff Timing:"
        grep "\[DEBUG-TIMING\].*Diff time:" "$FILE" | while read line; do
            local diffTime=$(echo $line | sed 's/.*Diff time: \([0-9]*\)ms.*/\1/')
            local mode=$(echo $line | sed 's/.*\[DEBUG-TIMING\] \([A-Z]*\) Diff.*/\1/')
            printf "    %s Diff: %4d ms\n" "$mode" "$diffTime"
        done
    fi
    
    # Commit summary (flush + merge)
    if grep -q "\[DEBUG-TIMING\].*COMMIT SUMMARY:" "$FILE" 2>/dev/null; then
        echo "  Commit Timing Summary:"
        grep "\[DEBUG-TIMING\].*COMMIT SUMMARY:" "$FILE" | while read line; do
            local mode=$(echo $line | sed 's/.*\[DEBUG-TIMING\] \([A-Z]*\) COMMIT.*/\1/')
            local flush=$(echo $line | sed 's/.*flush=\([0-9]*\)ms.*/\1/')
            local merge=$(echo $line | sed 's/.*merge=\([0-9]*\)ms.*/\1/')
            local total=$(echo $line | sed 's/.*TOTAL=\([0-9]*\)ms.*/\1/')
            printf "    %s: flush=%4dms | merge=%4dms | TOTAL=%4dms\n" \
                   "$mode" "$flush" "$merge" "$total"
        done
    fi
    
    # Resume path timing
    if grep -q "\[DEBUG-RESUME\]" "$FILE" 2>/dev/null; then
        echo "  Resume Path Timing:"
        grep "\[DEBUG-RESUME\] Resume path reached" "$FILE" | while read line; do
            local resumeTime=$(echo $line | sed 's/.*reached in \([0-9]*\)ms.*/\1/')
            printf "    Time to reach resume path: %4d ms\n" "$resumeTime"
        done
        
        grep "\[DEBUG-RESUME\] Total diff time:" "$FILE" | head -3 | while read line; do
            local diffTime=$(echo $line | sed 's/.*Total diff time: \([0-9]*\)ms.*/\1/')
            local toResume=$(echo $line | sed 's/.*time to resume path: \([0-9]*\)ms.*/\1/')
            local afterResume=$(echo $line | sed 's/.*indexing time after resume: \([0-9]*\)ms.*/\1/')
            printf "    Total diff: %4d ms | To resume: %4d ms | After resume: %4d ms\n" \
                   "$diffTime" "$toResume" "$afterResume"
        done
    fi
    
    # Skip stats (NodeStore optimization)
    if grep -q "\[DEBUG-SKIP\]" "$FILE" 2>/dev/null; then
        echo "  Skip Stats (NodeStore optimization):"
        local total_skip_full=0
        local total_processed=0
        local chunk_num=0
        grep "\[DEBUG-SKIP\]" "$FILE" | while read line; do
            chunk_num=$((chunk_num + 1))
            local skipFull=$(echo $line | sed 's/.*skipFull=\([0-9]*\).*/\1/')
            local skipIndexed=$(echo $line | sed 's/.*skipIndexed=\([0-9]*\).*/\1/')
            local processed=$(echo $line | sed 's/.*processed=\([0-9]*\).*/\1/')
            printf "    Chunk %2d: skipFull=%6d | skipIndexed=%5d | processed=%5d\n" \
                   "$chunk_num" "$skipFull" "$skipIndexed" "$processed"
        done
        
        # Summary
        echo ""
        echo "  Skip Summary:"
        local last_skip=$(grep "\[DEBUG-SKIP\]" "$FILE" | tail -1)
        local last_skip_full=$(echo $last_skip | sed 's/.*skipFull=\([0-9]*\).*/\1/')
        local last_processed=$(echo $last_skip | sed 's/.*processed=\([0-9]*\).*/\1/')
        local skip_pct=0
        if [ "$last_skip_full" -gt 0 ] && [ "$last_processed" -gt 0 ]; then
            skip_pct=$(echo "scale=1; $last_skip_full * 100 / ($last_skip_full + $last_processed)" | bc 2>/dev/null || echo "N/A")
        fi
        echo "    Final run: $last_skip_full nodes skipped (${skip_pct}% skip rate)"
    fi
    
    # PathTree Traversal stats (new optimization)
    if grep -q "\[DEBUG-PATHTREE-TRAVERSAL\]" "$FILE" 2>/dev/null; then
        echo ""
        echo "  PathTree Traversal Stats (NEW OPTIMIZATION):"
        echo "  ============================================"
        
        # Show traversal mode
        if grep -q "Using PathTree traversal mode" "$FILE" 2>/dev/null; then
            echo "    ✓ PathTree-driven traversal ENABLED"
        else
            echo "    ⚠ Standard EditorDiff mode (PathTree traversal disabled)"
        fi
        
        # Show PathTree stats
        grep "\[DEBUG-PATHTREE-TRAVERSAL\] PathTree stats:" "$FILE" 2>/dev/null | while read line; do
            local total=$(echo $line | sed 's/.*total=\([0-9]*\).*/\1/')
            local fp=$(echo $line | sed 's/.*fullyProcessed=\([0-9]*\).*/\1/')
            local nfp=$(echo $line | sed 's/.*notFullyProcessed=\([0-9]*\).*/\1/')
            local eo=$(echo $line | sed 's/.*enterOnly=\([0-9]*\).*/\1/')
            printf "    PathTree: total=%d | fullyProcessed=%d | notFullyProcessed=%d | enterOnly=%d\n" \
                   "$total" "$fp" "$nfp" "$eo"
        done
        
        # Show traversal stats
        local chunk_num=0
        grep "\[DEBUG-PATHTREE-TRAVERSAL\] pathTreeTraversals=" "$FILE" 2>/dev/null | while read line; do
            chunk_num=$((chunk_num + 1))
            local ptTrav=$(echo $line | sed 's/.*pathTreeTraversals=\([0-9]*\).*/\1/')
            local ssTrav=$(echo $line | sed 's/.*segmentStoreTraversals=\([0-9]*\).*/\1/')
            local ptLookup=$(echo $line | sed 's/.*pathTreeChildLookups=\([0-9]*\).*/\1/')
            local ssLookup=$(echo $line | sed 's/.*segmentStoreChildLookups=\([0-9]*\).*/\1/')
            
            # Calculate savings percentage
            local totalTrav=$((ptTrav + ssTrav))
            local pct="0"
            if [ "$totalTrav" -gt 0 ]; then
                pct=$(echo "scale=1; $ptTrav * 100 / $totalTrav" | bc 2>/dev/null || echo "N/A")
            fi
            
            printf "    Chunk %2d: PathTree=%5d | SegmentStore=%5d | Savings=%s%%\n" \
                   "$chunk_num" "$ptTrav" "$ssTrav" "$pct"
        done
        
        # Summary
        local last_pt_stat=$(grep "\[DEBUG-PATHTREE-TRAVERSAL\] pathTreeTraversals=" "$FILE" 2>/dev/null | tail -1)
        if [ -n "$last_pt_stat" ]; then
            local lastPtTrav=$(echo $last_pt_stat | sed 's/.*pathTreeTraversals=\([0-9]*\).*/\1/')
            local lastSsTrav=$(echo $last_pt_stat | sed 's/.*segmentStoreTraversals=\([0-9]*\).*/\1/')
            local lastTotal=$((lastPtTrav + lastSsTrav))
            local lastPct="0"
            if [ "$lastTotal" -gt 0 ]; then
                lastPct=$(echo "scale=1; $lastPtTrav * 100 / $lastTotal" | bc 2>/dev/null || echo "N/A")
            fi
            echo ""
            echo "    *** Final Traversal: $lastPtTrav from PathTree, $lastSsTrav from SegmentStore ($lastPct% optimization) ***"
        fi
    fi
    
    # Chunk commit timing
    if grep -q "\[DEBUG-TIMING\] CHUNK COMMIT SUMMARY:" "$FILE" 2>/dev/null; then
        echo "  Per-Chunk Commit Timing:"
        local chunk_num=0
        grep "\[DEBUG-TIMING\] CHUNK COMMIT SUMMARY:" "$FILE" | while read line; do
            chunk_num=$((chunk_num + 1))
            local flush=$(echo $line | sed 's/.*flush=\([0-9]*\)ms.*/\1/')
            local merge=$(echo $line | sed 's/.*merge=\([0-9]*\)ms.*/\1/')
            local save=$(echo $line | sed 's/.*saveState=\([0-9]*\)ms.*/\1/')
            local total=$(echo $line | sed 's/.*TOTAL=\([0-9]*\)ms.*/\1/')
            printf "    Chunk %2d: flush=%4dms | merge=%4dms | save=%4dms | TOTAL=%4dms\n" \
                   "$chunk_num" "$flush" "$merge" "$save" "$total"
        done
    fi
    
    # Print per-chunk search results if available
    if grep -q "CHUNK_RESULT:" "$FILE" 2>/dev/null; then
        echo ""
        echo "  Per-Chunk Search Results:"
        echo "  -------------------------"
        grep "CHUNK_RESULT:" "$FILE" | while read line; do
            local cycle=$(echo $line | sed 's/.*cycle=\([0-9]*\).*/\1/')
            local results=$(echo $line | sed 's/.*results=\([0-9-]*\).*/\1/')
            local ctime=$(echo $line | sed 's/.*time=\([0-9]*\).*/\1/')
            local path=$(echo $line | sed 's/.*path=\(.*\)/\1/')
            printf "    Chunk %2d: %4d results (%3d ms) - %s\n" "$cycle" "$results" "$ctime" "$path"
        done
    fi
    
    # Check for PathTree dump files
    if ls pathtree_dump_*.json 2>/dev/null | head -1 > /dev/null; then
        echo ""
        echo "  PathTree Dump Files:"
        echo "  --------------------"
        ls -la pathtree_dump_*.json 2>/dev/null | while read line; do
            echo "    $line"
        done
    fi
    
    echo ""
}

# Print header
echo ""
echo "================================================================================"
echo "RESULTS"
echo "================================================================================"
echo ""
printf "%-8s | %-8s | %-8s | %-8s | %-6s | %9s | %10s | %10s | %-5s\n" \
       "Store" "Nodes" "Mode" "Chunk" "PTTrav" "Time(s)" "Throughput" "Verified" "Runs" | tee -a "$SUMMARY_FILE"
echo "---------|----------|----------|----------|--------|-----------|------------|------------|-------" | tee -a "$SUMMARY_FILE"

# Run scenarios
for scenario in "${SCENARIOS[@]}"; do
    read -r STORE NODES CHUNK RESUME PATHTREE_TRAVERSAL <<< "$scenario"
    run_single_scenario "$STORE" "$NODES" "$CHUNK" "$RESUME" "$PATHTREE_TRAVERSAL"
done

echo ""
echo "================================================================================"
echo "TEST COMPLETE"
echo "================================================================================"
echo ""
echo "Results saved to:"
echo "  - $OUTPUT_FILE (full output)"
echo "  - $SUMMARY_FILE (summary table)"
echo ""
