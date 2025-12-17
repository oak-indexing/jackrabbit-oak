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

# Test Scenarios: "STORE NODES CHUNK_SIZE"
SCENARIOS=(
    # Quick test - 10K nodes, 1K per chunk = 10 runs
    "SEGMENT 10000 1000"
    
    # Larger test - uncomment to run
    # "SEGMENT 50000 5000"
    # "SEGMENT 100000 10000"
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
    local SCENARIO_NAME="${STORE}_${NODES}_C${CHUNK}"
    
    echo "--------------------------------------------------------------------------------"
    echo "Running: Store=$STORE, Nodes=$NODES, ChunkSize=$CHUNK"
    echo "--------------------------------------------------------------------------------"
    
    # Run test using JUnit directly
    java $JVM_CONFIG \
         -Dperf.nodeStore=$STORE \
         -Dperf.nodeCount=$NODES \
         -Dperf.chunkSize=$CHUNK \
         -Doak.async.chunkSize=$CHUNK \
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
        print_stats_from_file "$SCENARIO_NAME.out" "$STORE" "$NODES" "$CHUNK"
    fi
    
    rm -f "$SCENARIO_NAME.out"
}

print_stats_from_file() {
    local FILE=$1
    local STORE=$2
    local NODES=$3
    local CHUNK=$4
    
    local TIME=""
    local THROUGHPUT=""
    local RUN_COUNT=""
    local QUERY_APPROVED=""
    
    # Parse metrics from test output
    while IFS= read -r line; do
        if [[ $line == "Total Time:"* ]]; then TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Throughput:"* ]]; then THROUGHPUT=$(echo $line | awk '{print $2}'); fi
        if [[ $line == "Run Count:"* ]]; then RUN_COUNT=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Query Approved (index):"* ]]; then QUERY_APPROVED=$(echo $line | awk '{print $4}'); fi
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
    
    # Warn if critical metrics are missing
    if [ "$TIME_SECONDS" = "N/A" ] || [ "$THROUGHPUT" = "N/A" ]; then
        echo "  ⚠ WARNING: Test may have failed - missing critical metrics"
        echo "  Check $FILE for errors"
    fi
    
    # Print formatted output
    printf "%-8s | %-8s | %-8s | %9s | %10s | %10s | %-5s\n" \
           "$STORE" "$NODES" "$CHUNK" "${TIME_SECONDS}s" "$THROUGHPUT" "$QUERY_APPROVED" "$RUN_COUNT" | tee -a "$SUMMARY_FILE"
}

# Print header
echo ""
echo "================================================================================"
echo "RESULTS"
echo "================================================================================"
echo ""
printf "%-8s | %-8s | %-8s | %9s | %10s | %10s | %-5s\n" \
       "Store" "Nodes" "Chunk" "Time(s)" "Throughput" "Verified" "Runs" | tee -a "$SUMMARY_FILE"
echo "---------|----------|----------|-----------|------------|------------|-------" | tee -a "$SUMMARY_FILE"

# Run scenarios
for scenario in "${SCENARIOS[@]}"; do
    read -r STORE NODES CHUNK <<< "$scenario"
    run_single_scenario "$STORE" "$NODES" "$CHUNK"
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
