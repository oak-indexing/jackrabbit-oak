#!/bin/bash
#
# Performance Comparison Script for Traditional vs Change Tracker Indexing
#
# Usage:
#   chmod +x compare_perf.sh
#   ./compare_perf.sh
#
# This script runs a series of performance tests defined in the SCENARIOS array
# across different JVM memory configurations. It collects metrics like time,
# throughput, memory usage, GC stats, and index sizes, and outputs a summary table.
#

# Output file for Maven test run
OUTPUT_FILE="perf_results.txt"
SUMMARY_FILE="perf_summary.txt"

# --- 1. Build & Test Execution ---

echo "================================================================================"
echo "Starting Performance Tests with External Parameters"
echo "JVM Options: $JVM_OPTS"
echo "================================================================================"

# Clean previous results
rm -f "$OUTPUT_FILE"
rm -f "$SUMMARY_FILE"

echo "Compiling oak-lucene once..."
mvn clean test-compile -pl oak-lucene -DskipTests > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi
echo "Compilation complete."

# Define Scenarios as arrays: Store Nodes Chunk
# Example: "MEMORY 1000 500"
# Find breaking points for SEGMENT and DOCUMENT (Mongo) and add aggressive scale-up
SCENARIOS=(
    # Memory baseline (sanity check)
    # "MEMORY 100 1000"
    # "MEMORY 1000 1000"
    # "MEMORY 10000 2000"
    # "MEMORY 50000 5000"
    # # Segment store: ramp up to break
    "SEGMENT 20000 2000"
    # "SEGMENT 50000 5000"
    # "SEGMENT 100000 5000"
    # "SEGMENT 250000 5000"
    # "SEGMENT 500000 5000"
    # "SEGMENT 1000000 5000"
    # DocumentNodeStore (Mongo): ramp up to break
    # "DOCUMENT 20000 2000"
    # "DOCUMENT 50000 5000"
    # "DOCUMENT 100000 5000"
    # "DOCUMENT 250000 5000"
    # "DOCUMENT 500000 5000"
    # "DOCUMENT 1000000 5000"
)

# JVM Configurations to Loop Over
JVM_CONFIGS=(
    "-Xmx1G -Xms1G"
    # "-Xmx2G -Xms2G"
    # "-Xmx4G -Xms4G"
    # "-Xmx8G -Xms8G"
)

run_single_scenario() {
    local STORE=$1
    local NODES=$2
    local CHUNK=$3
    local CT=$4
    local MEM_CONFIG=$5
    local SCENARIO_NAME="${STORE}_${NODES}_${CHUNK}_CT${CT}_MEM${MEM_CONFIG// /-}"
    
    echo "--------------------------------------------------------------------------------"
    echo "Running: $STORE, Nodes: $NODES, Chunk: $CHUNK, CT: $CT, JVM: $MEM_CONFIG"
    echo "--------------------------------------------------------------------------------"
    
    # Ensure fresh report
    SUREFIRE_OUT="oak-lucene/target/surefire-reports/org.apache.jackrabbit.oak.plugins.index.lucene.BasicChangeTrackerPerfTest-output.txt"
    rm -f "$SUREFIRE_OUT"

    # Capture output to temp file
    mvn test -pl oak-lucene \
        -Dtest=BasicChangeTrackerPerfTest \
        -Dsurefire.useFile=false \
        -DfailIfNoTests=false \
        -Dbaseline.skip=true \
        -Dperf.nodeStore=$STORE -Dperf.nodeCount=$NODES -Dperf.chunkSize=$CHUNK -Dperf.useChangeTracker=$CT \
        -DargLine="$MEM_CONFIG -XX:+PrintGCDetails" > "$SCENARIO_NAME.out" 2>&1
    
    # Fix: Append surefire output file if stdout redirection didn't work (common with some surefire configs)
    if [ -f "$SUREFIRE_OUT" ]; then
        cat "$SUREFIRE_OUT" >> "$SCENARIO_NAME.out"
    fi

    # Append to main output file
    echo "### SCENARIO: $SCENARIO_NAME (JVM: $MEM_CONFIG) ###" >> "$OUTPUT_FILE"
    cat "$SCENARIO_NAME.out" >> "$OUTPUT_FILE"
    
    # Parse and print stats for this run
    print_stats_from_file "$SCENARIO_NAME.out" "$STORE" "$NODES" "$CHUNK" "$CT" "$MEM_CONFIG"
    
    rm "$SCENARIO_NAME.out"
}

print_stats_from_file() {
    local FILE=$1
    local STORE=$2
    local NODES=$3
    local CHUNK=$4
    local CT=$5
    local MEM_CONFIG=$6
    
    local STRATEGY="Traditional"
    if [ "$CT" == "true" ]; then STRATEGY="ChangeTracker"; fi
    
    local TIME=""
    local THROUGHPUT=""
    local MEM_MB=""
    local CPU=""
    local GC_COUNT=""
    local GC_TIME=""
    local PHASE1=""
    local PHASE3=""
    local DIRECT_BUF=""
    local DISK_USAGE=""
    local INDEX_SIZE=""
    local CT_INDEX_SIZE=""
    
    while IFS= read -r line; do
        if [[ $line == "Total Time:"* ]]; then TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Throughput:"* ]]; then THROUGHPUT=$(echo $line | awk '{print $2}'); fi
        if [[ $line == "Memory Delta:"* ]]; then 
            MEM_KB=$(echo $line | awk '{print $3}')
            MEM_MB=$((MEM_KB / 1024))
        fi
        if [[ $line == "Process CPU Time:"* ]]; then CPU=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "GC Count:"* ]]; then GC_COUNT=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "GC Time:"* ]]; then GC_TIME=$(echo $line | awk '{print $3}'); fi
        if [[ $line == "Phase 1 (Populate):"* ]]; then PHASE1=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Phase 3 (Index):"* ]]; then PHASE3=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Direct Buffer Memory:"* ]]; then DIRECT_BUF=$(echo $line | awk '{print $4}'); fi
        if [[ $line == "Disk Usage:"* ]]; then 
            DISK_KB=$(echo $line | awk '{print $3}')
            DISK_USAGE=$((DISK_KB / 1024))
        fi
        if [[ $line == "Main Index Size:"* ]]; then 
            IDX_KB=$(echo $line | awk '{print $4}')
            INDEX_SIZE=$((IDX_KB / 1024))
        fi
        if [[ $line == "CT Index Size:"* ]]; then 
            CT_IDX_KB=$(echo $line | awk '{print $4}')
            CT_INDEX_SIZE=$((CT_IDX_KB / 1024))
        fi
    done < "$FILE"
    
    # Truncate JVM config for display if needed
    local MEM_DISPLAY=$(echo $MEM_CONFIG | awk '{print $1}')
    
    printf "%-10s | %-10s | %-10s | %-10s | %-15s | %-15s | %-15s | %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "$MEM_DISPLAY" "$STORE" "$NODES" "$CHUNK" "$STRATEGY" "$TIME" "$THROUGHPUT" "$MEM_MB" "$CPU" "$GC_COUNT" "$GC_TIME" "$PHASE1" "$PHASE3" "$DIRECT_BUF" "$DISK_USAGE" "$INDEX_SIZE" "$CT_INDEX_SIZE" | tee -a "$SUMMARY_FILE"
}

# Header
printf "%-10s | %-10s | %-10s | %-10s | %-15s | %-15s | %-15s | %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "JVM" "Store" "Nodes" "Chunk" "Strategy" "Time(ms)" "Throughput" "Mem(MB)" "CPU(ms)" "GC(#)" "GC(ms)" "P1(ms)" "P3(ms)" "DirBuf(KB)" "Disk(MB)" "Idx(MB)" "CTIdx(MB)" | tee -a "$SUMMARY_FILE"
echo "---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------" | tee -a "$SUMMARY_FILE"

# Loop through JVM configs and scenarios
for jvm_opts in "${JVM_CONFIGS[@]}"; do
    for scenario in "${SCENARIOS[@]}"; do
        read -r STORE NODES CHUNK <<< "$scenario"
        
        # Run Traditional
        run_single_scenario "$STORE" "$NODES" "$CHUNK" "false" "$jvm_opts"
        
        # Run Change Tracker
        run_single_scenario "$STORE" "$NODES" "$CHUNK" "true" "$jvm_opts"
    done
done

echo "------------------------------------------------------------------------------------------------------------------------------------------------"
echo "Speedup Analysis:"

# Simple speedup calculation from the output file
# This is a simplified parser for the consolidated output
declare -a BASELINES
declare -a KEYS

while IFS= read -r line; do
    # Extract JVM config from scenario header line if present
    if [[ $line == "### SCENARIO:"* ]]; then
        # Format: ### SCENARIO: MEMORY_1000_500_CTfalse_MEM-Xmx1G (JVM: -Xmx1G -Xms1G) ###
        CURRENT_JVM=$(echo $line | grep -o "JVM: [^)]*" | cut -d' ' -f2-)
    fi
    
    if [[ $line == *"Performance Measurement:"* ]]; then
        STORE=$(echo $line | grep -o "MEMORY\|SEGMENT\|DOCUMENT")
        NODES=$(echo $line | grep -o "Nodes: [0-9]*" | awk '{print $2}')
        CHUNK=$(echo $line | grep -o "Chunk: [0-9]*" | awk '{print $2}')
        CT=$(echo $line | grep -o "CT: [a-z]*" | awk '{print $2}')
        
        # Use a unique key including JVM config
        # We'll strip spaces from JVM config for the key
        JVM_KEY="${CURRENT_JVM// /}"
        KEY="$STORE-$NODES-$CHUNK-$JVM_KEY"
    fi
    
    if [[ $line == "Total Time:"* ]]; then
        TIME=$(echo $line | awk '{print $3}')
        
        if [ "$CT" == "false" ]; then
            # Store baseline time. 
            echo "$KEY $TIME" >> "baselines.tmp"
        else
            # Retrieve baseline
            if [ -f "baselines.tmp" ]; then
                BASE_TIME=$(grep "^$KEY " "baselines.tmp" | tail -n 1 | awk '{print $2}')
                if [ ! -z "$BASE_TIME" ] && [ "$BASE_TIME" -gt 0 ]; then
                    SPEEDUP=$(echo "scale=2; $BASE_TIME / $TIME" | bc)
                    printf "JVM: %-15s | Scenario: %-30s | Speedup: %s x (Trad: %sms vs CT: %sms)\n" "$CURRENT_JVM" "$STORE-$NODES-$CHUNK" "$SPEEDUP" "$BASE_TIME" "$TIME"
                fi
            fi
        fi
    fi
done < "$OUTPUT_FILE"

rm -f "baselines.tmp"

