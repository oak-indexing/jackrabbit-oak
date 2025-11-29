#!/bin/bash

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

# Define Scenarios as arrays: Store Nodes Chunk
# Example: "MEMORY 1000 500"
SCENARIOS=(
    "MEMORY 1000 500"
    "MEMORY 10000 2000"
    "MEMORY 5000 10"
    "MEMORY 20000 5000"
    "SEGMENT 10000 2000"
    "DOCUMENT 2000 500"
)

# JVM Configurations to Loop Over
JVM_CONFIGS=(
    "-Xmx1G -Xms1G"
    "-Xmx2G -Xms2G"
    "-Xmx4G -Xms4G"
    "-Xmx8G -Xms8G"
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
    
    # Capture output to temp file
    mvn clean test -pl oak-lucene \
        -Dtest=BasicChangeTrackerPerfTest \
        -Dsurefire.useFile=false \
        -DfailIfNoTests=false \
        -Dbaseline.skip=true \
        -DargLine="$MEM_CONFIG -Dperf.nodeStore=$STORE -Dperf.nodeCount=$NODES -Dperf.chunkSize=$CHUNK -Dperf.useChangeTracker=$CT" > "$SCENARIO_NAME.out" 2>&1
    
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
    
    printf "%-10s | %-10s | %-10s | %-10s | %-15s | %-15s | %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "$MEM_DISPLAY" "$STORE" "$NODES" "$CHUNK" "$STRATEGY" "$TIME" "$THROUGHPUT" "$MEM_MB" "$CPU" "$PHASE1" "$PHASE3" "$DIRECT_BUF" "$DISK_USAGE" "$INDEX_SIZE" "$CT_INDEX_SIZE" | tee -a "$SUMMARY_FILE"
}

# Header
printf "%-10s | %-10s | %-10s | %-10s | %-15s | %-15s | %-15s | %-15s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" "JVM" "Store" "Nodes" "Chunk" "Strategy" "Time(ms)" "Throughput" "Mem(MB)" "CPU(ms)" "P1(ms)" "P3(ms)" "DirBuf(KB)" "Disk(MB)" "Idx(MB)" "CTIdx(MB)" | tee -a "$SUMMARY_FILE"
echo "------------------------------------------------------------------------------------------------------------------------------------------------" | tee -a "$SUMMARY_FILE"

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

