#!/bin/bash

# Full Performance Test Suite
# Runs comprehensive performance tests with multiple heap sizes

cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak/oak-lucene

echo "========================================="
echo "FULL PERFORMANCE TEST SUITE"
echo "damAssetLucene-13 with 12 Aggregates"
echo "Multi-Heap Stress Testing"
echo "========================================="
echo ""

# Configuration
HEAP_SIZES=${HEAP_SIZES:-2g,4g,6g}
BULK_SIZES=${BULK_SIZES:-10000,50000,100000,200000,300000,400000,500000}
UPDATE_PERCENTS=${UPDATE_PERCENTS:-10,25,50,75,100}

echo "Configuration:"
echo "  Heap Sizes:        $HEAP_SIZES"
echo "  Bulk Sizes:        $BULK_SIZES"
echo "  Update Percents:   $UPDATE_PERCENTS"
echo ""
echo "========================================="
echo ""

# Get classpath
CP="target/classes:target/test-classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)"

# Function to rebuild
rebuild() {
    echo ""
    echo "Rebuilding oak-commons and oak-lucene..."
    cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak
    mvn clean install -pl oak-commons -am -DskipTests -Dbaseline.skip=true -Drat.skip=true -q
    mvn install -pl oak-lucene -am -DskipTests -Dbaseline.skip=true -Drat.skip=true -q
    if [ $? -ne 0 ]; then
        echo "✗ Build failed"
        exit 1
    fi
    cd oak-lucene
    echo "✓ Build complete"
    echo ""
    # Refresh classpath
    CP="target/classes:target/test-classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)"
}

# Create reports directory
REPORT_DIR="target/perf-reports"
mkdir -p "$REPORT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY_FILE="$REPORT_DIR/summary-$TIMESTAMP.txt"

echo "Summary will be saved to: $SUMMARY_FILE"
echo ""

# Function to run test with specific heap size
run_test_with_heap() {
    local HEAP=$1
    local REPORT_FILE="$REPORT_DIR/perf-test-${HEAP}-$TIMESTAMP.txt"
    
    echo "=========================================" | tee -a "$SUMMARY_FILE"
    echo "Testing with Heap: $HEAP" | tee -a "$SUMMARY_FILE"
    echo "=========================================" | tee -a "$SUMMARY_FILE"
    echo "" | tee -a "$SUMMARY_FILE"
    
    java -Xmx$HEAP \
         -Xms$HEAP \
         -XX:+UseG1GC \
         -XX:MaxGCPauseMillis=200 \
         -Dtest.bulk.sizes=$BULK_SIZES \
         -Dtest.update.percents=$UPDATE_PERCENTS \
         -cp "$CP" \
         org.junit.runner.JUnitCore \
         org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.perf.ChangeTrackingPerformanceTest \
         2>&1 | tee "$REPORT_FILE"
    
    local EXIT_CODE=${PIPESTATUS[0]}
    
    # If test failed with compilation error, rebuild and retry
    if [ $EXIT_CODE -ne 0 ]; then
        if grep -q "Unresolved compilation" "$REPORT_FILE"; then
            echo ""
            echo "⚠ Compilation error detected, rebuilding..."
            rebuild
            
        echo "Retrying test execution..."
        java -Xmx$HEAP \
             -Xms$HEAP \
             -XX:+UseG1GC \
             -XX:MaxGCPauseMillis=200 \
             -Dtest.bulk.sizes=$BULK_SIZES \
             -Dtest.update.percents=$UPDATE_PERCENTS \
             -cp "$CP" \
             org.junit.runner.JUnitCore \
             org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.perf.ChangeTrackingPerformanceTest \
             2>&1 | tee -a "$REPORT_FILE"
            
            EXIT_CODE=${PIPESTATUS[0]}
        fi
    fi
    
    # Extract summary for this heap size
    echo "" | tee -a "$SUMMARY_FILE"
    echo "Results for $HEAP:" | tee -a "$SUMMARY_FILE"
    echo "-------------------" | tee -a "$SUMMARY_FILE"
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Status: ✓ PASSED" | tee -a "$SUMMARY_FILE"
    else
        echo "Status: ✗ FAILED (exit code: $EXIT_CODE)" | tee -a "$SUMMARY_FILE"
    fi
    
    # Extract breaking point analysis
    grep "Breaking Point Analysis:" "$REPORT_FILE" -A 20 | grep -E "(Bulk|Update|Mixed|OutOfMemory|OK|BREAKING)" | head -15 | tee -a "$SUMMARY_FILE"
    
    echo "" | tee -a "$SUMMARY_FILE"
    echo "Detailed report: $REPORT_FILE" | tee -a "$SUMMARY_FILE"
    echo "" | tee -a "$SUMMARY_FILE"
    
    return $EXIT_CODE
}

# Run tests for each heap size
IFS=',' read -ra HEAP_ARRAY <<< "$HEAP_SIZES"
OVERALL_EXIT=0

for HEAP in "${HEAP_ARRAY[@]}"; do
    run_test_with_heap "$HEAP"
    if [ $? -ne 0 ]; then
        OVERALL_EXIT=1
    fi
    echo ""
    echo "Waiting 5 seconds before next test..."
    sleep 5
done

echo ""
echo "========================================="
echo "ALL TESTS COMPLETE"
echo "========================================="
echo ""
echo "Summary saved to: $SUMMARY_FILE"
echo ""
echo "========================================="
echo "COMPARATIVE SUMMARY"
echo "========================================="
cat "$SUMMARY_FILE"
echo "========================================="
echo ""

exit $OVERALL_EXIT

