#!/bin/bash

# Quick test with very small dataset to verify implementation
# This bypasses Maven's OSGi manifest issues

cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak/oak-lucene

echo "========================================="
echo "QUICK PERFORMANCE TEST"
echo "Small dataset to verify implementation"
echo "========================================="
echo ""

# Get classpath
CP="target/classes:target/test-classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)"

# Run with small heap to test quickly
java -Xmx512m \
     -Dtest.bulk.sizes=10,20 \
     -Dtest.update.percents=10,20 \
     -Dtest.mixed.iterations=2 \
     -cp "$CP" \
     org.junit.runner.JUnitCore \
     org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.perf.ChangeTrackingPerformanceTest

echo ""
echo "========================================="
echo "Test execution complete"
echo "========================================="

