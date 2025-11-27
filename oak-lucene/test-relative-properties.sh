#!/bin/bash

cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak/oak-lucene

echo "========================================="
echo "Testing CONTAINS with Relative Properties"
echo "========================================="

# First rebuild oak-lucene
echo "Step 1: Rebuilding oak-lucene..."
cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak
mvn clean install -pl oak-lucene -am -DskipTests -Dbaseline.skip=true -Drat.skip=true 2>&1 | tail -20

cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak/oak-lucene

echo ""
echo "Step 2: Running test03b_FulltextSearchRelativeProperties (Traditional Mode)..."
mvn surefire:test -Dtest='ChangeTrackingE2ETest#test03b_FulltextSearchRelativeProperties' \
    -Dtest.use.change.tracking=false \
    -Dbaseline.skip=true -Drat.skip=true 2>&1 | tee /tmp/test-relative-traditional.log

echo ""
echo "========================================="
echo "Test Results:"
cat target/surefire-reports/org.apache.jackrabbit.oak.plugins.index.lucene.ChangeTrackingE2ETest-output.txt 2>/dev/null | \
    grep -E "(TEST 3b|CONTAINS|LIKE|Relative|results|✓|✗)" || echo "No output found"

