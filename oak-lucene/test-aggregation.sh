#!/bin/bash
cd "$(dirname "$0")"

echo "========================================="
echo "Testing Fulltext with Aggregation"
echo "========================================="

# Compile the updated test
echo "Step 1: Compiling oak-lucene module..."
cd ..
mvn clean install -pl oak-lucene -am -DskipTests -Dbaseline.skip=true -Drat.skip=true -q

if [ $? -ne 0 ]; then
    echo "❌ Compilation failed!"
    exit 1
fi

echo "✓ Compilation successful"
echo ""

# Run test in traditional mode
echo "Step 2: Running test in TRADITIONAL mode..."
cd oak-lucene
mvn surefire:test -Dtest='ChangeTrackingE2ETest#test03b*' -DuseChangeTracking=false 2>&1 | grep -E "(T E S T S|Tests run|Assertion|CONTAINS|LIKE|SUMMARY|===)" | tail -50

echo ""
echo "========================================="
echo ""

# Run test in change tracking mode
echo "Step 3: Running test in CHANGE TRACKING mode..."
mvn surefire:test -Dtest='ChangeTrackingE2ETest#test03b*' -DuseChangeTracking=true 2>&1 | grep -E "(T E S T S|Tests run|Assertion|CONTAINS|LIKE|SUMMARY|===)" | tail -50

echo ""
echo "========================================="
echo "Test execution complete!"
echo "========================================="

