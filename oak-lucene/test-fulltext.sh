#!/bin/bash
# Quick script to test fulltext CONTAINS queries in E2E test

set -e

echo "========================================="
echo "Testing Fulltext CONTAINS Queries"
echo "========================================="

cd "$(dirname "$0")"

echo ""
echo "Step 1: Compiling oak-lucene..."
mvn compile test-compile -DskipTests -q 2>&1 | tail -5

echo ""
echo "Step 2: Running test03_FulltextSearch (Traditional Mode)..."
mvn surefire:test \
  -Dtest=ChangeTrackingE2ETest#test03_FulltextSearch \
  -DuseChangeTracking=false \
  -Dmaven.main.skip=true \
  2>&1 | grep -A 30 "test03_FulltextSearch\|Fulltext\|CONTAINS\|LIKE\|BUILD"

echo ""
echo "========================================="
echo "Test Complete"
echo "========================================="

