#!/bin/bash

# Run ResumeIndexingE2ETest directly, bypassing Maven's OSGi manifest issues
# This is the same approach used in trailblazer branch for ChangeTracker tests

cd "$(dirname "$0")"

echo "========================================="
echo "RESUME INDEXING E2E TEST"
echo "Bypassing Maven bundle plugin issues"
echo "========================================="
echo ""

# Step 1: Compile classes directly (bypasses bundle plugin)
echo "Step 1: Compiling classes (bypassing bundle plugin)..."
mvn compiler:compile compiler:testCompile -Denforcer.skip=true -q 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✓ Compilation successful"
else
    echo "⚠ Compilation had issues, trying to continue with existing classes..."
fi

echo ""
echo "Step 2: Building classpath using Maven..."

# Build classpath from target directories
CP="target/classes:target/test-classes"

# Get dependencies classpath
DEPS=$(cd .. && mvn -pl oak-lucene dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout 2>/dev/null)
if [ -n "$DEPS" ]; then
    CP="$CP:$DEPS"
    echo "✓ Maven classpath obtained"
else
    echo "Maven classpath failed, using fallback..."
    # Fallback: add jars from target and m2 repo
    for jar in $(find ~/.m2/repository/org/apache/jackrabbit -name "oak-*.jar" 2>/dev/null | grep -v sources | grep -v javadoc | head -100); do
        CP="$CP:$jar"
    done
    for jar in $(find ~/.m2/repository -name "*.jar" 2>/dev/null | grep -E "(lucene-|guava-|slf4j-|junit-|hamcrest-|commons-|jackson-|javax.jcr|mongo|segment)" | grep -v sources | grep -v javadoc | head -200); do
        CP="$CP:$jar"
    done
fi

echo ""
echo "Step 3: Running ResumeIndexingE2ETest..."
echo ""

# Run the test with JUnit directly
java -Xmx1g \
     -Doak.async.chunkSize=3 \
     -Djava.awt.headless=true \
     -cp "$CP" \
     org.junit.runner.JUnitCore \
     org.apache.jackrabbit.oak.plugins.index.lucene.ResumeIndexingE2ETest

EXIT_CODE=$?

echo ""
echo "========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Tests PASSED"
else
    echo "✗ Tests FAILED (exit code: $EXIT_CODE)"
fi
echo "========================================="

exit $EXIT_CODE
