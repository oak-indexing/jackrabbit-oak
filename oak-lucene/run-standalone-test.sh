#!/bin/bash
cd "$(dirname "$0")"

echo "Building classpath..."
CP="target/test-classes:target/classes"

# Add all dependencies
for jar in $(find ~/.m2/repository -name "*.jar" 2>/dev/null | grep -E "(oak-|lucene-|guava|slf4j|junit|hamcrest|commons-|jackson-|jcr-)" | head -200); do
    CP="$CP:$jar"
done

echo "Running test..."
java -DuseChangeTracking=true \
     -Xmx1g \
     -cp "$CP" \
     org.junit.runner.JUnitCore \
     org.apache.jackrabbit.oak.plugins.index.lucene.ChangeTrackingE2ETest 2>&1 | grep -E "(test03b|Assertion|Q[0-9]|SUMMARY|===|OK|FAILURES)" | head -50

