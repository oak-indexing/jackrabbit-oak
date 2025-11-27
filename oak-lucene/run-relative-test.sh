#!/bin/bash

# Simple script to run RelativePropertyFulltextTest

cd /Users/mokatari/adobe/hackathon/hackathon-dec-2025/jackrabbit-oak/oak-lucene

echo "Running RelativePropertyFulltextTest..."
mvn test -Dtest=RelativePropertyFulltextTest -DfailIfNoTests=false 2>&1 | tail -100
