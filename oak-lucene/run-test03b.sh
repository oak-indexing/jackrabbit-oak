#!/bin/bash
cd "$(dirname "$0")"

# Run the test using the already compiled classes
mvn surefire:test -Dtest='ChangeTrackingE2ETest#test03b_FulltextSearchRelativeProperties' -DuseChangeTracking=true 2>&1 | tail -100

