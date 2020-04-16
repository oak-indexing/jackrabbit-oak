package org.apache.jackrabbit.oak.benchmark.util;

public class TestHelper {

    /**
     * Generates a unique index name from the given suggestion.
     * @param name name suggestion
     * @return unique index name
     */
    public static String getUniqueIndexName(String name) {
        return name + System.currentTimeMillis();
    }

}
