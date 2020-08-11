package org.apache.jackrabbit.oak.plugins.index;

public class IndexingLaneException extends Exception {

    public IndexingLaneException(String message) {
        super(message);
    }

    public IndexingLaneException(String message, Throwable cause) {
        super(message, cause);
    }
}
