package org.apache.jackrabbit.oak.plugins.index;

public interface IndexingLaneTask {

    String getLaneName();

    void execute() throws IndexingLaneException;

}
