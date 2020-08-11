package org.apache.jackrabbit.oak.plugins.index;

public interface IndexingLaneTaskProvider {

    IndexingLaneTask getTaskForLane(String lane);

}
