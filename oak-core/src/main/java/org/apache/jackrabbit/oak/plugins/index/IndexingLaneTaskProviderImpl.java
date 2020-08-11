package org.apache.jackrabbit.oak.plugins.index;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

@Component(service = IndexingLaneTaskProvider.class)
public class IndexingLaneTaskProviderImpl implements IndexingLaneTaskProvider {

    private final Map<String, IndexingLaneTask> indexingTasks = new ConcurrentHashMap<>();

    @Reference(name = "indexingTasks",
            policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY,
            service = IndexingLaneTask.class
    )
    public void bindSanityCheckers(IndexingLaneTask laneTask){
        indexingTasks.put(checkNotNull(laneTask.getLaneName()), laneTask);
    }

    public void unbindSanityCheckers(IndexingLaneTask laneTask){
        indexingTasks.remove(laneTask.getLaneName());
    }


    @Override
    public IndexingLaneTask getTaskForLane(String lane) {
        return indexingTasks.get(lane);
    }

}
