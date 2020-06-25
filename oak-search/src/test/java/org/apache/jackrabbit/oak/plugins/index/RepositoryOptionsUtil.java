package org.apache.jackrabbit.oak.plugins.index;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public abstract class RepositoryOptionsUtil {

    public enum NodeStoreType {
        MEMORY_NODE_STORE
    }

    public final int defaultAsyncIndexingTimeInSeconds = 5;
    public final long indexCorruptIntervalInMillis = 100;

    public boolean isAsync() {
        return isAsync;
    }

    private boolean isAsync;
    private boolean isInitialized;

    protected NodeStore nodeStore;
    protected Oak oak;
    //protected NodeStoreType nodeStoreType = NodeStoreType.MEMORY_NODE_STORE;

    /*
Override this to create some other repo initializer if needed
// Make sure to call super.initialize(builder)
 */
    private InitialContent getInitialContent() {
        return new InitialContent() {
            @Override
            public void initialize(@NotNull NodeBuilder builder) {
                super.initialize(builder);
                // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
                NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
                oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());
            }
        };
    }

    protected abstract void createDefaultOak(NodeStoreType nodeStoreType, NodeStore nodeStore, boolean isAsync);

    // Override this to provide a different flavour of node store
    // like segment or mongo mk
    // Tests would need to handle the cleanup accordingly.
    // TODO provide a util here so that test classes simply need to mention the type of store they want to create
    // for now, memory store should suffice.

    protected NodeStore createNodeStore(NodeStoreType memoryNodeStore) {
        switch (memoryNodeStore) {
            case MEMORY_NODE_STORE:
            default:
                return new MemoryNodeStore();
        }
    }

    public  Oak getOak(){
        return oak;
    }



}
