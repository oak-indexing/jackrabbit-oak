package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public abstract class TestRepositoryBuilder {

    protected int defaultAsyncIndexingTimeInSeconds = 5;
    protected long indexCorruptIntervalInMillis = 100;
    protected boolean isAsync;
    // private static String elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
    //private ElasticConnection esConnection;

    protected NodeStore nodeStore;
    protected IndexEditorProvider editorProvider;
    protected QueryIndexProvider indexProvider;
    protected AsyncIndexUpdate asyncIndexUpdate;
    protected TrackingCorruptIndexHandler trackingCorruptIndexHandler;
    protected InitialContent initialContent;
    protected SecurityProvider securityProvider;
    protected IndexEditorProvider indexEditorProvider;
    protected QueryIndexProvider queryIndexProvider;
    protected QueryEngineSettings queryEngineSettings;


    public TestRepositoryBuilder() {
        this.isAsync = isAsync;
        this.nodeStore = createNodeStore(RepositoryOptionsUtil.NodeStoreType.MEMORY_NODE_STORE);
//        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
//                editorProvider,
//                new NodeCounterEditorProvider()
//        )));
        this.trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(indexCorruptIntervalInMillis, TimeUnit.MILLISECONDS);
//        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        initialContent = getInitialContent();
        securityProvider = new OpenSecurityProvider();
        indexEditorProvider = new PropertyIndexEditorProvider();
        queryIndexProvider = new NodeTypeIndexProvider();
    }

    protected InitialContent getInitialContent() {
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

    abstract protected RepositoryOptionsUtil build();
// {
//        Oak oak = new Oak(nodeStore)
//                .with(initialContent)
//                .with(securityProvider)
//                .with(editorProvider)
//                .with(indexProvider)
//                .with(indexProvider)
//                .with(queryIndexProvider);
//        if (isAsync) {
//            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
//        }
//        return new ElasticRepositoryOptionsUtil(oak).with(isAsync).with(asyncIndexUpdate);
//    }

    // Override this to provide a different flavour of node store
    // like segment or mongo mk
    // Tests would need to handle the cleanup accordingly.
    // TODO provide a util here so that test classes simply need to mention the type of store they want to create
    // for now, memory store should suffice.

    protected NodeStore createNodeStore(RepositoryOptionsUtil.NodeStoreType memoryNodeStore) {
        switch (memoryNodeStore) {
            case MEMORY_NODE_STORE:
            default:
                return new MemoryNodeStore();
        }
    }


    /*
        Add more setters as and when needed to configure oak as required
     */
    public void setAsync(boolean async) {
        isAsync = async;
    }

    public void setDefaultAsyncIndexingTimeInSeconds(int defaultAsyncIndexingTimeInSeconds) {
        this.defaultAsyncIndexingTimeInSeconds = defaultAsyncIndexingTimeInSeconds;
    }

    public void setIndexCorruptIntervalInMillis(long indexCorruptIntervalInMillis) {
        this.indexCorruptIntervalInMillis = indexCorruptIntervalInMillis;
    }

    public void setNodeStore(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public void setEditorProvider(IndexEditorProvider editorProvider) {
        this.editorProvider = editorProvider;
    }

    public void setIndexProvider(QueryIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    public void setAsyncIndexUpdate(AsyncIndexUpdate asyncIndexUpdate) {
        this.asyncIndexUpdate = asyncIndexUpdate;
    }

    public void setTrackingCorruptIndexHandler(TrackingCorruptIndexHandler trackingCorruptIndexHandler) {
        this.trackingCorruptIndexHandler = trackingCorruptIndexHandler;
    }

    public void setInitialContent(InitialContent initialContent) {
        this.initialContent = initialContent;
    }

    public void setSecurityProvider(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    public void setIndexEditorProvider(IndexEditorProvider indexEditorProvider) {
        this.indexEditorProvider = indexEditorProvider;
    }

    public void setQueryIndexProvider(QueryIndexProvider queryIndexProvider) {
        this.queryIndexProvider = queryIndexProvider;
    }
}
