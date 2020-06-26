package org.apache.jackrabbit.oak.plugins.index;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticRepositoryOptionsUtil;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class ElasticTestRepositoryBuilder extends TestRepositoryBuilder{

//    private int defaultAsyncIndexingTimeInSeconds = 5;
//    private long indexCorruptIntervalInMillis = 100;
//    private boolean isAsync;
    private ElasticConnection esConnection;
//
//    private NodeStore nodeStore;
//    private IndexEditorProvider editorProvider;
//    private QueryIndexProvider indexProvider;
//    private AsyncIndexUpdate asyncIndexUpdate;
//    private TrackingCorruptIndexHandler trackingCorruptIndexHandler;
//    private InitialContent initialContent;
//    private SecurityProvider securityProvider;
//    private IndexEditorProvider indexEditorProvider;
//    private QueryIndexProvider queryIndexProvider;


    public ElasticTestRepositoryBuilder(ElasticConnectionRule elasticRule) {
        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
//        this.isAsync = isAsync;
        //nodeStore = createNodeStore(RepositoryOptionsUtil.NodeStoreType.MEMORY_NODE_STORE);
        this.editorProvider = (ElasticIndexEditorProvider) getIndexEditorProvider();
        this.indexProvider = new ElasticIndexProvider(esConnection);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));
//        this.trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
//        trackingCorruptIndexHandler.setCorruptInterval(indexCorruptIntervalInMillis, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        //initialContent = getInitialContent();
        //securityProvider = new OpenSecurityProvider();
        //indexEditorProvider = new PropertyIndexEditorProvider();
        //queryIndexProvider = new NodeTypeIndexProvider();

    }

//    private InitialContent getInitialContent() {
//        return new InitialContent() {
//            @Override
//            public void initialize(@NotNull NodeBuilder builder) {
//                super.initialize(builder);
//                // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
//                NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
//                oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());
//            }
//        };
//    }

    public RepositoryOptionsUtil build() {
        Oak oak = new Oak(nodeStore)
                .with(initialContent)
                .with(securityProvider)
                .with(editorProvider)
                .with(indexProvider)
                .with(indexProvider)
                .with(queryIndexProvider);
        if (isAsync) {
            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
        }
        return new ElasticRepositoryOptionsUtil(oak).with(isAsync).with(asyncIndexUpdate);
    }

    private IndexEditorProvider getIndexEditorProvider() {
        return new ElasticIndexEditorProvider(esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }

    // Override this to provide a different flavour of node store
    // like segment or mongo mk
    // Tests would need to handle the cleanup accordingly.
    // TODO provide a util here so that test classes simply need to mention the type of store they want to create
    // for now, memory store should suffice.
}
