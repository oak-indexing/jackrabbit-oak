package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.RepositoryOptionsUtil;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
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

public class ElasticRepositoryOptionsUtil extends RepositoryOptionsUtil {

    private int defaultAsyncIndexingTimeInSeconds = 5;
    private long indexCorruptIntervalInMillis = 100;
    private boolean isAsync;
    private static String elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
    private ElasticConnection esConnection;

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    public ElasticRepositoryOptionsUtil(NodeStoreType nodeStoreType, boolean isAsync) {
        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.isAsync = isAsync;
        nodeStore = createNodeStore(nodeStoreType);
        createDefaultOak(nodeStoreType, nodeStore, isAsync);
    }

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

    protected void createDefaultOak(NodeStoreType nodeStoreType, NodeStore nodeStore, boolean isAsync) {
        ElasticIndexEditorProvider editorProvider = (ElasticIndexEditorProvider) getIndexEditorProvider();
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(esConnection);
        AsyncIndexUpdate asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));
        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(indexCorruptIntervalInMillis, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        Oak oak = new Oak(nodeStore)
                .with(getInitialContent())
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());
        if (isAsync) {
            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
        }
        this.oak = oak;
    }

    private IndexEditorProvider getIndexEditorProvider() {
        return new ElasticIndexEditorProvider(esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }

    public boolean isAsync() {
        return isAsync;
    }
}
