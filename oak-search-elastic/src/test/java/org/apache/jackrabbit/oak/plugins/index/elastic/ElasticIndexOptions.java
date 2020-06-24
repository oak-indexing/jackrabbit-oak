package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexOptions;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public class ElasticIndexOptions extends IndexOptions {
    private boolean isAsyncIndex;
    //protected AsyncIndexUpdate asyncIndexUpdate;
    private long indexCorruptIntervalInMillis = 100;
    private int asyncIndexingTimeInSeconds = 5;
    ElasticConnection esConnection;

    public ElasticIndexOptions(ElasticConnection esConnection) {
        this.esConnection = esConnection;
    }

    @Override
    public String getIndexType() {
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
    }

    //    public long getIndexCorruptIntervalInMillis() {
//        return indexCorruptIntervalInMillis;
//    }
//
//    public void setIndexCorruptIntervalInMillis(long indexCorruptIntervalInMillis) {
//        this.indexCorruptIntervalInMillis = indexCorruptIntervalInMillis;
//    }
//
//    public int getAsyncIndexingTimeInSeconds() {
//        return asyncIndexingTimeInSeconds;
//    }
//
//    public void setAsyncIndexingTimeInSeconds(int asyncIndexingTimeInSeconds) {
//        this.asyncIndexingTimeInSeconds = asyncIndexingTimeInSeconds;
//    }
//
//    protected AsyncIndexUpdate getAsyncIndexUpdate(String asyncName, NodeStore store, IndexEditorProvider editorProvider) {
//        return new AsyncIndexUpdate(asyncName, store, editorProvider);
//    }
//
    // Override this in extending test class to provide different ExtractedTextCache if needed
    public IndexEditorProvider getIndexEditorProvider() {
        return new ElasticIndexEditorProvider(esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }

    public Oak addAsyncIndexingLanesToOak(Oak oak) {
        // Override this in extending classes to configure different
        // indexing lanes with different time limits.
        return oak.withAsyncIndexing("async", asyncIndexingTimeInSeconds);
    }

    @Override
    protected <T>Tree setIndex(Root root, String idxName, T builder) {
        return ((ElasticIndexDefinitionBuilder)builder).build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    @Override
    protected <T> T createIndex(T builder, String... propNames) {
        ElasticIndexDefinitionBuilder elasticIndexDefinitionBuilder = (ElasticIndexDefinitionBuilder) builder;
        if (!isAsyncIndex()) {
            elasticIndexDefinitionBuilder = (ElasticIndexDefinitionBuilder) elasticIndexDefinitionBuilder.noAsync();
        }
        IndexDefinitionBuilder.IndexRule indexRule = elasticIndexDefinitionBuilder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }

    protected ElasticIndexDefinitionBuilder createIndex(ElasticIndexDefinitionBuilder builder, String... propNames) {
//        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        if (!isAsyncIndex()) {
            builder = (ElasticIndexDefinitionBuilder) builder.noAsync();
        }
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }

    protected IndexDefinitionBuilder createIndexDefinitionBuilder() {
        return new ElasticIndexDefinitionBuilder();
    }

//    public boolean isAsyncIndex() {
//        return isAsyncIndex;
//    }
//
//    public void setAsyncIndex(boolean asyncIndex) {
//        isAsyncIndex = asyncIndex;
//    }

}
