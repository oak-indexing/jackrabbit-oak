package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public abstract class IndexOptions {
    private boolean isAsyncIndex;
    //protected AsyncIndexUpdate asyncIndexUpdate;
    private long indexCorruptIntervalInMillis = 100;
    private int asyncIndexingTimeInSeconds = 5;
    //ElasticConnection esConnection;

    /*public IndexOptions(ElasticConnection esConnection) {
        this.esConnection = esConnection;
    }*/

    public abstract String getIndexType();

    public long getIndexCorruptIntervalInMillis() {
        return indexCorruptIntervalInMillis;
    }

    public void setIndexCorruptIntervalInMillis(long indexCorruptIntervalInMillis) {
        this.indexCorruptIntervalInMillis = indexCorruptIntervalInMillis;
    }

    public int getAsyncIndexingTimeInSeconds() {
        return asyncIndexingTimeInSeconds;
    }

    public void setAsyncIndexingTimeInSeconds(int asyncIndexingTimeInSeconds) {
        this.asyncIndexingTimeInSeconds = asyncIndexingTimeInSeconds;
    }

    public AsyncIndexUpdate getAsyncIndexUpdate(String asyncName, NodeStore store, IndexEditorProvider editorProvider) {
        return new AsyncIndexUpdate(asyncName, store, editorProvider);
    }

    // Override this in extending test class to provide different ExtractedTextCache if needed
    public abstract IndexEditorProvider getIndexEditorProvider();

    public Oak addAsyncIndexingLanesToOak(Oak oak) {
        // Override this in extending classes to configure different
        // indexing lanes with different time limits.
        return oak.withAsyncIndexing("async", asyncIndexingTimeInSeconds);
    }

    protected abstract <T> Tree setIndex(Root root, String idxName, T builder);

//    protected <T>T createIndex(T builder, String... propNames) {
////        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
//        if (!isAsyncIndex()) {
//            builder = builder.noAsync();
//        }
//        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
//        for (String propName : propNames) {
//            indexRule.property(propName).propertyIndex();
//        }
//        return builder;
//    }

    protected abstract <T> T createIndex(T builder, String... propNames);

    protected abstract <T> T createIndexDefinitionBuilder();

    public boolean isAsyncIndex() {
        return isAsyncIndex;
    }

    public void setAsyncIndex(boolean asyncIndex) {
        isAsyncIndex = asyncIndex;
    }

}
