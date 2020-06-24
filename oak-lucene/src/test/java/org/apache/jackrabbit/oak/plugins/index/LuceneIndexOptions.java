package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public class LuceneIndexOptions extends IndexOptions {
    private boolean isAsyncIndex;
    //protected AsyncIndexUpdate asyncIndexUpdate;
    private long indexCorruptIntervalInMillis = 100;
    private int asyncIndexingTimeInSeconds = 5;

    @Override
    public String getIndexType() {
        return "lucene";
    }
    // Override this in extending test class to provide different ExtractedTextCache if needed
    public IndexEditorProvider getIndexEditorProvider() {
        return new LuceneIndexEditorProvider();
    }

    public Oak addAsyncIndexingLanesToOak(Oak oak) {
        // Override this in extending classes to configure different
        // indexing lanes with different time limits.
        return oak.withAsyncIndexing("async", asyncIndexingTimeInSeconds);
    }

    protected <T>Tree setIndex(Root root, String idxName, T builder) {
        IndexDefinitionBuilder indexDefinitionBuilder = (IndexDefinitionBuilder) builder;
        return indexDefinitionBuilder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    protected <T> T createIndex(T builder, String... propNames) {
        IndexDefinitionBuilder luceneIndexDefinitionBuilder = (IndexDefinitionBuilder) builder;
        if (!isAsyncIndex()) {
            luceneIndexDefinitionBuilder = luceneIndexDefinitionBuilder.noAsync();
        }
        IndexDefinitionBuilder.IndexRule indexRule = luceneIndexDefinitionBuilder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }

    protected IndexDefinitionBuilder createIndexDefinitionBuilder() {
        return new IndexDefinitionBuilder();
    }

//    public boolean isAsyncIndex() {
//        return isAsyncIndex;
//    }
//
//    public void setAsyncIndex(boolean asyncIndex) {
//        isAsyncIndex = asyncIndex;
//    }

}
