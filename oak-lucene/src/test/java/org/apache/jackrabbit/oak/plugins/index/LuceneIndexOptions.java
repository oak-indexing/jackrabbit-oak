package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;

public class LuceneIndexOptions extends IndexOptions {
    private boolean isAsyncIndex;
    //protected AsyncIndexUpdate asyncIndexUpdate;
    private long indexCorruptIntervalInMillis = 100;
    private int asyncIndexingTimeInSeconds = 5;

    @Override
    public String getIndexType() {
        return "lucene";
    }

    protected IndexDefinitionBuilder createIndexDefinitionBuilder() {
        return new LuceneIndexDefinitionBuilder();
    }

}
