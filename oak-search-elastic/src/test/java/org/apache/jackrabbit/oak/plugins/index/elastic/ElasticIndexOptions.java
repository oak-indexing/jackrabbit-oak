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

    @Override
    public String getIndexType() {
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
    }

    protected IndexDefinitionBuilder createIndexDefinitionBuilder() {
        return new ElasticIndexDefinitionBuilder();
    }
}
