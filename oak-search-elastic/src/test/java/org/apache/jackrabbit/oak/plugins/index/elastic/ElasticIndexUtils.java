package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public class ElasticIndexUtils {

    // Utility methods accessing directly Elasticsearch

    protected boolean exists(Tree index, ElasticConnection esConnection, NodeStore nodeStore) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index, esConnection, nodeStore);

        try {
            return esConnection.getClient().indices()
                    .exists(new GetIndexRequest(esIdxDef.getRemoteIndexAlias()), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected long countDocuments(Tree index, ElasticConnection esConnection, NodeStore nodeStore) {
        ElasticIndexDefinition esIdxDef = getElasticIndexDefinition(index, esConnection, nodeStore);

        CountRequest request = new CountRequest(esIdxDef.getRemoteIndexAlias());
        try {
            return esConnection.getClient().count(request, RequestOptions.DEFAULT).getCount();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private ElasticIndexDefinition getElasticIndexDefinition(Tree index, ElasticConnection esConnection,
                                                             NodeStore nodeStore) {
        return new ElasticIndexDefinition(
                nodeStore.getRoot(),
                nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(index.getName()),
                index.getPath(),
                esConnection.getIndexPrefix());
    }
}
