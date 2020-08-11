package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.plugins.index.IndexingLaneException;
import org.apache.jackrabbit.oak.plugins.index.IndexingLaneTask;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexTracker;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ElasticIndexCleaner implements IndexingLaneTask {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexCleaner.class);

    private final ElasticConnection elasticConnection;
    private final ElasticIndexProvider elasticIndexProvider;

    public ElasticIndexCleaner(ElasticConnection elasticConnection, ElasticIndexProvider elasticIndexProvider) {
        this.elasticConnection = elasticConnection;
        this.elasticIndexProvider = elasticIndexProvider;
    }

    @Override
    public String getLaneName() {
        return "elastic-async";
    }

    @Override
    public void execute() throws IndexingLaneException {
        ElasticIndexTracker indexTracker= elasticIndexProvider.getElasticIndexTracker();
        if (EMPTY_NODE.equals(indexTracker.getRoot())) {
            LOG.debug("Index tracker not yet updated. Returning.");
            return;
        }
        InputStream inputStream;
        try {
            inputStream = elasticConnection.getClient().getLowLevelClient()
                    .performRequest(new Request("GET", "_cat/indices/" + elasticConnection.getIndexPrefix() + "*"))
                    .getEntity().getContent();
        } catch (IOException e) {
            throw new IndexingLaneException("Could not obtain index names from elasticsearch server", e);
        }
        List<String> indices = new BufferedReader(new InputStreamReader(inputStream))
                .lines().map(l -> l.split(" ")[2])
                .collect(Collectors.toList());
        Set<String> existingIndices = new HashSet<>();
        for (String indexPath : indexTracker.getIndexNodePaths()) {
            ElasticIndexDefinition indexDefinition = (ElasticIndexDefinition) indexTracker.getIndexDefinition(indexPath);
            if (indexDefinition != null) {
                existingIndices.add(indexDefinition.getRemoteIndexName());
            } else {
                LOG.warn("Found no index definition for {}", indexPath);
            }
        }

        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        indices.stream().filter(path -> !existingIndices.contains(path)).forEach(deleteIndexRequest::indices);
        if (deleteIndexRequest.indices() != null && deleteIndexRequest.indices().length > 0) {
            String indexString = Arrays.toString(deleteIndexRequest.indices());
            try {
                AcknowledgedResponse acknowledgedResponse = elasticConnection.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                LOG.info("Deleting remote indices {}", indexString);
                if (!acknowledgedResponse.isAcknowledged()) {
                    throw new IndexingLaneException("Could not delete remote indices " + indexString);
                }
            } catch (IOException e) {
                throw new IndexingLaneException("Could not delete remote indices " + indexString, e);
            }
        }
    }
}
