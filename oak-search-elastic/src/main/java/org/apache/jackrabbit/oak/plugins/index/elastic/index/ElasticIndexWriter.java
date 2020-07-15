/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticIndexWriter implements FulltextIndexWriter<ElasticDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexWriter.class);

    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private final NodeBuilder definitionBuilder;
    private final int FAILED_DOC_COUNT_FOR_STATUS_NODE = Integer.getInteger("failedDocStatusLimit", 10000);

    /**
     * Coordinates communication between bulk processes. It has a main controller registered at creation time and
     * de-registered on {@link ElasticIndexWriter#close(long)}. Each bulk request register a new party in
     * this Phaser in {@link OakBulkProcessorListener#beforeBulk(long, BulkRequest)} and de-register itself when
     * the request returns.
     */
    private final Phaser phaser = new Phaser(1); // register main controller
    /**
     * Key-value structure to keep the history of bulk requests. Keys are the bulk execution ids, the boolean
     * value is {@code true} when at least an update is performed, otherwise {@code false}.
     */
    private final ConcurrentHashMap<Long, Boolean> updatesMap = new ConcurrentHashMap<>();
    private final Set<String> failedDocSet = new HashSet<>();
    private final BulkProcessor bulkProcessor;

    ElasticIndexWriter(@NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition, @NotNull NodeBuilder definitionBuilder) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.definitionBuilder = definitionBuilder;
        bulkProcessor = initBulkProcessor();
    }

    @TestOnly
    ElasticIndexWriter(@NotNull ElasticConnection elasticConnection,
                       @NotNull ElasticIndexDefinition indexDefinition,
                       @NotNull BulkProcessor bulkProcessor,
                       @NotNull NodeBuilder definitionBuilder) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.definitionBuilder = definitionBuilder;
        this.bulkProcessor = bulkProcessor;
    }

    private BulkProcessor initBulkProcessor() {
        return BulkProcessor.builder((request, bulkListener) ->
                        elasticConnection.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                new OakBulkProcessorListener())
                .setBulkActions(indexDefinition.bulkActions)
                .setBulkSize(new ByteSizeValue(indexDefinition.bulkSizeBytes))
                .setFlushInterval(TimeValue.timeValueMillis(indexDefinition.bulkFlushIntervalMs))
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                                TimeValue.timeValueMillis(indexDefinition.bulkRetriesBackoff), indexDefinition.bulkRetries)
                )
                .build();
    }

    @Override
    public void updateDocument(String path, ElasticDocument doc) {
        IndexRequest request = new IndexRequest(indexDefinition.getRemoteIndexAlias())
                .id(ElasticIndexUtils.idFromPath(path))
                .source(doc.build(), XContentType.JSON);
        bulkProcessor.add(request);
    }

    @Override
    public void deleteDocuments(String path) {
        DeleteRequest request = new DeleteRequest(indexDefinition.getRemoteIndexAlias())
                .id(ElasticIndexUtils.idFromPath(path));
        bulkProcessor.add(request);
    }

    @Override
    public boolean close(long timestamp) {
        LOG.trace("Calling close on bulk processor {}", bulkProcessor);
        bulkProcessor.close();
        LOG.trace("Bulk Processor {} closed", bulkProcessor);

        // de-register main controller
        final int phase = phaser.arriveAndDeregister();

        try {
            phaser.awaitAdvanceInterruptibly(phase, indexDefinition.bulkFlushIntervalMs * 5, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
            LOG.error("Error waiting for bulk requests to return", e);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Bulk identifier -> update status = {}", updatesMap);
        }
        return updatesMap.containsValue(Boolean.TRUE);
    }

    protected void provisionIndex() throws IOException {
        // check if index already exists
        boolean exists = elasticConnection.getClient().indices().exists(
                new GetIndexRequest(indexDefinition.getRemoteIndexName()), RequestOptions.DEFAULT
        );
        if (exists) {
            LOG.info("Index {} already exists. Skip index provision", indexDefinition.getRemoteIndexName());
            return;
        }

        final IndicesClient indicesClient = elasticConnection.getClient().indices();
        final String indexName = indexDefinition.getRemoteIndexName();

        // create the new index
        final CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(indexDefinition);
        try {
            if (LOG.isDebugEnabled()) {
                final String requestMsg = Strings.toString(request.toXContent(jsonBuilder(), EMPTY_PARAMS));
                LOG.debug("Creating Index with request {}", requestMsg);
            }
            CreateIndexResponse response = indicesClient.create(request, RequestOptions.DEFAULT);
            LOG.info("Updated settings for index {}. Response acknowledged: {}",
                    indexDefinition.getRemoteIndexName(), response.isAcknowledged());
            checkResponseAcknowledgement(response, "Create index call not acknowledged for index " + indexName);
        } catch (ElasticsearchStatusException ese) {
            // We already check index existence as first thing in this method, if we get here it means we have got into
            // a conflict (eg: multiple cluster nodes provision concurrently).
            // Elasticsearch does not have a CREATE IF NOT EXIST, need to inspect exception
            // https://github.com/elastic/elasticsearch/issues/19862
            if (ese.status().getStatus() == 400 && ese.getDetailedMessage().contains("resource_already_exists_exception")) {
                LOG.warn("Index {} already exists. Ignoring error", indexName);
            } else throw ese;
        }

        // update the mapping
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest(indexDefinition.getRemoteIndexAlias());
        GetAliasesResponse aliasesResponse = indicesClient.getAlias(getAliasesRequest, RequestOptions.DEFAULT);
        Map<String, Set<AliasMetadata>> aliases = aliasesResponse.getAliases();
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        for (String oldIndexName : aliases.keySet()) {
            IndicesAliasesRequest.AliasActions removeAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
            removeAction.index(oldIndexName).alias(indexDefinition.getRemoteIndexAlias());
            indicesAliasesRequest.addAliasAction(removeAction);
        }
        IndicesAliasesRequest.AliasActions addAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
        addAction.index(indexName).alias(indexDefinition.getRemoteIndexAlias());
        indicesAliasesRequest.addAliasAction(addAction);
        AcknowledgedResponse updateAliasResponse = indicesClient.updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        checkResponseAcknowledgement(updateAliasResponse, "Update alias call not acknowledged for alias "
                + indexDefinition.getRemoteIndexAlias());
        LOG.info("Updated alias {} to index {}. Response acknowledged: {}", indexDefinition.getRemoteIndexAlias(),
                indexName, updateAliasResponse.isAcknowledged());

        // once the alias has been updated, we can safely remove the old index
        deleteOldIndices(indicesClient, aliases.keySet());
    }

    private void checkResponseAcknowledgement(AcknowledgedResponse response, String exceptionMessage) {
        if (!response.isAcknowledged()) {
            throw new IllegalStateException(exceptionMessage);
        }
    }

    private void deleteOldIndices(IndicesClient indicesClient, Set<String> indices) throws IOException {
        if (indices.size() == 0)
            return;
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        for (String oldIndexName : indices) {
            deleteIndexRequest.indices(oldIndexName);
        }
        AcknowledgedResponse deleteIndexResponse = indicesClient.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        checkResponseAcknowledgement(deleteIndexResponse, "Delete index call not acknowledged for indices " + indices);
        LOG.info("Deleted indices {}. Response acknowledged: {}", indices.toString(), deleteIndexResponse.isAcknowledged());
    }

    private class OakBulkProcessorListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest bulkRequest) {
            // register new bulk party
            phaser.register();
            // init update status
            updatesMap.put(executionId, Boolean.FALSE);

            LOG.debug("Sending bulk with id {} -> {}", executionId, bulkRequest.getDescription());
            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk Requests: \n{}", bulkRequest.requests()
                                .stream()
                                .map(DocWriteRequest::toString)
                                .collect(Collectors.joining("\n"))
                );
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
            LOG.debug("Bulk with id {} processed with status {} in {}", executionId, bulkResponse.status(), bulkResponse.getTook());
            if (LOG.isTraceEnabled()) {
                try {
                    LOG.trace(Strings.toString(bulkResponse.toXContent(jsonBuilder(), EMPTY_PARAMS)));
                } catch (IOException e) {
                    LOG.error("Error decoding bulk response", e);
                }
            }
            if (bulkResponse.hasFailures()) { // check if some operations failed to execute

                NodeBuilder status = definitionBuilder.child(IndexDefinition.STATUS_NODE);
                // Read the current failed paths (if any) on the :status node into failedDocList
                if (status.getProperty(IndexDefinition.FAILED_DOC_PATHS) != null) {
                    for (String str : status.getProperty(IndexDefinition.FAILED_DOC_PATHS).getValue(Type.STRINGS)) {
                        failedDocSet.add(str);
                    }
                }

                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        failedDocSet.add(bulkItemResponse.getId());
                        // Log entry to be used to parse logs to get the failed doc id/path if needed
                        LOG.error("ElasticIndex Update Doc Failure: Error while adding/updating doc with id : [{}]", bulkItemResponse.getId());
                        LOG.error("Failure Details: BulkItem ID: " + failure.getId() + ", Failure Cause: {}", failure.getCause());
                    } else {
                        // Set indexUpdated to true even if 1 item was updated successfully
                        updatesMap.put(executionId, Boolean.TRUE);
                    }
                }

                if (failedDocSet.size() > FAILED_DOC_COUNT_FOR_STATUS_NODE) {
                    LOG.debug("Failed Docs count exceeds the persistence limit. Will skip persisting paths of more failed docs." +
                            "Please analyze logs to identify the failing docs. Search for ElasticIndex Update Doc Failure");
                } else {
                    status.setProperty(IndexDefinition.FAILED_DOC_PATHS, failedDocSet, Type.STRINGS);
                }

            } else {
                updatesMap.put(executionId, Boolean.TRUE);
            }
            phaser.arriveAndDeregister();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
            LOG.error("ElasticIndex Update Bulk Failure : Bulk with id {} threw an error", executionId, throwable);
            phaser.arriveAndDeregister();
        }
    }

}
