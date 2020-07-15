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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticBulkProcessorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticBulkProcessorHandler.class);
    private final int FAILED_DOC_COUNT_FOR_STATUS_NODE = Integer.getInteger("oak.failedDocStatusLimit", 10000);

    private static final String SYNC_RT_MODE = "rt";

    protected final ElasticConnection elasticConnection;
    protected final ElasticIndexDefinition indexDefinition;
    private final NodeBuilder definitionBuilder;
    protected final BulkProcessor bulkProcessor;
    private final Set<String> failedDocSet = new HashSet<>();

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

    protected final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected long totalOperations = 0;

    private ElasticBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                        @NotNull ElasticIndexDefinition indexDefinition,
                                        @NotNull NodeBuilder definitionBuilder) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.definitionBuilder = definitionBuilder;
        this.bulkProcessor = initBulkProcessor();
    }

    /**
     * Returns an ElasticBulkProcessorHandler instance based on the index definition configuration.
     * <p>
     * The `sync-mode` property can be set to `rt` (real-time). In this case the returned handler will be real-time.
     * This option is available for sync index definitions only.
     */
    public static ElasticBulkProcessorHandler getBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                                                      @NotNull ElasticIndexDefinition indexDefinition,
                                                                      @NotNull NodeBuilder definitionBuilder) {
        PropertyState async = indexDefinition.getDefinitionNodeState().getProperty("async");

        if (async != null) {
            return new ElasticBulkProcessorHandler(elasticConnection, indexDefinition, definitionBuilder);
        }

        PropertyState syncMode = indexDefinition.getDefinitionNodeState().getProperty("sync-mode");
        if (syncMode != null && SYNC_RT_MODE.equals(syncMode.getValue(Type.STRING))) {
            return new RealTimeBulkProcessorHandler(elasticConnection, indexDefinition, definitionBuilder);
        }

        return new ElasticBulkProcessorHandler(elasticConnection, indexDefinition, definitionBuilder);
    }

    private BulkProcessor initBulkProcessor() {
        return BulkProcessor.builder(requestConsumer(),
                new OakBulkProcessorListener())
                .setBulkActions(indexDefinition.bulkActions)
                .setBulkSize(new ByteSizeValue(indexDefinition.bulkSizeBytes))
                .setFlushInterval(TimeValue.timeValueMillis(indexDefinition.bulkFlushIntervalMs))
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                        TimeValue.timeValueMillis(indexDefinition.bulkRetriesBackoff), indexDefinition.bulkRetries)
                )
                .build();
    }

    protected BiConsumer<BulkRequest, ActionListener<BulkResponse>> requestConsumer() {
        return (request, bulkListener) -> elasticConnection.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
    }

    public void add(DocWriteRequest<?> request) {
        bulkProcessor.add(request);
        totalOperations++;
    }

    public boolean close() {
        if (totalOperations <= 0) {
            LOG.debug("No operations executed in this processor. Close immediately");
            return false;
        }
        isClosed.set(true);
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
                if (status.hasProperty(IndexDefinition.FAILED_DOC_PATHS)) {
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
                    LOG.info("Failed Docs count exceeds the persistence limit. Will skip persisting paths of more failed docs." +
                            "Failing docs should be mentioned above under ElasticIndex Update Doc Failure");
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

    /**
     * {@link ElasticBulkProcessorHandler} extension with real time behaviour.
     * It also uses the same async bulk processor as the parent except for the last flush that waits until the
     * indexed documents are searchable.
     */
    protected static class RealTimeBulkProcessorHandler extends ElasticBulkProcessorHandler {

        private boolean isDataSearchable = false;

        private RealTimeBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                             @NotNull ElasticIndexDefinition indexDefinition,
                                             @NotNull NodeBuilder definitionBuilder) {
            super(elasticConnection, indexDefinition, definitionBuilder);
        }

        @Override
        protected BiConsumer<BulkRequest, ActionListener<BulkResponse>> requestConsumer() {
            return (request, bulkListener) -> {
                if (isClosed.get()) {
                    LOG.debug("Processor is closing. Next request with {} actions will block until the data is searchable",
                            request.requests().size());
                    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                    isDataSearchable = true;
                }
                elasticConnection.getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            };
        }

        @Override
        public boolean close() {
            boolean closed = super.close();
            // it could happen that close gets called when the bulk has already been flushed. In these cases we trigger
            // an actual refresh to make sure the docs are searchable before returning from the method
            if (totalOperations > 0 && !isDataSearchable) {
                LOG.debug("Forcing refresh");
                try {
                    this.elasticConnection.getClient()
                            .indices()
                            .refresh(new RefreshRequest(indexDefinition.getRemoteIndexAlias()), RequestOptions.DEFAULT);
                } catch (IOException e) {
                    LOG.warn("Error refreshing index " + indexDefinition.getRemoteIndexAlias(), e);
                }
            }
            return closed;
        }
    }
}
