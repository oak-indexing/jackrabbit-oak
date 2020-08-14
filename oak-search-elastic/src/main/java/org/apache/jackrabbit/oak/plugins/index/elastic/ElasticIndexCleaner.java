/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

/**
 * Deletes those remote elastic indexes for which no index definitions exist in repository. The remote indexes are not deleted
 * the first time they are discovered. A dangling remote index is deleted in subsequent runs of this cleaner only after a
 * given threshold of time has passed since the dangling index was first discovered.
 */
public class ElasticIndexCleaner implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexCleaner.class);

    private final ElasticConnection elasticConnection;
    private final NodeStore nodeStore;
    private final String indexPrefix;
    private final Map<String, Long> danglingRemoteIndicesMap;
    private final int threshold;

    /**
     * Constructs a new instance of index cleaner with the given parameters.
     * @param elasticConnection elastic connection to use
     * @param nodeStore node store where index definitions exist
     * @param thresholdInSeconds time in seconds before which a dangling remote index won't be deleted.
     */
    public ElasticIndexCleaner(ElasticConnection elasticConnection, NodeStore nodeStore, int thresholdInSeconds) {
        this.elasticConnection = elasticConnection;
        this.nodeStore = nodeStore;
        this.indexPrefix = elasticConnection.getIndexPrefix();
        danglingRemoteIndicesMap = new HashMap<>();
        this.threshold = thresholdInSeconds;
    }

    public void run() {
        try {
            NodeState root = nodeStore.getRoot();
            InputStream inputStream;
            inputStream = elasticConnection.getClient().getLowLevelClient()
                    .performRequest(new Request("GET", "_cat/indices/" + elasticConnection.getIndexPrefix() + "*"))
                    .getEntity().getContent();
            List<String> remoteIndices = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().map(l -> l.split(" ")[2])
                    .collect(Collectors.toList());
            // remove entry of remote index names which don't exist now
            List<String> externallyDeletedIndices = danglingRemoteIndicesMap.keySet().stream().filter(index -> !remoteIndices.contains(index)).collect(Collectors.toList());
            externallyDeletedIndices.forEach(danglingRemoteIndicesMap::remove);
            if (remoteIndices.isEmpty()) {
                LOG.debug("No remote index found with prefix {}", indexPrefix);
                return;
            }
            Set<String> existingIndices = new HashSet<>();
            root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNodeEntries().forEach(childNodeEntry -> {
                PropertyState typeProperty = childNodeEntry.getNodeState().getProperty(IndexConstants.TYPE_PROPERTY_NAME);
                if (typeProperty != null && typeProperty.getValue(Type.STRING).equals(ElasticIndexDefinition.TYPE_ELASTICSEARCH)) {
                    String indexPath = "/" + INDEX_DEFINITIONS_NAME + "/" + childNodeEntry.getName();
                    String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(indexPrefix, childNodeEntry.getNodeState(), indexPath);
                    if (remoteIndexName != null) {
                        existingIndices.add(remoteIndexName);
                    }
                }
            });
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
            for (String remoteIndexName : remoteIndices) {
                if (!existingIndices.contains(remoteIndexName)) {
                    Long curTime = System.currentTimeMillis();
                    Long oldTime = danglingRemoteIndicesMap.putIfAbsent(remoteIndexName, curTime);
                    if (threshold == 0 || (oldTime != null && curTime - oldTime >= TimeUnit.SECONDS.toMillis(threshold))) {
                        deleteIndexRequest.indices(remoteIndexName);
                        danglingRemoteIndicesMap.remove(remoteIndexName);
                    }
                } else {
                    danglingRemoteIndicesMap.remove(remoteIndexName);
                }
            }
            if (deleteIndexRequest.indices() != null && deleteIndexRequest.indices().length > 0) {
                String indexString = Arrays.toString(deleteIndexRequest.indices());
                AcknowledgedResponse acknowledgedResponse = elasticConnection.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                LOG.info("Deleting remote indices {}", indexString);
                if (!acknowledgedResponse.isAcknowledged()) {
                    LOG.error("Could not delete remote indices " + indexString);
                }
            }
        } catch (IOException e) {
            LOG.error("Could not delete remote indices", e);
        }
    }
}
