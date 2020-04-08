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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchConnection;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Cache-based {@code IndexStatistics} implementation providing statistics for Elasticsearch reducing
 * network operations.
 *
 * Statistic values expire after 10 minutes but are refreshed in background when accessed after 1 minute.
 */
class ElasticsearchIndexStatistics implements IndexStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIndexStatistics.class);

    private static final LoadingCache<CountRequestDescriptor, Integer> STATS_CACHE;

    static {

        final Function<CountRequestDescriptor, Integer> count = (CountRequestDescriptor crd) -> {
            CountRequest countRequest = new CountRequest(crd.indexName);
            if (crd.fieldName != null) {
                countRequest.query(QueryBuilders.existsQuery(crd.fieldName));
            } else {
                countRequest.query(QueryBuilders.matchAllQuery());
            }

            try {
                CountResponse response = crd.connection.getClient().count(countRequest, RequestOptions.DEFAULT);
                return (int) response.getCount();
            } catch (IOException e) {
                LOG.warn("Unable to retrieve statistics for index " + crd.indexName, e);
                return 100000;
            }
        };

        STATS_CACHE = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(1L, TimeUnit.MINUTES) // https://github.com/google/guava/wiki/CachesExplained#refresh
                .expireAfterWrite(10L, TimeUnit.MINUTES)
                .build(new CacheLoader<CountRequestDescriptor, Integer>() {
                    @Override
                    public Integer load(CountRequestDescriptor countRequestDescriptor) {
                        return count.apply(countRequestDescriptor);
                    }
                });
    }

    private final ElasticsearchConnection elasticsearchConnection;
    private final ElasticsearchIndexDefinition indexDefinition;

    ElasticsearchIndexStatistics(@NotNull ElasticsearchConnection elasticsearchConnection,
                                 @NotNull ElasticsearchIndexDefinition indexDefinition) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.indexDefinition = indexDefinition;
    }

    @Override
    public int numDocs() {
        return STATS_CACHE.getUnchecked(
                new CountRequestDescriptor(elasticsearchConnection, indexDefinition.getRemoteIndexName(), null)
        );
    }

    @Override
    public int getDocCountFor(String key) {
        return STATS_CACHE.getUnchecked(
                new CountRequestDescriptor(elasticsearchConnection, indexDefinition.getRemoteIndexName(), null)
        );
    }

    private static class CountRequestDescriptor {

        @NotNull
        final ElasticsearchConnection connection;
        @NotNull
        final String indexName;
        @Nullable
        final String fieldName;

        public CountRequestDescriptor(@NotNull ElasticsearchConnection connection,
                                      @NotNull String indexName, @Nullable String fieldName) {
            this.connection = connection;
            this.indexName = indexName;
            this.fieldName = fieldName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountRequestDescriptor that = (CountRequestDescriptor) o;
            return indexName.equals(that.indexName) &&
                    Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, fieldName);
        }
    }
}
