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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ElasticsearchClientFactory implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClientFactory.class);

    private static final ElasticsearchClientFactory INSTANCE = new ElasticsearchClientFactory();

    private final ConcurrentMap<ElasticsearchCoordinate, RestHighLevelClient> clientMap = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private ElasticsearchClientFactory() {
    }

    public static ElasticsearchClientFactory getInstance() {
        return INSTANCE;
    }

    public RestHighLevelClient getClient(ElasticsearchCoordinate esCoord) {
        lock.readLock().lock();
        try {
            return clientMap.computeIfAbsent(esCoord, elasticsearchCoordinate -> {
                LOG.info("Creating client {}", elasticsearchCoordinate);
                return new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(elasticsearchCoordinate.getHost(),
                                        elasticsearchCoordinate.getPort(),
                                        elasticsearchCoordinate.getScheme())
                        ));
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            clientMap.values().forEach(restHighLevelClient -> {
                try {
                    restHighLevelClient.close();
                } catch (IOException e) {
                    LOG.error("Error occurred while closing a connection", e);
                }
            });
            clientMap.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
