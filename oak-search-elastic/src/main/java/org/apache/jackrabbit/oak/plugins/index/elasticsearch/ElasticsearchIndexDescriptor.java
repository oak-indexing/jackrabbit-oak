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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.client.RestHighLevelClient;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.INVALID_FILENAME_CHARS;

public class ElasticsearchIndexDescriptor {

    private static final int MAX_NAME_LENGTH = 255;

    private static final String INVALID_CHARS_REGEX = Pattern.quote(INVALID_FILENAME_CHARS
            .stream()
            .map(Object::toString)
            .collect(Collectors.joining("")));

    private final ElasticsearchCoordinate coordinate;
    private final String indexName;

    public ElasticsearchIndexDescriptor(IndexDefinition indexDefinition, @NotNull ElasticsearchCoordinate defaultCoordinate) {
        ElasticsearchCoordinate c = build(indexDefinition);
        coordinate = c != null ? c : defaultCoordinate;
        indexName = getRemoteIndexName(indexDefinition);
    }

    public RestHighLevelClient getClient() {
        return ElasticsearchClientFactory.getInstance().getClient(coordinate);
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinate, indexName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchIndexDescriptor that = (ElasticsearchIndexDescriptor) o;
        return coordinate.equals(that.coordinate) &&
                indexName.equals(that.indexName);
    }

    private ElasticsearchCoordinate build(IndexDefinition definition) {
        final NodeState node = definition.getDefinitionNodeState();
        if (node == null
                || !node.hasProperty(ElasticsearchCoordinate.SCHEME_PROP)
                || !node.hasProperty(ElasticsearchCoordinate.HOST_PROP)
                || !node.hasProperty(ElasticsearchCoordinate.PORT_PROP)) {
            return null;
        }

        String scheme = node.getString(ElasticsearchCoordinate.SCHEME_PROP);
        String host = node.getString(ElasticsearchCoordinate.HOST_PROP);
        int port = (int) node.getLong(ElasticsearchCoordinate.PORT_PROP);

        return new ElasticsearchCoordinate(scheme, host, port);
    }

    private String getRemoteIndexName(IndexDefinition definition) {
        String suffix = definition.getUniqueId();

        if (suffix == null) {
            suffix = String.valueOf(definition.getReindexCount());
        }

        return getESSafeIndexName(definition.getIndexPath() + "-" + suffix);
    }

    /**
     * <ul>
     *     <li>abc -> abc</li>
     *     <li>xy:abc -> xyabc</li>
     *     <li>/oak:index/abc -> abc</li>
     * </ul>
     * <p>
     * The resulting file name would be truncated to MAX_NAME_LENGTH
     */
    private static String getESSafeIndexName(String indexPath) {
        String name = StreamSupport
                .stream(PathUtils.elements(indexPath).spliterator(), false)
                .limit(3) //Max 3 nodeNames including oak:index which is the immediate parent for any indexPath
                .filter(p -> !"oak:index".equals(p))
                .map(ElasticsearchIndexDescriptor::getESSafeName)
                .collect(Collectors.joining("_"));

        if (name.length() > MAX_NAME_LENGTH) {
            name = name.substring(0, MAX_NAME_LENGTH);
        }
        return name;
    }

    /**
     * Convert {@code e} to Elasticsearch safe index name.
     * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
     */
    private static String getESSafeName(String suggestedIndexName) {
        return suggestedIndexName.replaceAll(INVALID_CHARS_REGEX, "").toLowerCase();
    }

}
