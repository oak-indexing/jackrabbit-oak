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
import org.elasticsearch.client.RestHighLevelClient;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.common.Strings.INVALID_FILENAME_CHARS;

public class ElasticsearchIndexCoordinateImpl implements ElasticsearchIndexCoordinate {

    private static final int MAX_NAME_LENGTH = 255;

    private final ElasticsearchCoordinate esCoord;
    private final String esIndexName;

    ElasticsearchIndexCoordinateImpl(@NotNull ElasticsearchCoordinate esCoord, IndexDefinition indexDefinition) {
        this.esCoord = esCoord;
        esIndexName = getRemoteIndexName(indexDefinition);
    }

    @Override
    public RestHighLevelClient getClient() {
        return esCoord.getClient();
    }

    @Override
    public String getEsIndexName() {
        return esIndexName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(esCoord, esIndexName);
    }

    @Override
    public boolean equals(Object o) {
        if (! (o instanceof ElasticsearchIndexCoordinateImpl)) {
            return false;
        }
        ElasticsearchIndexCoordinateImpl other = (ElasticsearchIndexCoordinateImpl)o;
        return hashCode() == other.hashCode()
                && esCoord.equals(other.esCoord)
                && esIndexName.equals(other.esIndexName);
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
     *
     * The resulting file name would be truncated to MAX_NAME_LENGTH
     */
    private static String getESSafeIndexName(String indexPath) {
        String name = StreamSupport
                .stream(PathUtils.elements(indexPath).spliterator(), false)
                .limit(3) //Max 3 nodeNames including oak:index which is the immediate parent for any indexPath
                .filter(p -> !"oak:index".equals(p))
                .map(ElasticsearchIndexCoordinateImpl::getESSafeName)
                .collect(Collectors.joining("_"));

        if (name.length() > MAX_NAME_LENGTH){
            name = name.substring(0, MAX_NAME_LENGTH);
        }
        return name;
    }

    /**
     * Convert {@code e} to Elasticsearch safe index name.
     * Ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
     */
    private static String getESSafeName(String suggestedIndexName) {
        String invalidCharsRegex = Pattern.quote(INVALID_FILENAME_CHARS
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining("")));
        return suggestedIndexName.replaceAll(invalidCharsRegex, "").toLowerCase();
    }

}
