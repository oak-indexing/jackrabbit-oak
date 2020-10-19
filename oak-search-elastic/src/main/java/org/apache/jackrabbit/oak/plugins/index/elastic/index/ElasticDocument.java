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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.BlobByteSource;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toDoubleArray;

class ElasticDocument {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocument.class);

    private final String path;
    private final Set<String> fulltext;
    private final Set<String> suggest;
    private final List<String> notNullProps;
    private final List<String> nullProps;
    private final Map<String, Object> properties;
    private final Map<String, Object> similarityFields;

    ElasticDocument(String path) {
        this.path = path;
        this.fulltext = new LinkedHashSet<>();
        this.suggest = new LinkedHashSet<>();
        this.notNullProps = new ArrayList<>();
        this.nullProps = new ArrayList<>();
        this.properties = new HashMap<>();
        this.similarityFields = new HashMap<>();
    }

    void addFulltext(String value) {
        fulltext.add(value);
    }

    void addFulltextRelative(String path, String value) {
        addProperty(FieldNames.createFulltextFieldName(path), value);
    }

    void addSuggest(String value) {
        suggest.add(value);
    }

    void notNullProp(String propName) {
        notNullProps.add(propName);
    }

    void nullProp(String propName) {
        nullProps.add(propName);
    }

    // ES for String values (that are not interpreted as date or numbers etc) would analyze in the same
    // field and would index a sub-field "keyword" for non-analyzed value.
    // ref: https://www.elastic.co/blog/strings-are-dead-long-live-strings
    // (interpretation of date etc: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html)
    void addProperty(String fieldName, Object value) {
        properties.put(fieldName, value);
    }

    void addSimField(String name, Blob value) throws IOException{
        byte[] bytes = new BlobByteSource(value).read();
        similarityFields.put(FieldNames.createSimilarityFieldName(name), toDoubleArray(bytes));
    }

    void indexAncestors(String path) {
        String parPath = PathUtils.getParentPath(path);
        int depth = PathUtils.getDepth(path);

        addProperty(FieldNames.ANCESTORS, parPath);
        addProperty(FieldNames.PATH_DEPTH, depth);
    }

    public String build() {
        String ret;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field(FieldNames.PATH, path);
                if (fulltext.size() > 0) {
                    builder.field(FieldNames.FULLTEXT, fulltext);
                }
                if (suggest.size() > 0) {
                    builder.startArray(FieldNames.SUGGEST);
                    for(String val: suggest) {
                        builder.startObject().field("value", val).endObject();
                    }
                    builder.endArray();
                }
                if (notNullProps.size() > 0) {
                    builder.field(FieldNames.NOT_NULL_PROPS, notNullProps);
                }
                if (nullProps.size() > 0) {
                    builder.field(FieldNames.NULL_PROPS, nullProps);
                }
                for (Map.Entry<String, Object> simProp: similarityFields.entrySet()) {
                    builder.field(simProp.getKey(), simProp.getValue());
                }
                for (Map.Entry<String, Object> prop : properties.entrySet()) {
                    builder.field(prop.getKey(), prop.getValue());
                }
            }
            builder.endObject();

            ret = Strings.toString(builder);
        } catch (IOException e) {
            LOG.error("Error serializing document - path: {}, properties: {}, fulltext: {}, suggest: {}, " +
                            "notNullProps: {}, nullProps: {}",
                    path, properties, fulltext, suggest, notNullProps, nullProps, e);
            ret = null;
        }

        return ret;
    }

    @Override
    public String toString() {
        return build();
    }

    public static Collection<Field> newSimilarityFields(String name, Blob value) throws IOException {
        Collection<Field> fields = new ArrayList<>(1);
        byte[] bytes = new BlobByteSource(value).read();
        fields.add(newSimilarityField(name, bytes));
        return fields;
    }

    private static Field newSimilarityField(String name, byte[] bytes) {
        return newSimilarityField(name, ElasticIndexUtils.toDoubleString(bytes));
    }

    private static Field newSimilarityField(String name, String value) {
        return new TextField(FieldNames.createSimilarityFieldName(name), value, Field.Store.YES);
    }
}
