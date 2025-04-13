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
package org.apache.jackrabbit.oak.plugins.index.elastic.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Pattern ARRAY_PATTERN = Pattern.compile("([a-zA-Z0-9_\\-]+)?\\[(\\d+)]");

    /**
     * Extracts a value from a JSON string using a custom delimiter and `[n]` array syntax.
     *
     * Example path: "user.orders[0].id"
     *
     * @param json      The JSON string
     * @param path      Path string (e.g., "user.orders[0].id")
     * @param delimiter Delimiter used in the path (e.g., ".")
     * @param <T>       Expected return type
     * @return Extracted value or null
     * @throws Exception on parsing failure
     */
    @SuppressWarnings("unchecked")
    public static <T> T getPropertyValue(String json, String path, String delimiter) throws Exception {
        JsonNode node = mapper.readTree(json);
        String[] segments = path.split(Pattern.quote(delimiter));

        for (String segment : segments) {
            Matcher matcher = ARRAY_PATTERN.matcher(segment);

            // Handle array with or without parent key
            if (matcher.matches()) {
                String key = matcher.group(1); // can be null
                int index = Integer.parseInt(matcher.group(2));

                if (key != null && !key.isEmpty()) {
                    node = node.path(key);
                }

                if (!node.isArray() || index >= node.size()) return null;
                node = node.get(index);

            } else {
                node = node.path(segment);
            }

            if (node.isMissingNode()) return null;
        }

        if (node == null || node.isNull()) return null;
        if (node.isTextual()) return (T) node.asText();
        if (node.isNumber()) return (T) node.numberValue();
        if (node.isBoolean()) return (T) Boolean.valueOf(node.booleanValue());
        if (node.isObject() || node.isArray()) return (T) node;
        return (T) node.toString();
    }
}
