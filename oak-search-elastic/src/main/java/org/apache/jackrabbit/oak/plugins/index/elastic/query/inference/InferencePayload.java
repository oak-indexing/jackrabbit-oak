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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.JsonObjectBuilder;
import org.apache.jackrabbit.commons.json.JsonParser;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.JsonUtil;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for inference payload
 */
public class InferencePayload {
    public static final String DEFAULT_INPUT_KEY = "inputKey";
    public static final String INPUT_KEY = System.getProperty("org.apache.jackrabbit.oak.search.inference.payload.inputKey", DEFAULT_INPUT_KEY);
    private static final Logger log = LoggerFactory.getLogger(InferencePayload.class);
    private final Map<String, Object> inferencePayloadMap;
    NodeBuilder inferencePayloadBuilder;
    String textKeyValue;
    boolean isValidInferencePayload = true;


    public InferencePayload(String inferenceModelName, NodeState nodeState) {
//        inferencePayloadBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
//        copyFirstLevelNodeState(nodeState, inferencePayloadBuilder);
//        if (nodeState.hasProperty(INPUT_KEY)) {
//            if (nodeState.getProperty(INPUT_KEY).getType() == Type.STRING) {
//                textKeyValue = nodeState.getProperty(INPUT_KEY).getValue(Type.STRING);
//            } else if (nodeState.getProperty(INPUT_KEY).getType() == Type.STRINGS) {
//                if (nodeState.getProperty(INPUT_KEY).count() == 1) {
//                    textKeyValue = nodeState.getProperty(INPUT_KEY).getValue(Type.STRINGS).iterator().next();
//                    inferencePayloadBuilder.setProperty(textKeyValue, List.of(), Type.STRINGS);
//                } else {
//                    isValidInferencePayload = false;
//                    log.warn("Inference payload textKey property should be of type String, or String[] with only one value" +
//                            " for modelConfig {}", inferenceModelName);
//                }
//            }
//        } else {
//            isValidInferencePayload = false;
//            log.warn("Inference payload input property {} is missing for modelConfig {}", INPUT_KEY, inferenceModelName);
//        }
//        if (!INPUT_KEY.equals(textKeyValue)){
//            inferencePayloadBuilder.removeProperty(INPUT_KEY);
//        }

        inferencePayloadMap = JsonUtil.convertNodeStateToMap(nodeState, 1,0);
        inferencePayloadMap.remove(INPUT_KEY);
        textKeyValue = "input";

    }

    public boolean isValidInferencePayload() {
        return isValidInferencePayload;
    }

    private static void copyFirstLevelNodeState(NodeState source, NodeBuilder target) {
        // Copy properties
        for (PropertyState property : source.getProperties()) {
            target.setProperty(property);
        }
    }

    /* 
     * Get the inference payload as a json string
     * 
     * @param text
     * @return
     */
    public String getInferencePayload(String text) {

        // This creates a shallow copy - only the map structure is cloned but values are still references
        Map<String, Object> inferencePayloadMapCopy = new HashMap<>(inferencePayloadMap);
        inferencePayloadMapCopy.put(textKeyValue, List.of(text));

//        NodeBuilder inferencePayloadBuilder = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
//        copyFirstLevelNodeState(this.inferencePayloadBuilder.getNodeState(), inferencePayloadBuilder);
//        if (Type.STRING.equals(Objects.requireNonNull(inferencePayloadBuilder.getProperty(textKeyValue)).getType())) {
//            inferencePayloadBuilder.setProperty(textKeyValue, text, Type.STRING);
//        } else {
//            inferencePayloadBuilder.setProperty(textKeyValue, List.of(text), Type.STRINGS);
//        }
//
//
//
//        NodeStateToMapConverter nodeStateToMapConverter = new NodeStateToMapConverter();
//        Map<String, Object> inferencePayloadMap = nodeStateToMapConverter.convert(inferencePayloadBuilder.getNodeState());
        ObjectMapper obj = new ObjectMapper();
        try {

            return obj.writerWithDefaultPrettyPrinter().writeValueAsString(inferencePayloadMapCopy);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
//        return inferencePayloadBuilder.getNodeState().toString();
    }

    private static class NodeStateToMapConverter {

        public static Map<String, Object> convert(NodeState nodeState) {
            Map<String, Object> result = new HashMap<>();

            // Add properties
            for (PropertyState property : nodeState.getProperties()) {
                if (property.isArray()) {
                    result.put(property.getName(), property.getValue(Type.STRINGS));
                } else {
                    result.put(property.getName(), property.getValue(Type.STRING));
                }
            }

            // Add child nodes recursively
            Iterator<String> childNames = nodeState.getChildNodeNames().iterator();
            while (childNames.hasNext()) {
                String childName = childNames.next();
                NodeState childNode = nodeState.getChildNode(childName);
                result.put(childName, convert(childNode));
            }

            return result;
        }
    }
} 