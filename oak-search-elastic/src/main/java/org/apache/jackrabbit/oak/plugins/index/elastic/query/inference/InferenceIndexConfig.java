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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for Inference Index settings.
 * Represents the configuration structure for inference-enabled indexes.
 */
public class InferenceIndexConfig {
    private final static Logger LOG = LoggerFactory.getLogger(InferenceIndexConfig.class.getName());
    public static final InferenceIndexConfig NOOP = new InferenceIndexConfig();
    public static final String TYPE = "inferenceIndexConfig";
    public static final String ENRICHER_CONFIG = "enricherConfig";

    /**
     * The enricher configuration as JSON string.
     */
    private final String enricherConfig;
    /**
     * Map of inference model configurations keyed by their names.
     */
    private final Map<String, InferenceModelConfig> inferenceModelConfigs;

    private InferenceIndexConfig() {
        this.enricherConfig = "{}";
        this.inferenceModelConfigs = Collections.emptyMap();
    }

    public boolean isValid(NodeState nodeState) {
        return true;
//        return nodeState != null && nodeState.exists() &&
//                nodeState.getProperty(InferenceConstants.ENRICHER_CONFIG) != null &&
    }

    public InferenceIndexConfig(NodeState nodeState) {
        if (isValid(nodeState)){
            this.enricherConfig = nodeState.hasProperty(InferenceConstants.ENRICHER_CONFIG) ?
                    nodeState.getProperty(InferenceConstants.ENRICHER_CONFIG).getValue(Type.STRING) : "{}";
            this.inferenceModelConfigs = new HashMap<>();
            // Iterate through child nodes to find inference model configs
            for (String childName : nodeState.getChildNodeNames()) {
                NodeState childNode = nodeState.getChildNode(childName);
                if (isInferenceModelConfig(childNode)) {
                    inferenceModelConfigs.put(childName, new InferenceModelConfig(childName, childNode));
                }
            }
        } else {
            this.enricherConfig = "{}";
            this.inferenceModelConfigs = Collections.emptyMap();
        }
    }

    private boolean isInferenceModelConfig(NodeState nodeState) {
        return nodeState.hasProperty(InferenceConstants.TYPE) &&
                nodeState.getProperty(InferenceConstants.TYPE).getValue(Type.STRING).equals(InferenceModelConfig.TYPE);
    }

    /**
     * @return The enricher configuration JSON string
     */
    public String getEnricherConfig() {
        return enricherConfig;
    }

    /**
     * @return Map of inference model configurations keyed by their names
     */
    public Map<String, InferenceModelConfig> getInferenceModelConfigs() {
        return inferenceModelConfigs;
    }

    /**
     * Gets the default inference model configuration if one exists
     *
     * @return The default InferenceModelConfig.java or null if none is marked as default
     */
    public InferenceModelConfig getDefaultModel() {
        return inferenceModelConfigs.values().stream()
                .filter(InferenceModelConfig::isDefault)
                .findFirst()
                .orElse(null);
    }

    @Override
    public String toString() {
        return TYPE + "{" +
                ENRICHER_CONFIG + "='" + enricherConfig + '\'' +
                ", " + InferenceModelConfig.TYPE + "=" + inferenceModelConfigs +
                '}';
    }
} 