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

/**
 * Configuration class for Inference Model settings.
 */
public class InferenceModelConfig {
    public static final String MODEL = "model";
    public static final String EMBEDDING_SERVICE_URL = "embeddingServiceUrl";
    public static final String SIMILARITY_THRESHOLD = "similarityThreshold";
    public static final String INFERENCE_PAYLOAD = "inferencePayload";
    public static final String INFERENCE_MODEL_CONFIG = "InferenceModelConfig";
    public static final String MIN_TERMS = "minTerms";
    public static final String IS_DEFAULT = "isDefault";
    public static final String ENABLED = "enabled";
    public static final String HEADER = "header";
    private static final Logger log = LoggerFactory.getLogger(InferenceModelConfig.class);

    private final String model;
    private final String embeddingServiceUrl;
    private final double similarityThreshold;
    private final long minTerms;
    private final boolean isDefault;
    private final boolean enabled;
    private final String type;
    private final InferenceHeaderPayload header;
    private final InferencePayload payload;
    private final String inferenceModelName;

    public InferenceModelConfig(String inferenceModelName, NodeState nodeState) {
        this.inferenceModelName = inferenceModelName;
        this.model = nodeState.getProperty(MODEL).getValue(Type.STRING);
        this.embeddingServiceUrl = nodeState.getProperty(EMBEDDING_SERVICE_URL).getValue(Type.STRING);
        this.similarityThreshold = nodeState.getProperty(SIMILARITY_THRESHOLD).getValue(Type.DOUBLE);
        this.minTerms = nodeState.getProperty(MIN_TERMS).getValue(Type.LONG);
        this.isDefault = nodeState.getProperty(IS_DEFAULT).getValue(Type.BOOLEAN);

        this.header = new InferenceHeaderPayload(nodeState.getChildNode(HEADER));
        this.payload = new InferencePayload(inferenceModelName, nodeState.getChildNode(INFERENCE_PAYLOAD));
        this.type = INFERENCE_MODEL_CONFIG;
        if ( payload.isValidInferencePayload() == true) {
            log.warn("Invalid inference payload for modelConfig {}, force disabling this modelConfig", inferenceModelName);
            this.enabled = false;
        }
        else {
            this.enabled = nodeState.getProperty(ENABLED).getValue(Type.BOOLEAN);
        }
    }

    // Getters
    public String getModel() {
        return model;
    }

    public String getEmbeddingServiceUrl() {
        return embeddingServiceUrl;
    }

    public double getSimilarityThreshold() {
        return similarityThreshold;
    }

    public long getMinTerms() {
        return minTerms;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getType() {
        return type;
    }

    public InferenceHeaderPayload getHeader() {
        return header;
    }

    public InferencePayload getPayload() {
        return this.payload;
    }

    @Override
    public String toString() {
        return INFERENCE_MODEL_CONFIG +"{" +
                MODEL +"='" + model + '\'' +
                ", "+ EMBEDDING_SERVICE_URL +"='" + embeddingServiceUrl + '\'' +
                ", "+ SIMILARITY_THRESHOLD + similarityThreshold +
                ", "+ MIN_TERMS +"=" + minTerms +
                ", "+ IS_DEFAULT +"=" + isDefault +
                ", "+ ENABLED +"=" + enabled +
                ", "+ HEADER +"=" + header +
                ", "+ INFERENCE_PAYLOAD +"=" + payload +
                '}';
    }
}