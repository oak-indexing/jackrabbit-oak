package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InferenceQueryConfig {
    private final String inferenceModelConfig;

    public InferenceQueryConfig(String queryConfig) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode jsonNode1 = objectMapper.readTree(queryConfig);
            inferenceModelConfig =  jsonNode1.get(InferenceModelConfig.TYPE).asText();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getInferenceModelConfig() {
        return inferenceModelConfig;
    }
}
