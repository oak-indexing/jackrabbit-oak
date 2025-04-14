package org.apache.jackrabbit.oak.spi.query.fulltext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InferenceQueryConfig {
    //TODO use this in single class
    public static final String TYPE = "inferenceModelConfig";
    private final String inferenceModelConfig;

    public InferenceQueryConfig(String queryConfig) {
        if (queryConfig.isBlank()){
            this.inferenceModelConfig = null;
            return;
        } else if (queryConfig.equals("{}")) {
            this.inferenceModelConfig = "";
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode jsonNode1 = objectMapper.readTree(queryConfig);
                inferenceModelConfig = jsonNode1.get(TYPE).asText();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getInferenceModelConfig() {
        return inferenceModelConfig;
    }
}
