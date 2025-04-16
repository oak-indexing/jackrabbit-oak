package org.apache.jackrabbit.oak.spi.query.fulltext;

import org.apache.jackrabbit.oak.json.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferenceQuery {
    private static final Logger LOG = LoggerFactory.getLogger(InferenceQuery.class);
    private static final String DEFAULT_INFERENCE_QUERY_CONFIG_PREFIX = "?";
    private static final String INFERENCE_QUERY_CONFIG_PREFIX_KEY = "org.apache.jackrabbit.oak.search.inference.query.prefix";
    private static final String INFERENCE_QUERY_CONFIG_PREFIX = System.getProperty(
            INFERENCE_QUERY_CONFIG_PREFIX_KEY, DEFAULT_INFERENCE_QUERY_CONFIG_PREFIX);

    private final String queryInferenceConfig;
    private final String queryText;

    public InferenceQuery(String text) {
        String[] components = parseText(text);
        this.queryInferenceConfig = components[0];
        this.queryText = components[1];
    }

    private String[] parseText(String inputtext) {
        String text = inputtext.trim();
        // Remove the first delimiter
        if (text.startsWith(INFERENCE_QUERY_CONFIG_PREFIX) && text.charAt(1) == '{') {
            text = text.substring(1);

            // Try to find the end of the JSON part by parsing incrementally
            int possibleEndIndex = 0;
            String jsonPart = null;
            String queryTextPart = null;
            int jsonEndDelimiterIndex = -1;

            while (possibleEndIndex < text.length()) {
                possibleEndIndex = text.indexOf(INFERENCE_QUERY_CONFIG_PREFIX, possibleEndIndex + 1);
                if (possibleEndIndex == -1) {
                    possibleEndIndex = 1;
//                    LOG.warn("Could not find valid JSON part ending with '?'");
                }
                String candidateJson = text.substring(0, possibleEndIndex);
                // Verify if this is valid JSON using Oak's JsopTokenizer
                if (JsonUtils.isValidJson(candidateJson, false)) {
                    jsonPart = candidateJson;
                    jsonEndDelimiterIndex = possibleEndIndex;
                    break;
                } else {
                    continue;
                }
            }

//            String queryTextPart = null;
            if (jsonPart == null) {
                // If we reach here, it means we couldn't find a valid JSON part
                //TODO check if we should use jsonPart as empty or null
                jsonPart = "";
                queryTextPart = text;
                LOG.warn("Query starts with InferenceQueryPrefix: {}, but without valid json part," +
                                " if case this prefix is a valid fulltext query prefix, please update {} with different prefix value",
                        INFERENCE_QUERY_CONFIG_PREFIX, INFERENCE_QUERY_CONFIG_PREFIX_KEY);

            } else {
                // Extract query text part (everything after the JSON part delimiter)
                queryTextPart = text.substring(jsonEndDelimiterIndex + 1).trim();

            }
            return new String[]{jsonPart, queryTextPart};
        } else {
            //TODO check if we should use jsonPart as empty or null
            return new String[]{"", text};
        }
    }

    public String getQueryInferenceConfig() {
        return queryInferenceConfig;
    }

    public String getQueryText() {
        return queryText;
    }
}