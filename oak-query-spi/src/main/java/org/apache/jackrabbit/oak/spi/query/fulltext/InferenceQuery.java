package org.apache.jackrabbit.oak.spi.query.fulltext;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

public class InferenceQuery {
    private static final String JSON_DELIMITER = "?";

    private final String queryInferenceConfig;
    private final String queryText;

    public InferenceQuery(String text) {
        validateInputText(text);
        String[] components = parseText(text);
        this.queryInferenceConfig = components[0];
        this.queryText = components[1];
    }

    private void validateInputText(String text) {
        if (StringUtils.isBlank(text)) {
            throw new IllegalArgumentException("Input text cannot be null or empty");
        }

        if (!text.startsWith(JSON_DELIMITER)) {
            throw new IllegalArgumentException("Text must start with '?' delimiter");
        }
    }

    private String[] parseText(String text) {
        // Remove the first delimiter
        if (text.startsWith(JSON_DELIMITER) && text.charAt(1) == '{') {
            text = text.substring(1);

            // Try to find the end of the JSON part by parsing incrementally
            int possibleEndIndex = 0;
            String jsonPart = null;
            int jsonEndDelimiterIndex = -1;

            while (possibleEndIndex < text.length()) {
                possibleEndIndex = text.indexOf(JSON_DELIMITER, possibleEndIndex + 1);
                if (possibleEndIndex == -1) {
                    throw new IllegalArgumentException("Could not find valid JSON part ending with '?'");
                }

                String candidateJson = text.substring(0, possibleEndIndex);
                try {
                    // Verify if this is valid JSON using Oak's JsopTokenizer
                    JsopReader reader = new JsopTokenizer(candidateJson);
                    validateJson(reader);
                    jsonPart = candidateJson;
                    jsonEndDelimiterIndex = possibleEndIndex;
                    break;
                } catch (IllegalArgumentException e) {
                    // Not valid JSON yet, continue searching
                    continue;
                }
            }

//            String queryTextPart = null;
            if (jsonPart == null) {
                // If we reach here, it means we couldn't find a valid JSON part
//                String queryTextPart = text.substring(1).trim();
                throw new IllegalArgumentException("Could not find valid JSON part in the input");
            } else {

            }

            // Extract query text part (everything after the JSON part delimiter)
            String queryTextPart = text.substring(jsonEndDelimiterIndex + 1).trim();

            if (StringUtils.isBlank(jsonPart)) {
                throw new IllegalArgumentException("JSON part cannot be empty");
            }

            if (StringUtils.isBlank(queryTextPart)) {
                throw new IllegalArgumentException("Query text part cannot be empty");
            }

            return new String[]{jsonPart, queryTextPart};
        }
        else {
            return new String[]{"", text};
        }
    }

    private void validateJson(JsopReader reader) {
        // Validate JSON structure
        if (reader.matches('{')) {
            // Object
            while (!reader.matches('}')) {
                reader.read(JsopReader.STRING);
                reader.read(':');
                readJsonValue(reader);
                reader.matches(',');
            }
        }
        /*
        // Array support is not needed.
        else if (reader.matches('[')) {
            // Array
            while (!reader.matches(']')) {
                readJsonValue(reader);
                reader.matches(',');
            }
        }*/
         else {
            readJsonValue(reader);
        }

        // Ensure we've reached the end
        if (reader.read() != JsopReader.END) {
            throw new IllegalArgumentException("Invalid JSON: unexpected content after end");
        }
    }

    private void readJsonValue(JsopReader reader) {
        int token = reader.read();
        switch (token) {
            case JsopReader.STRING:
            case JsopReader.NUMBER:
            case JsopReader.TRUE:
            case JsopReader.FALSE:
            case JsopReader.NULL:
                break;
            case '{':
                while (!reader.matches('}')) {
                    reader.read(JsopReader.STRING);
                    reader.read(':');
                    readJsonValue(reader);
                    reader.matches(',');
                }
                break;
            case '[':
                while (!reader.matches(']')) {
                    readJsonValue(reader);
                    reader.matches(',');
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid JSON value");
        }
    }

    public String getQueryInferenceConfig() {
        return queryInferenceConfig;
    }

    public String getQueryText() {
        return queryText;
    }
}