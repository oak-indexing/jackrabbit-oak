package org.apache.jackrabbit.oak.commons.json;

public class JsonUtils {

    public static boolean isValidJson(String text, boolean isJsonArray) {
        if (text == null) {
            return false;
        }
        
        JsopReader reader = new JsopTokenizer(text);
        return validateJson(reader, isJsonArray);
    }

    private static boolean validateJson(JsopReader reader, boolean isJsonArray) {
        // Validate JSON structure
        if (reader.matches('{')) {
            // Object
            boolean first = true;
            while (!reader.matches('}')) {
                if (!first && !reader.matches(',')) {
                    return false;
                }
                if (!reader.matches(JsopReader.STRING)) {
                    return false;
                }
                if (!reader.matches(':')) {
                    return false;
                }
                if (!readJsonValue(reader)) {
                    return false;
                }
                first = false;
            }
        }
        else if (reader.matches('[')) {
            if (!isJsonArray) {
                return false;
            }
            // Array
            boolean first = true;
            while (!reader.matches(']')) {
                if (!first && !reader.matches(',')) {
                    return false;
                }
                if (!readJsonValue(reader)) {
                    return false;
                }
                first = false;
            }
        }
        else {
            if (!readJsonValue(reader)) {
                return false;
            }
        }

        // Ensure we've reached the end
        return reader.read() == JsopReader.END;
    }

    private static boolean readJsonValue(JsopReader reader) {
        int token = reader.read();
        switch (token) {
            case JsopReader.STRING:
            case JsopReader.NUMBER:
            case JsopReader.TRUE:
            case JsopReader.FALSE:
            case JsopReader.NULL:
                return true;
            case '{':
                boolean firstObject = true;
                while (!reader.matches('}')) {
                    if (!firstObject && !reader.matches(',')) {
                        return false;
                    }
                    if (!reader.matches(JsopReader.STRING)) {
                        return false;
                    }
                    if (!reader.matches(':')) {
                        return false;
                    }
                    if (!readJsonValue(reader)) {
                        return false;
                    }
                    firstObject = false;
                }
                return true;
            case '[':
                boolean firstArray = true;
                while (!reader.matches(']')) {
                    if (!firstArray && !reader.matches(',')) {
                        return false;
                    }
                    if (!readJsonValue(reader)) {
                        return false;
                    }
                    firstArray = false;
                }
                return true;
            default:
                return false;
        }
    }
}
