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
package org.apache.jackrabbit.oak.query.ast;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * Service for reranking search results using Metarank.
 * This service takes the results from Elasticsearch and sends them to Metarank
 * for reranking based on personalization, business rules, or other factors.
 */
public class RerankService {
    private static final Logger LOG = LoggerFactory.getLogger(RerankService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    // Configurable Metarank endpoint
    private static String METARANK_ENDPOINT = System.getProperty(
        "org.apache.jackrabbit.oak.plugins.index.elastic.metarank.endpoint",
        "http://localhost:8080/rank/xgboost");

    // Configurable Metarank model name
//    private static String METARANK_MODEL = System.getProperty(
//        "org.apache.jackrabbit.oak.plugins.index.elastic.metarank.model",
//        "xgboost");

    // Configurable timeout for Metarank API calls in milliseconds
    private static long METARANK_TIMEOUT_MS = Long.parseLong(System.getProperty(
        "org.apache.jackrabbit.oak.plugins.index.elastic.metarank.timeout",
        "1000"));

    /**
     * Reranks the search results using Metarank.
     *
     * @param items The list of search results to be reranked
     * @return A new list with reranked results
     */
    public static ArrayList<IndexRow> reRank(String userId, ArrayList<IndexRow> items) {
        try {
            LOG.debug("Starting reranking of {} search results using Metarank", items.size());

            // If list is empty or has only one item, no need to rerank
            if (items.isEmpty() || items.size() == 1) {
                LOG.debug("No need to rerank {} items", items.size());
                return items;
            }

            // Prepare data for Metarank
            List<IndexRow> rerankedItems = callMetarankService(userId, items);

            LOG.debug("Completed reranking of search results using Metarank");
            return new ArrayList<>(rerankedItems);
        } catch (Exception e) {
            LOG.error("Error during reranking with Metarank, returning original results", e);
            // In case of error, return the original items
            return items;
        }
    }

    /**
     * Calls the Metarank service to rerank the search results.
     *
     * @param items The list of search results to be reranked
     * @return A list of reranked search results
     */
    private static List<IndexRow> callMetarankService(String userId, List<IndexRow> items) {
        try {
            // Create the request payload for Metarank
            ObjectNode requestBody = createMetarankRequest(userId, items);

            // Call Metarank API
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(METARANK_ENDPOINT))
                .timeout(Duration.ofMillis(METARANK_TIMEOUT_MS))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody.toString()))
                .build();

            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                LOG.warn("Metarank returned non-200 status code: {}, body: {}", response.statusCode(), response.body());
                return items; // Return original items if reranking failed
            }

            // Process the response and rerank items
            return processMetarankResponse(response.body(), items);
        } catch (IOException | InterruptedException e) {
            LOG.error("Failed to call Metarank service", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return items; // Return original items if reranking failed
        }
    }

    /**
     * Creates a request payload for the Metarank API.
     *
     * @param items The list of search results to be reranked
     * @return A JSON object representing the Metarank request
     */
    private static ObjectNode createMetarankRequest(String userId, List<IndexRow> items) {
        ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
//
//        // Set the model to use for reranking
//        requestBody.put("model", METARANK_MODEL);
//
//        // Create the items array
//        ArrayNode itemsArray = requestBody.putArray("items");
//        itemsArray.addObject()
//            .put("userId", userId);
//
//        // Add each item to the request
//        for (int i = 0; i < items.size(); i++) {
//            IndexRow row = items.get(i);
//            ObjectNode item = itemsArray.addObject();
//            item.put("id", row.getPath());
//
//            // Get score if available
//            PropertyValue scoreValue = row.getValue(QueryConstants.JCR_SCORE);
//            double score = scoreValue != null ? Double.parseDouble(scoreValue.getValue(org.apache.jackrabbit.oak.api.Type.STRING)) : 1.0;
//            item.put("score", score);
//
//            // Add excerpts as features if available
//            PropertyValue excerptValue = row.getValue(QueryConstants.REP_EXCERPT);
//            if (excerptValue != null) {
//                ObjectNode features = item.putObject("features");
//                features.put("excerpt", excerptValue.getValue(org.apache.jackrabbit.oak.api.Type.STRING));
//            }
//        }

        // Set event type
        requestBody.put("event", "ranking");

        // Add a unique ID (UUID) for the ranking event
        requestBody.put("id", UUID.randomUUID().toString());

        // Add current timestamp in milliseconds
        requestBody.put("timestamp", String.valueOf(System.currentTimeMillis()));

        // Add user ID
        requestBody.put("user", userId);

        // Add items array
        ArrayNode itemsArray = requestBody.putArray("items");
        for (IndexRow row : items) {
            ObjectNode item = itemsArray.addObject();
            item.put("id", row.getPath());  // assumes getPath() gives the unique content id
        }

        return requestBody;
    }

    /**
     * Processes the response from the Metarank API and reranks the items accordingly.
     *
     * @param responseBody  The response body from the Metarank API
     * @param originalItems The original list of search results
     * @return A list of reranked search results
     */
//    private static List<IndexRow> processMetarankResponse(String responseBody, List<IndexRow> originalItems) {
//        try {
//            // Parse the response
//            ObjectNode responseJson = (ObjectNode) OBJECT_MAPPER.readTree(responseBody);
//            ArrayNode rankedItems = (ArrayNode) responseJson.get("items");
//
//            if (rankedItems == null || rankedItems.size() == 0) {
//                LOG.warn("Metarank returned empty or invalid response: {}", responseBody);
//                return originalItems;
//            }
//
//            // Create a map of path to new score
//            Map<String, Double> newScores = new HashMap<>();
//            for (int i = 0; i < rankedItems.size(); i++) {
//                ObjectNode item = (ObjectNode) rankedItems.get(i);
//                String id = item.get("id").asText();
//                double score = item.get("score").asDouble();
//                newScores.put(id, score);
//            }
//
//            // Update scores and sort items
//            List<IndexRow> rerankedItems = new ArrayList<>(originalItems.size());
//
//            // For each original item, create a new one with the updated score
//            for (IndexRow originalRow : originalItems) {
//                Double newScore = newScores.getOrDefault(originalRow.getPath(), 1.0);
//                // Create a new IndexRow with the updated score but keep all other properties the same
//                IndexRow newRow = new RerankedIndexRow(originalRow, newScore);
//                rerankedItems.add(newRow);
//            }
//
//            // Sort by score in descending order
//            rerankedItems.sort(Comparator.comparing((IndexRow row) -> {
//                PropertyValue scoreValue = row.getValue(QueryConstants.JCR_SCORE);
//                return scoreValue != null ? Double.parseDouble(scoreValue.getValue(org.apache.jackrabbit.oak.api.Type.STRING)) : 0.0;
//            }).reversed());
//
//            return rerankedItems;
//        } catch (Exception e) {
//            LOG.error("Failed to process Metarank response", e);
//            return originalItems; // Return original items if processing failed
//        }
//    }

    private static List<IndexRow> processMetarankResponse(String responseBody, List<IndexRow> originalItems) {
        try {
            ObjectNode responseJson = (ObjectNode) OBJECT_MAPPER.readTree(responseBody);
            ArrayNode rankedItems = (ArrayNode) responseJson.get("items");

            if (rankedItems == null || rankedItems.size() == 0) {
                LOG.warn("Metarank returned empty or invalid response: {}", responseBody);
                return originalItems;
            }

            //  Extract reranked items with score > 0.0
            LinkedHashMap<String, Double> rerankedScores = new LinkedHashMap<>();
            for (JsonNode itemNode : rankedItems) {
                String id = itemNode.get("item").asText();
                double score = itemNode.get("score").asDouble();
                if (score > 0.0) {
                    rerankedScores.put(id, score); // maintain order
                }
            }

            //  map from path to original item
            Map<String, IndexRow> originalMap = new HashMap<>();
            for (IndexRow row : originalItems) {
                originalMap.put(row.getPath(), row);
            }

            //  Add reranked items to final list first
            List<IndexRow> finalList = new ArrayList<>();
            Set<String> rerankedIds = new HashSet<>();
            for (String id : rerankedScores.keySet()) {
                IndexRow originalRow = originalMap.get(id);
                if (originalRow != null) {
                    finalList.add(originalRow);
                    rerankedIds.add(id);
                }
            }

            //  Add original items that were not reranked (or scored 0)
            for (IndexRow row : originalItems) {
                if (!rerankedIds.contains(row.getPath())) {
                    finalList.add(row);
                }
            }

            return finalList;

        } catch (Exception e) {
            LOG.error("Failed to process Metarank response", e);
            return originalItems;
        }
    }


    /**
     * A simple implementation of IndexRow that wraps another IndexRow but with a different score.
     */
    private static class RerankedIndexRow implements IndexRow {
        private final IndexRow original;
        private final double score;

        RerankedIndexRow(IndexRow original, double score) {
            this.original = original;
            this.score = score;
        }

        @Override
        public boolean isVirtualRow() {
            return original.isVirtualRow();
        }

        @Override
        public String getPath() {
            return original.getPath();
        }

        @Override
        public PropertyValue getValue(String columnName) {
            if (QueryConstants.JCR_SCORE.equals(columnName)) {
                return PropertyValues.newDouble(score);
            }
            return original.getValue(columnName);
        }
    }

    /**
     * A simple Cursor implementation that iterates over a list of IndexRows.
     */
    private static class ListCursor implements Cursor {
        private final List<IndexRow> items;
        private int position = 0;

        ListCursor(List<IndexRow> items) {
            this.items = items;
        }

        @Override
        public boolean hasNext() {
            return position < items.size();
        }

        @Override
        public IndexRow next() {
            return items.get(position++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSize(org.apache.jackrabbit.oak.api.Result.SizePrecision precision, long max) {
            return items.size();
        }
    }
}