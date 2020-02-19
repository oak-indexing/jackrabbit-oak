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
package org.apache.jackrabbit.oak.index.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexName;

/**
 * Utility that allows to merge index definitions.
 */
public class IndexDefMerger {

    private static HashSet<String> IGNORE_LEVEL_0 = new HashSet<>(Arrays.asList(
            "reindex", "refresh", "seed", "reindexCount"));

    /**
     * Merge index definition changes.
     *
     * @param ancestor the common ancestor (the old product index, e.g. lucene)
     * @param custom the latest customized version (e.g. lucene-custom-1)
     * @param product the latest product index (e.g. lucene-2)
     * @return the merged index definition (e.g. lucene-2-custom-1)
     */
    public static JsonObject merge(JsonObject ancestor, JsonObject custom, JsonObject product) {
        ArrayList<String> conflicts = new ArrayList<>();
        JsonObject merged = merge(0, ancestor, custom, product, conflicts);
        if (!conflicts.isEmpty()) {
            throw new UnsupportedOperationException("Conflicts detected: " + conflicts);
        }
        return merged;
    }

    private static boolean isSame(String a, String b) {
        if (a == null || b == null) {
            return a == b;
        }
        return a.equals(b);
    }

    private static boolean isSame(JsonObject a, JsonObject b) {
        if (a == null || b == null) {
            return a == b;
        }
        return a.toString().equals(b.toString());
    }

    private static JsonObject merge(int level, JsonObject ancestor, JsonObject custom, JsonObject product,
                ArrayList<String> conflicts) {
        JsonObject merged = new JsonObject(true);
        LinkedHashMap<String, Boolean> properties = new LinkedHashMap<>();
        if (ancestor == null) {
            ancestor = new JsonObject();
        }
        for(String p : ancestor.getProperties().keySet()) {
            properties.put(p,  true);
        }
        if (custom == null) {
            custom = new JsonObject();
        }
        for(String p : custom.getProperties().keySet()) {
            properties.put(p,  true);
        }
        if (product == null) {
            product = new JsonObject();
        }
        for(String p : product.getProperties().keySet()) {
            properties.put(p,  true);
        }
        for(String k : properties.keySet()) {
            if (level == 0 && IGNORE_LEVEL_0.contains(k)) {
                // ignore some properties
                continue;
            }
            if (k.startsWith(":")) {
                // ignore hidden properties
                continue;
            }
            String ap = ancestor.getProperties().get(k);
            String cp = custom.getProperties().get(k);
            String pp = product.getProperties().get(k);
            String result;
            if (isSame(ap, pp) || isSame(cp, pp)) {
                result = cp;
            } else if (isSame(ap, cp)) {
                result = pp;
            } else {
                conflicts.add("Could not merge value; property=" + k + "; ancestor=" + ap + "; custom=" + cp + "; product=" + pp);
                result = ap;
            }
            if (result != null) {
                merged.getProperties().put(k, result);
            }
        }
        LinkedHashMap<String, Boolean> children = new LinkedHashMap<>();
        for(String c : ancestor.getChildren().keySet()) {
            children.put(c,  true);
        }
        for(String c : custom.getChildren().keySet()) {
            children.put(c,  true);
        }
        for(String c : product.getChildren().keySet()) {
            children.put(c,  true);
        }
        for(String k : children.keySet()) {
            if (k.startsWith(":")) {
                // ignore hidden nodes
                continue;
            }
            JsonObject a = ancestor.getChildren().get(k);
            JsonObject c = custom.getChildren().get(k);
            JsonObject p = product.getChildren().get(k);
            JsonObject result;
            if (isSame(a, p) || isSame(c, p)) {
                result = c;
            } else if (isSame(a, c)) {
                result = p;
            } else {
                result = merge(level + 1, a, c, p, conflicts);
            }
            if (result != null) {
                merged.getChildren().put(k, result);
            }
        }
        return merged;
    }

    /**
     * For indexes that were modified both by the customer and in the product, merge
     * the changes, and create a new index.
     *
     * The new index (if any) is stored in the "newIndexes" object.
     *
     * @param newIndexes the new indexes
     * @param allIndexes all index definitions (including the new ones)
     */
    public static void merge(JsonObject newIndexes, JsonObject allIndexes) {

        // TODO when merging, we keep the product index, so two indexes are created.
        // e.g. lucene, lucene-custom-1, lucene-2, lucene-2-custom-1
        // but if we don't have lucene-2, then we can't merge lucene-3.

        // TODO when merging, e.g. lucene-2-custom-1 is created. but
        // it is only imported in the read-write repo, not in the read-only
        // repository currently. so this new index won't be used;
        // instead, lucene-2 will be used

        List<IndexName> newNames = newIndexes.getChildren().keySet().stream().map(s -> IndexName.parse(s))
                .collect(Collectors.toList());
        Collections.sort(newNames);
        List<IndexName> allNames = allIndexes.getChildren().keySet().stream().map(s -> IndexName.parse(s))
                .collect(Collectors.toList());
        Collections.sort(allNames);
        HashMap<String, JsonObject> mergedMap = new HashMap<>();
        for (IndexName n : newNames) {
            if (n.getCustomerVersion() == 0) {
                IndexName latest = n.getLatestCustomized(allNames);
                IndexName ancestor = n.getLatestProduct(allNames);
                if (latest != null && ancestor != null) {
                    if (n.compareTo(latest) <= 0 || n.compareTo(ancestor) <= 0) {
                        // ignore older versions of indexes
                        continue;
                    }
                    JsonObject latestCustomized = allIndexes.getChildren().get(latest.getNodeName());
                    JsonObject latestAncestor = allIndexes.getChildren().get(ancestor.getNodeName());
                    JsonObject newProduct = newIndexes.getChildren().get(n.getNodeName());
                    JsonObject merged = merge(latestAncestor, latestCustomized, newProduct);
                    mergedMap.put(n.nextCustomizedName(), merged);
                }
            }
        }
        for (Entry<String, JsonObject> e : mergedMap.entrySet()) {
            newIndexes.getChildren().put(e.getKey(), e.getValue());
        }
    }

}