/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

public class ElasticPropertyDefinition extends PropertyDefinition {

    SimilaritySearchParameters similaritySearchParameters;

    public ElasticPropertyDefinition(IndexDefinition.IndexingRule idxDefn, String nodeName, NodeState defn) {
        super(idxDefn, nodeName, defn);
        if (this.useInSimilarity) {
            similaritySearchParameters = new SimilaritySearchParameters(getOptionalValue(defn, "L", 20),
                    getOptionalValue(defn, "k", 15), getOptionalValue(defn, "w", 500),
                    getOptionalValue(defn, "model", "lsh"), getOptionalValue(defn, "similarity", "l2"),
                    getOptionalValue(defn, "candidates", 500), getOptionalValue(defn, "probes", 3));
        }
    }

    /**
     * Class for defining parameters for similarity search based on https://elastiknn.com/api
     */
    public static class SimilaritySearchParameters {

        /**
         * Number of hash tables. Generally, increasing this value increases recall.
         */
        private final int L;
        /**
         * Number of hash functions combined to form a single hash value. Generally, increasing this value increases precision.
         */
        private final int k;
        /**
         * Integer bucket width.
         */
        private final int w;
        /**
         * Possible values - lsh, exact
         */
        private final String model;
        /**
         * Possible values l2 (with lsh or exact model), l1 (with exact model), A (angular distance - with exact model)
         */
        private final String similarity;
        /**
         * Take the top vectors with the most matching hashes and compute their exact similarity to the query vector. The candidates parameter
         * controls the number of exact similarity computations. Specifically, we compute exact similarity for the top candidates candidate vectors
         * in each segment. As a reminder, each Elasticsearch index has >= 1 shards, and each shard has >= 1 segments. That means if you set
         * "candiates": 200 for an index with 2 shards, each with 3 segments, then you’ll compute the exact similarity for 2 * 3 * 200 = 1200 vectors.
         * candidates must be set to a number greater or equal to the number of Elasticsearch results you want to get. Higher values generally mean
         * higher recall and higher latency.
         */
        private final int candidates;
        /**
         * Number of probes for using the multiprobe search technique. Default value is zero. Max value is 3^k. Generally, increasing probes will
         * increase recall, will allow you to use a smaller value for L with comparable recall, but introduces some additional computation at query time.
         */
        private final int probes;

        public SimilaritySearchParameters(int l, int k, int w, String model, String similarity, int candidates, int probes) {
            L = l;
            this.k = k;
            this.w = w;
            this.model = model;
            this.similarity = similarity;
            this.candidates = candidates;
            this.probes = probes;
        }

        public int getL() {
            return L;
        }

        public int getK() {
            return k;
        }

        public int getW() {
            return w;
        }

        public String getModel() {
            return model;
        }

        public String getSimilarity() {
            return similarity;
        }

        public int getCandidates() {
            return candidates;
        }

        public int getProbes() {
            return probes;
        }
    }

    public SimilaritySearchParameters getSimilaritySearchParameters() {
        return similaritySearchParameters;
    }
}
