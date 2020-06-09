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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets;

import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResponseListener;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ElasticStatisticalFacetAsyncProvider
        extends ElasticSecureFacetAsyncProvider
        implements ElasticResponseListener.AggregationListener {

    private final int sampleSize;
    private long totalHits;

    private final Random rGen;
    private int sampled = 0;
    private int seen = 0;
    private long accessibleCount = 0;

    private final Map<String, List<FulltextIndex.Facet>> facetMap = new HashMap<>();

    private final CountDownLatch latch = new CountDownLatch(1);

    ElasticStatisticalFacetAsyncProvider(QueryIndex.IndexPlan indexPlan,
                                         ElasticRequestHandler elasticRequestHandler,
                                         ElasticResponseHandler elasticResponseHandler,
                                         long randomSeed, int sampleSize) {
        super(indexPlan, elasticRequestHandler, elasticResponseHandler);
        this.sampleSize = sampleSize;
        this.rGen = new Random(randomSeed);
    }

    @Override
    public void startData(long totalHits) {
        this.totalHits = totalHits;
    }

    @Override
    public void on(SearchHit searchHit) {
        if (totalHits < sampleSize) {
            super.on(searchHit);
        } else {
            if (sampleSize == sampled) {
                return;
            }
            int r = rGen.nextInt((int) (totalHits - seen)) + 1;
            seen++;

            if (r <= sampleSize - sampled) {
                sampled++;
                final String path = elasticResponseHandler.getPath(searchHit);
                if (indexPlan.getFilter().isAccessible(path)) {
                    accessibleCount++;
                }
            }
        }
    }

    @Override
    public void on(Aggregations aggregations) {
        for (String field: facetFields) {
            Terms terms = aggregations.get(field);
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            final List<FulltextIndex.Facet> facetList = new ArrayList<>();
            for (Terms.Bucket bucket : buckets) {
                facetList.add(new FulltextIndex.Facet(bucket.getKeyAsString(), (int) bucket.getDocCount()));
            }
            facetMap.put(field, facetList);
        }
    }

    @Override
    public void endData() {
        if (totalHits < sampleSize) {
            super.endData();
        } else {
            for (String facet: facetMap.keySet()) {
                facetMap.compute(facet, (s, facets1) -> updateLabelAndValueIfRequired(facets1));
            }
            latch.countDown();
        }
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        if (totalHits < sampleSize) {
            return super.getFacets(numberOfFacets, columnName);
        } else {
            LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
            try {
                latch.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Error while waiting for facets", e);
            }
            LOG.trace("Reading facets for {} from {}", columnName, facetMap);
            return facetMap.get(FulltextIndex.parseFacetField(columnName));
        }
    }

    private List<FulltextIndex.Facet> updateLabelAndValueIfRequired(List<FulltextIndex.Facet> labelAndValues) {
        if (accessibleCount < sampleSize) {
            int numZeros = 0;
            List<FulltextIndex.Facet> newValues;
            {
                List<FulltextIndex.Facet> proportionedLVs = new LinkedList<>();
                for (FulltextIndex.Facet labelAndValue : labelAndValues) {
                    long count = labelAndValue.getCount() * accessibleCount / sampleSize;
                    if (count == 0) {
                        numZeros++;
                    }
                    proportionedLVs.add(new FulltextIndex.Facet(labelAndValue.getLabel(), Math.toIntExact(count)));
                }
                labelAndValues = proportionedLVs;
            }
            if (numZeros > 0) {
                newValues = new LinkedList<>();
                for (FulltextIndex.Facet lv : labelAndValues) {
                    if (lv.getCount() > 0) {
                        newValues.add(lv);
                    }
                }
            } else {
                newValues = labelAndValues;
            }
            return newValues;
        } else {
            return labelAndValues;
        }
    }
}
