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
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@link ElasticFacetProvider} that subscribes to Elastic SearchHit events to return only accessible facets.
 */
class ElasticSecureFacetAsyncProvider implements ElasticFacetProvider, ElasticResponseListener.SearchHitListener {

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticSecureFacetAsyncProvider.class);

    protected final Set<String> facetFields;
    private final Map<String, List<FulltextIndex.Facet>> facets = new ConcurrentHashMap<>();
    protected final ElasticResponseHandler elasticResponseHandler;
    protected final Predicate<String> isAccessible;

    private final CountDownLatch latch = new CountDownLatch(1);

    ElasticSecureFacetAsyncProvider(
            ElasticRequestHandler elasticRequestHandler,
            ElasticResponseHandler elasticResponseHandler,
            Predicate<String> isAccessible
    ) {
        this.elasticResponseHandler = elasticResponseHandler;
        this.isAccessible = isAccessible;
        this.facetFields = elasticRequestHandler.facetFields().collect(Collectors.toSet());
    }

    @Override
    public Set<String> sourceFields() {
        return facetFields;
    }

    @Override
    public boolean isFullScan() {
        return true;
    }

    @Override
    public void on(SearchHit searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null && isAccessible.test(path)) {
            Map<String, Object> sourceMap = searchHit.getSourceAsMap();
            for (String field: facetFields) {
                Object value = sourceMap.get(field);
                if (value != null) {
                    facets.compute(field, (facetKey, facetValues) -> {
                        FulltextIndex.Facet facet = new FulltextIndex.Facet(value.toString(), 1);
                        if (facetValues == null) {
                            List<FulltextIndex.Facet> values = new ArrayList<>();
                            values.add(facet);
                            return values;
                        } else {
                            int index = facetValues.indexOf(facet);
                            if (index < 0) {
                                facetValues.add(facet);
                            } else {
                                FulltextIndex.Facet oldFacet = facetValues.remove(index);
                                facetValues.add(new FulltextIndex.Facet(oldFacet.getLabel(), oldFacet.getCount() + 1));
                            }
                            return facetValues;
                        }
                    });
                }
            }
        }
    }

    @Override
    public void endData() {
        // order by count (desc) and then by label (asc)
        facets.values().forEach(facetValues -> facetValues.sort((f1, f2) -> {
            if (f1.getCount() == f2.getCount()) {
                return f1.getLabel().compareTo(f2.getLabel());
            } else return f2.getCount() - f1.getCount();
        }));
        LOG.trace("End data {}", facets);
        latch.countDown();
    }

    @Override
    public List<FulltextIndex.Facet> getFacets(int numberOfFacets, String columnName) {
        LOG.trace("Requested facets for {} - Latch count: {}", columnName, latch.getCount());
        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Error while waiting for facets", e);
        }
        LOG.trace("Reading facets for {} from {}", columnName, facets);
        return facets.get(FulltextIndex.parseFacetField(columnName));
    }
}
