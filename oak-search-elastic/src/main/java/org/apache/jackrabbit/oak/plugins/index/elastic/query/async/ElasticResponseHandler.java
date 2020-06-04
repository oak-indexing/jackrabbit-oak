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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.elasticsearch.search.SearchHit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiPredicate;

class ElasticResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResponseHandler.class);

    private final IndexPlan indexPlan;
    private final PlanResult planResult;
    private final BiPredicate<String, IndexPlan> rowInclusionPredicate;

    ElasticResponseHandler(@NotNull IndexPlan indexPlan, @NotNull PlanResult planResult,
                           BiPredicate<String, IndexPlan> rowInclusionPredicate) {
        this.indexPlan = indexPlan;
        this.planResult = planResult;
        this.rowInclusionPredicate = rowInclusionPredicate;
    }

    public FulltextResultRow toRow(SearchHit hit, FulltextIndex.FacetProvider facetProvider) {
        final Map<String, Object> sourceMap = hit.getSourceAsMap();
        String path = (String) sourceMap.get(FieldNames.PATH);

        if ("".equals(path)) {
            path = "/";
        }
        if (planResult.isPathTransformed()) {
            String originalPath = path;
            path = planResult.transformPath(path);

            if (path == null) {
                LOG.trace("Ignoring path {} : Transformation returned null", originalPath);
                return null;
            }
        }

        if (rowInclusionPredicate != null && !rowInclusionPredicate.test(path, indexPlan)) {
            LOG.trace("Path {} not included because of hierarchy inclusion rules", path);
            return null;
        }
        return new FulltextResultRow(path, hit.getScore(), null, facetProvider, null);
    }
}
