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
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;

public interface ElasticFacetProvider extends FulltextIndex.FacetProvider, ElasticResponseListener {

    static ElasticFacetProvider getProvider(
            SecureFacetConfiguration facetConfiguration,
            QueryIndex.IndexPlan indexPlan,
            ElasticRequestHandler requestHandler,
            ElasticResponseHandler responseHandler
    ) {
        final ElasticFacetProvider facetProvider;
        switch (facetConfiguration.getMode()) {
            case INSECURE:
                facetProvider = new ElasticInsecureFacetAsyncProvider();
                break;
            case STATISTICAL:
                facetProvider = new ElasticStatisticalFacetAsyncProvider(
                        indexPlan, requestHandler, responseHandler,
                        facetConfiguration.getRandomSeed(), facetConfiguration.getStatisticalFacetSampleSize()
                );
                break;
            case SECURE:
            default:
                facetProvider = new ElasticSecureFacetAsyncProvider(indexPlan, requestHandler, responseHandler);

        }
        return facetProvider;
    }

}
