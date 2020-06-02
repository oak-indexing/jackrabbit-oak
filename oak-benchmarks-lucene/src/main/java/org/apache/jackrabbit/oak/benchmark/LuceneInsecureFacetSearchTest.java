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
package org.apache.jackrabbit.oak.benchmark;


import javax.jcr.Node;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_INSECURE;

public class LuceneInsecureFacetSearchTest extends LuceneFacetSearchTest {

    @Override
    protected void beforeSuite() throws Exception {
        super.beforeSuite();
        Node indexNode = defaultContext.session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(indexName);
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", defaultContext.session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        defaultContext.session.save();
    }

    public LuceneInsecureFacetSearchTest(Boolean storageEnabled) {
        super(storageEnabled);
    }
}
