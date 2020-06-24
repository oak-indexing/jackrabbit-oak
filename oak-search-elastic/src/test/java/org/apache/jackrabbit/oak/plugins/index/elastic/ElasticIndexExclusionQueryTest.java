/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexExclusionQueryTest;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static javax.jcr.PropertyType.TYPENAME_BINARY;
import static javax.jcr.PropertyType.TYPENAME_STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_TYPES;

public class ElasticIndexExclusionQueryTest extends IndexExclusionQueryTest {

    protected static final Logger LOG = LoggerFactory.getLogger(ElasticIndexExclusionQueryTest.class);

    private static final String NOT_IN = "notincluded";
    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
    private ElasticConnection esConnection;

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);


    protected RepositoryOptionsUtil repositoryOptionsUtil;
    protected ElasticIndexOptions indexOptions;
    protected NodeStore nodeStore;
    protected AsyncIndexUpdate asyncIndexUpdate;

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        elasticRule.closeElasticConnection();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new RepositoryOptionsUtil();
        indexOptions = new ElasticIndexOptions(esConnection);

        esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        ElasticIndexEditorProvider editorProvider = (ElasticIndexEditorProvider) indexOptions.getIndexEditorProvider();
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(esConnection);

        nodeStore = repositoryOptionsUtil.createNodeStore();

        asyncIndexUpdate = indexOptions.getAsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));

        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(indexOptions.getIndexCorruptIntervalInMillis(), TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);

        Oak oak = new Oak(nodeStore)
                .with(repositoryOptionsUtil.getInitialContent())
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());

        if (indexOptions.isAsyncIndex()) {
            oak = indexOptions.addAsyncIndexingLanesToOak(oak);
        }
        return oak.createContentRepository();
    }

    protected void assertEventually(Runnable r) {
        ElasticTestUtils.assertEventually(r,
                ((indexOptions.isAsyncIndex() ? indexOptions.getAsyncIndexingTimeInSeconds() : 0) + BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
        Tree lucene = createTestIndexNode(root.getTree("/"), ElasticIndexDefinition.TYPE_ELASTICSEARCH);
        lucene.setProperty(INCLUDE_PROPERTY_TYPES,
                of(TYPENAME_BINARY, TYPENAME_STRING), STRINGS);
        lucene.setProperty(EXCLUDE_PROPERTY_NAMES, of(NOT_IN), STRINGS);
        useV2(lucene);
        root.commit();
    }

    public static void useV2(Tree idxTree) {
        if (!IndexFormatVersion.getDefault().isAtLeast(IndexFormatVersion.V2)) {
            idxTree.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        }
    }
}
