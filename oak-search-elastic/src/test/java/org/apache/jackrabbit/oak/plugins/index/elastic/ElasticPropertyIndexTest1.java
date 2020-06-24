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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexTest;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class ElasticPropertyIndexTest1 extends PropertyIndexTest {

    protected RepositoryOptionsUtil repositoryOptionsUtil;
    //protected ElasticIndexOptions indexOptions;
    private static String elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
    private ElasticConnection esConnection;
    private NodeStore nodeStore;

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);


    public ElasticPropertyIndexTest1(){
//        elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
        esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        indexOptions = new ElasticIndexOptions(esConnection);
        repositoryOptionsUtil = new RepositoryOptionsUtil();
        //indexOptions = new ElasticIndexOptions();
    }


    @Override
    protected ContentRepository createRepository() {
        ElasticIndexEditorProvider editorProvider = (ElasticIndexEditorProvider)indexOptions.getIndexEditorProvider();
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(esConnection);

        nodeStore = repositoryOptionsUtil.createNodeStore();

        AsyncIndexUpdate asyncIndexUpdate = indexOptions.getAsyncIndexUpdate("async", nodeStore, compose(newArrayList(
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

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

}
