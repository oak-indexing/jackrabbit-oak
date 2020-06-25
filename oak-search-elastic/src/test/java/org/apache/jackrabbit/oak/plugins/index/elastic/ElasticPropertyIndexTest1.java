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

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexTest;
import org.apache.jackrabbit.oak.plugins.index.RepositoryOptionsUtil;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class ElasticPropertyIndexTest1 extends PropertyIndexTest {

    //protected ElasticRepositoryOptionsUtil repositoryOptionsUtil;
    private static String elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
    private ElasticConnection esConnection;
    private NodeStore nodeStore;

    //    @ClassRule
    public static ElasticConnectionRule elasticRule;// = new ElasticConnectionRule(elasticConnectionString);


    public ElasticPropertyIndexTest1() {
        repositoryOptionsUtil = new ElasticRepositoryOptionsUtil(RepositoryOptionsUtil.NodeStoreType.MEMORY_NODE_STORE, false);
//        repositoryOptionsUtil.initialize();
        elasticRule = ElasticRepositoryOptionsUtil.elasticRule;
//        elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
//        esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
//                elasticRule.getElasticConnectionFromString();
        indexOptions = new ElasticIndexOptions();
        //repositoryOptionsUtil = new RepositoryOptionsUtil();
    }


    @Override
    protected ContentRepository createRepository() {
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

}
