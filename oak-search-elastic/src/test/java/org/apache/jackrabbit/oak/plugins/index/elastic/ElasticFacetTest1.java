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
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.ElasticTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.FacetTest1;
import org.apache.jackrabbit.oak.plugins.index.RepositoryOptionsUtil;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import java.io.IOException;

public class ElasticFacetTest1 extends FacetTest1 {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticFacetTest1.class);
    private static final PerfLogger LOG_PERF = new PerfLogger(LOG);
    private static final String FACET_PROP = "facets";

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static String elasticConnectionString = "http://mokatari-ubuntu:9200";//System.getProperty("elasticConnectionString");
    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);


    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        anonymousSession.logout();
        adminSession.logout();
        elasticRule.closeElasticConnection();
    }

    protected Repository createJcrRepository() throws RepositoryException {
        indexOptions = new ElasticIndexOptions();
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        return repository;
    }

    private void assertEventually(Runnable r) {
        ElasticTestUtils.assertEventually(r, ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

}
