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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;


import com.github.dockerjava.api.DockerClient;
import org.elasticsearch.Version;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assume.assumeNotNull;

/*
To be used as a @ClassRule
 */
public class ElasticsearchConnectionRule extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConnectionRule.class);
    private ElasticsearchConnectionDetails elasticsearchConnectionDetails;
    private String elasticSearchConnectionString;
    private boolean useDocker = false;

    public ElasticsearchConnectionRule(String elasticSearchConnectionString) {
        this.elasticSearchConnectionString = elasticSearchConnectionString;
    }

    public ElasticsearchContainer elastic;

    /*
    Executed once in the test class' execution lifecycle, after the execution of apply()
     */
    @Override
    protected void before() {
        if(useDocker) {
            elasticsearchConnectionDetails = new ElasticsearchConnectionDetails(elastic.getContainerIpAddress(),
                    elastic.getMappedPort(ElasticsearchConnection.DEFAULT_PORT),
                    ElasticsearchConnection.DEFAULT_SCHEME,
                    null, null);
        }
    }

    /*
    This is the first method to be executed. It gets executed exactly once at the beginning of the test class execution.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        Statement s = super.apply(base, description);
        // see if docker is to be used or not... initialize docker rule only if that's the case.

        if (elasticSearchConnectionString == null || getElasticsearchConnectionDetailsfromString() == null) {
            checkIfDockerClientAvailable();
            elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT);
            s = elastic.apply(s, description);
            useDocker = true;
        }
        return s;
    }

    @Override
    protected void after() {
        //TODO: See if something needs to be cleaned up at test class level ??
    }

    public ElasticsearchConnectionDetails getElasticsearchConnectionDetailsfromString() {

        if (elasticsearchConnectionDetails == null) {
            try {
                URI uri = new URI(elasticSearchConnectionString);
                String host = uri.getHost();
                String scheme = uri.getScheme();
                int port = uri.getPort();
                String query = uri.getQuery();

                String api_key = null;
                String api_secret = null;
                if (query != null) {
                    api_key = query.split(",")[0].split("=")[1];
                    api_secret = query.split(",")[1].split("=")[1];
                }
                elasticsearchConnectionDetails = new ElasticsearchConnectionDetails(host, port, scheme, api_key, api_secret);
            } catch (URISyntaxException e) {
                return null;
            }
        }
        return elasticsearchConnectionDetails;
    }


    private void checkIfDockerClientAvailable() {
        DockerClient client = null;
        try {
            client = DockerClientFactory.instance().client();
        } catch (Exception e) {
            LOG.warn("Docker is not available and elasticConnectionDetails sys prop not specified or incorrect" +
                    ", Elastic tests will be skipped");
        }
        assumeNotNull(client);
    }
}
