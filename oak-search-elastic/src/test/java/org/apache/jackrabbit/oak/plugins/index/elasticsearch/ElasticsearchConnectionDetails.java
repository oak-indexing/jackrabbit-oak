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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import java.io.IOException;

public class ElasticsearchConnectionDetails {

    private String elasticHost;
    private int elasticPort;
    private String elasticScheme;
    private String apiKeyId;
    private String apiKeySecret;
    private ElasticsearchConnection esConnection;

    public ElasticsearchConnectionDetails(String elasticHost, int elasticPort, String elasticScheme, String apiKeyId, String apiKeySecret) {
        this.elasticHost = elasticHost;
        this.elasticPort = elasticPort;
        this.elasticScheme = elasticScheme;
        this.apiKeyId = apiKeyId;
        this.apiKeySecret = apiKeySecret;
    }

    public ElasticsearchConnection getEsConnection() {

        if (esConnection == null) {
            ElasticsearchConnection.Builder.BuildStep step = ElasticsearchConnection.newBuilder()
                    .withIndexPrefix("ElasticTest_" + System.currentTimeMillis())
                    .withConnectionParameters(elasticScheme, elasticHost, elasticPort);
            if (apiKeyId != null && apiKeySecret != null) {
                step = step.withApiKeys(apiKeyId, apiKeySecret);
            }
            esConnection = step.build();

        }
        return esConnection;
    }

    public void closeESConnection() throws IOException {
        if (esConnection != null) {
            esConnection.close();
            esConnection = null;
        }
    }
}
