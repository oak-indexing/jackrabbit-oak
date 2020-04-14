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

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class represents an Elasticsearch Connection with the related <code>RestHighLevelClient</code>.
 * As per Elasticsearch documentation: the client is thread-safe, there should be one instance per application and it
 * must be closed when it is not needed anymore.
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_changing_the_client_8217_s_initialization_code.html
 * <p>
 * The getClient() initializes the rest client on the first call.
 * Once close() is invoked this instance cannot be used anymore.
 */
public class ElasticsearchConnection implements Closeable {

    protected static final String SCHEME_PROP = "elasticsearch.scheme";
    protected static final String DEFAULT_SCHEME = "http";
    protected static final String HOST_PROP = "elasticsearch.host";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final String PORT_PROP = "elasticsearch.port";
    protected static final int DEFAULT_PORT = 9200;
    protected static final String API_KEY_ID_PROP = "elasticsearch.apiKeyId";
    protected static final String API_KEY_SECRET_PROP = "elasticsearch.apiKeySecret";

    protected static final Supplier<ElasticsearchConnection> defaultConnection = () ->
            new ElasticsearchConnection(DEFAULT_SCHEME, DEFAULT_HOST, DEFAULT_PORT);

    private String scheme;
    private String host;
    private int port;

    // API key credentials
    private String apiKeyId;
    private String apiKeySecret;

    private volatile RestHighLevelClient client;

    private AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * Creates an {@code ElasticsearchConnection} instance with the given scheme, host address and port that requires no
     * authentication.
     *
     * @param scheme the name {@code HttpHost.scheme} name
     * @param host   the hostname (IP or DNS name)
     * @param port   the port number
     */
    public ElasticsearchConnection(String scheme, String host, Integer port) {
        this(scheme, host, port, null, null);
    }

    /**
     * Creates an {@code ElasticsearchConnection} instance with the given scheme, host address and port that support API
     * key-based authentication.
     *
     * @param scheme       the name {@code HttpHost.scheme} name
     * @param host         the hostname (IP or DNS name)
     * @param port         the port number
     * @param apiKeyId     the unique id of the API key
     * @param apiKeySecret the generated API secret
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html#security-api-keys">
     * Elasticsearch Security API Keys
     * </a>
     */
    public ElasticsearchConnection(String scheme, String host, Integer port, String apiKeyId, String apiKeySecret) {
        if (scheme == null || host == null || port == null) {
            throw new IllegalArgumentException();
        }
        this.scheme = scheme;
        this.host = host;
        this.port = port;

        this.apiKeyId = apiKeyId;
        this.apiKeySecret = apiKeySecret;
    }

    public RestHighLevelClient getClient() {
        if (isClosed.get()) {
            throw new IllegalStateException("Already closed");
        }

        // double checked locking to get good performance and avoid double initialization
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));
                    if (apiKeyId != null && apiKeySecret != null) {
                        String apiKeyAuth = Base64.getEncoder().encodeToString(
                                (apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8)
                        );
                        Header[] defaultHeaders = new Header[]{new BasicHeader("Authorization", "ApiKey " + apiKeyAuth)};
                        builder.setDefaultHeaders(defaultHeaders);
                    }
                    client = new RestHighLevelClient(builder);
                }
            }
        }
        return client;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public synchronized void close() throws IOException {
        if (client != null) {
            client.close();
        }
        isClosed.set(true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchConnection that = (ElasticsearchConnection) o;
        return port == that.port &&
                Objects.equals(scheme, that.scheme) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getScheme(), getHost(), getPort());
    }

    @Override
    public String toString() {
        return getScheme() + "://" + getHost() + ":" + getPort();
    }

}
