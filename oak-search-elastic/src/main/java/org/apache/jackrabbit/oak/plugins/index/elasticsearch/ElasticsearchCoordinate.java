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

import java.util.Objects;

public class ElasticsearchCoordinate {

    protected static final String SCHEME_PROP = "elasticsearch.scheme";
    protected static final String DEFAULT_SCHEME = "http";
    protected static final String HOST_PROP = "elasticsearch.host";
    protected static final String DEFAULT_HOST = "127.0.0.1";
    protected static final String PORT_PROP = "elasticsearch.port";
    protected static final int DEFAULT_PORT = 9200;

    protected static final ElasticsearchCoordinate DEFAULT =
            new ElasticsearchCoordinate(DEFAULT_SCHEME, DEFAULT_HOST, DEFAULT_PORT);

    private final String scheme;
    private final String host;
    private final int port;

    protected ElasticsearchCoordinate(String scheme, String host, Integer port) {
        if (scheme == null || host == null || port == null) {
            throw new IllegalArgumentException();
        }
        this.scheme = scheme;
        this.host = host;
        this.port = port;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchCoordinate that = (ElasticsearchCoordinate) o;
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
