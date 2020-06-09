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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.jetbrains.annotations.NotNull;

/**
 * Facade for possible requests to be done to Lucene, like queries,
 * spellchecking requests, etc..
 *
 * @param <T> the actual Lucene class representing the request / use case.
 */
class ElasticRequestFacade<T> {

    private final T elasticRequest;

    ElasticRequestFacade(@NotNull T elasticRequest) {
        this.elasticRequest = elasticRequest;
    }

    T getElasticRequest() {
        return elasticRequest;
    }

    @Override
    public String toString() {
        return elasticRequest.toString();
    }
}
