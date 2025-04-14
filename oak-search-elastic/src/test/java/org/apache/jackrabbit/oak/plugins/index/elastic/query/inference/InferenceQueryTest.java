/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.spi.query.fulltext.InferenceQuery;
import org.junit.Test;

import static org.junit.Assert.*;

public class InferenceQueryTest {
    
    private void assertInferenceQuery(String input, String expectedJson, String expectedQuery) {
        InferenceQuery query = new InferenceQuery(input);
        assertEquals(expectedJson, query.getQueryInferenceConfig());
        assertEquals(expectedQuery, query.getQueryText());
    }

    @Test
    public void testSimpleJsonAndQuery() {
        assertInferenceQuery(
            "?{\"key\":\"value\"}?simple query",
            "{\"key\":\"value\"}",
            "simple query"
        );
    }

    @Test
    public void testComplexJsonAndQuery() {
        assertInferenceQuery(
            "?{\"nested\":{\"key1\":\"value1\",\"key2\":123}}?complex query text",
            "{\"nested\":{\"key1\":\"value1\",\"key2\":123}}",
            "complex query text"
        );
    }

    @Test
    public void testJsonWithQuestionMarkAndQuery() {
        assertInferenceQuery(
            "?{\"question\":\"What is 2 + 2?\"}?How to solve math?",
            "{\"question\":\"What is 2 + 2?\"}",
            "How to solve math?"
        );
    }

    @Test
    public void testComplexJsonWithMultipleQuestionMarks() {
        assertInferenceQuery(
            "?{\"q1\":\"First question?\",\"q2\":\"Second question?\"}?Final question?",
            "{\"q1\":\"First question?\",\"q2\":\"Second question?\"}",
            "Final question?"
        );
    }

    @Test
    public void testJsonWithSpecialCharacters() {
        assertInferenceQuery(
            "?{\"special\":\"!@#$%^&*()\"}?query with special chars",
            "{\"special\":\"!@#$%^&*()\"}",
            "query with special chars"
        );
    }

    @Test
    public void testJsonWithEscapedQuotes() {
        assertInferenceQuery(
            "?{\"quoted\":\"Text with \\\"quoted\\\" content\"}?query text",
            "{\"quoted\":\"Text with \\\"quoted\\\" content\"}",
            "query text"
        );
    }

    @Test
    public void testJsonWithNullValues() {
        assertInferenceQuery(
            "?{\"nullValue\":null,\"valid\":\"value\"}?query text",
            "{\"nullValue\":null,\"valid\":\"value\"}",
            "query text"
        );
    }

    // Error cases can be grouped together
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInputs() {
        assertIllegalArgumentException("");
        assertIllegalArgumentException(null);
        assertIllegalArgumentException("{\"key\":\"value\"}?query");
        assertIllegalArgumentException("?{\"key\":\"value\"}query");
        assertIllegalArgumentException("?{\"key\":\"value\"?query");
        assertIllegalArgumentException("??query text");
        assertIllegalArgumentException("?{\"key\":\"value\"}?");
    }

    private void assertIllegalArgumentException(String input) {
        new InferenceQuery(input);
    }
} 