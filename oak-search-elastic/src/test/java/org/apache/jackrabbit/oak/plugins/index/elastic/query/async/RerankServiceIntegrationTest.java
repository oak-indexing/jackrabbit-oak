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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for querying with Elastic index.
 */
public class RerankServiceIntegrationTest extends ElasticAbstractQueryTest {
    private static final Logger LOG = LoggerFactory.getLogger(RerankServiceIntegrationTest.class);

    /**
     * Creates an index definition.
     */
    private IndexDefinitionBuilder createIndexDefinition(String... properties) {
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/content");

        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String property : properties) {
            indexRule.property(property).propertyIndex().analyzed().nodeScopeIndex();
        }

        return builder;
    }


    @Test
    public void testBasicQuery() throws Exception {
        String jcrIndexName = UUID.randomUUID().toString();

        // Create index definition with multiple properties
        IndexDefinitionBuilder builder = createIndexDefinition("title", "description", "updatedBy");
        Tree index = setIndex(jcrIndexName, builder);
        root.commit();

        // Add test content
        createTestContent();

        // Let the index catch up
        assertEventually(() -> {
            assertEquals(6, countDocuments(index));
        });

        // Execute a query that will return multiple results
        String query = "SELECT [jcr:path] FROM [nt:base] WHERE ISDESCENDANTNODE('/content') and CONTAINS(*, '?{\"shouldRerank\":true}?smartphone')";
//        String query = "SELECT * FROM [nt:base] WHERE ISDESCENDANTNODE('/content') and CONTAINS(*, 'smartphone')";

        // Execute the query
        List<String> results = executeQuery(query, SQL2, true, true);
//        List<String> results = executeQuery(query, Query.JCR_SQL2);

        // Verify we have results
        assertEventually(() -> assertTrue("Query should return results", results.size() == 2));

        LOG.info("Query results: {}", results);
    }

    // Helper methods

    private void createTestContent() throws Exception {
        Tree content = root.getTree("/").addChild("content");

        // Create product nodes with different titles and descriptions
        createProduct(content, "product1", "Premium smartphone",
            "High-end smartphone with advanced camera features and long battery life");

        createProduct(content, "product2", "Budget smartphone",
            "Affordable smartphone with basic features for everyday use");

        createProduct(content, "product3", "Gaming Laptop",
            "Powerful laptop designed for gaming with high-performance graphics");

        createProduct(content, "product4", "Ultrabook Laptop",
            "Thin and light laptop with long battery life for business professionals");

        createProduct(content, "product5", "Wireless Earbuds",
            "Premium wireless earbuds with noise cancellation and long battery life");

        root.commit();
    }

    private void createProduct(Tree parent, String name, String title, String description) {
        Tree product = parent.addChild(name);
        product.setProperty("title", title);
        product.setProperty("description", description);
        product.setProperty("category", "electronics");
        product.setProperty("price", Math.random() * 1000);
    }
} 