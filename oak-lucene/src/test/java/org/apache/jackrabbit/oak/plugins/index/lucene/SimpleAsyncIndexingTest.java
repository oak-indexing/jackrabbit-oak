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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Simple test to verify async indexing setup works correctly.
 * Base test pattern from AsyncIndexStatsUpdateCallbackTest.
 */
public class SimpleAsyncIndexingTest {
    
    private ContentRepository repository;
    private Root root;
    private NodeStore nodeStore;
    private AsyncIndexUpdate asyncIndexUpdate;
    private LuceneIndexEditorProvider luceneEditorProvider;
    private LuceneIndexProvider luceneIndexProvider;
    
    @Before
    public void setup() throws Exception {
        repository = createRepository();
        root = repository.login(null, null).getLatestRoot();
    }
    
    @After
    public void teardown() {
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
        }
    }
    
    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        luceneEditorProvider = new LuceneIndexEditorProvider();
        luceneIndexProvider = new LuceneIndexProvider();
        
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, luceneEditorProvider);
        
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneIndexProvider)
                .with((Observer) luceneIndexProvider)
                .with(luceneEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }
    
    @Test
    public void testBasicAsyncIndexing() throws Exception {
        // Create index definition
        LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
        idxb.indexRule("nt:base")
                .property("foo").analyzed().nodeScopeIndex().propertyIndex();
        idxb.build(root.getTree("/oak:index").addChild("luceneTest"));
        root.commit();
        
        // Add content
        root.getTree("/").addChild("content").setProperty("foo", "bar");
        root.commit();
        
        // Run async indexing
        asyncIndexUpdate.run();
        
        // Query content - should find 1 result
        String query = "select [jcr:path] from [nt:base] where [foo] = 'bar'";
        int resultCount = 0;
        for (org.apache.jackrabbit.oak.api.ResultRow row : root.getQueryEngine().executeQuery(
                query, javax.jcr.query.Query.JCR_SQL2, null, null).getRows()) {
            resultCount++;
        }
        
        assertEquals("Should find indexed content", 1, resultCount);
    }
}

