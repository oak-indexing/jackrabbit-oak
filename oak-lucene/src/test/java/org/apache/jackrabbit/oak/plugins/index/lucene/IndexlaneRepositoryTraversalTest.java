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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import ch.qos.logback.classic.Level;
import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.property.RecursiveDelete;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests: index lane only traverses repository if atleast one index under /oak:index have
 * the index-lane under consideration.
 */
public class IndexlaneRepositoryTraversalTest {

    private static final String indexLaneLog = "lane: async not present for indexes under /oak:index";
    private final long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;
    private MemoryBlobStore blobStore;

    protected Root root;

    private AsyncIndexUpdate asyncIndexUpdate;

    NodeStore nodeStore;
    LogCustomizer customLogger;

    @Before
    public void before() throws Exception {
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
        customLogger = LogCustomizer
                .forLogger(AsyncIndexUpdate.class.getName())
                .enable(Level.INFO).create();
        customLogger.starting();
    }

    @After
    public void after() {
        customLogger.finished();
    }

    protected ContentRepository createRepository() {
        nodeStore = new MemoryNodeStore();
        blobStore = new MemoryBlobStore();
        blobStore.setBlockSizeMin(48);//make it as small as possible

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                luceneIndexEditorProvider,
                new NodeCounterEditorProvider()
        )));
        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(INDEX_CORRUPT_INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(luceneIndexEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(new PropertyIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();
    }

    @Test
    public void RespositoryTraversalIfLaneIsPresent() throws Exception {
        Tree test1 = root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild("mynodetype");
        test1.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        test1.setProperty("type", "property");
        test1.setProperty("propertyNames", ImmutableList.of("jcr:primaryType", "jcr:mixinTypes"), Type.NAMES);
        test1.setProperty("declaringNodeTypes", ImmutableList.of("oak:QueryIndexDefinition"), Type.NAMES);
        test1.setProperty("nodeTypeListDefined", true);
        test1.setProperty("reindex", true);
        root.commit();
        asyncIndexUpdate.run();
        List<String> logs = customLogger.getLogs();
        assertFalse(isIndexLaneNotPresentLog(logs));
    }

    @Test
    public void noRespositoryTraversalIfLaneIsNotPresent() throws Exception {
        deleteIndexDefinitions("/oak:index");
        asyncIndexUpdate.run();
        List<String> logs = customLogger.getLogs();
        assertTrue(isIndexLaneNotPresentLog(logs));
    }

    private void deleteIndexDefinitions(String path) throws CommitFailedException {
        RecursiveDelete rd = new RecursiveDelete(nodeStore, EmptyHook.INSTANCE, () -> CommitInfo.EMPTY);
        rd.setBatchSize(100);
        rd.run(path);
    }

    private boolean isIndexLaneNotPresentLog(List<String> logs) {
        for (String log : logs) {
            if (log.equals(indexLaneLog)) {
                return true;
            }
        }
        return false;
    }
}
