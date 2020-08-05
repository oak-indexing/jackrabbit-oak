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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.StrictPathRestrictionWarnCommonTest;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class LuceneStrictPathRestrictionWarnCommonTest extends StrictPathRestrictionWarnCommonTest {

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private String corDir = null;
    private String cowDir = null;

    private LuceneIndexEditorProvider editorProvider;

    private TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();

    private NodeStore nodeStore;

    private LuceneIndexProvider provider;

    private ResultCountingIndexProvider queryIndexProvider;

    private QueryEngineSettings queryEngineSettings = new QueryEngineSettings();

    private final String warnMessage = "Index definition of index used have path restrictions and query won't return nodes from " +
            "those restricted paths";

    private final String queryImplLogger = "org.apache.jackrabbit.oak.query.QueryImpl";

    @Before
    public void loggingAppenderStart() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        context.getLogger(queryImplLogger).addAppender(listAppender);
    }

    @After
    public void loggingAppenderStop() {
        listAppender.stop();
    }

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        indexOptions = new LuceneIndexOptions();
        IndexCopier copier = null;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw  new RuntimeException(e);
        }editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        provider = new LuceneIndexProvider(copier);
        queryIndexProvider = new ResultCountingIndexProvider(provider);
        nodeStore = new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT);
        queryEngineSettings.setStrictPathRestriction(StrictPathRestriction.WARN.name());
        return new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(queryIndexProvider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .with(queryEngineSettings)
                .createContentRepository();
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

}
