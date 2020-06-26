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
package org.apache.jackrabbit.oak.plugins.index.lucene;

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
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexTest;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class LucenePropertyIndexTest1 extends PropertyIndexTest {

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService, temporaryFolder).build();
        indexOptions = new LuceneIndexOptions();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    private Tree createIndex(String name, Set<String> propNames) throws CommitFailedException {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    public static Tree createIndex(Tree index, String name, Set<String> propNames) throws CommitFailedException {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, LuceneIndexConstants.TYPE_LUCENE);
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    /*@Test
    public void pathExcludeWithPathRestrictionsEnabled() throws Exception {
        Tree idx = createIndex("test1", of("propa", "propb"));
        idx.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 10);
        test.addChild("a").addChild("b").setProperty("propa", 10);
        test.addChild("c").setProperty("propa", 10);
        test.addChild("c").addChild("d").setProperty("propa", 10);
        root.commit();

        assertFalse(explain("select [jcr:path] from [nt:base] where [propa] = 10").contains("lucene:test1"));
        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')"), containsString("lucene:test1"));

        assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')", asList("/test/c/d"));

        //Make some change and then check
        Tree testc = root.getTree("/").getChild("test").getChild("c");
        testc.addChild("e").addChild("f").setProperty("propa", 10);
        root.commit();
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c')", asList("/test/c/d", "/test/c/e/f"));
        assertThat(explain("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c') and not(isDescendantNode('/test/c/e'))"), containsString("lucene:test1"));
        assertQuery("select [jcr:path] from [nt:base] where [propa] = 10 and isDescendantNode('/test/c') and not(isDescendantNode('/test/c/e'))", asList("/test/c/d"));
    }*/

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

}
