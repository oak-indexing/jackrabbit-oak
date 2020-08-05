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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexDescendantSuggestionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;

public class LuceneIndexDescendantSuggestionCommonTest extends IndexDescendantSuggestionCommonTest{
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

//    @Before
//    public void before() throws Exception {
//        LuceneIndexProvider provider = new LuceneIndexProvider();
//
//        Jcr jcr = new Jcr()
//                .with(((QueryIndexProvider) provider))
//                .with((Observer) provider)
//                .with(new LuceneIndexEditorProvider());
//
//        repository = jcr.createRepository();
//        session = (JackrabbitSession) repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
//        root = session.getRootNode();
//
//        createContent();
//        session.save();
//    }
//
//    @After
//    public void after() {
//        session.logout();
//        shutdown(repository);
//    }

    @Override
    protected Repository createJcrRepository() throws RepositoryException {
        indexOptions = new LuceneIndexOptions();
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService, temporaryFolder).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        return repository;
    }

}
