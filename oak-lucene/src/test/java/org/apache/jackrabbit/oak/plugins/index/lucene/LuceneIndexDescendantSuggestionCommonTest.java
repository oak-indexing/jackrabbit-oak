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
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexDescendantSuggestionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.After;
import org.junit.Before;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.SimpleCredentials;

import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;

public class LuceneIndexDescendantSuggestionCommonTest extends IndexDescendantSuggestionCommonTest{
    @Before
    public void before() throws Exception {
        LuceneIndexProvider provider = new LuceneIndexProvider();

        Jcr jcr = new Jcr()
                .with(((QueryIndexProvider) provider))
                .with((Observer) provider)
                .with(new LuceneIndexEditorProvider());

        repository = jcr.createRepository();
        session = (JackrabbitSession) repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        root = session.getRootNode();

        createContent();
        session.save();
    }

    @After
    public void after() {
        session.logout();
        shutdown(repository);
    }

    private void createContent() throws Exception {
        indexOptions = new LuceneIndexOptions();
        /*
        Make content with following structure:
        * sugg-idx is a simple index to suggest node names on type "oak:Unstructured"
        * test[1-6] nodes would be "oak:Unstructured".
        * all other nodes, unless required are "nt:unstructured"
        * Index on one sub-tree is on nt:base so that we can do sub-tree suggestion test with unambiguous indices
        */

        //  /oak:index/sugg-idx, /test1
        createSuggestIndex(root, "sugg-idx", NT_OAK_UNSTRUCTURED, PROPDEF_PROP_NODE_NAME);
        root.addNode("test1", NT_OAK_UNSTRUCTURED);

        /*
            /content1
                /tree1
                    /test2
                /tree2
                    /test3
         */
        Node content1 = root.addNode("content1", NT_UNSTRUCTURED);
        Node tree1 = content1.addNode("tree1", NT_UNSTRUCTURED);
        tree1.addNode("test2", NT_OAK_UNSTRUCTURED);
        Node tree2 = content1.addNode("tree2", NT_UNSTRUCTURED);
        tree2.addNode("test3", NT_OAK_UNSTRUCTURED);

        //  /content2/oak:index/sugg-idx, /content2/test4
        Node content2 = root.addNode("content2", NT_UNSTRUCTURED);
        createSuggestIndex(content2, "sugg-idx", NT_OAK_UNSTRUCTURED, PROPDEF_PROP_NODE_NAME);
        content2.addNode("test4", NT_OAK_UNSTRUCTURED);

        //  /content3/oak:index/sugg-idx, /content3/test5, /content3/sC/test6
        Node content3 = root.addNode("content3", NT_UNSTRUCTURED);
        createSuggestIndex(content3, "sugg-idx", NT_BASE, PROPDEF_PROP_NODE_NAME);
        content3.addNode("test5", NT_OAK_UNSTRUCTURED);
        Node subChild = content3.addNode("sC", NT_UNSTRUCTURED);
        subChild.addNode("test6", NT_OAK_UNSTRUCTURED);
    }
}
