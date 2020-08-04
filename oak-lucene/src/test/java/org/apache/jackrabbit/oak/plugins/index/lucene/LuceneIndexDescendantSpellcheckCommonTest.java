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
import org.apache.jackrabbit.oak.plugins.index.IndexDescendantSpellcheckCommonTest;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.junit.After;
import org.junit.Before;

import javax.jcr.SimpleCredentials;

import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;

public class LuceneIndexDescendantSpellcheckCommonTest extends IndexDescendantSpellcheckCommonTest {

    @Before
    public void before() throws Exception {
        indexOptions = new LuceneIndexOptions();
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

}
