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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import com.github.dockerjava.api.DockerClient;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.elasticsearch.Version;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import javax.jcr.security.Privilege;
import java.util.List;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_ANALYZED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_USE_IN_SPELLCHECK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class ElasticSpellcheckTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSpellcheckTest.class);
    private Closer closer;
    private Session session;
    private QueryManager qe;
    Repository repository;
    private Node indexNode;
    private static final String TEST_INDEX = "testIndex";

    @Rule
    public final ElasticsearchContainer elastic =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT);

    @BeforeClass
    public static void beforeMethod() {
        DockerClient client = null;
        try {
            client = DockerClientFactory.instance().client();
        } catch (Exception e) {
            LOG.warn("Docker is not available, ElasticFacetTest will be skipped");
        }
        assumeNotNull(client);
    }

    @Before
    public void setup() throws Exception {
        closer = Closer.create();
        createRepository();
        createIndex();
        indexNode = session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(TEST_INDEX);
    }

    private void createRepository() throws RepositoryException {
        ElasticConnection connection = ElasticConnection.newBuilder()
                .withIndexPrefix("" + System.nanoTime())
                .withConnectionParameters(
                        ElasticConnection.DEFAULT_SCHEME,
                        elastic.getContainerIpAddress(),
                        elastic.getMappedPort(ElasticConnection.DEFAULT_PORT)
                ).build();
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(connection);

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
        Oak oak = new Oak(nodeStore)
                .with(editorProvider)
                .with(indexProvider);

        Jcr jcr = new Jcr(oak);
        repository = jcr.createRepository();

        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        closer.register(session::logout);

        // we'd always query anonymously
        Session anonSession = repository.login(new GuestCredentials(), null);
        anonSession.refresh(true);
        anonSession.save();
        closer.register(anonSession::logout);
        qe = anonSession.getWorkspace().getQueryManager();
    }

    private class IndexSkeleton {
        IndexDefinitionBuilder indexDefinitionBuilder;
        IndexDefinitionBuilder.IndexRule indexRule;

        void initialize() {
            initialize(JcrConstants.NT_BASE);
        }

        void initialize(String nodeType) {
            indexDefinitionBuilder = new ElasticIndexDefinitionBuilder();
            indexDefinitionBuilder.evaluatePathRestrictions();
            indexRule = indexDefinitionBuilder.indexRule(nodeType);
        }

        void build() throws RepositoryException {
            indexDefinitionBuilder.build(session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(TEST_INDEX));
        }
    }

    private void createIndex() throws RepositoryException {
        IndexSkeleton indexSkeleton = new IndexSkeleton();
        indexSkeleton.initialize();
        indexSkeleton.indexDefinitionBuilder.noAsync();
        indexSkeleton.indexRule.property("cons").propertyIndex();
        indexSkeleton.indexRule.property("foo").propertyIndex();//.getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexSkeleton.indexRule.property("foo").getBuilderTree().setProperty(PROP_USE_IN_SPELLCHECK, true, Type.BOOLEAN);
        indexSkeleton.indexRule.property("foo").getBuilderTree().setProperty(PROP_ANALYZED, true, Type.BOOLEAN);

        //indexSkeleton.indexRule.property("bar").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexSkeleton.build();
    }

    @Test
    public void testSpellcheckSingleWord() throws Exception {
        //Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", session));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "descent");
        Node n2 = n1.addNode("node2");
        n2.setProperty("foo", "decent");
        session.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('desent')";
        Query q = qm.createQuery(sql, Query.SQL);
        assertEventually(() -> {
            try {
                assertEquals("[decent, descent]", getResult(q.execute(), "rep:spellcheck()").toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testSpellcheckSingleWordWithDescendantNode() throws Exception {
        //Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", session));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "descent");
        Node n2 = n1.addNode("node2");
        n2.setProperty("foo", "decent");
        session.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('desent') and isDescendantNode('/parent/node1')";
        Query q = qm.createQuery(sql, Query.SQL);
        assertEventually(() -> {
            try {
                assertEquals("[decent]", getResult(q.execute(), "rep:spellcheck()").toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testSpellcheckMultipleWords() throws Exception {
        session.save();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", session));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "it is always a good idea to go visiting ontario");
        Node n2 = par.addNode("node2");
        n2.setProperty("foo", "ontario is a nice place to live in");
        Node n3 = par.addNode("node3");
        n2.setProperty("foo", "I flied to ontario for voting for the major polls");
        Node n4 = par.addNode("node4");
        n2.setProperty("foo", "I will go voting in ontario, I always voted since I've been allowed to");
        session.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('votin in ontari')";
        Query q = qm.createQuery(sql, Query.SQL);

        assertEventually(() -> {
            try {
                assertEquals("[voting in ontario]", getResult(q.execute(), "rep:spellcheck()").toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Node deny(Node node) throws RepositoryException {
        AccessControlUtils.deny(node, "anonymous", Privilege.JCR_ALL);
        return node;
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    static List<String> getResult(QueryResult result, String propertyName) throws RepositoryException {
        List<String> results = Lists.newArrayList();
        RowIterator it = null;

            it = result.getRows();
            while (it.hasNext()) {
                Row row = it.nextRow();
                results.add(row.getValue(propertyName).getString());
            }

        return results;
    }

    private static void assertEventually(Runnable r) {
        ElasticTestUtils.assertEventually(r, BULK_FLUSH_INTERVAL_MS_DEFAULT * 3);
    }

}
