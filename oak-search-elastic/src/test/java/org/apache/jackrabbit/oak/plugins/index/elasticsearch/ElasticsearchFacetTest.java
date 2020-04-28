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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;

public class ElasticsearchFacetTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchFacetTest.class);
    private static final String FACET_PROP = "facets";
    private Closer closer;
    private Session session;
    private QueryManager qe;
    Repository repository;

//    @Rule
//    public final ElasticsearchContainer elastic =
//            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT);
//
//    @BeforeClass
//    public static void beforeMethod() {
//        DockerClient client = null;
//        try {
//            client = DockerClientFactory.instance().client();
//        } catch (Exception e) {
//            LOG.warn("Docker is not available, ElasticsearchPropertyIndexTest will be skipped");
//        }
//        assumeNotNull(client);
//    }


    @Before
    public void setup() throws Exception {
        closer = Closer.create();
        createRepository();
    }

    private void createRepository() {
        ElasticsearchConnection connection = ElasticsearchConnection.newBuilder()
                .withIndexPrefix("" + System.nanoTime())
                .withConnectionParameters(
                        ElasticsearchConnection.DEFAULT_SCHEME,
//                        elastic.getContainerIpAddress(),
//                        elastic.getMappedPort(ElasticsearchConnection.DEFAULT_PORT)
                        "mokatari-ubuntu", 9200
                ).build();
        ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(connection);

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
        Oak oak = new Oak(nodeStore)
                .with(editorProvider)
                .with(indexProvider);

        Jcr jcr = new Jcr(oak);
        repository = jcr.createRepository();

        try {
            session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
        closer.register(session::logout);

        // we'd always query anonymously
        Session anonSession = null;
        try {
            anonSession = repository.login(new GuestCredentials(), null);
            anonSession.refresh(true);
            anonSession.save();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
        closer.register(anonSession::logout);
        try {
            qe = anonSession.getWorkspace().getQueryManager();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    private class IndexSkeleton {
        String indexName = "testindex";
        IndexDefinitionBuilder indexDefinitionBuilder;
        IndexDefinitionBuilder.IndexRule indexRule;
        void initialize() {
            initialize(JcrConstants.NT_BASE);
        }
        void initialize(String nodeType) {
            indexDefinitionBuilder = new ElasticsearchIndexDefinitionBuilder();
            indexRule = indexDefinitionBuilder.indexRule(nodeType);
        }
        void build() throws RepositoryException {
            indexDefinitionBuilder.build(session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(indexName));
        }
    }

    @Test
    public void propertyFacetQuery() throws Exception {
        IndexSkeleton indexSkeleton = new IndexSkeleton();
        indexSkeleton.initialize();
        indexSkeleton.indexDefinitionBuilder.noAsync();
        indexSkeleton.indexRule.property("cons").propertyIndex();
        indexSkeleton.indexRule.property("foo").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexSkeleton.indexRule.property("bar").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexSkeleton.build();
        session.save();
        Node testNode = session.getRootNode().addNode("test");
        testNode.addNode("a").setProperty("foo", "l1");
        testNode.getNode("a").setProperty("cons", "val");
        testNode.addNode("b").setProperty("foo", "l2");
        testNode.getNode("b").setProperty("cons", "val");
        testNode.addNode("c").setProperty("foo", "l1");
        testNode.getNode("c").setProperty("cons", "val");
        testNode.addNode("d").setProperty("foo", "l1");
        testNode.getNode("d").setProperty("cons", "val");
        testNode.addNode("e").setProperty("bar", "m1");
        testNode.getNode("e").setProperty("cons", "val");
        testNode.addNode("f").setProperty("bar", "m2");
        testNode.getNode("f").setProperty("cons", "val");

        AccessControlUtils.grantAllToEveryone(session, "/");
        session.save();
        Thread.sleep(5000);

        String query = "SELECT [rep:facet(foo)], [rep:facet(bar)] FROM [nt:base] where [cons] = 'val'";
        String query1 = "select [jcr:path] from [nt:base] where [foo]= 'l1'";

        Query q = qe.createQuery(query, Query.JCR_SQL2);
        QueryResult queryResult = q.execute();

        List<String> nodePaths = new ArrayList<String>();
        RowIterator it = queryResult.getRows();
        while (it.hasNext()) {
            Row row = (Row) it.next();
            nodePaths.add(row.getPath());
        }
        Map<String, Integer> facetMap = getFacets(queryResult);
        Assert.assertArrayEquals(new String[]{"/test/a", "/test/b", "/test/c", "/test/d", "/test/e", "/test/f"}, nodePaths.toArray());
        Assert.assertArrayEquals(new Integer[]{3, 1, 1, 1}, new Integer[]{facetMap.get("l1"), facetMap.get("l2"), facetMap.get("m1"), facetMap.get("m2")});
    }

    private Map<String, Integer> getFacets(QueryResult queryResult) throws Exception {
        Map<String, Integer> map = Maps.newHashMap();
        FacetResult facetResult = new FacetResult(queryResult);
        Set<String> dims = facetResult.getDimensions();
        for (String dim : dims) {
            List<FacetResult.Facet> facets = facetResult.getFacets(dim);
            for (FacetResult.Facet facet : facets) {
                map.put(facet.getLabel(), facet.getCount());
            }
        }
        return map;
    }

    private static IndexDefinitionBuilder createIndex1(String... propNames) {
        IndexDefinitionBuilder builder = new ElasticsearchIndexDefinitionBuilder().noAsync();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String propName : propNames) {
            indexRule.property(propName).propertyIndex();
        }
        return builder;
    }
}
