package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticCleanerTest extends ElasticAbstractQueryTest {

    @Override
    protected boolean useAsyncIndexing() {
        return true;
    }

    @Test
    public void testIndexDeletion() throws Exception {
        IndexDefinitionBuilder builder = createIndex("propa");
        builder.async("async");
        builder.indexRule("nt:base").property("propa");

        String indexId1 = UUID.randomUUID().toString();
        setIndex(indexId1, builder);
        root.commit();

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propa", "Simple test");
        root.commit();

        String indexPath = "/" + INDEX_DEFINITIONS_NAME + "/" + indexId1;
        assertEventually(() -> {
            NodeState indexState = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexId1);
            String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState,
                    indexPath);
            try {
                assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        NodeState indexState = nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexId1);

        root.refresh();
        root.getTree(indexPath).remove();
        root.commit();

        assertFalse(nodeStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexId1).exists());

        ElasticIndexCleaner cleaner = new ElasticIndexCleaner(esConnection, nodeStore, 5);
        cleaner.run();

        String remoteIndexName = ElasticIndexNameHelper.getRemoteIndexName(esConnection.getIndexPrefix(), indexState,
                indexPath);

        assertTrue(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));

        Thread.sleep(5000);
        cleaner.run();
        assertFalse(esConnection.getClient().indices().exists(new GetIndexRequest(remoteIndexName), RequestOptions.DEFAULT));
    }

}
