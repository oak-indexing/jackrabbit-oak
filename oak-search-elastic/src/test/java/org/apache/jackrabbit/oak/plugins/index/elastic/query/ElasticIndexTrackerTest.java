package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertTrue;

public class ElasticIndexTrackerTest extends ElasticAbstractQueryTest {

    private static final String INITIAL_INDEX_NAME = "index-1";
    private static final String INITIAL_INDEX_PATH = "/" + INDEX_DEFINITIONS_NAME + "/" + INITIAL_INDEX_NAME;

    @Override
    protected boolean useAsyncIndexing() {
        return true;
    }

    @Override
    protected InitialContent getInitialContent() {
        return new InitialContent() {
            @Override
            public void initialize(@NotNull NodeBuilder builder) {
                super.initialize(builder);
                NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
                NodeBuilder indexBuilder = oiBuilder.child(INITIAL_INDEX_NAME);
                indexBuilder.setProperty("type", ElasticIndexDefinition.TYPE_ELASTICSEARCH);
                indexBuilder.setProperty("async", "async");
            }
        };
    }

    @Test
    public void testInitialIndexTracking() throws Exception {
        ElasticIndexTracker tracker = indexProvider.getElasticIndexTracker();
        Set<String> stringSet = tracker.getIndexNodePaths();
        assertTrue(stringSet.size() == 1 && stringSet.contains(INITIAL_INDEX_PATH));
    }

    @Test
    public void immediateElasticIndexTracking() throws Exception {

        IndexDefinitionBuilder builder = createIndex("propa");
        builder.async("async");
        builder.indexRule("nt:base").property("propa");

        String indexId = UUID.randomUUID().toString();
        Tree index = setIndex(indexId, builder);
        root.commit();

        ElasticIndexTracker tracker = indexProvider.getElasticIndexTracker();
        Set<String> stringSet = tracker.getIndexNodePaths();
        assertTrue(stringSet.size() == 2 && stringSet.contains(index.getPath()));
    }

}
