package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.IndexNodeManager;
import org.apache.jackrabbit.oak.plugins.index.search.update.ReaderRefreshPolicy;
import org.apache.jackrabbit.oak.spi.state.NodeState;

class ElasticIndexNodeManager extends IndexNodeManager<ElasticIndexNode> {

    private final ElasticIndexNode elasticIndexNode;
    private final String path;

    ElasticIndexNodeManager(ElasticConnection elasticConnection, String path, NodeState root) {
        this.path = path;
        this.elasticIndexNode = new ElasticIndexNode(root, path, elasticConnection) {
            @Override
            public void release() {
                ElasticIndexNodeManager.this.release();
                super.release();
            }
        };
    }

    @Override
    protected String getName() {
        return path;
    }

    @Override
    protected ElasticIndexNode getIndexNode() {
        return elasticIndexNode;
    }

    @Override
    protected IndexDefinition getDefinition() {
        return elasticIndexNode.getDefinition();
    }

    @Override
    protected ReaderRefreshPolicy getReaderRefreshPolicy() {
        return ReaderRefreshPolicy.NEVER;
    }

    @Override
    protected void refreshReaders() {
        // do nothing
    }

    @Override
    protected void releaseResources() {
        // do nothing
    }
}
