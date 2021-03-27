package org.apache.jackrabbit.oak.index.indexer.document;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class DocumentStoreIndexer extends DocumentStoreIndexerBase implements Closeable {

    private final ExtendedIndexHelper extendedIndexHelper;

    public DocumentStoreIndexer(ExtendedIndexHelper extendedIndexHelper, IndexerSupport indexerSupport) throws IOException {
        super(extendedIndexHelper, indexerSupport);
        this.extendedIndexHelper = extendedIndexHelper;
        setProviders();
    }

    private NodeStateIndexerProvider createLuceneIndexProvider() throws IOException {
        return new LuceneIndexerProvider(extendedIndexHelper, indexerSupport);
    }

    protected List<NodeStateIndexerProvider> createProviders() throws IOException {
        List<NodeStateIndexerProvider> providers = ImmutableList.of(
                createLuceneIndexProvider()
        );

        providers.forEach(closer::register);
        return providers;
    }

    @Override
    protected void preIndexOpertaions(List<NodeStateIndexer> indexers) {
        // NOOP
    }
}
