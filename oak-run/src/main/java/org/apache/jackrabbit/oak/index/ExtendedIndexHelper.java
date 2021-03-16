package org.apache.jackrabbit.oak.index;

import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.datastore.DataStoreTextWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexInfoProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExtendedIndexHelper extends IndexHelper implements Closeable {

    private LuceneIndexHelper luceneIndexHelper;
    private ExtractedTextCache extractedTextCache;
    private final Closer closer = Closer.create();

    public ExtendedIndexHelper(NodeStore store, BlobStore blobStore, Whiteboard whiteboard,
                               File outputDir, File workDir, List<String> indexPaths) {
        super(store, blobStore, whiteboard, outputDir, workDir, indexPaths);
    }

    public LuceneIndexHelper getLuceneIndexHelper(){
        if (luceneIndexHelper == null) {
            luceneIndexHelper = new LuceneIndexHelper(this);
            closer.register(luceneIndexHelper);
        }
        return luceneIndexHelper;
    }

    @Override
    protected void bindIndexInfoProviders(IndexInfoServiceImpl indexInfoService) {
        indexInfoService.bindInfoProviders(new LuceneIndexInfoProvider(store, getAsyncIndexInfoService(), workDir));
        indexInfoService.bindInfoProviders(new PropertyIndexInfoProvider(store));
    }

    public ExtractedTextCache getExtractedTextCache() {
        if (extractedTextCache == null) {
            extractedTextCache = new ExtractedTextCache(FileUtils.ONE_MB * 5, TimeUnit.HOURS.toSeconds(5));
        }
        return extractedTextCache;
    }

    public void setPreExtractedTextDir(File dir) throws IOException {
        getExtractedTextCache().setExtractedTextProvider(new DataStoreTextWriter(dir, true));
    }
}
