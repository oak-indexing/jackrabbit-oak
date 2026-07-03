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

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetsConfigProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.lucene.facet.FacetsConfig;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexEditorContext extends FulltextIndexEditorContext implements FacetsConfigProvider {
    
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexEditorContext.class);
    
    private FacetsConfig facetsConfig;

    private final IndexAugmentorFactory augmentorFactory;

    LuceneIndexEditorContext(NodeState root, NodeBuilder definition,
                             @Nullable IndexDefinition indexDefinition,
                             IndexUpdateCallback updateCallback,
                             FulltextIndexWriterFactory indexWriterFactory,
                             ExtractedTextCache extractedTextCache,
                             IndexAugmentorFactory augmentorFactory,
                             IndexingContext indexingContext, boolean asyncIndexing) {
        super(root, definition, indexDefinition, updateCallback, indexWriterFactory, extractedTextCache,
            indexingContext, asyncIndexing);
        this.augmentorFactory = augmentorFactory;
        
        // Register COMMIT_PROGRESS handler for resumable indexing
        registerCommitProgressCallback(indexingContext);
    }
    
    /**
     * Register a callback that finalizes the Lucene writer at resumable-indexing
     * chunk boundaries. {@code CHUNK_COMMIT} closes the writer so the documents
     * indexed in the chunk are actually persisted to the (possibly CopyOnWrite)
     * directory and become queryable; a bare {@code COMMIT_PROGRESS} flush is not
     * sufficient because it does not await the copy-to-remote nor record the
     * OAK-2029 status.
     */
    private void registerCommitProgressCallback(IndexingContext indexingContext) {
        indexingContext.registerIndexCommitCallback(new IndexCommitCallback() {
            @Override
            public void commitProgress(IndexCommitCallback.IndexProgress indexProgress) {
                try {
                    if (indexProgress == IndexCommitCallback.IndexProgress.CHUNK_COMMIT) {
                        LOG.info("[CHUNK_COMMIT] Closing Lucene writer for index: {}",
                                 getDefinition().getIndexPath());
                        closeWriterIfPresent();
                        LOG.info("[CHUNK_COMMIT] Successfully closed Lucene writer");
                    } else if (indexProgress == IndexCommitCallback.IndexProgress.COMMIT_PROGRESS) {
                        LOG.info("[COMMIT_PROGRESS] Flushing Lucene writer for index: {}",
                                 getDefinition().getIndexPath());
                        flushWriter();
                        LOG.info("[COMMIT_PROGRESS] Successfully flushed Lucene writer");
                    }
                } catch (IOException e) {
                    LOG.error("[{}] Failed to finalize Lucene writer", indexProgress, e);
                }
            }
        });
    }

    @Override
    public IndexDefinition.Builder newDefinitionBuilder() {
        return new LuceneIndexDefinition.Builder();
    }

    @Override
    public LuceneDocumentMaker newDocumentMaker(IndexDefinition.IndexingRule rule, String path){
        //Faceting is only enabled for async mode
        FacetsConfigProvider facetsConfigProvider = isAsyncIndexing() ? this : null;
        return new LuceneDocumentMaker(getTextExtractor(), facetsConfigProvider, augmentorFactory,
            definition, rule, path);
    }

    @Override
    public LuceneIndexWriter getWriter() {
        return (LuceneIndexWriter)super.getWriter();
    }

    @Override
    public FacetsConfig getFacetsConfig() {
        if (facetsConfig == null){
            facetsConfig = FacetHelper.getFacetsConfig(definitionBuilder);
        }
        return facetsConfig;
    }

    /** Only set for testing
     * @param c clock
     * */
    public static void setClock(Clock c) {
        FulltextIndexEditorContext.setClock(c);
    }
}