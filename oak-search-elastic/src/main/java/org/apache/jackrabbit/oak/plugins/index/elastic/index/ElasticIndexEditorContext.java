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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.DocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditorContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ElasticIndexEditorContext extends FulltextIndexEditorContext<ElasticDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexEditorContext.class);

    ElasticIndexEditorContext(NodeState root,
                              NodeBuilder definition, @Nullable ElasticIndexDefinition indexDefinition,
                              IndexUpdateCallback updateCallback,
                              ElasticIndexWriterFactory indexWriterFactory,
                              ExtractedTextCache extractedTextCache,
                              IndexingContext indexingContext,
                              boolean asyncIndexing) {
        super(root, definition, indexDefinition, updateCallback, indexWriterFactory, extractedTextCache, indexingContext, asyncIndexing);
        registerCommitProgressCallback(indexingContext);
    }

    /**
     * Registers a callback that durably persists buffered documents at resumable-indexing chunk
     * boundaries. Both {@code COMMIT_PROGRESS} and {@code CHUNK_COMMIT} flush the writer, which
     * for Elasticsearch force-sends the buffered bulk operations (and waits for acknowledgement
     * when configured) without unregistering the index. Without this, documents indexed in a
     * chunk could remain buffered in the bulk ingester when the resume checkpoint is persisted;
     * a resumed run would then skip those already-marked paths and lose the documents. Unlike
     * Lucene there is no separate close-per-chunk step: a durable bulk flush is the guarantee,
     * and the writer stays open for the next chunk.
     */
    private void registerCommitProgressCallback(IndexingContext indexingContext) {
        indexingContext.registerIndexCommitCallback(indexProgress -> {
            if (indexProgress == IndexCommitCallback.IndexProgress.CHUNK_COMMIT
                    || indexProgress == IndexCommitCallback.IndexProgress.COMMIT_PROGRESS) {
                try {
                    flushWriter();
                } catch (IOException e) {
                    LOG.error("[{}] Failed to flush Elastic writer for index {}", indexProgress,
                            getDefinition().getIndexPath(), e);
                }
            }
        });
    }

    @Override
    public ElasticIndexDefinition.Builder newDefinitionBuilder() {
        return new ElasticIndexDefinition.Builder(((ElasticIndexDefinition) definition).getIndexPrefix());
    }

    @Override
    public DocumentMaker<ElasticDocument> newDocumentMaker(IndexDefinition.IndexingRule rule, String path) {
        return new ElasticDocumentMaker(getTextExtractor(), getDefinition(), rule, path);
    }

    @Override
    public ElasticIndexWriter getWriter() {
        return (ElasticIndexWriter) super.getWriter();
    }

    @Override
    public boolean storedIndexDefinitionEnabled() {
        return false;
    }
}
