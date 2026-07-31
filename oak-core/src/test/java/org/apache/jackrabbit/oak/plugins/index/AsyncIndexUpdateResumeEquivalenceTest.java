/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class AsyncIndexUpdateResumeEquivalenceTest {

    private static final int NODE_COUNT = 50;

    /** Store with an async property index on "foo" and no content yet. */
    private static NodeStore storeWithIndex() throws CommitFailedException {
        NodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, Set.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return store;
    }

    private static void addContent(NodeStore store) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        for (int i = 0; i < NODE_COUNT; i++) {
            builder.child("n" + i).setProperty("foo", "v" + i);
        }
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /** Encoded property values stored under the property index content node. */
    private static Set<String> indexedKeys(NodeStore store) {
        Set<String> keys = new HashSet<>();
        NodeState content = store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME)
                .getChildNode("fooIndex").getChildNode(INDEX_CONTENT_NODE_NAME);
        for (String k : content.getChildNodeNames()) {
            keys.add(k);
        }
        return keys;
    }

    /** Drives the lane until it reports finished; returns the number of run() cycles used. */
    private static int drain(AsyncIndexUpdate async) {
        int cycles = 0;
        for (int i = 0; i < 200; i++) {
            async.run();
            cycles++;
            if (async.isFinished()) {
                break;
            }
        }
        return cycles;
    }

    /**
     * Base-lane seamless resume must converge to the SAME index content as a monolithic run,
     * and must actually chunk. Chunking only engages on incremental work (before != MISSING)
     * unless the reindex toggle is on, so we establish a checkpoint first, then add the content
     * that must be chunked.
     */
    @Test
    public void chunkedMatchesMonolithicAndActuallyChunks() throws Exception {
        System.setProperty("oak.async.chunkSize", "10");
        try {
            // --- monolithic baseline ---
            NodeStore mono = storeWithIndex();
            AsyncIndexUpdate monoAsync =
                    new AsyncIndexUpdate("async", mono, new PropertyIndexEditorProvider());
            drain(monoAsync);            // initial reindex over empty content -> checkpoint
            addContent(mono);
            drain(monoAsync);            // single incremental pass (toggle off)
            Set<String> monolithic = indexedKeys(mono);

            // --- seamless-resume (chunked) ---
            NodeStore ch = storeWithIndex();
            AsyncIndexUpdate chAsync =
                    new AsyncIndexUpdate("async", ch, new PropertyIndexEditorProvider());
            chAsync.setResumableAsyncEnabledForTest(true);
            drain(chAsync);              // initial pass over empty content -> checkpoint (not chunked)
            addContent(ch);
            int chunkedCycles = drain(chAsync);   // incremental -> chunked over NODE_COUNT nodes
            Set<String> chunked = indexedKeys(ch);

            assertFalse("index content must not be empty", monolithic.isEmpty());
            assertEquals(NODE_COUNT, monolithic.size());
            assertEquals("chunked resume must index the same keys as monolithic",
                    monolithic, chunked);
            assertTrue("chunked incremental run must take multiple cycles, got " + chunkedCycles,
                    chunkedCycles > 1);
        } finally {
            System.clearProperty("oak.async.chunkSize");
        }
    }

    /**
     * Reindex-from-scratch (before == MISSING_NODE) must also chunk and converge. Here the content
     * exists BEFORE the first indexer run, so the very first pass is an initial reindex over all
     * NODE_COUNT nodes; with resume enabled it must spread that reindex across multiple chunk
     * cycles and still index the exact same keys as a monolithic reindex — without livelocking.
     */
    @Test
    public void chunkedReindexFromScratchMatchesMonolithicAndActuallyChunks() throws Exception {
        System.setProperty("oak.async.chunkSize", "10");
        try {
            // --- monolithic reindex baseline (resume off -> single pass) ---
            NodeStore mono = storeWithIndex();
            addContent(mono);            // content present before the first run
            AsyncIndexUpdate monoAsync =
                    new AsyncIndexUpdate("async", mono, new PropertyIndexEditorProvider());
            drain(monoAsync);            // initial reindex over all content, monolithic
            Set<String> monolithic = indexedKeys(mono);

            // --- chunked reindex-from-scratch (resume on) ---
            NodeStore ch = storeWithIndex();
            addContent(ch);              // content present before the first run
            AsyncIndexUpdate chAsync =
                    new AsyncIndexUpdate("async", ch, new PropertyIndexEditorProvider());
            chAsync.setResumableAsyncEnabledForTest(true);
            int chunkedCycles = drain(chAsync);   // initial reindex must chunk over NODE_COUNT nodes
            Set<String> chunked = indexedKeys(ch);

            assertFalse("index content must not be empty", monolithic.isEmpty());
            assertEquals(NODE_COUNT, monolithic.size());
            assertEquals("chunked reindex-from-scratch must index the same keys as monolithic",
                    monolithic, chunked);
            assertTrue("chunked reindex must take multiple cycles, got " + chunkedCycles,
                    chunkedCycles > 1);
            assertTrue("chunked reindex must converge (not livelock), got " + chunkedCycles
                    + " cycles", chunkedCycles < 200);
        } finally {
            System.clearProperty("oak.async.chunkSize");
        }
    }
}
