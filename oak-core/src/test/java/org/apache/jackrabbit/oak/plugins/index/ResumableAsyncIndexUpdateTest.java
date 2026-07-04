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
package org.apache.jackrabbit.oak.plugins.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Test;

public class ResumableAsyncIndexUpdateTest {

    @Test
    public void laneNameHelpers() {
        assertEquals("resume_async", ResumableAsyncIndexUpdate.resumeLaneName("async"));
        assertEquals("resume_fulltext-async", ResumableAsyncIndexUpdate.resumeLaneName("fulltext-async"));
        assertEquals("async", ResumableAsyncIndexUpdate.baseLaneName("resume_async"));
        assertTrue(ResumableAsyncIndexUpdate.isResumeLane("resume_async"));
        assertFalse(ResumableAsyncIndexUpdate.isResumeLane("async"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void baseLaneNameRejectsNonResume() {
        ResumableAsyncIndexUpdate.baseLaneName("async");
    }

    @Test
    public void firstResumeRunSeedsFromBaseCheckpoint() throws CommitFailedException {
        MemoryNodeStore store = new MemoryNodeStore();
        // simulate the base lane having indexed up to checkpoint "cp-base"
        NodeBuilder b = store.getRoot().builder();
        b.child(":async").setProperty("async", "cp-base");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

        NodeState async = store.getRoot().getChildNode(":async");
        // resume lane has no :async/resume_async yet -> must seed from base "cp-base"
        assertEquals("cp-base", r.resolveBeforeCheckpoint(async));
    }

    @Test
    public void revertedManagedDefIsReindexedAndStateDeleted() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder b = store.getRoot().builder();
        b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/x");
        NodeBuilder def = b.child("oak:index").child("myIndex");
        def.setProperty("type", "property");
        def.setProperty("async", "async");
        def.setProperty(":resumeManaged", true);   // was managed by the resume lane
        // no "mode" property -> reverted
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

        NodeBuilder root = store.getRoot().builder();
        r.cleanupRevertedIndexes(root);

        NodeBuilder healed = root.getChildNode("oak:index").getChildNode("myIndex");
        assertTrue(healed.getBoolean("reindex"));
        assertFalse(healed.hasProperty(":resumeManaged"));   // marker cleared
        assertFalse(root.getChildNode(":async").hasChildNode("resume_async-resume"));
    }

    @Test
    public void ordinaryNeverManagedDefIsUntouched() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder b = store.getRoot().builder();
        NodeBuilder def = b.child("oak:index").child("plainIndex");
        def.setProperty("type", "property");
        def.setProperty("async", "async");
        // no mode, no :resumeManaged marker -> ordinary index, never in resume mode
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

        NodeBuilder root = store.getRoot().builder();
        r.cleanupRevertedIndexes(root);

        NodeBuilder untouched = root.getChildNode("oak:index").getChildNode("plainIndex");
        assertFalse(untouched.getBoolean("reindex"));         // NOT flagged (no reindex storm)
        assertFalse(untouched.hasProperty(":resumeManaged"));
    }

    @Test
    public void resumeModeDefIsMarkedAndStateRetained() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder b = store.getRoot().builder();
        b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/x");
        NodeBuilder def = b.child("oak:index").child("liveIndex");
        def.setProperty("type", "property");
        def.setProperty("async", "async");
        def.setProperty("mode", "resume");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

        NodeBuilder root = store.getRoot().builder();
        r.cleanupRevertedIndexes(root);

        NodeBuilder live = root.getChildNode("oak:index").getChildNode("liveIndex");
        assertFalse(live.getBoolean("reindex"));              // active resume def not reindexed
        assertTrue(live.getBoolean(":resumeManaged"));        // stamped as managed
        assertTrue(root.getChildNode(":async").hasChildNode("resume_async-resume")); // state retained
    }

    @Test
    public void mixedLiveAndRevertedDefsHandledIndependently() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder b = store.getRoot().builder();
        b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/x");
        NodeBuilder live = b.child("oak:index").child("liveIndex");
        live.setProperty("type", "property");
        live.setProperty("async", "async");
        live.setProperty("mode", "resume");
        NodeBuilder reverted = b.child("oak:index").child("revertedIndex");
        reverted.setProperty("type", "property");
        reverted.setProperty("async", "async");
        reverted.setProperty(":resumeManaged", true);   // was managed, mode now removed
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

        NodeBuilder root = store.getRoot().builder();
        r.cleanupRevertedIndexes(root);

        NodeBuilder oakIndex = root.getChildNode("oak:index");
        assertFalse(oakIndex.getChildNode("liveIndex").getBoolean("reindex"));
        assertTrue(oakIndex.getChildNode("revertedIndex").getBoolean("reindex"));
        assertFalse(oakIndex.getChildNode("revertedIndex").hasProperty(":resumeManaged"));
        // a resume-mode def still remains -> resume state is NOT deleted
        assertTrue(root.getChildNode(":async").hasChildNode("resume_async-resume"));
    }

    @Test
    public void chunkedRunCoversReindexOnlyWhenToggleOn() {
        String prev = System.getProperty("oak.async.chunkSize");
        System.setProperty("oak.async.chunkSize", "100");
        try {
            ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                    ResumableAsyncIndexUpdate.resumeLaneName("async"), new MemoryNodeStore(),
                    new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);
            NodeState missing = org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
            NodeState present = new MemoryNodeStore().getRoot(); // any non-MISSING state

            // incremental (before != MISSING_NODE) is always chunked when chunk size is set
            assertTrue(r.isChunkedRun(present));

            // initial/reindex (before == MISSING_NODE) chunks ONLY when the toggle is on
            r.setResumableReindexEnabledForTest(false);
            assertFalse("reindex not chunked when toggle off", r.isChunkedRun(missing));

            r.setResumableReindexEnabledForTest(true);
            assertTrue("reindex chunked when toggle on", r.isChunkedRun(missing));
        } finally {
            if (prev == null) System.clearProperty("oak.async.chunkSize");
            else System.setProperty("oak.async.chunkSize", prev);
        }
    }

    @Test
    public void normalLaneReindexesResumeDefWhenToggleOff() {
        NodeBuilder resumeReindexing = EmptyNodeState.EMPTY_NODE.builder();
        resumeReindexing.setProperty("async", "async");
        resumeReindexing.setProperty(IndexConstants.MODE_PROPERTY_NAME, IndexConstants.MODE_RESUME);
        resumeReindexing.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        // toggle OFF: normal lane (resumeLane=false) MUST pick it up to run the native reindex
        assertTrue(IndexUpdate.isIncluded("async", resumeReindexing, false, /*resumableReindexEnabled*/ false));
        // toggle ON: normal lane still skips it (resume lane handles the reindex)
        assertFalse(IndexUpdate.isIncluded("async", resumeReindexing, false, /*resumableReindexEnabled*/ true));
        // resume lane (resumeLane=true) always processes its own resume-mode defs
        assertTrue(IndexUpdate.isIncluded("async", resumeReindexing, true, /*resumableReindexEnabled*/ false));
    }

    @Test
    public void resumeLanePausesDuringNativeReindexThenResets() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder b = store.getRoot().builder();
        b.child(":async").setProperty("resume_async", "cp-pause");        // C_pause checkpoint
        b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/y");
        NodeBuilder def = b.child("oak:index").child("idx");
        def.setProperty("type", "property");
        def.setProperty("async", "async");
        def.setProperty(IndexConstants.MODE_PROPERTY_NAME, IndexConstants.MODE_RESUME);
        def.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);       // native reindex in flight
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);
        r.setResumableReindexEnabledForTest(false);   // toggle OFF

        // reindex in flight -> pause
        assertTrue(r.shouldPauseForNativeReindex(store.getRoot()));

        // native reindex completes: clear the reindex flag
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("oak:index").child("idx").setProperty(IndexConstants.REINDEX_PROPERTY_NAME, false);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse(r.shouldPauseForNativeReindex(store.getRoot()));

        // reset: cursor node dropped, C_pause checkpoint retained
        NodeBuilder root = store.getRoot().builder();
        r.resetAfterNativeReindex(root);
        assertFalse(root.getChildNode(":async").hasChildNode("resume_async-resume"));
        assertEquals("cp-pause", root.getChildNode(":async").getString("resume_async"));

        // when the toggle is ON, the resume lane never pauses (it owns the reindex)
        r.setResumableReindexEnabledForTest(true);
        NodeBuilder b3 = store.getRoot().builder();
        b3.child("oak:index").child("idx").setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        store.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertFalse(r.shouldPauseForNativeReindex(store.getRoot()));
    }
}
