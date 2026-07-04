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
    public void revertDeletesResumeStateAndFlagsReindex() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder b = store.getRoot().builder();
        // resume state exists for the lane
        b.child(":async").child("resume_async-resume").setProperty("lastIndexedPath", "/content/x");
        // an index def that was mode=resume but is now reverted (mode removed)
        NodeBuilder def = b.child("oak:index").child("myIndex");
        def.setProperty("type", "property");
        def.setProperty("async", "async");            // still async
        // no "mode" property -> reverted
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ResumableAsyncIndexUpdate r = new ResumableAsyncIndexUpdate(
                ResumableAsyncIndexUpdate.resumeLaneName("async"), store,
                new PropertyIndexEditorProvider(), StatisticsProvider.NOOP, false);

        NodeBuilder root = store.getRoot().builder();
        r.cleanupRevertedIndexes(root);

        assertTrue(root.getChildNode("oak:index").getChildNode("myIndex").getBoolean("reindex"));
        assertFalse(root.getChildNode(":async").hasChildNode("resume_async-resume"));
    }
}
