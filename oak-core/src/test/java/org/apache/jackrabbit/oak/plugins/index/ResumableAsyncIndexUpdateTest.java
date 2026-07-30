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

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class ResumableAsyncIndexUpdateTest {

    static class TestableResume extends ResumableAsyncIndexUpdate {
        TestableResume(String lane, NodeStore store) {
            super(lane, store, new PropertyIndexEditorProvider());
        }
        boolean chunked(NodeState before) {
            return isChunkedRun(before);
        }
    }

    @Test
    public void laneNameHelpers() {
        assertEquals("resume_async", ResumableAsyncIndexUpdate.resumeLaneName("async"));
        assertTrue(ResumableAsyncIndexUpdate.isResumeLane("resume_async"));
        assertFalse(ResumableAsyncIndexUpdate.isResumeLane("async"));
    }

    @Test
    public void resumeLaneAlwaysChunksIncrementalWhenConfigured() {
        System.setProperty("oak.async.chunkTimeMs", "1000");
        try {
            TestableResume r = new TestableResume("resume_async", new MemoryNodeStore());
            // no FT_RESUMABLE_ASYNC needed on the resume lane
            assertTrue(r.chunked(new MemoryNodeStore().getRoot())); // incremental => chunk
            assertFalse(r.chunked(MISSING_NODE));                    // reindex only when reindex toggle on
            r.setResumableReindexEnabledForTest(true);
            assertTrue(r.chunked(MISSING_NODE));                     // reindex chunks with toggle on
        } finally {
            System.clearProperty("oak.async.chunkTimeMs");
        }
    }
}
