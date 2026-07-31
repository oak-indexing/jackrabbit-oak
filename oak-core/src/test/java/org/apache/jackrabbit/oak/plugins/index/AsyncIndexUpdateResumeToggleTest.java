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

import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

public class AsyncIndexUpdateResumeToggleTest {

    private static final String PROP_RESUME_LANES = "oak.async.resumeLanes";

    private AsyncIndexUpdate newAsync(String lane) {
        NodeStore store = new MemoryNodeStore();
        return new AsyncIndexUpdate(lane, store, new PropertyIndexEditorProvider());
    }

    /** Registers a {@code Feature} on a fresh whiteboard and flips its toggle to the given state. */
    private Feature newFeature(Whiteboard whiteboard, boolean enabled) {
        Feature feature = Feature.newFeature("FT_RESUMABLE_ASYNC", whiteboard);
        List<FeatureToggle> toggles = WhiteboardUtils.getServices(whiteboard, FeatureToggle.class);
        toggles.get(0).setEnabled(enabled);
        return feature;
    }

    @Test
    public void parseResumeLanesSplitsTrimsAndDropsBlanks() {
        Set<String> lanes = AsyncIndexUpdate.parseResumeLanes(" async , fulltext-async ,, ");
        assertEquals(2, lanes.size());
        assertTrue(lanes.contains("async"));
        assertTrue(lanes.contains("fulltext-async"));
    }

    @Test
    public void parseResumeLanesEmptyForNullOrBlank() {
        assertTrue(AsyncIndexUpdate.parseResumeLanes(null).isEmpty());
        assertTrue(AsyncIndexUpdate.parseResumeLanes("   ").isEmpty());
    }

    @Test
    public void disabledByDefault() {
        assertFalse(newAsync("async").isResumableAsyncEnabled());
    }

    @Test
    public void testOverrideForcesEnabledRegardlessOfLaneList() {
        AsyncIndexUpdate a = newAsync("async");
        a.setResumableAsyncEnabledForTest(true);
        assertTrue(a.isResumableAsyncEnabled());
    }

    @Test
    public void enabledFeatureAndLaneInAllowlistEnablesResumableAsync() {
        System.setProperty(PROP_RESUME_LANES, "async");
        try {
            Whiteboard whiteboard = new DefaultWhiteboard();
            try (Feature feature = newFeature(whiteboard, true)) {
                AsyncIndexUpdate a = newAsync("async");
                a.setResumableAsyncFeature(feature);
                assertTrue(a.isResumableAsyncEnabled());
            }
        } finally {
            System.clearProperty(PROP_RESUME_LANES);
        }
    }

    @Test
    public void enabledFeatureButLaneNotInAllowlistStaysDisabled() {
        System.setProperty(PROP_RESUME_LANES, "async");
        try {
            Whiteboard whiteboard = new DefaultWhiteboard();
            try (Feature feature = newFeature(whiteboard, true)) {
                AsyncIndexUpdate a = newAsync("fulltext-async");
                a.setResumableAsyncFeature(feature);
                assertFalse(a.isResumableAsyncEnabled());
            }
        } finally {
            System.clearProperty(PROP_RESUME_LANES);
        }
    }

    @Test
    public void disabledFeatureWithLaneInAllowlistStaysDisabled() {
        System.setProperty(PROP_RESUME_LANES, "async");
        try {
            Whiteboard whiteboard = new DefaultWhiteboard();
            try (Feature feature = newFeature(whiteboard, false)) {
                AsyncIndexUpdate a = newAsync("async");
                a.setResumableAsyncFeature(feature);
                assertFalse(a.isResumableAsyncEnabled());
            }
        } finally {
            System.clearProperty(PROP_RESUME_LANES);
        }
    }

    /** Exposes the protected chunk seams so defaults can be asserted without a full run. */
    static class TestableAsync extends AsyncIndexUpdate {
        TestableAsync(String lane, NodeStore store) {
            super(lane, store, new PropertyIndexEditorProvider());
        }
        boolean chunked(NodeState before) {
            return isChunkedRun(before);
        }
        long effChunkSize() {
            return effectiveChunkSize();
        }
        boolean resolveFlag(String prop) {
            return resolveResumeFlag(prop);
        }
    }

    @Test
    public void baseNeverChunksWhenResumeDisabled() {
        TestableAsync a = new TestableAsync("async", new MemoryNodeStore());
        a.setResumableAsyncEnabledForTest(false);
        assertFalse(a.chunked(MISSING_NODE));
    }

    @Test
    public void baseChunksBothPathsWhenResumeEnabledAndChunkConfigured() {
        System.setProperty("oak.async.chunkTimeMs", "1000");
        try {
            TestableAsync a = new TestableAsync("async", new MemoryNodeStore());
            a.setResumableAsyncEnabledForTest(true);
            // FT_RESUMABLE_ASYNC is the single gate: both incremental and reindex chunk.
            assertTrue(a.chunked(new MemoryNodeStore().getRoot())); // incremental
            assertTrue(a.chunked(MISSING_NODE));                    // reindex
        } finally {
            System.clearProperty("oak.async.chunkTimeMs");
        }
    }

    @Test
    public void enablingToggleAloneChunksBothPathsWithDefaultChunkSize() {
        // No chunkSize / chunkTimeMs set: FT_RESUMABLE_ASYNC is the top switch, so enabling
        // it alone yields a working chunked config with the default chunk size.
        TestableAsync a = new TestableAsync("async", new MemoryNodeStore());
        a.setResumableAsyncEnabledForTest(true);
        assertEquals(AsyncIndexUpdate.DEFAULT_RESUME_CHUNK_SIZE, a.effChunkSize());
        assertTrue(a.chunked(new MemoryNodeStore().getRoot())); // incremental
        assertTrue(a.chunked(MISSING_NODE));                    // reindex
    }

    @Test
    public void defaultChunkSizeOnlyAppliesWhenResumeEnabled() {
        TestableAsync a = new TestableAsync("async", new MemoryNodeStore());
        a.setResumableAsyncEnabledForTest(false);
        assertEquals(0, a.effChunkSize());
    }

    @Test
    public void explicitChunkSizeOverridesDefault() {
        System.setProperty("oak.async.chunkSize", "250");
        try {
            TestableAsync a = new TestableAsync("async", new MemoryNodeStore());
            a.setResumableAsyncEnabledForTest(true);
            assertEquals(250, a.effChunkSize());
        } finally {
            System.clearProperty("oak.async.chunkSize");
        }
    }

    @Test
    public void explicitZeroChunkSizeDisablesCountChunking() {
        System.setProperty("oak.async.chunkSize", "0");
        try {
            TestableAsync a = new TestableAsync("async", new MemoryNodeStore());
            a.setResumableAsyncEnabledForTest(true);
            assertEquals(0, a.effChunkSize());
            // Falls back to time-based chunking only; with none configured, no chunking.
            assertFalse(a.chunked(MISSING_NODE));
        } finally {
            System.clearProperty("oak.async.chunkSize");
        }
    }

    @Test
    public void resumeTuningFlagsDefaultOnWhenResumeEnabledAndOffOtherwise() {
        // PTBIN (SLIM) format is part of the resume default set: on when resume is enabled.
        String prop = "oak.async.pathTreeSlimFormat";
        TestableAsync a = new TestableAsync("async", new MemoryNodeStore());

        a.setResumableAsyncEnabledForTest(true);
        assertTrue(a.resolveFlag(prop)); // default on when resume enabled

        a.setResumableAsyncEnabledForTest(false);
        assertFalse(a.resolveFlag(prop)); // default off when resume disabled

        // Explicit value always wins, regardless of resume state.
        System.setProperty(prop, "false");
        try {
            a.setResumableAsyncEnabledForTest(true);
            assertFalse(a.resolveFlag(prop));
        } finally {
            System.clearProperty(prop);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void baseRejectsResumeLaneName() {
        new AsyncIndexUpdate("resume_async", new MemoryNodeStore(), new PropertyIndexEditorProvider());
    }

    @Test
    public void resumeSubclassAcceptsResumeLaneName() {
        // must not throw
        new ResumableAsyncIndexUpdate("resume_async", new MemoryNodeStore(), new PropertyIndexEditorProvider());
    }
}
