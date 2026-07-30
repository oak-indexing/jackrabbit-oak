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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
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
}
