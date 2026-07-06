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
package org.apache.jackrabbit.oak.plugins.index.resume;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PathTreeTest {

    private PathTree buildSampleTree() {
        PathTree tree = new PathTree();
        // /a is fully processed (frontier node)
        tree.markEnterCompleted("/a");
        tree.markLeaveCompleted("/a");
        // /b/c is fully processed, /b itself is still in-progress
        tree.markEnterCompleted("/b");
        tree.markEnterCompleted("/b/c");
        tree.markLeaveCompleted("/b/c");
        return tree;
    }

    @Test
    public void binaryFormatRoundTrip() throws IOException {
        PathTree original = buildSampleTree();

        NodeBuilder builder = new MemoryNodeStore().getRoot().builder();
        original.serializeSlimBinaryTo(builder);

        PropertyState binaryFormatProp = builder.getProperty("binaryFormat");
        assertTrue("binaryFormat marker must be set", binaryFormatProp != null && binaryFormatProp.getValue(Type.BOOLEAN));

        PropertyState blobProp = builder.getProperty("pathTreeData");
        assertTrue("pathTreeData must be a Type.BINARY property", blobProp != null && blobProp.getType() == Type.BINARY);

        // The STRINGS-array properties from the legacy encoding must not be written
        assertNull(builder.getProperty("paths"));
        assertNull(builder.getProperty("enterFlags"));
        assertNull(builder.getProperty("leaveFlags"));
        assertNull(builder.getProperty("frontierFlags"));

        PathTree restored = PathTree.deserializeSlimFrom(builder.getNodeState());

        assertTrue(restored.isFullyProcessed("/a"));
        assertTrue(restored.isFullyProcessed("/b/c"));
        assertTrue(restored.isEnterCompleted("/b"));
        assertFalse(restored.isExactPathFullyProcessed("/b"));
        // "/b/d" was never visited but is implicitly fully processed via ancestor
        // checking once "/b" itself is fully processed; here "/b" is not, so it
        // must fall back to false rather than throwing.
        assertFalse(restored.isFullyProcessed("/b/d"));
    }

    @Test
    public void deserializeAutoDispatchesToBinaryFormat() throws IOException {
        PathTree original = buildSampleTree();

        NodeBuilder builder = new MemoryNodeStore().getRoot().builder();
        original.serializeSlimBinaryTo(builder);

        assertTrue(PathTree.isSlimFormat(builder.getNodeState()));
        PathTree restored = PathTree.deserializeAuto(builder.getNodeState());
        assertTrue(restored.isFullyProcessed("/a"));
        assertTrue(restored.isFullyProcessed("/b/c"));
    }

    @Test
    public void legacyStringsFormatStillReadable() {
        PathTree original = buildSampleTree();

        NodeBuilder builder = new MemoryNodeStore().getRoot().builder();
        original.serializeSlimTo(builder);

        assertNull("legacy STRINGS format must not set the new binary marker",
            builder.getProperty("binaryFormat"));

        PathTree restored = PathTree.deserializeSlimFrom(builder.getNodeState());
        assertTrue(restored.isFullyProcessed("/a"));
        assertTrue(restored.isFullyProcessed("/b/c"));
        assertTrue(restored.isEnterCompleted("/b"));
    }

    @Test
    public void binaryFormatMatchesFullyProcessedCountOfStringsFormat() throws IOException {
        PathTree original = buildSampleTree();
        assertEquals(2, original.getFullyProcessedCount());

        NodeBuilder builder = new MemoryNodeStore().getRoot().builder();
        original.serializeSlimBinaryTo(builder);
        PathTree restored = PathTree.deserializeSlimFrom(builder.getNodeState());

        assertEquals(original.getFullyProcessedCount(), restored.getFullyProcessedCount());
    }
}
