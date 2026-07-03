/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.SimpleCredentials;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test to verify that diff traversal order is stable/deterministic.
 * 
 * This is critical for PathTree-based skip optimization - if order is stable,
 * we can reliably skip already-processed nodes without re-reading from NodeStore.
 */
public class StableTraversalOrderTest {

    private NodeStore nodeStore;
    private ContentRepository repository;
    private Root root;

    @Before
    public void setup() throws Exception {
        // Use SegmentNodeStore for realistic testing
        nodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        
        repository = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .createContentRepository();
        
        root = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null).getLatestRoot();
    }

    @After
    public void tearDown() throws IOException {
        if (repository != null) {
            repository = null;
        }
    }

    /**
     * Test that diff traversal order is stable across multiple runs.
     */
    @Test
    public void testStableTraversalOrder() throws Exception {
        System.out.println("\n=== TEST: Stable Traversal Order ===\n");
        
        // Create initial content
        NodeState before = nodeStore.getRoot();
        
        // Add content
        Tree content = root.getTree("/").addChild("content");
        Tree dam = content.addChild("dam");
        
        // Create nodes with various names to test ordering
        for (int i = 0; i < 100; i++) {
            Tree asset = dam.addChild("asset-" + i);
            asset.setProperty("title", "Asset " + i);
            asset.addChild("jcr:content").setProperty("data", "content-" + i);
        }
        
        root.commit();
        NodeState after = nodeStore.getRoot();
        
        System.out.println("Created 100 assets with jcr:content children");
        
        // Run diff multiple times and record order
        List<String> run1Enter = new ArrayList<>();
        List<String> run1Leave = new ArrayList<>();
        List<String> run2Enter = new ArrayList<>();
        List<String> run2Leave = new ArrayList<>();
        List<String> run3Enter = new ArrayList<>();
        List<String> run3Leave = new ArrayList<>();
        
        // Run 1
        System.out.println("\n--- Run 1 ---");
        TraversalOrderRecorder recorder1 = new TraversalOrderRecorder(run1Enter, run1Leave);
        EditorDiff.process(recorder1, before, after);
        System.out.println("Enter count: " + run1Enter.size());
        System.out.println("Leave count: " + run1Leave.size());
        
        // Run 2
        System.out.println("\n--- Run 2 ---");
        TraversalOrderRecorder recorder2 = new TraversalOrderRecorder(run2Enter, run2Leave);
        EditorDiff.process(recorder2, before, after);
        System.out.println("Enter count: " + run2Enter.size());
        System.out.println("Leave count: " + run2Leave.size());
        
        // Run 3
        System.out.println("\n--- Run 3 ---");
        TraversalOrderRecorder recorder3 = new TraversalOrderRecorder(run3Enter, run3Leave);
        EditorDiff.process(recorder3, before, after);
        System.out.println("Enter count: " + run3Enter.size());
        System.out.println("Leave count: " + run3Leave.size());
        
        // Verify order is stable
        System.out.println("\n--- Verification ---");
        
        assertEquals("Enter order should be same between run 1 and 2", run1Enter, run2Enter);
        assertEquals("Enter order should be same between run 2 and 3", run2Enter, run3Enter);
        assertEquals("Leave order should be same between run 1 and 2", run1Leave, run2Leave);
        assertEquals("Leave order should be same between run 2 and 3", run2Leave, run3Leave);
        
        System.out.println("✅ STABLE ORDER VERIFIED - All 3 runs produced identical traversal order!");
        
        // Print first 20 and last 20 paths for debugging
        System.out.println("\nFirst 20 enter paths:");
        for (int i = 0; i < Math.min(20, run1Enter.size()); i++) {
            System.out.println("  " + i + ": " + run1Enter.get(i));
        }
        
        System.out.println("\nLast 20 enter paths:");
        for (int i = Math.max(0, run1Enter.size() - 20); i < run1Enter.size(); i++) {
            System.out.println("  " + i + ": " + run1Enter.get(i));
        }
    }

    /**
     * Test with larger dataset to ensure stability at scale.
     */
    @Test
    public void testStableOrderWithLargeDataset() throws Exception {
        System.out.println("\n=== TEST: Stable Order with Large Dataset (1000 nodes) ===\n");
        
        NodeState before = nodeStore.getRoot();
        
        // Create larger dataset
        Tree content = root.getTree("/").addChild("largeContent");
        for (int i = 0; i < 1000; i++) {
            Tree node = content.addChild("node-" + i);
            node.setProperty("index", i);
            if (i % 10 == 0) {
                // Add some nested content
                Tree child = node.addChild("child");
                child.setProperty("parent", i);
            }
        }
        
        root.commit();
        NodeState after = nodeStore.getRoot();
        
        System.out.println("Created 1000 nodes with nested children");
        
        List<String> run1Enter = new ArrayList<>();
        List<String> run1Leave = new ArrayList<>();
        List<String> run2Enter = new ArrayList<>();
        List<String> run2Leave = new ArrayList<>();
        
        TraversalOrderRecorder recorder1 = new TraversalOrderRecorder(run1Enter, run1Leave);
        EditorDiff.process(recorder1, before, after);
        
        TraversalOrderRecorder recorder2 = new TraversalOrderRecorder(run2Enter, run2Leave);
        EditorDiff.process(recorder2, before, after);
        
        assertEquals("Large dataset: Enter order should be stable", run1Enter, run2Enter);
        assertEquals("Large dataset: Leave order should be stable", run1Leave, run2Leave);
        
        System.out.println("✅ STABLE ORDER VERIFIED for 1000 nodes!");
        System.out.println("Total nodes traversed: " + run1Enter.size());
    }

    /**
     * Test that order is consistent even with hash-based ordering in SegmentStore.
     */
    @Test
    public void testHashBasedOrderingStability() throws Exception {
        System.out.println("\n=== TEST: Hash-Based Ordering Stability ===\n");
        
        NodeState before = nodeStore.getRoot();
        
        // Create nodes with names that would have different hash values
        Tree content = root.getTree("/").addChild("hashTest");
        String[] names = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", 
                          "eta", "theta", "iota", "kappa", "lambda", "mu",
                          "001", "100", "010", "abc", "xyz", "def"};
        
        for (String name : names) {
            Tree node = content.addChild(name);
            node.setProperty("name", name);
        }
        
        root.commit();
        NodeState after = nodeStore.getRoot();
        
        System.out.println("Created " + names.length + " nodes with varied names");
        
        List<String> run1Enter = new ArrayList<>();
        List<String> run1Leave = new ArrayList<>();
        List<String> run2Enter = new ArrayList<>();
        List<String> run2Leave = new ArrayList<>();
        
        // Run multiple times
        for (int i = 0; i < 5; i++) {
            List<String> enterList = new ArrayList<>();
            List<String> leaveList = new ArrayList<>();
            TraversalOrderRecorder recorder = new TraversalOrderRecorder(enterList, leaveList);
            EditorDiff.process(recorder, before, after);
            
            if (i == 0) {
                run1Enter.addAll(enterList);
                run1Leave.addAll(leaveList);
            } else {
                assertEquals("Run " + (i+1) + " enter order should match run 1", run1Enter, enterList);
                assertEquals("Run " + (i+1) + " leave order should match run 1", run1Leave, leaveList);
            }
        }
        
        System.out.println("✅ HASH-BASED ORDERING IS STABLE across 5 runs!");
        System.out.println("Traversal order (enter):");
        for (String path : run1Enter) {
            if (path.startsWith("/hashTest/")) {
                System.out.println("  " + path);
            }
        }
    }

    /**
     * Editor that records traversal order.
     */
    private static class TraversalOrderRecorder implements Editor {
        private final List<String> enterPaths;
        private final List<String> leavePaths;
        private final String path;

        TraversalOrderRecorder(List<String> enterPaths, List<String> leavePaths) {
            this("/", enterPaths, leavePaths);
        }

        TraversalOrderRecorder(String path, List<String> enterPaths, List<String> leavePaths) {
            this.path = path;
            this.enterPaths = enterPaths;
            this.leavePaths = leavePaths;
        }

        @Override
        public void enter(NodeState before, NodeState after) {
            enterPaths.add(path);
        }

        @Override
        public void leave(NodeState before, NodeState after) {
            leavePaths.add(path);
        }

        @Override
        public void propertyAdded(PropertyState after) { }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) { }

        @Override
        public void propertyDeleted(PropertyState before) { }

        @Override
        @Nullable
        public Editor childNodeAdded(String name, NodeState after) {
            String childPath = path.equals("/") ? "/" + name : path + "/" + name;
            return new TraversalOrderRecorder(childPath, enterPaths, leavePaths);
        }

        @Override
        @Nullable
        public Editor childNodeChanged(String name, NodeState before, NodeState after) {
            String childPath = path.equals("/") ? "/" + name : path + "/" + name;
            return new TraversalOrderRecorder(childPath, enterPaths, leavePaths);
        }

        @Override
        @Nullable
        public Editor childNodeDeleted(String name, NodeState before) {
            String childPath = path.equals("/") ? "/" + name : path + "/" + name;
            return new TraversalOrderRecorder(childPath, enterPaths, leavePaths);
        }
    }
}

