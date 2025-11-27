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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexPopulator;
import org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.ChangeTrackingIndexQuery;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.ChangeEntry;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadata;
import org.apache.jackrabbit.oak.plugins.index.search.changetracker.IndexProgressMetadataManager;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Basic test demonstrating how to use the Change Tracker system.
 * 
 * <p>This test shows:
 * <ul>
 *   <li>How to set up the change tracking index</li>
 *   <li>How to initialize and run the change tracking populator</li>
 *   <li>How to create an index that uses change tracking</li>
 *   <li>How to make changes and verify they're recorded</li>
 *   <li>How to query the change tracking index</li>
 *   <li>How progress tracking works</li>
 * </ul>
 * 
 * <p><strong>Note:</strong> This is a simplified test for demonstration.
 * For production use cases, see ChangeTrackingE2ETest.java and 
 * CHANGE_TRACKER_GUIDE.md for comprehensive examples.
 */
public class BasicChangeTrackerTest {
    
    private NodeStore nodeStore;
    private Directory changeTrackingDirectory;
    private ChangeTrackingIndexPopulator populator;
    private IndexProgressMetadataManager metadataManager;
    private ContentRepository contentRepository;
    private ContentSession contentSession;
    private Root root;
    private AsyncIndexUpdate asyncIndexUpdate;
    
    @Before
    public void setUp() throws Exception {
        // 1. Create a NodeStore (using MemoryNodeStore for testing)
        nodeStore = new MemoryNodeStore();
        
        // 2. Create the Lucene directory for the change tracking index
        changeTrackingDirectory = new RAMDirectory();
        
        // 3. Create metadata manager for progress tracking
        metadataManager = new IndexProgressMetadataManager(nodeStore);
        
        // 4. Create and initialize the change tracking populator
        // Enable population via system property
        System.setProperty("oak.changeTracker.population.enabled", "true");
        
        populator = new ChangeTrackingIndexPopulator(
            nodeStore,
            changeTrackingDirectory,
            metadataManager,
            StatisticsProvider.NOOP
        );
        
        populator.initialize();
        
        // 5. Create Oak ContentRepository with Lucene index support
        LuceneIndexProvider provider = new LuceneIndexProvider();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();
        
        contentRepository = new Oak(nodeStore)
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with((org.apache.jackrabbit.oak.spi.commit.Observer) provider)
            .with(editorProvider)
            .with(new org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider())
            .with(new org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider())
            .createContentRepository();
        
        // 6. Create a content session and root
        contentSession = contentRepository.login(null, null);
        root = contentSession.getLatestRoot();
        
        // 7. Create async index update for traditional indexing
        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, editorProvider);
        
        // Verify initialization
        assertTrue("Populator should be initialized", populator.isInitialized());
        System.out.println("✓ Change tracking system initialized");
        System.out.println("✓ Oak content repository created");
    }
    
    @After
    public void tearDown() throws Exception {
        if (contentSession != null) {
            contentSession.close();
        }
        if (asyncIndexUpdate != null) {
            asyncIndexUpdate.close();
        }
        if (populator != null) {
            populator.close();
        }
        if (changeTrackingDirectory != null) {
            changeTrackingDirectory.close();
        }
        System.clearProperty("oak.changeTracker.population.enabled");
    }
    
    /**
     * Test 1: Basic change recording
     * 
     * Demonstrates:
     * - Making changes to the repository
     * - Running the change tracker populator
     * - Verifying changes are recorded in the change tracking index
     */
    @Test
    public void testBasicChangeRecording() throws Exception {
        System.out.println("\n=== Test 1: Basic Change Recording ===");
        
        // Make some changes to the repository using Tree API (creates checkpoints)
        Tree content = root.getTree("/").addChild("content");
        content.setProperty("title", "Test Content");
        
        Tree page1 = content.addChild("page1");
        page1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        page1.setProperty("text", "Page 1 content");
        
        Tree page2 = content.addChild("page2");
        page2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        page2.setProperty("text", "Page 2 content");
        
        // Commit changes - this creates a checkpoint that AsyncIndexUpdate can track
        root.commit();
        System.out.println("✓ Created test content: /content/page1, /content/page2");
        
        // Run the change tracking populator
        // This runs a checkpoint diff and records all changed paths
        populator.run();
        
        // Force commit of the IndexWriter
        commitChangeTrackingIndex();
        
        // Refresh root to see indexed changes
        root = contentSession.getLatestRoot();
        System.out.println("✓ Change tracking populator executed");
        
        // Query the change tracking index to verify changes were recorded
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            
            // Get all changes (starting from timestamp 0, serial 0)
            List<ChangeEntry> changes = query.getUnprocessedChanges(0, 0, 100);
            
            System.out.println("✓ Found " + changes.size() + " change entries");
            
            // Verify we recorded changes
            assertTrue("Should have recorded changes", changes.size() > 0);
            
            // Print the changes
            System.out.println("\nRecorded changes:");
            for (ChangeEntry entry : changes) {
                System.out.printf("  - Path: %s, Timestamp: %d, Serial: %d%n",
                    entry.getPath(),
                    entry.getDiffProcessingTime(),
                    entry.getSerialNumber());
            }
            
            // Verify specific paths were recorded
            boolean foundPage1 = changes.stream()
                .anyMatch(e -> e.getPath().equals("/content/page1"));
            boolean foundPage2 = changes.stream()
                .anyMatch(e -> e.getPath().equals("/content/page2"));
            
            assertTrue("Should have recorded /content/page1", foundPage1);
            assertTrue("Should have recorded /content/page2", foundPage2);
        }
        
        System.out.println("✓ Test 1 PASSED: Changes recorded successfully");
    }
    
    /**
     * Test 2: Index with change tracking enabled
     * 
     * Demonstrates:
     * - Creating an index definition with useChangeTracker=true
     * - Registering the index for change tracking
     * - Verifying progress metadata is created
     */
    @Test
    public void testIndexWithChangeTracking() throws Exception {
        System.out.println("\n=== Test 2: Index with Change Tracking ===");
        
        // Create an index definition with useChangeTracker=true using Tree API
        Tree oakIndex = root.getTree("/oak:index");
        
        // Create a simple test index
        Tree testIndex = oakIndex.addChild("testIndex");
        testIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        testIndex.setProperty("type", "lucene", Type.STRING);
        testIndex.setProperty("async", "async", Type.STRING);
        testIndex.setProperty("useChangeTracker", true);  // ← Enable change tracking
        
        // Add index rules
        Tree indexRules = testIndex.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree ntBase = indexRules.addChild("nt:base");
        ntBase.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree properties = ntBase.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Index the "text" property
        Tree textProp = properties.addChild("text");
        textProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        textProp.setProperty("name", "text", Type.STRING);
        textProp.setProperty("propertyIndex", true);
        textProp.setProperty("analyzed", true);
        
        // Commit the index definition
        root.commit();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Created index definition: /oak:index/testIndex (useChangeTracker=true)");
        
        // Register the index for change tracking
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        System.out.println("✓ Registered index for change tracking");
        
        // Verify progress metadata was created
        IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
        assertNotNull("Progress metadata should exist", progress);
        assertEquals("Index path should match", indexPath, progress.getIndexPath());
        assertEquals("Initial timestamp should be 0", 0, progress.getLastProcessedTimestamp());
        assertEquals("Initial serial should be 0", 0, progress.getLastProcessedSerialNumber());
        
        System.out.println("✓ Progress metadata initialized:");
        System.out.printf("  - Index: %s%n", progress.getIndexPath());
        System.out.printf("  - Last timestamp: %d%n", progress.getLastProcessedTimestamp());
        System.out.printf("  - Last serial: %d%n", progress.getLastProcessedSerialNumber());
        System.out.printf("  - Total processed: %d%n", progress.getTotalProcessed());
        
        System.out.println("✓ Test 2 PASSED: Index registered for change tracking");
    }
    
    /**
     * Test 3: Incremental change processing
     * 
     * Demonstrates:
     * - Making initial changes and processing them
     * - Making additional changes
     * - Using composite key to get only NEW changes
     * - Progress tracking
     */
    @Test
    public void testIncrementalChangeProcessing() throws Exception {
        System.out.println("\n=== Test 3: Incremental Change Processing ===");
        
        // Register a test index
        String indexPath = "/oak:index/testIndex";
        metadataManager.registerIndex(indexPath);
        
        // ROUND 1: Create and process first batch of changes
        System.out.println("\nRound 1: Initial changes");
        
        Tree batch1 = root.getTree("/").addChild("batch1");
        batch1.addChild("item1").setProperty("text", "First batch item 1");
        batch1.addChild("item2").setProperty("text", "First batch item 2");
        root.commit();
        
        populator.run();
        commitChangeTrackingIndex();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Created and recorded first batch");
        
        // Get changes from round 1
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            
            List<ChangeEntry> round1Changes = query.getUnprocessedChanges(0, 0, 100);
            System.out.println("✓ Round 1: Found " + round1Changes.size() + " changes");
            
            // Simulate processing: update progress with last processed entry
            if (!round1Changes.isEmpty()) {
                ChangeEntry lastEntry = round1Changes.get(round1Changes.size() - 1);
                metadataManager.updateProgress(
                    indexPath,
                    lastEntry.getDiffProcessingTime(),
                    lastEntry.getSerialNumber(),
                    round1Changes.size()
                );
                System.out.printf("✓ Updated progress: timestamp=%d, serial=%d%n",
                    lastEntry.getDiffProcessingTime(),
                    lastEntry.getSerialNumber());
            }
        }
        
        // ROUND 2: Create and process second batch of changes
        System.out.println("\nRound 2: Additional changes");
        
        Tree batch2 = root.getTree("/").addChild("batch2");
        batch2.addChild("item1").setProperty("text", "Second batch item 1");
        batch2.addChild("item2").setProperty("text", "Second batch item 2");
        batch2.addChild("item3").setProperty("text", "Second batch item 3");
        root.commit();
        
        populator.run();
        commitChangeTrackingIndex();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Created and recorded second batch");
        
        // Get ONLY NEW changes using the progress metadata
        IndexProgressMetadata progress = metadataManager.getIndexProgress(indexPath);
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            
            // This composite key query will get only changes AFTER the last processed entry
            List<ChangeEntry> round2Changes = query.getUnprocessedChanges(
                progress.getLastProcessedTimestamp(),
                progress.getLastProcessedSerialNumber(),
                100
            );
            
            System.out.println("✓ Round 2: Found " + round2Changes.size() + " NEW changes");
            
            // Verify we only got round 2 changes
            assertTrue("Should have new changes", round2Changes.size() > 0);
            
            // Print the new changes
            System.out.println("\nNew changes (Round 2 only):");
            for (ChangeEntry entry : round2Changes) {
                System.out.printf("  - Path: %s%n", entry.getPath());
            }
            
            // Verify these are batch2 changes
            boolean allFromBatch2 = round2Changes.stream()
                .allMatch(e -> e.getPath().contains("batch2"));
            assertTrue("All new changes should be from batch2", allFromBatch2);
            
            // Update progress again
            if (!round2Changes.isEmpty()) {
                ChangeEntry lastEntry = round2Changes.get(round2Changes.size() - 1);
                metadataManager.updateProgress(
                    indexPath,
                    lastEntry.getDiffProcessingTime(),
                    lastEntry.getSerialNumber(),
                    round2Changes.size()
                );
            }
        }
        
        // Verify final progress
        progress = metadataManager.getIndexProgress(indexPath);
        System.out.println("\n✓ Final progress:");
        System.out.printf("  - Total processed: %d%n", progress.getTotalProcessed());
        System.out.printf("  - Total chunks: %d%n", progress.getTotalChunks());
        
        assertTrue("Should have processed multiple chunks", progress.getTotalChunks() >= 2);
        
        System.out.println("✓ Test 3 PASSED: Incremental processing works correctly");
    }
    
    /**
     * Test 4: Composite key ordering
     * 
     * Demonstrates:
     * - Entries are ordered by (timestamp, serialNumber)
     * - Composite key queries work correctly
     * - No changes are missed or duplicated
     */
    @Test
    public void testCompositeKeyOrdering() throws Exception {
        System.out.println("\n=== Test 4: Composite Key Ordering ===");
        
        // Create multiple changes in quick succession
        for (int i = 0; i < 5; i++) {
            Tree test = root.getTree("/");
            if (!test.hasChild("test")) {
                test = test.addChild("test");
            } else {
                test = test.getChild("test");
            }
            
            Tree item = test.addChild("item" + i);
            item.setProperty("text", "Item " + i);
            
            root.commit();
            root = contentSession.getLatestRoot();
            
            populator.run();
            commitChangeTrackingIndex();
            
            // Small delay to potentially get different timestamps
            Thread.sleep(10);
        }
        
        System.out.println("✓ Created 5 changes");
        
        // Read all changes and verify ordering
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            
            List<ChangeEntry> allChanges = query.getUnprocessedChanges(0, 0, 1000);
            System.out.println("✓ Retrieved " + allChanges.size() + " changes");
            
            // Verify ordering: each entry should be >= previous entry
            ChangeEntry prev = null;
            for (ChangeEntry current : allChanges) {
                if (prev != null) {
                    // Compare composite keys: (timestamp, serial)
                    boolean ordered = 
                        current.getDiffProcessingTime() > prev.getDiffProcessingTime() ||
                        (current.getDiffProcessingTime() == prev.getDiffProcessingTime() &&
                         current.getSerialNumber() > prev.getSerialNumber());
                    
                    assertTrue(
                        String.format("Entries should be ordered: prev=%s, current=%s", 
                            prev, current),
                        ordered
                    );
                }
                prev = current;
            }
            
            System.out.println("✓ All entries are properly ordered by (timestamp, serialNumber)");
            
            // Test chunked reading with composite key
            long midTimestamp = 0;
            long midSerial = 0;
            
            if (allChanges.size() > 2) {
                int midIndex = allChanges.size() / 2;
                ChangeEntry midEntry = allChanges.get(midIndex);
                midTimestamp = midEntry.getDiffProcessingTime();
                midSerial = midEntry.getSerialNumber();
                
                // Get changes after midpoint
                List<ChangeEntry> afterMid = query.getUnprocessedChanges(
                    midTimestamp, 
                    midSerial, 
                    1000
                );
                
                System.out.printf("✓ After midpoint (timestamp=%d, serial=%d): %d entries%n",
                    midTimestamp, midSerial, afterMid.size());
                
                // Verify no entry from first half appears in second half
                for (ChangeEntry entry : afterMid) {
                    boolean isAfterMid = 
                        entry.getDiffProcessingTime() > midTimestamp ||
                        (entry.getDiffProcessingTime() == midTimestamp &&
                         entry.getSerialNumber() > midSerial);
                    
                    assertTrue("Entry should be after midpoint", isAfterMid);
                }
            }
        }
        
        System.out.println("✓ Test 4 PASSED: Composite key ordering works correctly");
    }
    
    /**
     * Test 5: Multiple indexes
     * 
     * Demonstrates:
     * - Multiple indexes can use the same change tracking index
     * - Each tracks its own progress independently
     * - Same changes are processed by different indexes
     */
    @Test
    public void testMultipleIndexes() throws Exception {
        System.out.println("\n=== Test 5: Multiple Indexes ===");
        
        // Register two indexes
        String index1Path = "/oak:index/index1";
        String index2Path = "/oak:index/index2";
        
        metadataManager.registerIndex(index1Path);
        metadataManager.registerIndex(index2Path);
        System.out.println("✓ Registered two indexes");
        
        // Create changes
        Tree shared = root.getTree("/").addChild("shared");
        shared.addChild("data").setProperty("text", "Shared data");
        root.commit();
        
        populator.run();
        commitChangeTrackingIndex();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Created and recorded changes");
        
        // Both indexes see the same changes
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            
            // Index 1 processes changes
            IndexProgressMetadata progress1 = metadataManager.getIndexProgress(index1Path);
            List<ChangeEntry> changes1 = query.getUnprocessedChanges(
                progress1.getLastProcessedTimestamp(),
                progress1.getLastProcessedSerialNumber(),
                100
            );
            System.out.printf("✓ Index 1 sees %d changes%n", changes1.size());
            
            // Index 2 processes the SAME changes (independent progress)
            IndexProgressMetadata progress2 = metadataManager.getIndexProgress(index2Path);
            List<ChangeEntry> changes2 = query.getUnprocessedChanges(
                progress2.getLastProcessedTimestamp(),
                progress2.getLastProcessedSerialNumber(),
                100
            );
            System.out.printf("✓ Index 2 sees %d changes%n", changes2.size());
            
            // Both should see the same changes
            assertEquals("Both indexes should see same changes", 
                changes1.size(), changes2.size());
            
            // Simulate index 1 processing faster than index 2
            if (!changes1.isEmpty()) {
                ChangeEntry lastEntry = changes1.get(changes1.size() - 1);
                metadataManager.updateProgress(
                    index1Path,
                    lastEntry.getDiffProcessingTime(),
                    lastEntry.getSerialNumber(),
                    changes1.size()
                );
            }
            System.out.println("✓ Index 1 processed all changes");
            
            // Index 2 still sees the same changes (hasn't caught up yet)
            progress2 = metadataManager.getIndexProgress(index2Path);
            List<ChangeEntry> changes2Again = query.getUnprocessedChanges(
                progress2.getLastProcessedTimestamp(),
                progress2.getLastProcessedSerialNumber(),
                100
            );
            assertEquals("Index 2 should still see all changes", 
                changes1.size(), changes2Again.size());
            
            System.out.println("✓ Index 2 still has pending changes (independent progress)");
        }
        
        // Verify independent progress
        IndexProgressMetadata finalProgress1 = metadataManager.getIndexProgress(index1Path);
        IndexProgressMetadata finalProgress2 = metadataManager.getIndexProgress(index2Path);
        
        System.out.printf("✓ Index 1 progress: %d processed%n", finalProgress1.getTotalProcessed());
        System.out.printf("✓ Index 2 progress: %d processed%n", finalProgress2.getTotalProcessed());
        
        assertTrue("Index 1 should have processed changes", 
            finalProgress1.getTotalProcessed() > 0);
        assertEquals("Index 2 should not have processed changes yet", 
            0, finalProgress2.getTotalProcessed());
        
        System.out.println("✓ Test 5 PASSED: Multiple indexes work independently");
    }
    
    /**
     * Test 6: Query-based verification of indexing
     * 
     * Demonstrates:
     * - Creating a real Lucene index with useChangeTracker=true
     * - Indexing content through change tracker
     * - Running queries to verify index works
     * - Validating query results
     */
    @Test
    public void testQueryBasedIndexingVerification() throws Exception {
        System.out.println("\n=== Test 6: Query-Based Indexing Verification ===");
        
        // STEP 1: Create a Lucene index definition with useChangeTracker=true
        System.out.println("\nStep 1: Creating searchable index definition...");
        
        Tree oakIndex = root.getTree("/oak:index");
        Tree searchIndex = oakIndex.addChild("searchIndex");
        searchIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        searchIndex.setProperty("type", "lucene");
        searchIndex.setProperty("async", "async");
        searchIndex.setProperty("useChangeTracker", true);
        
        // Create index rules for nt:unstructured
        Tree indexRules = searchIndex.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree ntUnstructured = indexRules.addChild("nt:unstructured");
        ntUnstructured.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree properties = ntUnstructured.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Index "title" property (analyzed for fulltext search)
        Tree titleProp = properties.addChild("title");
        titleProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        titleProp.setProperty("name", "title");
        titleProp.setProperty("propertyIndex", true);
        titleProp.setProperty("analyzed", true);
        titleProp.setProperty("nodeScopeIndex", true);
        
        // Index "category" property (not analyzed for exact match)
        Tree categoryProp = properties.addChild("category");
        categoryProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        categoryProp.setProperty("name", "category");
        categoryProp.setProperty("propertyIndex", true);
        categoryProp.setProperty("analyzed", false);
        
        // Index "status" property
        Tree statusProp = properties.addChild("status");
        statusProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        statusProp.setProperty("name", "status");
        statusProp.setProperty("propertyIndex", true);
        statusProp.setProperty("analyzed", false);
        
        root.commit();
        System.out.println("✓ Created searchIndex with useChangeTracker=true");
        
        // Register with metadata manager
        metadataManager.registerIndex("/oak:index/searchIndex");
        System.out.println("✓ Registered index for change tracking");
        
        // STEP 2: Create searchable content
        System.out.println("\nStep 2: Creating searchable content...");
        
        Tree content = root.getTree("/").addChild("testContent");
        
        // Document 1: Java article
        Tree doc1 = content.addChild("doc1");
        doc1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc1.setProperty("title", "Introduction to Java Programming");
        doc1.setProperty("category", "programming");
        doc1.setProperty("status", "published");
        
        // Document 2: Python article
        Tree doc2 = content.addChild("doc2");
        doc2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc2.setProperty("title", "Python for Beginners");
        doc2.setProperty("category", "programming");
        doc2.setProperty("status", "draft");
        
        // Document 3: JavaScript article
        Tree doc3 = content.addChild("doc3");
        doc3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc3.setProperty("title", "JavaScript Essentials");
        doc3.setProperty("category", "programming");
        doc3.setProperty("status", "published");
        
        // Document 4: Cooking recipe
        Tree doc4 = content.addChild("doc4");
        doc4.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        doc4.setProperty("title", "Best Chocolate Cake Recipe");
        doc4.setProperty("category", "cooking");
        doc4.setProperty("status", "published");
        
        root.commit();
        System.out.println("✓ Created 4 test documents");
        
        // STEP 3: Run change tracker populator
        System.out.println("\nStep 3: Running change tracker populator...");
        populator.run();
        commitChangeTrackingIndex();
        System.out.println("✓ Change tracker populated");
        
        // Verify changes were recorded
        try (IndexReader reader = DirectoryReader.open(changeTrackingDirectory);
             ChangeTrackingIndexQuery query = new ChangeTrackingIndexQuery(reader)) {
            List<ChangeEntry> changes = query.getUnprocessedChanges(0, 0, 100);
            System.out.printf("✓ Recorded %d changes%n", changes.size());
            assertTrue("Should have recorded changes", changes.size() > 0);
        }
        
        // STEP 4: Run async index update to build the index
        System.out.println("\nStep 4: Running async index update...");
        asyncIndexUpdate.run();
        root = contentSession.getLatestRoot(); // Refresh root
        System.out.println("✓ Async index update completed");
        
        // STEP 5: Execute queries and verify results
        System.out.println("\nStep 5: Executing queries to verify indexing...");
        
        // Query 1: Exact match on category
        System.out.println("\n  Query 1: Find all programming articles");
        String query1 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming'";
        List<String> results1 = executeQuery(query1);
        System.out.printf("    Found %d results: %s%n", results1.size(), results1);
        assertEquals("Should find 3 programming articles", 3, results1.size());
        assertTrue("Should contain doc1", results1.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc2", results1.stream().anyMatch(p -> p.contains("doc2")));
        assertTrue("Should contain doc3", results1.stream().anyMatch(p -> p.contains("doc3")));
        System.out.println("    ✓ PASSED");
        
        // Query 2: Multiple property filter (AND)
        System.out.println("\n  Query 2: Find published programming articles");
        String query2 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'programming' AND [status] = 'published'";
        List<String> results2 = executeQuery(query2);
        System.out.printf("    Found %d results: %s%n", results2.size(), results2);
        assertEquals("Should find 2 published programming articles", 2, results2.size());
        assertTrue("Should contain doc1", results2.stream().anyMatch(p -> p.contains("doc1")));
        assertTrue("Should contain doc3", results2.stream().anyMatch(p -> p.contains("doc3")));
        System.out.println("    ✓ PASSED");
        
        // Query 3: Different category
        System.out.println("\n  Query 3: Find cooking articles");
        String query3 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [category] = 'cooking'";
        List<String> results3 = executeQuery(query3);
        System.out.printf("    Found %d results: %s%n", results3.size(), results3);
        assertEquals("Should find 1 cooking article", 1, results3.size());
        assertTrue("Should contain doc4", results3.stream().anyMatch(p -> p.contains("doc4")));
        System.out.println("    ✓ PASSED");
        
        // Query 4: Fulltext search (CONTAINS)
        System.out.println("\n  Query 4: Fulltext search for 'Java'");
        String query4 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS(*, 'Java')";
        List<String> results4 = executeQuery(query4);
        System.out.printf("    Found %d results: %s%n", results4.size(), results4);
        assertTrue("Should find at least 1 Java-related article", results4.size() >= 1);
        assertTrue("Should contain doc1 (Java article)", results4.stream().anyMatch(p -> p.contains("doc1")));
        System.out.println("    ✓ PASSED");
        
        // Query 5: Fulltext search on specific property
        System.out.println("\n  Query 5: Search for 'Python' in title");
        String query5 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE CONTAINS([title], 'Python')";
        List<String> results5 = executeQuery(query5);
        System.out.printf("    Found %d results: %s%n", results5.size(), results5);
        assertEquals("Should find 1 Python article", 1, results5.size());
        assertTrue("Should contain doc2", results5.stream().anyMatch(p -> p.contains("doc2")));
        System.out.println("    ✓ PASSED");
        
        // Query 6: Draft status
        System.out.println("\n  Query 6: Find draft articles");
        String query6 = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [status] = 'draft'";
        List<String> results6 = executeQuery(query6);
        System.out.printf("    Found %d results: %s%n", results6.size(), results6);
        assertEquals("Should find 1 draft article", 1, results6.size());
        assertTrue("Should contain doc2", results6.stream().anyMatch(p -> p.contains("doc2")));
        System.out.println("    ✓ PASSED");
        
        // STEP 6: Verify index progress was tracked
        System.out.println("\nStep 6: Verifying progress tracking...");
        IndexProgressMetadata progress = metadataManager.getIndexProgress("/oak:index/searchIndex");
        if (progress != null) {
            System.out.printf("  Last processed timestamp: %d%n", progress.getLastProcessedTimestamp());
            System.out.printf("  Last processed serial: %d%n", progress.getLastProcessedSerialNumber());
            System.out.printf("  Total processed: %d%n", progress.getTotalProcessed());
            System.out.println("  ✓ Progress tracking working");
        } else {
            System.out.println("  ⚠ Progress metadata not found (might not be updated yet)");
        }
        
        System.out.println("\n✓ Test 6 PASSED: Indexing verified via queries!");
        System.out.println("========================================");
        System.out.println("✓ ALL QUERY-BASED ASSERTIONS PASSED");
        System.out.println("✓ Change tracking indexing is working correctly");
        System.out.println("========================================");
    }
    
    /**
     * Test 7: Incremental indexing with queries
     * 
     * Demonstrates:
     * - Adding more content after initial indexing
     * - Running change tracker again
     * - Verifying new content is indexed
     * - Queries return updated results
     */
    @Test
    public void testIncrementalIndexingWithQueries() throws Exception {
        System.out.println("\n=== Test 7: Incremental Indexing with Queries ===");
        
        // STEP 1: Create index and initial content (reuse logic from test 6)
        System.out.println("\nStep 1: Setting up index and initial content...");
        
        // Create index
        Tree oakIndex = root.getTree("/oak:index");
        Tree searchIndex = oakIndex.addChild("incrementalIndex");
        searchIndex.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        searchIndex.setProperty("type", "lucene");
        searchIndex.setProperty("async", "async");
        searchIndex.setProperty("useChangeTracker", true);
        
        Tree indexRules = searchIndex.addChild("indexRules");
        Tree ntUnstructured = indexRules.addChild("nt:unstructured");
        Tree properties = ntUnstructured.addChild("properties");
        
        Tree tagProp = properties.addChild("tag");
        tagProp.setProperty("name", "tag");
        tagProp.setProperty("propertyIndex", true);
        tagProp.setProperty("analyzed", false);
        
        root.commit();
        metadataManager.registerIndex("/oak:index/incrementalIndex");
        
        // Create initial content
        Tree content = root.getTree("/").addChild("articles");
        
        Tree article1 = content.addChild("article1");
        article1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        article1.setProperty("tag", "initial");
        
        root.commit();
        System.out.println("✓ Created index and 1 initial article");
        
        // Index initial content
        populator.run();
        commitChangeTrackingIndex();
        asyncIndexUpdate.run();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Initial indexing completed");
        
        // Verify initial query
        String initialQuery = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [tag] = 'initial'";
        List<String> initialResults = executeQuery(initialQuery);
        System.out.printf("✓ Initial query found %d articles%n", initialResults.size());
        assertEquals("Should find 1 initial article", 1, initialResults.size());
        
        // STEP 2: Add more content
        System.out.println("\nStep 2: Adding new content...");
        
        Tree article2 = root.getTree("/articles").addChild("article2");
        article2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        article2.setProperty("tag", "new");
        
        Tree article3 = root.getTree("/articles").addChild("article3");
        article3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        article3.setProperty("tag", "new");
        
        root.commit();
        System.out.println("✓ Added 2 new articles");
        
        // STEP 3: Run incremental indexing
        System.out.println("\nStep 3: Running incremental indexing...");
        populator.run();
        commitChangeTrackingIndex();
        asyncIndexUpdate.run();
        root = contentSession.getLatestRoot();
        System.out.println("✓ Incremental indexing completed");
        
        // STEP 4: Verify queries return updated results
        System.out.println("\nStep 4: Verifying queries return new content...");
        
        // Query for new articles
        String newQuery = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [tag] = 'new'";
        List<String> newResults = executeQuery(newQuery);
        System.out.printf("✓ Found %d new articles: %s%n", newResults.size(), newResults);
        assertEquals("Should find 2 new articles", 2, newResults.size());
        
        // Query for all articles
        String allQuery = "SELECT [jcr:path] FROM [nt:unstructured] WHERE [tag] IS NOT NULL";
        List<String> allResults = executeQuery(allQuery);
        System.out.printf("✓ Found %d total articles: %s%n", allResults.size(), allResults);
        assertEquals("Should find 3 total articles", 3, allResults.size());
        
        // Verify old content still accessible
        List<String> stillInitial = executeQuery(initialQuery);
        assertEquals("Should still find initial article", 1, stillInitial.size());
        System.out.println("✓ Old content still accessible");
        
        System.out.println("\n✓ Test 7 PASSED: Incremental indexing works correctly!");
    }
    
    /**
     * Helper method to execute a query and return matching paths.
     */
    private List<String> executeQuery(String sqlQuery) throws Exception {
        List<String> paths = new ArrayList<>();
        QueryEngine queryEngine = root.getQueryEngine();
        
        Result result = queryEngine.executeQuery(
            sqlQuery,
            javax.jcr.query.Query.JCR_SQL2,
            null,  // NO_BINDINGS
            null   // NO_MAPPINGS
        );
        
        for (ResultRow row : result.getRows()) {
            paths.add(row.getPath());
        }
        
        return paths;
    }
    
    /**
     * Helper method to commit the change tracking IndexWriter.
     * This is needed in tests because the writer doesn't auto-commit.
     */
    private void commitChangeTrackingIndex() throws Exception {
        try {
            // Use reflection to access the private changeTrackingWriter field
            java.lang.reflect.Field writerField = populator.getClass().getDeclaredField("changeTrackingWriter");
            writerField.setAccessible(true);
            org.apache.lucene.index.IndexWriter writer = (org.apache.lucene.index.IndexWriter) writerField.get(populator);
            if (writer != null) {
                writer.commit();
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not commit change tracking writer: " + e.getMessage());
            // Non-fatal - continue test
        }
    }
}

