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
package org.apache.jackrabbit.oak.plugins.index.search.changetracker;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit test for {@link IndexProgressMetadata}.
 */
public class IndexProgressMetadataTest {

    private static final String TEST_INDEX_PATH = "/oak:index/damAssetLucene";

    @Test
    public void testBuilder() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .lastProcessedTimestamp(1234567890000L)
                .lastProcessedSerialNumber(42L)
                .currentChunkStart(1234567880000L)
                .currentChunkEnd(1234567890000L)
                .processingStarted(1701234567890L)
                .lastChunkCommit(1701234567920L)
                .totalProcessed(1250000L)
                .totalChunks(125L)
                .build();
        
        assertEquals(TEST_INDEX_PATH, metadata.getIndexPath());
        assertEquals(1234567890000L, metadata.getLastProcessedTimestamp());
        assertEquals(42L, metadata.getLastProcessedSerialNumber());
        assertEquals(1234567880000L, metadata.getCurrentChunkStart());
        assertEquals(1234567890000L, metadata.getCurrentChunkEnd());
        assertEquals(1701234567890L, metadata.getProcessingStarted());
        assertEquals(1701234567920L, metadata.getLastChunkCommit());
        assertEquals(1250000L, metadata.getTotalProcessed());
        assertEquals(125L, metadata.getTotalChunks());
    }

    @Test
    public void testDefaultBuilder() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .build();
        
        assertEquals(TEST_INDEX_PATH, metadata.getIndexPath());
        assertEquals(0L, metadata.getLastProcessedTimestamp());
        assertEquals(0L, metadata.getLastProcessedSerialNumber());
        assertEquals(0L, metadata.getCurrentChunkStart());
        assertEquals(0L, metadata.getCurrentChunkEnd());
        assertEquals(0L, metadata.getProcessingStarted());
        assertEquals(0L, metadata.getLastChunkCommit());
        assertEquals(0L, metadata.getTotalProcessed());
        assertEquals(0L, metadata.getTotalChunks());
    }

    @Test
    public void testToString() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .lastProcessedTimestamp(1678886400000L)
                .lastProcessedSerialNumber(123L)
                .build();
        
        String str = metadata.toString();
        assertTrue(str.contains("indexPath='/oak:index/damAssetLucene'"));
        assertTrue(str.contains("lastProcessedTimestamp=1678886400000"));
        assertTrue(str.contains("lastProcessedSerialNumber=123"));
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderMissingPath() {
        new IndexProgressMetadata.Builder().build();
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderEmptyPath() {
        new IndexProgressMetadata.Builder()
                .indexPath("")
                .build();
    }

    @Test
    public void testHasProcessedChanges() {
        // No changes processed
        IndexProgressMetadata metadata1 = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .lastProcessedTimestamp(0L)
                .build();
        assertFalse(metadata1.hasProcessedChanges());
        
        // Changes processed
        IndexProgressMetadata metadata2 = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .lastProcessedTimestamp(100L)
                .build();
        assertTrue(metadata2.hasProcessedChanges());
    }

    @Test
    public void testIsProcessingChunk() {
        // Not processing
        IndexProgressMetadata metadata1 = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .currentChunkStart(0L)
                .currentChunkEnd(0L)
                .lastChunkCommit(0L)
                .build();
        assertFalse(metadata1.isProcessingChunk());
        
        // Processing chunk
        IndexProgressMetadata metadata2 = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .currentChunkStart(1000L)
                .currentChunkEnd(2000L)
                .lastChunkCommit(1500L)
                .build();
        assertTrue(metadata2.isProcessingChunk());
        
        // Chunk completed
        IndexProgressMetadata metadata3 = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .currentChunkStart(1000L)
                .currentChunkEnd(2000L)
                .lastChunkCommit(2000L)
                .build();
        assertFalse(metadata3.isProcessingChunk());
    }

    @Test
    public void testBuilderChaining() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath(TEST_INDEX_PATH)
                .lastProcessedTimestamp(100L)
                .lastProcessedSerialNumber(10L)
                .currentChunkStart(50L)
                .currentChunkEnd(100L)
                .processingStarted(200L)
                .lastChunkCommit(250L)
                .totalProcessed(1000L)
                .totalChunks(10L)
                .build();
        
        assertNotNull(metadata);
        assertEquals(TEST_INDEX_PATH, metadata.getIndexPath());
    }
}

