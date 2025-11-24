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
 * Tests for {@link IndexProgressMetadata}.
 */
public class IndexProgressMetadataTest {
    
    @Test
    public void testBuilder() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/damAssetLucene")
                .lastProcessedTimestamp(1234567890000L)
                .lastProcessedSerialNumber(42L)
                .currentChunkStart(1234567880000L)
                .currentChunkEnd(1234567890000L)
                .processingStarted(1701234567890L)
                .lastChunkCommit(1701234567920L)
                .totalProcessed(1250000L)
                .totalChunks(125L)
                .averageChunkTime(120000L)
                .lastChunkSize(10000)
                .build();
        
        assertEquals("/oak:index/damAssetLucene", metadata.getIndexPath());
        assertEquals(1234567890000L, metadata.getLastProcessedTimestamp());
        assertEquals(42L, metadata.getLastProcessedSerialNumber());
        assertEquals(1234567880000L, metadata.getCurrentChunkStart());
        assertEquals(1234567890000L, metadata.getCurrentChunkEnd());
        assertEquals(1701234567890L, metadata.getProcessingStarted());
        assertEquals(1701234567920L, metadata.getLastChunkCommit());
        assertEquals(1250000L, metadata.getTotalProcessed());
        assertEquals(125L, metadata.getTotalChunks());
        assertEquals(120000L, metadata.getAverageChunkTime());
        assertEquals(10000, metadata.getLastChunkSize());
    }
    
    @Test(expected = IllegalStateException.class)
    public void testBuilderMissingIndexPath() {
        new IndexProgressMetadata.Builder()
                .lastProcessedTimestamp(1234567890000L)
                .build();
    }
    
    @Test
    public void testHasProcessedChanges_true() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(1L)
                .build();
        
        assertTrue(metadata.hasProcessedChanges());
    }
    
    @Test
    public void testHasProcessedChanges_falseWhenZeroTimestamp() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(0L)
                .lastProcessedSerialNumber(0L)
                .build();
        
        assertFalse(metadata.hasProcessedChanges());
    }
    
    @Test
    public void testHasProcessedChanges_trueWhenOnlyTimestamp() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(0L)
                .build();
        
        assertTrue(metadata.hasProcessedChanges());
    }
    
    @Test
    public void testEquals() {
        IndexProgressMetadata metadata1 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(42L)
                .totalProcessed(100L)
                .build();
        
        IndexProgressMetadata metadata2 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(42L)
                .totalProcessed(100L)
                .build();
        
        IndexProgressMetadata metadata3 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/other")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(42L)
                .totalProcessed(100L)
                .build();
        
        // Same values should be equal
        assertEquals(metadata1, metadata2);
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
        
        // Different index path should not be equal
        assertNotEquals(metadata1, metadata3);
    }
    
    @Test
    public void testEqualsWithDifferentTimestamp() {
        IndexProgressMetadata metadata1 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(42L)
                .build();
        
        IndexProgressMetadata metadata2 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(2000L)  // Different timestamp
                .lastProcessedSerialNumber(42L)
                .build();
        
        assertNotEquals(metadata1, metadata2);
    }
    
    @Test
    public void testEqualsWithDifferentSerialNumber() {
        IndexProgressMetadata metadata1 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(42L)
                .build();
        
        IndexProgressMetadata metadata2 = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .lastProcessedTimestamp(1000L)
                .lastProcessedSerialNumber(43L)  // Different serial
                .build();
        
        assertNotEquals(metadata1, metadata2);
    }
    
    @Test
    public void testToString() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/damAssetLucene")
                .lastProcessedTimestamp(1234567890000L)
                .lastProcessedSerialNumber(42L)
                .totalProcessed(1000L)
                .build();
        
        String str = metadata.toString();
        assertTrue(str.contains("/oak:index/damAssetLucene"));
        assertTrue(str.contains("1234567890000"));
        assertTrue(str.contains("42"));
        assertTrue(str.contains("1000"));
    }
    
    @Test
    public void testDefaultValues() {
        // Builder should allow building with just required fields
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .build();
        
        assertEquals("/oak:index/test", metadata.getIndexPath());
        assertEquals(0L, metadata.getLastProcessedTimestamp());
        assertEquals(0L, metadata.getLastProcessedSerialNumber());
        assertEquals(0L, metadata.getCurrentChunkStart());
        assertEquals(0L, metadata.getCurrentChunkEnd());
        assertEquals(0L, metadata.getProcessingStarted());
        assertEquals(0L, metadata.getLastChunkCommit());
        assertEquals(0L, metadata.getTotalProcessed());
        assertEquals(0L, metadata.getTotalChunks());
        assertEquals(0L, metadata.getAverageChunkTime());
        assertEquals(0, metadata.getLastChunkSize());
    }
    
    @Test
    public void testProgressCalculation() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .totalProcessed(75000L)
                .totalChunks(75L)
                .averageChunkTime(1000L)
                .build();
        
        // Verify stats
        assertEquals(75000L, metadata.getTotalProcessed());
        assertEquals(75L, metadata.getTotalChunks());
        
        // Average = 1000 per chunk
        assertEquals(1000L, metadata.getAverageChunkTime());
    }
    
    @Test
    public void testChunkBoundaries() {
        IndexProgressMetadata metadata = new IndexProgressMetadata.Builder()
                .indexPath("/oak:index/test")
                .currentChunkStart(1000L)
                .currentChunkEnd(2000L)
                .build();
        
        assertEquals(1000L, metadata.getCurrentChunkStart());
        assertEquals(2000L, metadata.getCurrentChunkEnd());
        
        // Chunk range = 1000
        long chunkRange = metadata.getCurrentChunkEnd() - metadata.getCurrentChunkStart();
        assertEquals(1000L, chunkRange);
    }
}

