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
 * Tests for {@link ChangeEntry}.
 */
public class ChangeEntryTest {
    
    @Test
    public void testBuilder() {
        ChangeEntry entry = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        assertEquals("/content/dam/asset-123", entry.getPath());
        assertEquals(1234567890000L, entry.getDiffProcessingTime());
        assertEquals(42, entry.getSerialNumber());
    }
    
    @Test(expected = IllegalStateException.class)
    public void testBuilderMissingPath() {
        new ChangeEntry.Builder()
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
    }
    
    @Test(expected = IllegalStateException.class)
    public void testBuilderMissingDiffProcessingTime() {
        new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .serialNumber(42)
                .build();
    }
    
    @Test
    public void testCompositeKey() {
        ChangeEntry entry = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        String expected = "1234567890000:42:/content/dam/asset-123";
        assertEquals(expected, entry.getCompositeKey());
    }
    
    @Test
    public void testEquals() {
        ChangeEntry entry1 = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        ChangeEntry entry2 = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        ChangeEntry entry3 = new ChangeEntry.Builder()
                .path("/content/dam/asset-456")  // Different path
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        // Same path, timestamp, and serial should be equal
        assertEquals(entry1, entry2);
        assertEquals(entry1.hashCode(), entry2.hashCode());
        
        // Different path should not be equal
        assertNotEquals(entry1, entry3);
    }
    
    @Test
    public void testEqualsWithDifferentTimestamp() {
        ChangeEntry entry1 = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        ChangeEntry entry2 = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567891000L)  // Different timestamp
                .serialNumber(42)
                .build();
        
        assertNotEquals(entry1, entry2);
    }
    
    @Test
    public void testEqualsWithDifferentSerial() {
        ChangeEntry entry1 = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        ChangeEntry entry2 = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(43)  // Different serial
                .build();
        
        assertNotEquals(entry1, entry2);
    }
    
    @Test
    public void testToString() {
        ChangeEntry entry = new ChangeEntry.Builder()
                .path("/content/dam/asset-123")
                .diffProcessingTime(1234567890000L)
                .serialNumber(42)
                .build();
        
        String str = entry.toString();
        assertTrue(str.contains("/content/dam/asset-123"));
        assertTrue(str.contains("1234567890000"));
        assertTrue(str.contains("42"));
    }
}

