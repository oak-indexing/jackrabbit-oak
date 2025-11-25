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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a change entry in the change tracking index.
 * Each entry records that a node at a specific path changed during a diff run.
 * 
 * <p>This lightweight record contains:
 * <ul>
 *   <li><strong>Path</strong> - The absolute path of the changed node</li>
 *   <li><strong>Timestamp</strong> - When the diff was processed (for ordering and retention)</li>
 *   <li><strong>Serial number</strong> - Unique ordering within same timestamp</li>
 * </ul>
 * 
 * <p><strong>Design Note:</strong> We do NOT store checkpoint IDs in individual entries because:
 * <ul>
 *   <li>Change entries don't store node content, just paths</li>
 *   <li>Checkpoint info is only needed at processing level (which range to process)</li>
 *   <li>Storing checkpoints would duplicate data and complicate cleanup</li>
 * </ul>
 * 
 * <p>The change tracking index uses these entries to enable chunked processing
 * of repository changes, avoiding the need to diff the entire tree for each
 * async index update cycle.
 */
public class ChangeEntry {
    
    private final String path;
    private final long diffProcessingTime;
    private final long serialNumber;
    
    /**
     * Creates a new change entry.
     * 
     * @param path the absolute path of the changed node
     * @param diffProcessingTime the millisecond timestamp when this diff was processed
     * @param serialNumber unique sequence number within the same timestamp
     */
    public ChangeEntry(@NotNull String path,
                       long diffProcessingTime,
                       long serialNumber) {
        this.path = path;
        this.diffProcessingTime = diffProcessingTime;
        this.serialNumber = serialNumber;
    }
    
    /**
     * @return the absolute path of the changed node
     */
    @NotNull
    public String getPath() {
        return path;
    }
    
    /**
     * @return the millisecond timestamp when this diff was processed
     */
    public long getDiffProcessingTime() {
        return diffProcessingTime;
    }
    
    /**
     * @return the serial number for unique ordering within same timestamp
     */
    public long getSerialNumber() {
        return serialNumber;
    }
    
    /**
     * Creates a composite key for this entry used for ordering and deduplication.
     * Format: timestamp:serialNumber:path
     * 
     * @return the composite key
     */
    @NotNull
    public String getCompositeKey() {
        return diffProcessingTime + ":" + serialNumber + ":" + path;
    }
    
    @Override
    public String toString() {
        return "ChangeEntry{" +
                "path='" + path + '\'' +
                ", diffProcessingTime=" + diffProcessingTime +
                ", serialNumber=" + serialNumber +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        ChangeEntry that = (ChangeEntry) o;
        
        if (diffProcessingTime != that.diffProcessingTime) return false;
        if (serialNumber != that.serialNumber) return false;
        return path.equals(that.path);
    }
    
    @Override
    public int hashCode() {
        int result = path.hashCode();
        result = 31 * result + (int) (diffProcessingTime ^ (diffProcessingTime >>> 32));
        result = 31 * result + (int) (serialNumber ^ (serialNumber >>> 32));
        return result;
    }
    
    /**
     * Builder for creating ChangeEntry instances.
     */
    public static class Builder {
        private String path;
        private long diffProcessingTime;
        private long serialNumber;
        
        public Builder path(String path) {
            this.path = path;
            return this;
        }
        
        public Builder diffProcessingTime(long diffProcessingTime) {
            this.diffProcessingTime = diffProcessingTime;
            return this;
        }
        
        public Builder serialNumber(long serialNumber) {
            this.serialNumber = serialNumber;
            return this;
        }
        
        public ChangeEntry build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalStateException("path is required");
            }
            return new ChangeEntry(path, diffProcessingTime, serialNumber);
        }
    }
}

