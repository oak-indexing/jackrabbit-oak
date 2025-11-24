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
 *   <li>Path of the changed node</li>
 *   <li>Checkpoint range (checkpoint1 to checkpoint2) that updated this node</li>
 *   <li>Timestamp when the diff was processed</li>
 *   <li>Serial number for unique ordering within same timestamp</li>
 * </ul>
 * 
 * <p>The change tracking index uses these entries to enable chunked processing
 * of repository changes, avoiding the need to diff the entire tree for each
 * async index update cycle.
 */
public class ChangeEntry {
    
    private final String path;
    private final String checkpoint1;
    private final String checkpoint2;
    private final long diffProcessingTime;
    private final long serialNumber;
    
    /**
     * Creates a new change entry.
     * 
     * @param path the absolute path of the changed node
     * @param checkpoint1 the first checkpoint in the diff range that updated this node
     * @param checkpoint2 the last checkpoint in the diff range that updated this node
     * @param diffProcessingTime the millisecond timestamp when this diff was processed
     * @param serialNumber unique sequence number within the same timestamp
     */
    public ChangeEntry(@NotNull String path,
                       @NotNull String checkpoint1,
                       @NotNull String checkpoint2,
                       long diffProcessingTime,
                       long serialNumber) {
        this.path = path;
        this.checkpoint1 = checkpoint1;
        this.checkpoint2 = checkpoint2;
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
     * @return the first checkpoint in the diff range
     */
    @NotNull
    public String getCheckpoint1() {
        return checkpoint1;
    }
    
    /**
     * @return the last checkpoint in the diff range
     */
    @NotNull
    public String getCheckpoint2() {
        return checkpoint2;
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
                ", checkpoint1='" + checkpoint1 + '\'' +
                ", checkpoint2='" + checkpoint2 + '\'' +
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
        private String checkpoint1;
        private String checkpoint2;
        private long diffProcessingTime;
        private long serialNumber;
        
        public Builder path(String path) {
            this.path = path;
            return this;
        }
        
        public Builder checkpoint1(String checkpoint1) {
            this.checkpoint1 = checkpoint1;
            return this;
        }
        
        public Builder checkpoint2(String checkpoint2) {
            this.checkpoint2 = checkpoint2;
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
            if (checkpoint1 == null || checkpoint1.isEmpty()) {
                throw new IllegalStateException("checkpoint1 is required");
            }
            if (checkpoint2 == null || checkpoint2.isEmpty()) {
                throw new IllegalStateException("checkpoint2 is required");
            }
            return new ChangeEntry(path, checkpoint1, checkpoint2, diffProcessingTime, serialNumber);
        }
    }
}

