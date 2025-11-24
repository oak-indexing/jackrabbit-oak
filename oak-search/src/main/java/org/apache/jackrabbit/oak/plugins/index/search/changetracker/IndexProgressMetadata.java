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

/**
 * Tracks the progress of an index processing changes from the change tracking index.
 * 
 * <p>This metadata is persisted in the repository to enable crash recovery and
 * ensure that no changes are missed when processing is resumed.
 * 
 * <p>The composite key (lastProcessedTimestamp, lastProcessedSerialNumber) identifies
 * the last change entry that was successfully processed and committed by this index.
 */
public class IndexProgressMetadata {
    
    private final String indexPath;
    private final long lastProcessedTimestamp;
    private final long lastProcessedSerialNumber;
    private final long currentChunkStart;
    private final long currentChunkEnd;
    private final long processingStarted;
    private final long lastChunkCommit;
    private final long totalProcessed;
    private final long totalChunks;
    
    private IndexProgressMetadata(Builder builder) {
        this.indexPath = builder.indexPath;
        this.lastProcessedTimestamp = builder.lastProcessedTimestamp;
        this.lastProcessedSerialNumber = builder.lastProcessedSerialNumber;
        this.currentChunkStart = builder.currentChunkStart;
        this.currentChunkEnd = builder.currentChunkEnd;
        this.processingStarted = builder.processingStarted;
        this.lastChunkCommit = builder.lastChunkCommit;
        this.totalProcessed = builder.totalProcessed;
        this.totalChunks = builder.totalChunks;
    }
    
    @NotNull
    public String getIndexPath() {
        return indexPath;
    }
    
    public long getLastProcessedTimestamp() {
        return lastProcessedTimestamp;
    }
    
    public long getLastProcessedSerialNumber() {
        return lastProcessedSerialNumber;
    }
    
    public long getCurrentChunkStart() {
        return currentChunkStart;
    }
    
    public long getCurrentChunkEnd() {
        return currentChunkEnd;
    }
    
    public long getProcessingStarted() {
        return processingStarted;
    }
    
    public long getLastChunkCommit() {
        return lastChunkCommit;
    }
    
    public long getTotalProcessed() {
        return totalProcessed;
    }
    
    public long getTotalChunks() {
        return totalChunks;
    }
    
    /**
     * @return true if this index has processed at least one change
     */
    public boolean hasProcessedChanges() {
        return lastProcessedTimestamp > 0;
    }
    
    /**
     * @return true if currently processing a chunk
     */
    public boolean isProcessingChunk() {
        return currentChunkStart > 0 && currentChunkEnd > 0 &&
               lastChunkCommit < currentChunkEnd;
    }
    
    @Override
    public String toString() {
        return "IndexProgressMetadata{" +
                "indexPath='" + indexPath + '\'' +
                ", lastProcessedTimestamp=" + lastProcessedTimestamp +
                ", lastProcessedSerialNumber=" + lastProcessedSerialNumber +
                ", currentChunkStart=" + currentChunkStart +
                ", currentChunkEnd=" + currentChunkEnd +
                ", totalProcessed=" + totalProcessed +
                ", totalChunks=" + totalChunks +
                '}';
    }
    
    /**
     * Builder for IndexProgressMetadata.
     */
    public static class Builder {
        private String indexPath;
        private long lastProcessedTimestamp = 0;
        private long lastProcessedSerialNumber = 0;
        private long currentChunkStart = 0;
        private long currentChunkEnd = 0;
        private long processingStarted = 0;
        private long lastChunkCommit = 0;
        private long totalProcessed = 0;
        private long totalChunks = 0;
        
        public Builder indexPath(String indexPath) {
            this.indexPath = indexPath;
            return this;
        }
        
        public Builder lastProcessedTimestamp(long lastProcessedTimestamp) {
            this.lastProcessedTimestamp = lastProcessedTimestamp;
            return this;
        }
        
        public Builder lastProcessedSerialNumber(long lastProcessedSerialNumber) {
            this.lastProcessedSerialNumber = lastProcessedSerialNumber;
            return this;
        }
        
        public Builder currentChunkStart(long currentChunkStart) {
            this.currentChunkStart = currentChunkStart;
            return this;
        }
        
        public Builder currentChunkEnd(long currentChunkEnd) {
            this.currentChunkEnd = currentChunkEnd;
            return this;
        }
        
        public Builder processingStarted(long processingStarted) {
            this.processingStarted = processingStarted;
            return this;
        }
        
        public Builder lastChunkCommit(long lastChunkCommit) {
            this.lastChunkCommit = lastChunkCommit;
            return this;
        }
        
        public Builder totalProcessed(long totalProcessed) {
            this.totalProcessed = totalProcessed;
            return this;
        }
        
        public Builder totalChunks(long totalChunks) {
            this.totalChunks = totalChunks;
            return this;
        }
        
        public IndexProgressMetadata build() {
            if (indexPath == null || indexPath.isEmpty()) {
                throw new IllegalStateException("indexPath is required");
            }
            return new IndexProgressMetadata(this);
        }
    }
}

