/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.resume.PathTree;
import org.apache.jackrabbit.oak.plugins.index.resume.ResumeContext;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segregated resume/chunked variant of {@link AsyncIndexUpdate}. It runs on a
 * dedicated {@code resume_}-prefixed lane and processes only {@code mode=resume}
 * index definitions (see {@link IndexUpdate#isIncluded}).
 *
 * <p>All resumable/chunked behaviour lives in the seam overrides here; the base
 * {@link AsyncIndexUpdate} never chunks. There is no global {@code oak.async.resume}
 * gate any more: resume happens solely by running this class, and chunking engages
 * when the chunk configuration ({@code oak.async.chunkSize} / {@code oak.async.chunkTimeMs})
 * is set.
 */
public class ResumableAsyncIndexUpdate extends AsyncIndexUpdate {

    private static final Logger log = LoggerFactory.getLogger(ResumableAsyncIndexUpdate.class);

    /** Prefix that distinguishes a resume lane from its base lane. */
    public static final String RESUME_LANE_PREFIX = "resume_";

    /** Maps a base lane (e.g. {@code async}) to its resume lane (e.g. {@code resume_async}). */
    public static String resumeLaneName(String baseLane) {
        return RESUME_LANE_PREFIX + baseLane;
    }

    /**
     * Maps a resume lane back to its base lane, i.e. strips {@link #RESUME_LANE_PREFIX}.
     *
     * @throws IllegalArgumentException if {@code resumeLane} is not a resume lane.
     */
    public static String baseLaneName(String resumeLane) {
        if (!isResumeLane(resumeLane)) {
            throw new IllegalArgumentException(
                    "Not a resume lane name (missing '" + RESUME_LANE_PREFIX + "' prefix): " + resumeLane);
        }
        return resumeLane.substring(RESUME_LANE_PREFIX.length());
    }

    /** Whether {@code laneName} is a resume lane. */
    public static boolean isResumeLane(String laneName) {
        return laneName != null && laneName.startsWith(RESUME_LANE_PREFIX);
    }

    public ResumableAsyncIndexUpdate(@NotNull String resumeLaneName, @NotNull NodeStore store,
                                     @NotNull IndexEditorProvider provider, boolean switchOnSync) {
        super(resumeLaneName, store, provider, switchOnSync);
    }

    public ResumableAsyncIndexUpdate(@NotNull String resumeLaneName, @NotNull NodeStore store,
                                     @NotNull IndexEditorProvider provider, StatisticsProvider statsProvider,
                                     boolean switchOnSync) {
        super(resumeLaneName, store, provider, statsProvider, switchOnSync);
    }

    public ResumableAsyncIndexUpdate(@NotNull String resumeLaneName, @NotNull NodeStore store,
                                     @NotNull IndexEditorProvider provider) {
        super(resumeLaneName, store, provider);
    }

    @Override
    protected String indexMatchLaneName() {
        return baseLaneName(getName());
    }

    @Override
    protected String resolveBeforeCheckpoint(NodeState async) {
        String own = async.getString(getName());
        if (own != null) {
            return own;                       // resume lane already has its own checkpoint
        }
        return async.getString(baseLaneName(getName()));  // seed once from the base lane
    }

    @Override
    protected boolean isResumeLane() {
        return true;
    }

    @Override
    protected boolean isChunkedRun(NodeState before) {
        long chunkTimeMs = Long.getLong(PROP_CHUNK_TIME_MS, 0);
        return (configuredChunkSize > 0 || chunkTimeMs > 0) && before != MISSING_NODE;
    }

    @Override
    protected ResumeContext buildResumeContext(String resumeFromPath, PathTree pathTree, long chunkLimit) {
        if (resumeFromPath != null && !"/".equals(resumeFromPath)) {
            log.info("[{}] Created resume context from path: {} (PathTree has {} indexed nodes)",
                    getName(), resumeFromPath, pathTree.getIndexedNodes());
            return ResumeContext.createForResume(resumeFromPath, pathTree, (int) chunkLimit);
        }
        // For first run or non-resume, still use the PathTree to track what we index.
        log.debug("[{}] Created first-run resume context with PathTree", getName());
        return new ResumeContext(null, pathTree, (int) chunkLimit);
    }

    @Override
    protected boolean onChunkComplete(CommitFailedException exception,
                                      AsyncUpdateCallback callback,
                                      ResumeContext resumeContext,
                                      IndexUpdate indexUpdate,
                                      NodeBuilder builder,
                                      String beforeCheckpoint,
                                      String afterCheckpoint,
                                      AtomicReference<String> checkpointToReleaseRef) throws CommitFailedException {
        commitChunkAndSaveResumeState(exception, callback, resumeContext, indexUpdate,
                builder, beforeCheckpoint, afterCheckpoint);

        // Don't release any checkpoints - we need both beforeCheckpoint and afterCheckpoint.
        // They will be cleaned up when indexing completes.
        checkpointToReleaseRef.set(null);

        log.info("[{}] Chunk commit complete - index is incrementally searchable", getName());
        return true;
    }
}
