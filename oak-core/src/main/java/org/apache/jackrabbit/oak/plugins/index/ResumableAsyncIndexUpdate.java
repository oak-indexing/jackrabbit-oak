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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
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

    /** Hidden marker set on a def while it is managed by this resume lane; used to detect reverts. */
    private static final String RESUME_MANAGED_MARKER = ":resumeManaged";

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

    /**
     * Self-heals when an index definition that used to opt into this resume lane
     * ({@code mode=resume}) has been reverted (the {@code mode} property removed or
     * changed). Detection is marker-based: while a def is observed with
     * {@code mode=resume} it is stamped with the hidden {@link #RESUME_MANAGED_MARKER}
     * property; a def that carries the marker but no longer has {@code mode=resume}
     * has reverted, so it is flagged {@code reindex=true} (so the normal lane rebuilds
     * it cleanly) and the marker is removed. Defs that were never managed by this lane
     * (no marker, no {@code mode=resume}) are left untouched, so ordinary async indexes
     * are never force-reindexed just because {@code mode} is unset. Once no
     * {@code mode=resume} defs remain on this lane, the lane's own resume-state node
     * ({@code :async/<resumeLane>-resume}) is deleted.
     */
    void cleanupRevertedIndexes(NodeBuilder root) {
        String base = baseLaneName(getName());
        boolean anyResumeDefRemains = false;
        NodeBuilder defs = root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        if (defs.exists()) {
            for (String n : defs.getChildNodeNames()) {
                NodeBuilder def = defs.getChildNode(n);
                if (!def.hasProperty(IndexConstants.ASYNC_PROPERTY_NAME)) {
                    continue;
                }
                boolean matchesBase = IterableUtils.contains(
                        def.getProperty(IndexConstants.ASYNC_PROPERTY_NAME).getValue(Type.STRINGS), base);
                if (!matchesBase) {
                    continue;
                }
                boolean isResume = IndexConstants.MODE_RESUME.equals(
                        def.getString(IndexConstants.MODE_PROPERTY_NAME));
                boolean wasManaged = def.getBoolean(RESUME_MANAGED_MARKER);
                if (isResume) {
                    if (!wasManaged) {
                        def.setProperty(RESUME_MANAGED_MARKER, true);   // claim it (hidden marker)
                    }
                    anyResumeDefRemains = true;
                } else if (wasManaged) {
                    // this def WAS managed by the resume lane and has now reverted:
                    // rebuild cleanly on the normal lane and drop the marker
                    def.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
                    def.removeProperty(RESUME_MANAGED_MARKER);
                }
                // ordinary never-managed def (no mode, no marker): untouched
            }
        }
        if (!anyResumeDefRemains) {
            NodeBuilder async = root.getChildNode(ASYNC);
            String resumeNode = getName() + "-resume";
            if (async.hasChildNode(resumeNode)) {
                async.getChildNode(resumeNode).remove();
            }
            // The resume-lane checkpoint property is managed by the base full-completion
            // path; only the PathTree resume-state node needs removal here.
        }
    }

    @Override
    protected void afterRun(NodeBuilder builder, IndexUpdate indexUpdate, boolean fullyCompleted) {
        cleanupRevertedIndexes(builder);
    }
}
