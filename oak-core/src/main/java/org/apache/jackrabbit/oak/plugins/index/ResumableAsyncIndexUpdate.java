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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;

/**
 * Segregated resume/chunked variant of {@link AsyncIndexUpdate}. It runs on a dedicated
 * {@code resume_}-prefixed lane and always chunks (given chunk configuration), independent of
 * the {@code FT_RESUMABLE_ASYNC} toggle that governs base lanes. Moving an index onto a
 * {@code resume_} lane is a lane-name change and therefore reindexes on switch; all lane state
 * is keyed by this lane's own name under {@code :async} and never collides with the base lane.
 */
public class ResumableAsyncIndexUpdate extends AsyncIndexUpdate {

    /** Prefix that distinguishes a resume lane from its base lane. */
    public static final String RESUME_LANE_PREFIX = "resume_";

    /** Maps a base lane (e.g. {@code async}) to its resume lane (e.g. {@code resume_async}). */
    public static String resumeLaneName(String baseLane) {
        return RESUME_LANE_PREFIX + baseLane;
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
    protected boolean isResumeLane() {
        return true;
    }

    @Override
    protected boolean isChunkedRun(NodeState before) {
        boolean chunkConfigured = configuredChunkSize > 0 || Long.getLong(PROP_CHUNK_TIME_MS, 0) > 0;
        if (!chunkConfigured) {
            return false;
        }
        if (before != MISSING_NODE) {
            return true;                        // incremental always chunks
        }
        return isResumableReindexEnabled();     // initial/reindex chunks only when toggle ON
    }
}
