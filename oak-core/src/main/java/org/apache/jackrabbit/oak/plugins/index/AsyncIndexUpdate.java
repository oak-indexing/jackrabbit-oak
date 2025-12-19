/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_DONE;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate.MissingIndexProviderStrategy;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler.CorruptIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCounterMBeanEstimator;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.plugins.index.resume.PathTree;
import org.apache.jackrabbit.oak.plugins.index.resume.PathTreeEditorDiff;
import org.apache.jackrabbit.oak.plugins.index.resume.ResumeContext;
import org.apache.jackrabbit.oak.plugins.index.resume.ResumableEditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.Counting;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncIndexUpdate implements Runnable, Closeable {
    /**
     * Name of service property which determines the name of Async task
     */
    public static final String PROP_ASYNC_NAME = "oak.async";
    private static final String CONCURRENT_EXCEPTION_MSG ="The index was not updated. Waiting for the lease to expire (another copy might be still running); skipping this update. ";
    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    static final String ASYNC = ":async";

    private static final long DEFAULT_LIFETIME = TimeUnit.DAYS.toMillis(100);

    private static final CommitFailedException INTERRUPTED = new CommitFailedException(
            "Async", 1, "Indexing stopped forcefully");
    
    /**
     * Exception thrown when chunk limit is reached during diff traversal.
     * This is caught in the outer loop to trigger chunk commit and continue.
     */
    private static final CommitFailedException CHUNK_COMPLETE = new CommitFailedException(
            "Async", 2, "Chunk limit reached - commit and continue");

    /**
     * Timeout in milliseconds after which an async job would be considered as
     * timed out. Another node in cluster would wait for timeout before
     * taking over a running job
     */
    private static final long DEFAULT_ASYNC_TIMEOUT = TimeUnit.MINUTES.toMillis(
            Integer.getInteger("oak.async.lease.timeout", 15));

    private final String name;

    private final NodeStore store;

    private final IndexEditorProvider provider;
    
    // Chunk size for resumable indexing (nodes per chunk)
    private final long configuredChunkSize;

    /**
     * Property name which stores the timestamp upto which the repository is
     * indexed
     */
    private final String lastIndexedTo;

    private final long lifetime = DEFAULT_LIFETIME; // TODO: make configurable

    private final AsyncIndexStats indexStats;

    /** Flag to switch to synchronous updates once the index caught up to the repo */
    private final boolean switchOnSync;

    /**
     * Set of reindexed definitions updated between runs because a single diff
     * can report fewer definitions than there really are. Used in coordination
     * with the switchOnSync flag, so we know which def need to be updated after
     * a run with no changes.
     */
    private final Set<String> reindexedDefinitions = new HashSet<String>();

    private final MissingIndexProviderStrategy missingStrategy = new DefaultMissingIndexProviderStrategy();

    private final IndexTaskSpliter taskSplitter = new IndexTaskSpliter();

    private final Semaphore runPermit = new Semaphore(1);

    /**
     * Flag which would be set to true if the close operation is not
     * able to close within specific time. The flag would be an
     * indication to indexing thread to return straightway say by
     * throwing an exception
     */
    private final AtomicBoolean forcedStopFlag = new AtomicBoolean();

    private IndexMBeanRegistration mbeanRegistration;

    private long leaseTimeOut;

    /**
     * Controls the length of the interval (in minutes) at which an indexing
     * error is logged as 'warning'. for the rest of the indexing cycles errors
     * will be logged at 'debug' level
     */
    private static long ERROR_WARN_INTERVAL = TimeUnit.MINUTES.toMillis(Integer
            .getInteger("oak.async.warn.interval", 30));

    /**
     * Timeout in seconds for which close call would wait before forcefully
     * stopping the indexing thread
     */
    private int softTimeOutSecs = Integer.getInteger("oak.async.softTimeOutSecs", 2 * 60);

    private boolean closed;

    /**
     * The checkpoint cleanup interval in minutes. Defaults to 5 minutes.
     * Setting it to a negative value disables automatic cleanup. See OAK-4826.
     */
    private final int cleanupIntervalMinutes
            = Integer.getInteger("oak.async.checkpointCleanupIntervalMinutes", 5);


    /**
     * Setting this to true lead to lane execution (node traversal) even if there
     * is no index assigned to this lane under /oak:index. (Default value is true).
     */
    private final boolean traverseNodesIfLaneNotPresentInIndex
            = !Boolean.getBoolean("oak.async.traverseNodesIfLanePresentInIndex");

    /**
     * The time in minutes since the epoch when the last checkpoint cleanup ran.
     */
    private long lastCheckpointCleanUpTime;
    
    /**
     * Tracks ResumingEditor timing from the last run.
     * timeToResumePoint = time to reach the target path during resume
     * totalResumeTime = total time in ResumingEditor including overhead
     */
    private long lastResumeTimeToTarget = 0;
    private long lastResumeTotalTime = 0;

    private List<ValidatorProvider> validatorProviders = Collections.emptyList();

    private TrackingCorruptIndexHandler corruptIndexHandler = new TrackingCorruptIndexHandler();

    private final StatisticsProvider statisticsProvider;

    public AsyncIndexUpdate(@NotNull String name, @NotNull NodeStore store,
                            @NotNull IndexEditorProvider provider, boolean switchOnSync) {
        this(name, store, provider, StatisticsProvider.NOOP, switchOnSync);
    }

    public AsyncIndexUpdate(@NotNull String name, @NotNull NodeStore store,
                            @NotNull IndexEditorProvider provider, StatisticsProvider statsProvider, boolean switchOnSync) {
        this.name = checkValidName(name);
        this.lastIndexedTo = lastIndexedTo(name);
        this.store = requireNonNull(store);
        this.provider = requireNonNull(provider);
        this.switchOnSync = switchOnSync;
        this.leaseTimeOut = DEFAULT_ASYNC_TIMEOUT;
        this.statisticsProvider = statsProvider;
        this.indexStats = new AsyncIndexStats(name, statsProvider);
        this.corruptIndexHandler.setMeterStats(statsProvider.getMeter(TrackingCorruptIndexHandler.CORRUPT_INDEX_METER_NAME, StatsOptions.METRICS_ONLY));
        
        // Cache chunk size configuration at construction time
        this.configuredChunkSize = Long.getLong("oak.async.chunkSize", -1);
        
        if (configuredChunkSize > 0) {
            log.info("[{}] Resumable indexing enabled - chunkSize: {}", name, configuredChunkSize);
        }
    }

    public AsyncIndexUpdate(@NotNull String name, @NotNull NodeStore store,
                            @NotNull IndexEditorProvider provider) {
        this(name, store, provider, false);
    }

    public static String checkValidName(String asyncName){
        requireNonNull(asyncName, "async name should not be null");
        if (IndexConstants.ASYNC_REINDEX_VALUE.equals(asyncName)){
            return asyncName;
        }
        checkArgument(asyncName.endsWith("async"), "async name [%s] does not confirm to " +
                "naming pattern of ending with 'async'", asyncName);
        return asyncName;
    }

    public static boolean isAsyncLaneName(String asyncName){
        return IndexConstants.ASYNC_REINDEX_VALUE.equals(asyncName) || asyncName.endsWith("async");
    }

    /**
     * Index update callback that tries to raise the async status flag when
     * the first index change is detected.
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1292">OAK-1292</a>
     */



    public static class AsyncUpdateCallback implements IndexUpdateCallback, NodeTraversalCallback {
        /**
         * Interval in terms of number of nodes traversed after which
         * time would be checked for lease expiry
         */
        public static final int LEASE_CHECK_INTERVAL = 10;
        private final NodeStore store;

        /** The base checkpoint */
        private String checkpoint;

        /**
         * Property name which stores the temporary checkpoint that need to be released on the next run
         */
        private final String tempCpName;

        private final long leaseTimeOut;

        private final String name;

        private final String leaseName;

        private final AsyncIndexStats indexStats;

        private final AtomicBoolean forcedStop;

        private int updateLimit = Integer.getInteger("oak.async.chunkSize", -1);
        
        /**
         * Time limit per chunk in milliseconds. If > 0, commits when time limit is reached.
         */
        private long timeLimit = 0;
        
        /**
         * Start time of current chunk (for time-based chunking).
         */
        private long chunkStartTime = 0;

        private List<ValidatorProvider> validatorProviders = Collections.emptyList();

        /**
         * Expiration time of the last lease we committed, null if lease is
         * disabled
         */
        private Long lease = null;

        private boolean hasLease = false;
        
        /**
         * Flag set when chunk limit is reached during traversal.
         */
        private volatile boolean chunkLimitReached = false;
        
        /**
         * The path where chunk limit was reached - used as resume point for next run.
         */
        private volatile String chunkLastIndexedPath = null;
        
        /**
         * Flag to indicate we're in skip mode (skipping to resume point).
         * When true, don't count nodes toward chunk limit.
         */
        private volatile boolean inSkipMode = false;
        
        /**
         * Resume path - when set, we skip indexing until we reach this path.
         */
        private volatile String resumePath = null;
        
        /**
         * PathTree reference for checking if a path is already indexed.
         * Used to skip counting indexed nodes toward chunk limit.
         */
        private volatile PathTree pathTree = null;

        public AsyncUpdateCallback(NodeStore store, String name,
                                   long leaseTimeOut, String checkpoint,
                                   AsyncIndexStats indexStats, AtomicBoolean forcedStop) {
            this.store = store;
            this.name = name;
            this.forcedStop = forcedStop;
            this.leaseTimeOut = leaseTimeOut;
            this.checkpoint = checkpoint;
            this.tempCpName = getTempCpName(name);
            this.indexStats = indexStats;
            this.leaseName = leasify(name);
        }

        /**
         * Get the path where chunk limit was reached.
         * @return the last indexed path, or null if no chunk limit was reached
         */
        public String getChunkLastIndexedPath() {
            return chunkLastIndexedPath;
        }
        
        /**
         * Set skip mode - when true, nodes are being skipped
         * and should not count toward chunk limit.
         */
        public void setSkipMode(boolean skipMode) {
            this.inSkipMode = skipMode;
        }
        
        /**
         * Check if we're in skip mode (skipping to resume point).
         */
        public boolean isInSkipMode() {
            return inSkipMode;
        }
        
        /**
         * Set the resume path - we'll skip indexing until we reach this path.
         */
        public void setResumePath(String path) {
            this.resumePath = path;
        }
        
        /**
         * Set the PathTree for checking if paths are already indexed.
         */
        public void setPathTree(PathTree pathTree) {
            this.pathTree = pathTree;
        }
        
        /**
         * Set the time limit per chunk in milliseconds.
         */
        public void setTimeLimit(long timeMs) {
            this.timeLimit = timeMs;
            if (timeMs > 0) {
                this.chunkStartTime = System.currentTimeMillis();
            }
        }
        
        /**
         * Reset chunk start time (call at start of each chunk).
         */
        public void resetChunkStartTime() {
            this.chunkStartTime = System.currentTimeMillis();
        }

        protected void initLease() throws CommitFailedException {
            if (hasLease) {
                return;
            }
            NodeState root = store.getRoot();
            NodeState async = root.getChildNode(ASYNC);
            if(isLeaseCheckEnabled(leaseTimeOut)) {
                long now = getTime();
                this.lease = now + 2 * leaseTimeOut;
                long beforeLease = async.getLong(leaseName);
                if (beforeLease > now) {
                    throw newConcurrentUpdateException();
                }

                NodeBuilder builder = root.builder();
                builder.child(ASYNC).setProperty(leaseName, lease);
                mergeWithConcurrencyCheck(store, validatorProviders, builder, checkpoint, beforeLease, name);
            } else {
                lease = null;
                // remove stale lease info if needed
                if (async.hasProperty(leaseName)) {
                    NodeBuilder builder = root.builder();
                    builder.child(ASYNC).removeProperty(leaseName);
                    mergeWithConcurrencyCheck(store, validatorProviders,
                            builder, checkpoint, null, name);
                }
            }
            hasLease = true;
        }

        protected void prepare(String afterCheckpoint)
                throws CommitFailedException {
            if (!hasLease) {
                initLease();
            }
            NodeState root = store.getRoot();
            NodeBuilder builder = root.builder();
            NodeBuilder async = builder.child(ASYNC);

            updateTempCheckpoints(async, checkpoint, afterCheckpoint);
            mergeWithConcurrencyCheck(store, validatorProviders, builder, checkpoint, lease, name);

            // reset updates counter
            indexStats.reset();
        }
        
        private void updateTempCheckpoints(NodeBuilder async,
                                           String checkpoint, String afterCheckpoint) {
            indexStats.setReferenceCheckpoint(checkpoint);
            indexStats.setProcessedCheckpoint(afterCheckpoint);

            // try to drop temp cps, add 'currentCp' to the temp cps list
            // IMPORTANT: Don't release the afterCheckpoint - we need it for the current/resume diff!
            Set<String> temps = new HashSet<>();
            for (String cp : getStrings(async, tempCpName)) {
                if (cp.equals(checkpoint) || cp.equals(afterCheckpoint)) {
                    // Keep before and after checkpoints
                    temps.add(cp);
                    continue;
                }
                boolean released = store.release(cp);
                log.debug("[{}] Releasing temporary checkpoint {}: {}", name, cp, released);
                if (!released) {
                    temps.add(cp);
                }
            }
            temps.add(afterCheckpoint);
            async.setProperty(tempCpName, temps, Type.STRINGS);
            indexStats.setTempCheckpoints(temps);
        }

        boolean isDirty() {
            return indexStats.getUpdates() > 0;
        }

        void close() throws CommitFailedException {
            if (isLeaseCheckEnabled(leaseTimeOut)) {
                NodeBuilder builder = store.getRoot().builder();
                NodeBuilder async = builder.child(ASYNC);
                async.removeProperty(leaseName);
                mergeWithConcurrencyCheck(store, validatorProviders, builder,
                        async.getString(name), lease, name);
            }
        }

        @Override
        public void indexUpdate() throws CommitFailedException {
            checkIfStopped();
            indexStats.incUpdates();
        }

        @Override
        public void traversedNode(PathSource pathSource) throws CommitFailedException{
            checkIfStopped();
            
            String currentPath = pathSource.getPath();
            
            // OPTIMIZATION: Skip counting if this path is FULLY PROCESSED (enter+leave done)
            // This means:
            // 1. enter() was called (markEnterCompleted)
            // 2. All properties were processed (happens between enter and leave)
            // 3. All children were traversed  
            // 4. leave() was called (markLeaveCompleted)
            // 5. The node's content is definitely in Lucene
            //
            // We do NOT skip on just isIndexed() anymore because nodes with
            // enterCompleted but not leaveCompleted need to be re-processed
            if (pathTree != null && pathTree.isFullyProcessed(currentPath)) {
                // Path fully processed - skip entirely
                log.trace("[{}] Skipping fully processed path: {}", name, currentPath);
                return;
            }
            
            // Count this node as traversed (new node to index)
            long nodesRead = indexStats.incTraversal();
            
            // Check if chunk limit reached (by count)
            if (updateLimit > 0 && !chunkLimitReached && nodesRead >= updateLimit) {
                // Set chunk limit flags
                chunkLimitReached = true;
                chunkLastIndexedPath = currentPath;
                
                log.info("[{}] Chunk limit reached (COUNT) at path: {} (processed {} NEW nodes)", 
                    name, chunkLastIndexedPath, nodesRead);
                System.out.println("[DEBUG-CHUNK] COUNT limit reached: " + nodesRead + " nodes at " + currentPath);
                
                // Throw CHUNK_COMPLETE to stop EditorDiff and trigger commit
                throw CHUNK_COMPLETE;
            }
            
            // Check if chunk limit reached (by time)
            if (timeLimit > 0 && !chunkLimitReached && chunkStartTime > 0) {
                long elapsedMs = System.currentTimeMillis() - chunkStartTime;
                if (elapsedMs >= timeLimit) {
                    chunkLimitReached = true;
                    chunkLastIndexedPath = currentPath;
                    
                    log.info("[{}] Chunk limit reached (TIME) at path: {} (elapsed {}ms, processed {} nodes)", 
                        name, chunkLastIndexedPath, elapsedMs, nodesRead);
                    System.out.println("[DEBUG-CHUNK] TIME limit reached: " + elapsedMs + "ms at " + currentPath);
                    
                    throw CHUNK_COMPLETE;
                }
            }

            if (nodesRead % LEASE_CHECK_INTERVAL == 0 && isLeaseCheckEnabled(leaseTimeOut)) {
                long now = getTime();
                if (now + leaseTimeOut > lease) {
                    long newLease = now + 2 * leaseTimeOut;
                    NodeBuilder builder = store.getRoot().builder();
                    builder.child(ASYNC).setProperty(leaseName, newLease);
                    mergeWithConcurrencyCheck(store, validatorProviders, builder, checkpoint, lease, name);
                    lease = newLease;
                }
            }
        }

        protected long getTime() {
            return System.currentTimeMillis();
        }

        public void setCheckpoint(String checkpoint) {
            this.checkpoint = checkpoint;
        }

        public void setValidatorProviders(List<ValidatorProvider> validatorProviders) {
            this.validatorProviders = requireNonNull(validatorProviders);
        }
        
        public void setUpdateLimit(int limit) {
            this.updateLimit = limit;
        }

        private void checkIfStopped() throws CommitFailedException {
            if (forcedStop.get()){
                forcedStop.set(false);
                throw INTERRUPTED;
            }
        }
    }

    @Override
    public synchronized void run() {
        if (!shouldProceed()){
            return;
        }
        boolean permitAcquired = false;
        try{
            if (runPermit.tryAcquire()){
                permitAcquired = true;
                runWhenPermitted();
            } else {
                log.warn("[{}] Could not acquire run permit. Stop flag set to [{}] Skipping the run", name, forcedStopFlag);
            }
        } finally {
            if (permitAcquired){
                runPermit.release();
            }
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        int hardTimeOut = 5 * softTimeOutSecs;
        if(!runPermit.tryAcquire()){
            //First let current run complete without bothering it
            log.debug("[{}] [WAITING] Indexing in progress. Would wait for {} secs for it to finish", name, softTimeOutSecs);
            try {
                if(!runPermit.tryAcquire(softTimeOutSecs, TimeUnit.SECONDS)){
                    //We have now waited enough. So signal the indexer that it should return right away
                    //as soon as it sees the forcedStopFlag
                    log.debug("[{}] [SOFT LIMIT HIT] Indexing found to be in progress for more than [{}]s. Would " +
                            "signal it to now force stop", name, softTimeOutSecs);
                    forcedStopFlag.set(true);
                    if(!runPermit.tryAcquire(hardTimeOut, TimeUnit.SECONDS)){
                        //Index thread did not listened to our advice. So give up now and warn about it
                        log.warn("[{}] Indexing still not found to be complete. Giving up after [{}]s", name, hardTimeOut);
                    }
                } else {
                    log.info("[{}] [CLOSED OK] Async indexing run completed. Closing it now", name);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            log.info("[{}] Closed", name);
        }
        closed = true;
    }

    private void runWhenPermitted() {
        if (indexStats.isPaused()) {
            if (indexStats.forcedLeaseRelease){
                try {
                    clearLease();
                } catch (CommitFailedException e) {
                    log.warn("Unable to release lease, please try again", e);
                }
                indexStats.forcedLeaseRelease = false;
            }
            log.debug("[{}] Ignoring the run as indexing is paused", name);
            return;
        }
        log.debug("[{}] Running background index task", name);

        NodeState root = store.getRoot();
        NodeState async = root.getChildNode(ASYNC);

        if (isLeaseCheckEnabled(leaseTimeOut)) {
            // check for concurrent updates
            long leaseEndTime = async.getLong(leasify(name));
            long currentTime = System.currentTimeMillis();
            if (leaseEndTime > currentTime) {
                long leaseExpMsg = (leaseEndTime - currentTime) / 1000;
                String err = String.format(CONCURRENT_EXCEPTION_MSG +
                        "Time left for lease to expire %d s. Indexing can resume by %tT", leaseExpMsg, leaseEndTime);
                indexStats.failed(new Exception(err, newConcurrentUpdateException()));
                return;
            }
        }

        // start collecting runtime statistics
        preAsyncRunStatsStats(indexStats);

        // find the last indexed state, and check if there are recent changes
        NodeState before;
        String beforeCheckpoint = async.getString(name);
        AsyncUpdateCallback callback = newAsyncUpdateCallback(store,
                name, leaseTimeOut, beforeCheckpoint, indexStats,
                forcedStopFlag);
        if (beforeCheckpoint != null) {
            NodeState state = store.retrieve(beforeCheckpoint);
            if (state == null) {
                // to make sure we're not reading a stale root rev, we're
                // attempting a write+read via the lease-grab mechanics
                try {
                    callback.initLease();
                } catch (CommitFailedException e) {
                    indexStats.failed(e);
                    return;
                }
                root = store.getRoot();
                beforeCheckpoint = root.getChildNode(ASYNC).getString(name);
                if (beforeCheckpoint != null) {
                    state = store.retrieve(beforeCheckpoint);
                    callback.setCheckpoint(beforeCheckpoint);
                }
            }

            if (state == null) {
                log.warn(
                        "[{}] Failed to retrieve previously indexed checkpoint {}; re-running the initial index update",
                        name, beforeCheckpoint);
                beforeCheckpoint = null;
                callback.setCheckpoint(beforeCheckpoint);
                before = MISSING_NODE;
            } else if (noVisibleChanges(state, root) && !switchOnSync) {
                log.debug(
                        "[{}] No changes since last checkpoint; skipping the index update",
                        name);
                postAsyncRunStatsStatus(indexStats);
                return;
            } else {
                before = state;
            }
        } else {
            log.info("[{}] Initial index update", name);
            before = MISSING_NODE;
        }

        // Check for resume state from a previous interrupted run
        // Use a separate child node to avoid conflict with the checkpoint property
        String resumeNodeName = name + "-resume";
        String resumeFromPath = null;
        String resumeCheckpoint = null;
        NodeState laneNode = async.getChildNode(resumeNodeName);
        if (laneNode.exists() && laneNode.hasProperty("targetCheckpoint")) {
            resumeCheckpoint = laneNode.getString("targetCheckpoint");
            resumeFromPath = laneNode.getString("lastIndexedPath");
            
            // Verify the resume checkpoint still exists
            if (resumeCheckpoint != null && store.retrieve(resumeCheckpoint) != null) {
                log.info("[{}] Found resume state - checkpoint: {}, path: {}", 
                    name, resumeCheckpoint, resumeFromPath);
            } else {
                log.warn("[{}] Resume checkpoint {} no longer exists, starting fresh", 
                    name, resumeCheckpoint);
                resumeFromPath = null;
                resumeCheckpoint = null;
            }
        }

        // there are some recent changes, so let's create a new checkpoint (or use resume checkpoint)
        String afterTime = now();
        String oldThreadName = Thread.currentThread().getName();
        boolean threadNameChanged = false;

        String afterCheckpoint;
        NodeState after;
        
        if (resumeCheckpoint != null) {
            // BUG 2 FIXED: Resuming - use the existing checkpoint
            afterCheckpoint = resumeCheckpoint;
            after = store.retrieve(afterCheckpoint);
            if (after == null) {
                log.warn("[{}] Unable to retrieve resume checkpoint {}, creating new checkpoint", 
                    name, resumeCheckpoint);
                // Fall back to creating new checkpoint
                afterCheckpoint = store.checkpoint(lifetime, Map.of(
                        "creator", AsyncIndexUpdate.class.getSimpleName(),
                        "created", afterTime,
                        "thread", oldThreadName,
                        "name", name));
                after = store.retrieve(afterCheckpoint);
                resumeFromPath = null; // Reset resume path since checkpoint changed
            } else {
                log.info("[{}] Using resume checkpoint: {}", name, afterCheckpoint);
            }
        } else {
            // Normal flow - create new checkpoint
            afterCheckpoint = store.checkpoint(lifetime, Map.of(
                    "creator", AsyncIndexUpdate.class.getSimpleName(),
                    "created", afterTime,
                    "thread", oldThreadName,
                    "name", name));
            after = store.retrieve(afterCheckpoint);
        }
        if (after == null) {
            log.debug(
                    "[{}] Unable to retrieve newly created checkpoint {}, skipping the index update",
                    name, afterCheckpoint);
            //Do not update the status as technically the run is not complete
            return;
        }

        AtomicReference<String> checkpointToReleaseRef = new AtomicReference<>(afterCheckpoint);
        boolean updatePostRunStatus = false;
        try {
            String newThreadName = "async-index-update-" + name;
            log.trace("Switching thread name to {}", newThreadName);
            threadNameChanged = true;
            Thread.currentThread().setName(newThreadName);
            // BUG 2 FIXED: Pass resumeFromPath to updateIndex
            // beforeCheckpoint stays as last completed checkpoint (NOT changed to resumeCheckpoint)
            updatePostRunStatus = updateIndex(before, beforeCheckpoint, after,
                    afterCheckpoint, afterTime, callback, checkpointToReleaseRef, resumeFromPath);

            // Update checkpoint state if update completed
            if (updatePostRunStatus) {
                // the update succeeded, i.e. it no longer fails
                if (indexStats.didLastIndexingCycleFailed()) {
                    indexStats.fixed();
                }

                // the update succeeded, so we are sure we can release the earlier checkpoint -
                // otherwise the new checkpoint associated with the failed update
                // may still get released in the finally block (depending on where the index update failed)
                checkpointToReleaseRef.set(beforeCheckpoint);
                indexStats.setReferenceCheckpoint(afterCheckpoint);
                indexStats.setProcessedCheckpoint("");
                indexStats.releaseTempCheckpoint(afterCheckpoint);
            }

        } catch (Exception e) {
            indexStats.failed(e);

        } finally {
            if (threadNameChanged) {
                log.trace("Switching thread name back to {}", oldThreadName);
                Thread.currentThread().setName(oldThreadName);
            }
            // null during initial indexing
            // and skip release if this cp was used in a split operation
            String checkpointToRelease = checkpointToReleaseRef.get();
            if (checkpointToRelease != null
                    && !checkpointToRelease.equals(taskSplitter
                    .getLastReferencedCp())) {
                if (!store.release(checkpointToRelease)) {
                    log.debug("[{}] Unable to release checkpoint {}", name,
                            checkpointToRelease);
                }
            }
            maybeCleanUpCheckpoints();

            if (updatePostRunStatus) {
                postAsyncRunStatsStatus(indexStats);
            }
        }
    }

    private void clearLease() throws CommitFailedException {
        NodeState root = store.getRoot();
        NodeState async = root.getChildNode(ASYNC);
        String beforeCheckpoint = async.getString(name);
        String leaseName= leasify(name);
        if (async.hasProperty(leaseName)) {
            NodeBuilder builder = root.builder();
            builder.child(ASYNC).removeProperty(leaseName);
            mergeWithConcurrencyCheck(store, validatorProviders,
                    builder, beforeCheckpoint, null, name);
            log.info("Lease property removed for lane: {}", name);
        } else {
            log.info("No Lease property present for lane: {}", name);
        }

    }

    private boolean shouldProceed() {
        NodeState asyncNode = store.getRoot().getChildNode(":async");
        /*
            If /:async node already have the lane(under consideration) info, we can proceed ahead, as
            majorly this change is to stop repository traversal on very first run. If lane had already
            traversed nodes in repository there is no point stopping this now.
         */
        if (asyncNode.exists() && asyncNode.hasProperty(name)) {
            return true;
        }
        return traverseNodesIfLaneNotPresentInIndex || isIndexWithLanePresent();
    }

    /**
     *
     * @return true if there is at least one index present under /oak:index with indexingLane in action.
     */
    private boolean isIndexWithLanePresent() {
        NodeState oakIndexNode = store.getRoot().getChildNode("oak:index");
        if (!oakIndexNode.exists()) {
            log.info("lane: {} - no indexes exist under /oak:index", name);
            return false;
        }
        for (ChildNodeEntry childNodeEntry : oakIndexNode.getChildNodeEntries()) {
            PropertyState async = childNodeEntry.getNodeState().getProperty("async");
            if (async != null) {
                for (String s : async.getValue(Type.STRINGS)) {
                    if (s.equals(name)) {
                        return true;
                    }
                }
            }
        }
        log.info("lane: {} not present for indexes under /oak:index", name);
        return false;
    }

    private void markFailingIndexesAsCorrupt(NodeBuilder builder) {
        for (Map.Entry<String, CorruptIndexInfo> index : corruptIndexHandler.getCorruptIndexData(name).entrySet()){
            NodeBuilder indexBuilder = childBuilder(builder, index.getKey());
            CorruptIndexInfo info = index.getValue();
            if (!indexBuilder.hasProperty(IndexConstants.CORRUPT_PROPERTY_NAME)){
                String corruptSince = ISO8601.format(info.getCorruptSinceAsCal());
                indexBuilder.setProperty(
                        PropertyStates.createProperty(IndexConstants.CORRUPT_PROPERTY_NAME, corruptSince, Type.DATE));
                log.info("Marking [{}] as corrupt. The index is failing {}", info.getPath(), info.getStats());
            } else {
                log.debug("Failing index at [{}] is already marked as corrupt. The index is failing {}",
                        info.getPath(), info.getStats());
            }
        }
    }

    private static NodeBuilder childBuilder(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(requireNonNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    private void maybeCleanUpCheckpoints() {
        if (cleanupIntervalMinutes < 0) {
            log.debug("checkpoint cleanup skipped because cleanupIntervalMinutes set to: " + cleanupIntervalMinutes);
        } else if (indexStats.isFailing()) {
            log.debug("checkpoint cleanup skipped because index stats are failing: " + indexStats);
        } else {
            // clean up every five minutes by default
            long currentMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
            long scheduledInMinutes = (lastCheckpointCleanUpTime + cleanupIntervalMinutes) - currentMinutes;

            if (scheduledInMinutes > 0) {
                log.debug("checkpoint cleanup scheduled in " + scheduledInMinutes + " minutes");
            } else {
                try {
                    cleanUpCheckpoints();
                } catch (Throwable e) {
                    log.warn("Checkpoint clean up failed", e);
                }
                lastCheckpointCleanUpTime = currentMinutes;
            }
        }
    }

    void cleanUpCheckpoints() {
        log.debug("[{}] Cleaning up orphaned checkpoints", name);
        Set<String> keep = new HashSet<>();
        String cp = indexStats.getReferenceCheckpoint();
        if (cp == null) {
            log.warn("[{}] No reference checkpoint set in index stats", name);
            return;
        }
        keep.add(cp);
        keep.addAll(indexStats.tempCps);
        log.debug("Getting checkpoint info for {}", cp);
        Map<String, String> info = store.checkpointInfo(cp);
        String value = info.get("created");
        if (value != null) {
            // remove unreferenced AsyncIndexUpdate checkpoints:
            // - without 'created' info (checkpoint created before OAK-4826)
            // or
            // - 'created' value older than the current reference and
            //   not within the lease time frame
            long current = ISO8601.parse(value).getTimeInMillis();
            for (String checkpoint : store.checkpoints()) {
                info = store.checkpointInfo(checkpoint);
                String creator = info.get("creator");
                String created = info.get("created");
                String name = info.get("name");
                if (!keep.contains(checkpoint)
                        && this.name.equals(name)
                        && AsyncIndexUpdate.class.getSimpleName().equals(creator)
                        && (created == null || ISO8601.parse(created).getTimeInMillis() + leaseTimeOut < current)) {
                    if (store.release(checkpoint)) {
                        log.info("[{}] Removed orphaned checkpoint '{}' {}",
                                name, checkpoint, info);
                    }
                }
            }
        } else {
            log.info("Checkpoint Info : '{}' for the checkpoint - {} ; keep -- {}", info, cp, keep);
        }
    }

    protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store,
                                                         String name, long leaseTimeOut, String beforeCheckpoint,
                                                         AsyncIndexStats indexStats,
                                                         AtomicBoolean stopFlag) {
        AsyncUpdateCallback callback = new AsyncUpdateCallback(store, name, leaseTimeOut,
                beforeCheckpoint, indexStats, stopFlag);
        callback.setValidatorProviders(validatorProviders);
        
        // NOTE: Chunk limit is set in updateIndex() after we know if it's an initial index
        // This allows us to skip chunk limits during initial indexing
        
        return callback;
    }


    /**
     * Updates the index by comparing the before and after state of the repository.
     *
     * @param before the before state
     * @param beforeCheckpoint the before checkpoint
     * @param after the after state
     * @param afterCheckpoint the after checkpoint
     * @param afterTime the time of the after checkpoint
     * @param callback the callback
     * @param checkpointToReleaseRef reference to checkpoint to release
     * @param resumeFromPath the path to resume from (null or "/" for no resume)
     * @return true if the index was updated successfully
     * @throws CommitFailedException if the update failed
     */
    protected boolean updateIndex(NodeState before, String beforeCheckpoint,
                                  NodeState after, String afterCheckpoint, String afterTime,
                                  AsyncUpdateCallback callback,
                                  AtomicReference<String> checkpointToReleaseRef,
                                  String resumeFromPath) throws CommitFailedException {
        Stopwatch watch = Stopwatch.createStarted();
        boolean updatePostRunStatus = true;
        boolean progressLogged = false;
        IndexUpdate indexUpdate = null;

        // Prepare callback - resets counters
        callback.prepare(afterCheckpoint);
        
        // Check if resume indexing is enabled via system property
        boolean resumeEnabled = Boolean.getBoolean("oak.async.resume");
        long chunkTimeMs = Long.getLong("oak.async.chunkTimeMs", 0); // Time-based chunking
        
        // Chunk-based resumable indexing using PathTree
        boolean isInitialIndex = before == MISSING_NODE;
        boolean chunkedMode = resumeEnabled && (configuredChunkSize > 0 || chunkTimeMs > 0) && !isInitialIndex;
        
        // Log indexing mode
        String indexingMode = resumeEnabled ? "RESUME" : "NORMAL";
        System.out.println("[DEBUG-MODE] Indexing mode: " + indexingMode + 
            ", resumeEnabled=" + resumeEnabled + 
            ", chunkSize=" + configuredChunkSize + 
            ", chunkTimeMs=" + chunkTimeMs +
            ", isInitialIndex=" + isInitialIndex +
            ", chunkedMode=" + chunkedMode);
        
        if (chunkedMode) {
                callback.setUpdateLimit((int) configuredChunkSize);
            callback.setTimeLimit(chunkTimeMs);
            System.out.println("[DEBUG-CHUNK] Chunk mode enabled - updateLimit=" + configuredChunkSize + ", timeLimit=" + chunkTimeMs);
            log.info("[{}] Chunk-based indexing enabled - chunkSize: {}, chunkTimeMs: {}", name, configuredChunkSize, chunkTimeMs);
        } else if (isInitialIndex) {
            // Disable chunk limits during initial index - let it complete fully
            callback.setUpdateLimit(-1);
            callback.setTimeLimit(0);
            log.info("[{}] Initial index - chunk limits disabled until complete", name);
        } else {
            // Normal mode - no chunking
            callback.setUpdateLimit(-1);
            callback.setTimeLimit(0);
            log.info("[{}] Normal indexing mode - no chunking", name);
        }

        // Check for index tasks split requests
        taskSplitter.maybeSplit(beforeCheckpoint, callback.lease);
        
        boolean indexingFailed = true;
        NodeBuilder builder = store.getRoot().builder();

        try {
            markFailingIndexesAsCorrupt(builder);

            CommitInfo info = new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN,
                    Map.of(IndexConstants.CHECKPOINT_CREATION_TIME, afterTime));
            
            // Create ResumeContext for resumable indexing
            // Load PathTree if resuming, otherwise create new
            PathTree pathTree;
            String resumeNodeName = name + "-resume";
            NodeState asyncState = store.getRoot().getChildNode(ASYNC);
            NodeState resumeState = asyncState.getChildNode(resumeNodeName);
                
            // Track PathTree loading time
            long pathTreeLoadStartTime = System.currentTimeMillis();
            long pathTreeSerializedSize = 0;
            
            if (resumeFromPath != null && !"/".equals(resumeFromPath) && resumeState.exists()) {
                // Load PathTree from persisted state
                NodeState pathTreeState = resumeState.getChildNode("pathTree");
                if (pathTreeState.exists()) {
                    // Use auto-detect to handle both slim and full formats
                    boolean isSlimFormat = PathTree.isSlimFormat(pathTreeState);
                    pathTree = PathTree.deserializeAuto(pathTreeState);
                    long pathTreeLoadTime = System.currentTimeMillis() - pathTreeLoadStartTime;
                    
                    // Calculate sizes
                    int fullyProcessedCount = pathTree.getFullyProcessedCount();
                    int notFullyProcessedCount = pathTree.getNotFullyProcessedCount();
                    pathTreeSerializedSize = isSlimFormat ? 
                        pathTree.getEstimatedSerializedSize(true) : 
                        pathTree.getEstimatedSerializedSize(false);
                    
                    log.info("[{}] Loaded PathTree from resume state (format={}): {} (load time: {}ms, size: ~{} bytes)", 
                        name, isSlimFormat ? "SLIM" : "FULL", pathTree, pathTreeLoadTime, pathTreeSerializedSize);
                    System.out.println("[DEBUG-PATHTREE] Load time: " + pathTreeLoadTime + "ms, format: " + 
                        (isSlimFormat ? "SLIM" : "FULL") + ", nodes: " + pathTree.getTotalNodes() + 
                        ", indexed: " + pathTree.getIndexedNodes() + 
                        ", fullyProcessed: " + fullyProcessedCount +
                        ", unprocessed: " + notFullyProcessedCount +
                        ", estimated size: " + pathTreeSerializedSize + " bytes");
                } else {
                    pathTree = new PathTree();
                    log.info("[{}] No PathTree found in resume state, creating new", name);
                        }
            } else {
                pathTree = new PathTree();
                log.debug("[{}] Starting fresh run with new PathTree", name);
                }
                
            // Create ResumeContext with PathTree
            int chunkLimit = chunkedMode ? (int) configuredChunkSize : 0;
            ResumeContext resumeContext;
            if (resumeFromPath != null && !"/".equals(resumeFromPath)) {
                resumeContext = ResumeContext.createForResume(resumeFromPath, pathTree, chunkLimit);
                log.info("[{}] Created resume context from path: {} (PathTree has {} indexed nodes)", 
                    name, resumeFromPath, pathTree.getIndexedNodes());
            } else {
                // For first run or non-resume, still use the PathTree to track what we index
                resumeContext = new ResumeContext(null, pathTree, chunkLimit);
                log.debug("[{}] Created first-run resume context with PathTree", name);
            }
            
            // Create IndexUpdate with ResumeContext
            indexUpdate = new IndexUpdate(provider, name, after, builder, callback, callback, info, corruptIndexHandler, resumeContext)
                    .withMissingProviderStrategy(missingStrategy);
            configureRateEstimator(indexUpdate);
                            
            // Pass PathTree to callback for skip counting
            callback.setPathTree(pathTree);
            
            // Set callback state for compatibility (deprecated - use PathTree instead)
            final long diffStartTime = System.currentTimeMillis();
            final boolean isResuming = resumeFromPath != null && !"/".equals(resumeFromPath);
            
            if (isResuming) {
                                callback.setSkipMode(true);
                callback.setResumePath(resumeFromPath);
                log.info("[{}] Resuming from path: {} with PathTree ({} indexed nodes)", 
                    name, resumeFromPath, pathTree.getIndexedNodes());
                System.out.println("[DEBUG-RESUME] Starting resume from path: " + resumeFromPath + 
                    ", PathTree has " + pathTree.getIndexedNodes() + " indexed nodes");
                                } else {
                                            callback.setSkipMode(false);
                callback.setResumePath(null);
                log.debug("[{}] Starting from root with fresh PathTree", name);
            }
            
            // Track time to reach resume path
            final long[] resumePathReachedTime = {0};
            if (isResuming) {
                resumeContext.setOnResumePointReached(() -> {
                    resumePathReachedTime[0] = System.currentTimeMillis() - diffStartTime;
                    System.out.println("[DEBUG-RESUME] Resume path reached in " + resumePathReachedTime[0] + "ms");
                    log.info("[{}] Resume path {} reached in {}ms", name, resumeFromPath, resumePathReachedTime[0]);
                });
            }
            
            // Create editor
            Editor editor = VisibleEditor.wrap(indexUpdate);
            
            // Reset skip counters before diff
            IndexUpdate.resetSkipCounters();
            PathTreeEditorDiff.resetStats();
                                        
            // Check if PathTree traversal is enabled
            boolean usePathTreeTraversal = Boolean.getBoolean("oak.async.usePathTreeTraversal");
            
            CommitFailedException exception;
            if (usePathTreeTraversal && !pathTree.isEmpty() && isResuming) {
                // Use PathTree-driven traversal (avoids SegmentStore calls for fully-processed nodes)
                log.info("[{}] Using PathTree-driven traversal (PathTree has {} nodes, {} fully processed)", 
                    name, pathTree.getTotalNodes(), pathTree.getFullyProcessedCount());
                System.out.println("[DEBUG-PATHTREE-TRAVERSAL] Using PathTree traversal mode");
                                        
                // Get traversal stats before
                PathTree.TraversalStats statsBefore = pathTree.getTraversalStats();
                System.out.println("[DEBUG-PATHTREE-TRAVERSAL] PathTree stats: " + statsBefore);
                
                exception = PathTreeEditorDiff.process(editor, pathTree, before, after);
                                    } else {
                // Use standard EditorDiff
                log.debug("[{}] Using standard EditorDiff (usePathTreeTraversal={}, pathTreeEmpty={}, isResuming={})", 
                    name, usePathTreeTraversal, pathTree.isEmpty(), isResuming);
                System.out.println("[DEBUG-PATHTREE-TRAVERSAL] Using standard EditorDiff mode");
                
                exception = EditorDiff.process(editor, before, after);
            }
            
            long totalDiffTime = System.currentTimeMillis() - diffStartTime;
            indexStats.setDiffTimeMs(watch.elapsed(TimeUnit.MILLISECONDS));
            
            // Log diff timing
            System.out.println("[DEBUG-TIMING] " + indexingMode + " Diff time: " + totalDiffTime + "ms");
                                
            // Log skip statistics
            System.out.println("[DEBUG-SKIP] " + IndexUpdate.getSkipStats());
                                
            // Log PathTree traversal statistics
            if (usePathTreeTraversal) {
                System.out.println("[DEBUG-PATHTREE-TRAVERSAL] " + PathTreeEditorDiff.getStats());
                System.out.println("[DEBUG-PATHTREE-TIMING] " + PathTreeEditorDiff.getTimingStats());
                System.out.println("[DEBUG-PATHTREE-TIMING] SegmentStore I/O time: " + PathTreeEditorDiff.getSegmentStoreReadTimeMs() + "ms (this is the expensive part!)");
            }
            
            // Log resume timing
            if (isResuming) {
                System.out.println("[DEBUG-RESUME] Total diff time: " + totalDiffTime + "ms, " +
                    "time to resume path: " + resumePathReachedTime[0] + "ms, " +
                    "indexing time after resume: " + (totalDiffTime - resumePathReachedTime[0]) + "ms");
            }
            
            lastResumeTimeToTarget = resumePathReachedTime[0];
            lastResumeTotalTime = totalDiffTime;
            
            // Handle CHUNK_COMPLETE - commit chunk and save resume state, then exit
            // Check for:
            // 1. Static CHUNK_COMPLETE constant from AsyncUpdateCallback.traversedNode()
            // 2. CHUNK_COMPLETE: prefix from IndexUpdate.leave() or ResumableEditorDiff
            boolean isChunkComplete = (exception != null && 
                (exception == CHUNK_COMPLETE ||
                 ResumableEditorDiff.isChunkCompleteException(exception) || 
                 (exception.getMessage() != null && exception.getMessage().startsWith("CHUNK_COMPLETE:"))));
            
            if (isChunkComplete) {
                // Get chunk path from various sources:
                // 1. From callback (used by static CHUNK_COMPLETE)
                // 2. From ResumableEditorDiff exception
                // 3. From ResumeContext
                                String chunkPath = callback.getChunkLastIndexedPath();
                if (chunkPath == null) {
                    chunkPath = ResumableEditorDiff.getChunkCompletePath(exception);
                }
                if (chunkPath == null && resumeContext != null) {
                    chunkPath = resumeContext.getLastIndexedPath();
                }
                log.info("[{}] Chunk limit reached at path: {} - committing and saving resume state", name, chunkPath);
                
                // CRITICAL SEQUENCE FOR INCREMENTAL SEARCHABILITY:
                                
                // 1. Flush Lucene writers - this writes buffered documents to the index
                //    and updates the builder's :data node with the new index state
                long flushStartTime = System.currentTimeMillis();
                log.info("[{}] Flushing Lucene index writers for chunk commit", name);
                                indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_PROGRESS);
                long flushTime = System.currentTimeMillis() - flushStartTime;
                System.out.println("[DEBUG-TIMING] Lucene flush time: " + flushTime + "ms");
                                
                // 2. Merge builder to NodeStore - this makes the flushed index data visible
                //    The builder now contains updated :data nodes from the flush above
                long mergeStartTime = System.currentTimeMillis();
                log.info("[{}] Merging index updates to NodeStore", name);
                                mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint, callback.lease, name);
                long mergeTime = System.currentTimeMillis() - mergeStartTime;
                System.out.println("[DEBUG-TIMING] NodeStore merge time: " + mergeTime + "ms");
                
                // 3. IMPORTANT: The index is now searchable because:
                //    - Lucene writers were flushed (documents written to :data)
                //    - Builder with updated :data was merged to NodeStore
                //    - Queries will see the new data when they refresh the index tracker
                log.info("[{}] Index committed and incrementally searchable up to: {}", name, chunkPath);
                                
                // 4. Save resume state (including PathTree) under :async/{lane-name}-resume
                long saveStateStartTime = System.currentTimeMillis();
                                NodeBuilder resumeBuilder = store.getRoot().builder();
                String saveResumeNodeName = name + "-resume";
                NodeBuilder laneBuilder = resumeBuilder.child(ASYNC).child(saveResumeNodeName);
                                laneBuilder.setProperty("lastIndexedPath", chunkPath);
                                laneBuilder.setProperty("targetCheckpoint", afterCheckpoint);
                laneBuilder.setProperty("nodesProcessed", resumeContext.getNodesProcessed());
                
                // Serialize PathTree for efficient resume
                PathTree currentPathTree = resumeContext.getPathTree();
                
                int fullyProcessedCount = currentPathTree.getFullyProcessedCount();
                int notFullyProcessedCount = currentPathTree.getNotFullyProcessedCount();
                int totalNodes = currentPathTree.getTotalNodes();
                                
                // Calculate estimated sizes for comparison
                int fullSizeEstimate = currentPathTree.getEstimatedSerializedSize(false);
                int slimSizeEstimate = currentPathTree.getEstimatedSerializedSize(true);
                
                // Serialization format options:
                // ULTRA_SLIM: Just last processed path + in-progress chain (~100 bytes) - uses DFS order comparison
                // SLIM: Frontier nodes + in-progress chain (~200-500 bytes) - uses ancestor checking
                // FULL: All nodes (~1.5MB) - uses exact path lookup
                boolean useSlimFormat = Boolean.getBoolean("oak.async.pathTreeSlimFormat");
                boolean useUltraSlimFormat = Boolean.getBoolean("oak.async.pathTreeUltraSlimFormat");
                
                long serializeStartTime = System.currentTimeMillis();
                String formatUsed;
                if (useUltraSlimFormat) {
                    currentPathTree.serializeUltraSlimTo(laneBuilder.child("pathTree"));
                    formatUsed = "ULTRA_SLIM";
                } else if (useSlimFormat) {
                    currentPathTree.serializeSlimTo(laneBuilder.child("pathTree"));
                    formatUsed = "SLIM";
                } else {
                    currentPathTree.serializeTo(laneBuilder.child("pathTree"));
                    formatUsed = "FULL";
                }
                long serializeTime = System.currentTimeMillis() - serializeStartTime;
                
                log.info("[{}] Saving PathTree ({}): {} total nodes, {} fullyProcessed, {} unprocessed (serialize time: {}ms)", 
                    name, formatUsed, totalNodes, fullyProcessedCount, notFullyProcessedCount, serializeTime);
                System.out.println("[DEBUG-PATHTREE] Serialize time: " + serializeTime + "ms (format: " + 
                    formatUsed + "), total: " + 
                    totalNodes + ", fullyProcessed: " + fullyProcessedCount + 
                    ", unprocessed: " + notFullyProcessedCount +
                    (useUltraSlimFormat ? ", lastPath: " + currentPathTree.getLastFullyProcessedPath() : ""));
                System.out.println("[DEBUG-PATHTREE-SIZE] Full: ~" + fullSizeEstimate + " bytes, " +
                    "SLIM: ~" + slimSizeEstimate + " bytes (potential savings: " + 
                    (fullSizeEstimate > 0 ? (100 - slimSizeEstimate * 100 / fullSizeEstimate) : 0) + "%)");
                    
                // Save PathTree to filesystem for analysis
                String pathTreeFileName = "pathtree_chunk_" + System.currentTimeMillis() + ".json";
                currentPathTree.saveToFile(pathTreeFileName);
                
                    store.merge(resumeBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                long saveStateTime = System.currentTimeMillis() - saveStateStartTime;
                System.out.println("[DEBUG-TIMING] Resume state save time: " + saveStateTime + "ms");
                
                log.info("[{}] Saved resume state to :async/{}: path={}, checkpoint={}, tree={}", 
                    name, saveResumeNodeName, chunkPath, afterCheckpoint, currentPathTree);
                
                // Print comprehensive timing summary
                long totalChunkTime = flushTime + mergeTime + saveStateTime;
                System.out.println("[DEBUG-TIMING] CHUNK COMMIT SUMMARY: flush=" + flushTime + "ms, merge=" + 
                    mergeTime + "ms, saveState=" + saveStateTime + "ms, TOTAL=" + totalChunkTime + "ms");
                
                // Don't release any checkpoints - we need both beforeCheckpoint and afterCheckpoint
                // They will be cleaned up when indexing completes
                checkpointToReleaseRef.set(null);  // Don't release anything yet
                indexingFailed = false;
                
                log.info("[{}] Chunk commit complete - index is incrementally searchable", name);
                // IMPORTANT: Return false to indicate "chunk complete but not full completion"
                // This prevents runWhenPermitted() from:
                // 1. Updating the main checkpoint property (we only want resume state updated)
                // 2. Setting checkpointToReleaseRef to beforeCheckpoint (which would release it!)
                // The next run() will resume from the saved position
                return false;
                }
                
                // Other exceptions should be thrown
                if (exception != null) {
                    throw exception;
                }
                
            // EditorDiff completed successfully - full index complete
            log.info("[{}] Indexing complete (mode: {})", name, indexingMode);
            System.out.println("[DEBUG-MODE] Indexing complete, mode: " + indexingMode);
            
            // For normal mode (non-chunk), log timing for flush and merge
            long normalFlushStartTime = System.currentTimeMillis();
            indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_PROGRESS);
            long normalFlushTime = System.currentTimeMillis() - normalFlushStartTime;
            System.out.println("[DEBUG-TIMING] " + indexingMode + " Lucene flush time: " + normalFlushTime + "ms");
            
            // Update checkpoint state
            builder.child(ASYNC).setProperty(name, afterCheckpoint);
            builder.child(ASYNC).setProperty(PropertyStates.createProperty(lastIndexedTo, afterTime, Type.DATE));

            if (callback.isDirty() || before == MISSING_NODE) {
                if (switchOnSync) {
                    reindexedDefinitions.addAll(indexUpdate.getReindexedDefinitions());
                    updatePostRunStatus = false;
                } else {
                    updatePostRunStatus = true;
                }
            } else {
                if (switchOnSync) {
                    log.debug("[{}] No changes detected after diff; will try to switch to synchronous updates on {}",
                            name, reindexedDefinitions);

                    // No changes after diff, switch to sync on the async defs
                    for (String path : reindexedDefinitions) {
                        NodeBuilder c = builder;
                        for (String p : elements(path)) {
                            c = c.getChildNode(p);
                        }
                        if (c.exists() && !c.getBoolean(REINDEX_PROPERTY_NAME)) {
                            c.removeProperty(ASYNC_PROPERTY_NAME);
                        }
                    }
                    reindexedDefinitions.clear();
                    if (store.release(afterCheckpoint)) {
                        builder.child(ASYNC).removeProperty(name);
                        builder.child(ASYNC).removeProperty(lastIndexedTo);
                    } else {
                        log.debug("[{}] Unable to release checkpoint {}", name, afterCheckpoint);
                    }
                }
                updatePostRunStatus = true;
            }

            // Final merge with all remaining updates
            long normalMergeStartTime = System.currentTimeMillis();
            mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint, callback.lease, name);
            long normalMergeTime = System.currentTimeMillis() - normalMergeStartTime;
            System.out.println("[DEBUG-TIMING] " + indexingMode + " NodeStore merge time: " + normalMergeTime + "ms");
            System.out.println("[DEBUG-TIMING] " + indexingMode + " COMMIT SUMMARY: flush=" + normalFlushTime + "ms, merge=" + normalMergeTime + "ms, TOTAL=" + (normalFlushTime + normalMergeTime) + "ms");
            
            // Successfully merged - mark beforeCheckpoint for release
            checkpointToReleaseRef.set(beforeCheckpoint);
            indexingFailed = false;
            
            // Clear resume state after successful completion (always check, not just when resumeFromPath is set)
                try {
                    NodeBuilder cleanupBuilder = store.getRoot().builder();
                String cleanupResumeNodeName = name + "-resume";
                NodeBuilder asyncBuilder = cleanupBuilder.child(ASYNC);
                if (asyncBuilder.hasChildNode(cleanupResumeNodeName)) {
                    asyncBuilder.getChildNode(cleanupResumeNodeName).remove();
                        store.merge(cleanupBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    log.info("[{}] Cleared resume state after successful completion", name);
                    }
                } catch (CommitFailedException ex) {
                // Not critical if cleanup fails
                    log.warn("[{}] Failed to clear resume state: {}", name, ex.getMessage());
            }

            if (indexUpdate.isReindexingPerformed()) {
                log.info("[{}] Reindexing completed for indexes: {} in {} ({} ms)",
                        name, indexUpdate.getReindexStats(), watch, watch.elapsed(TimeUnit.MILLISECONDS));
                progressLogged = true;
            }

            corruptIndexHandler.markWorkingIndexes(indexUpdate.getUpdatedIndexPaths());

        } finally {
            if (indexUpdate != null) {
                if (!indexingFailed) {
                    indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_SUCCEDED);
                } else {
                    indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_FAILED);
                }
            }
            callback.close();
        }

        if (!progressLogged && indexingFailed == false) {
            String msg = "[{}] AsyncIndex update run completed in {}. Indexed {} nodes, {}";
            if (watch.elapsed(TimeUnit.MINUTES) >= 5) {
                log.info(msg, name, watch, indexStats.getUpdates(), indexStats.getNodesReadCount());
            } else {
                log.debug(msg, name, watch, indexStats.getUpdates(), indexStats.getNodesReadCount());
            }
        }

        return updatePostRunStatus;
    }
    
    /**
     * Backward-compatible overload for tests that don't specify resumeFromPath.
     * Defaults to "/" (no resume).
     */
    protected boolean updateIndex(NodeState before, String beforeCheckpoint,
                                  NodeState after, String afterCheckpoint, String afterTime,
                                  AsyncUpdateCallback callback,
                                  AtomicReference<String> checkpointToReleaseRef) throws CommitFailedException {
        return updateIndex(before, beforeCheckpoint, after, afterCheckpoint, afterTime, 
                          callback, checkpointToReleaseRef, "/");
    }

    private void configureRateEstimator(IndexUpdate indexUpdate) {
        //As metrics is an optional library guard the access with the check
        if (statisticsProvider.getClass().getSimpleName().equals("MetricStatisticsProvider")){
            MetricRegistry registry = ((MetricStatisticsProvider) statisticsProvider).getRegistry();
            indexUpdate.setTraversalRateEstimator(new MetricRateEstimator(name, registry));
        }

        NodeCounterMBeanEstimator estimator = new NodeCounterMBeanEstimator(store);
        indexUpdate.setNodeCountEstimator(estimator);
    }

    public static String leasify(String name) {
        return name + "-lease";
    }

    static String lastIndexedTo(String name) {
        return name + "-LastIndexedTo";
    }

    private static String getTempCpName(String name) {
        return name + "-temp";
    }

    private static boolean isLeaseCheckEnabled(long leaseTimeOut) {
        return leaseTimeOut > 0;
    }

    private static void mergeWithConcurrencyCheck(final NodeStore store, List<ValidatorProvider> validatorProviders,
                                                  NodeBuilder builder, final String checkpoint, final Long lease,
                                                  final String name) throws CommitFailedException {
        CommitHook concurrentUpdateCheck = new CommitHook() {
            @Override @NotNull
            public NodeState processCommit(
                    NodeState before, NodeState after, CommitInfo info)
                    throws CommitFailedException {
                // check for concurrent updates by this async task
                NodeState async = before.getChildNode(ASYNC);
                if ((checkpoint == null || Objects.equals(checkpoint, async.getString(name)))
                        &&
                        (lease == null      || lease == async.getLong(leasify(name)))) {
                    return after;
                } else {
                    throw newConcurrentUpdateException();
                }
            }
        };
        List<EditorProvider> editorProviders = new ArrayList<>();
        editorProviders.add(new ConflictValidatorProvider());
        editorProviders.addAll(validatorProviders);
        CompositeHook hooks = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                ConflictHook.of(new AnnotatingConflictHandler()),
                new EditorHook(CompositeEditorProvider.compose(editorProviders)),
                concurrentUpdateCheck);
        try {
            store.merge(builder, hooks, createCommitInfo());
        } catch (CommitFailedException ex) {
            // OAK-2961
            if (ex.isOfType(CommitFailedException.STATE) && ex.getCode() == 1) {
                throw newConcurrentUpdateException();
            } else {
                throw ex;
            }
        }
    }

    private static CommitInfo createCommitInfo() {
        Map<String, Object> info = Map.of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }

    /**
     * Milliseconds for the timeout
     */
    protected AsyncIndexUpdate setLeaseTimeOut(long leaseTimeOut) {
        this.leaseTimeOut = leaseTimeOut;
        return this;
    }

    protected long getLeaseTimeOut() {
        return leaseTimeOut;
    }

    protected AsyncIndexUpdate setCloseTimeOut(int timeOutInSec) {
        this.softTimeOutSecs = timeOutInSec;
        return this;
    }

    public void setValidatorProviders(List<ValidatorProvider> validatorProviders) {
        this.validatorProviders = requireNonNull(validatorProviders);
    }

    public void setCorruptIndexHandler(TrackingCorruptIndexHandler corruptIndexHandler) {
        this.corruptIndexHandler = requireNonNull(corruptIndexHandler);
    }
    
    TrackingCorruptIndexHandler getCorruptIndexHandler() {
        return corruptIndexHandler;
    }

    public boolean isClosed(){
        return closed || forcedStopFlag.get();
    }

    boolean isClosing(){
        return runPermit.hasQueuedThreads();
    }

    private static void preAsyncRunStatsStats(AsyncIndexStats stats) {
        stats.start(now());
    }

    private static void postAsyncRunStatsStatus(AsyncIndexStats stats) {
        stats.done(now());
    }

    private static String now() {
        return ISO8601.format(Calendar.getInstance());
    }

    public AsyncIndexStats getIndexStats() {
        return indexStats;
    }

    public long getLastDiffTimeMs() {
        return indexStats.getDiffTimeMs();
    }
    
    /**
     * Gets the time taken by ResumingEditor to reach the resume point (target path).
     * This measures only the traversal time up to the resume point, not the total time.
     * 
     * @return time in milliseconds to reach resume point, 0 if not resuming or not yet reached
     */
    public long getLastResumeTimeToTargetMs() {
        return lastResumeTimeToTarget;
    }
    
    /**
     * Gets the total time spent in ResumingEditor including overhead after reaching target.
     * 
     * @return total time in milliseconds spent in ResumingEditor, 0 if not resuming
     */
    public long getLastResumeTotalTimeMs() {
        return lastResumeTotalTime;
    }

    public boolean isFinished() {
        return indexStats.getStatus() == STATUS_DONE;
    }

    final class AsyncIndexStats extends AnnotatedStandardMBean implements  IndexStatsMBean {

        protected AsyncIndexStats(String name, StatisticsProvider statsProvider) {
            super(IndexStatsMBean.class);
            this.execStats = new ExecutionStats(name, statsProvider);
        }

        private String start = "";
        private String done = "";
        private String status = STATUS_INIT;
        private String referenceCp = "";
        private String processedCp = "";
        private Set<String> tempCps = new HashSet<String>();

        private volatile boolean isPaused;
        private volatile boolean forcedLeaseRelease;
        private volatile long updates;
        private volatile long nodesRead;
        private volatile long diffTimeMs;
        private final Stopwatch watch = Stopwatch.createUnstarted();
        private final ExecutionStats execStats;

        /** Flag to avoid repeatedly logging failure warnings */
        private volatile boolean failing = false;
        private long latestErrorWarn = 0;

        private String failingSince = "";
        private String latestError = null;
        private String latestErrorTime = "";
        private long consecutiveFailures = 0;

        public void start(String now) {
            status = STATUS_RUNNING;
            start = now;
            done = "";

            if (watch.isRunning()) {
                watch.reset();
            }
            watch.start();
        }

        public void done(String now) {
            if (corruptIndexHandler.isFailing(name)){
                status = STATUS_FAILING;
            } else {
                status = STATUS_DONE;
            }
            done = now;
            if (watch.isRunning()) {
                watch.stop();
            }
            execStats.doneOneCycle(watch.elapsed(TimeUnit.MILLISECONDS), updates);
            watch.reset();
        }

        public void failed(Exception e) {
            boolean isConcurrentUpdateException = (e.getMessage() != null)
                    && (e.getMessage().startsWith(CONCURRENT_EXCEPTION_MSG));
            if (e == INTERRUPTED){
                status = STATUS_INTERRUPTED;
                log.info("[{}] The index update interrupted", name);
                log.debug("[{}] The index update interrupted", name, e);
                return;
            }

            latestError = ExceptionUtils.getStackTrace(e);
            latestErrorTime = now();
            consecutiveFailures++;
            if (!failing) {
                // first occurrence of a failure
                failing = true;
                // reusing value so value display is consistent
                failingSince = latestErrorTime;
                latestErrorWarn = System.currentTimeMillis();
                if (isConcurrentUpdateException) {
                    log.info("[{}] The index update failed : {}", name,  e.getMessage());
                } else {
                    log.warn("[{}] The index update failed", name, e);
                }
            } else {
                // subsequent occurrences
                boolean warn = System.currentTimeMillis() - latestErrorWarn > ERROR_WARN_INTERVAL;
                if (warn) {
                    latestErrorWarn = System.currentTimeMillis();
                    if (isConcurrentUpdateException) {
                        log.info("[{}] The index update is still failing : {}", name,  e.getMessage());
                    } else {
                        log.warn("[{}] The index update is still failing", name, e);
                    }
                } else {
                    log.debug("[{}] The index update is still failing", name, e);
                }
            }
        }

        public void fixed() {
            if (corruptIndexHandler.isFailing(name)){
                log.info("[{}] Index update no longer fails but some corrupt indexes have been skipped {}", name,
                        corruptIndexHandler.getCorruptIndexData(name).keySet());
            } else {
                log.info("[{}] Index update no longer fails", name);
            }

            failing = false;
            failingSince = "";
            consecutiveFailures = 0;
            latestErrorWarn = 0;
            latestError = null;
            latestErrorTime = "";
        }

        public boolean isFailing() {
            return failing || corruptIndexHandler.isFailing(name);
        }

        public boolean didLastIndexingCycleFailed(){
            return failing;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getStart() {
            return start;
        }

        @Override
        public String getDone() {
            return done;
        }

        @Override
        public String getStatus() {
            return status;
        }

        @Override
        public String getLastIndexedTime() {
            PropertyState ps = store.getRoot().getChildNode(ASYNC).getProperty(lastIndexedTo);
            return ps != null ? ps.getValue(Type.STRING) : null;
        }

        @Override
        public void pause() {
            log.debug("[{}] Pausing the async indexer", name);
            this.isPaused = true;
        }

        @Override
        public String abortAndPause() {
            //First pause to avoid any race
            pause();
            //Set the forcedStop flag anyway. In resume this would be cleared
            forcedStopFlag.set(true);
            String msg = "";
            //Abort if any indexing run is in progress
            if (runPermit.availablePermits() == 0){
                msg = "Abort request placed for current run. ";
            }
            return msg + "Indexing is paused now. Invoke 'resume' to resume indexing";
        }

        @Override
        public String releaseLeaseForPausedLane() {
            if (this.isPaused()){
                this.forcedLeaseRelease = true;
                return "LeaseRelease flag set";
            }
            return "Please pause the lane to release lease";
        }

        @Override
        public void resume() {
            log.debug("[{}] Resuming the async indexer", name);
            this.isPaused = false;

            //Clear the forcedStop flag as fail safe
            forcedStopFlag.set(false);
            this.forcedLeaseRelease = false;
        }

        @Override
        public boolean isPaused() {
            return this.isPaused;
        }

        void reset() {
            this.updates = 0;
            this.nodesRead = 0;
        }

        long incUpdates() {
            return ++updates;
        }

        long incTraversal() {
            return ++nodesRead;
        }

        @Override
        public long getUpdates() {
            return updates;
        }

        @Override
        public long getNodesReadCount(){
            return nodesRead;
        }

        void setDiffTimeMs(long diffTimeMs) {
            this.diffTimeMs = diffTimeMs;
        }

        public long getDiffTimeMs() {
            return diffTimeMs;
        }

        void setReferenceCheckpoint(String checkpoint) {
            this.referenceCp = checkpoint;
        }

        @Override
        public String getReferenceCheckpoint() {
            return referenceCp;
        }

        @Override
        public String forceIndexLaneCatchup(String confirmMessage) throws CommitFailedException {

            if (!"CONFIRM".equals(confirmMessage)) {
                String msg = "Please confirm that you want to force the lane catch-up by passing 'CONFIRM' as argument";
                log.warn(msg);
                return msg;
            }

            try {
                log.info("Running a forced catch-up for indexing lane [{}]. ", name);
                // First we need to abort and pause the running indexing task
                this.abortAndPause();
                log.info("Aborted and paused async indexing for lane [{}]", name);
                // Release lease for the paused lane
                this.releaseLeaseForPausedLane();
                log.info("Released lease for paused lane [{}]", name);
                String newReferenceCheckpoint = store.checkpoint(lifetime, Map.of(
                        "creator", AsyncIndexUpdate.class.getSimpleName(),
                        "created", now(),
                        "thread", Thread.currentThread().getName(),
                        "name", name + "-forceModified"));
                String existingReferenceCheckpoint = this.referenceCp;
                log.info("Modifying the referred checkpoint for lane [{}] from {} to {}." +
                        " This means that any content modifications between these checkpoints will not reflect in the indexes on this lane." +
                        " Reindexing is needed to get this content indexed.", name, existingReferenceCheckpoint, newReferenceCheckpoint);
                NodeBuilder builder = store.getRoot().builder();
                builder.child(ASYNC).setProperty(name, newReferenceCheckpoint);
                this.referenceCp = newReferenceCheckpoint;
                mergeWithConcurrencyCheck(store, validatorProviders, builder, existingReferenceCheckpoint, null, name);
                // Remove the existing reference checkpoint
                if (store.release(existingReferenceCheckpoint)) {
                    log.info("Old reference checkpoint {} removed or didn't exist", existingReferenceCheckpoint);
                } else {
                    log.warn("Unable to remove old reference checkpoint {}. This can result in orphaned checkpoints and would need to be removed manually.", existingReferenceCheckpoint);
                }
                // Resume the paused lane;
                this.resume();
                log.info("Resumed async indexing for lane [{}]", name);
                return "Lane successfully forced to catch-up. New reference checkpoint is " + newReferenceCheckpoint + " . Please make sure to perform reindexing to get the diff content indexed.";
            } catch (Exception e) {
                log.error("Exception while trying to force update the indexing lane [{}]", name, e);
                if (this.isPaused()) {
                    this.resume();
                    log.info("Resuming the lane [{}] as it was paused during the operation", name);
                }
                return "Unable to complete the force update due to " + e.getMessage() + ".Please check logs for more details";
            }
        }

        void setProcessedCheckpoint(String checkpoint) {
            this.processedCp = checkpoint;
        }

        @Override
        public String getProcessedCheckpoint() {
            return processedCp;
        }

        void setTempCheckpoints(Set<String> tempCheckpoints) {
            this.tempCps = tempCheckpoints;
        }

        void releaseTempCheckpoint(String tempCheckpoint) {
            this.tempCps.remove(tempCheckpoint);
        }

        @Override
        public String getTemporaryCheckpoints() {
            return tempCps.toString();
        }

        @Override
        public long getTotalExecutionCount() {
            return execStats.getExecutionCounter().getCount();
        }

        @Override
        public CompositeData getExecutionCount() {
            return execStats.getExecutionCount();
        }

        @Override
        public CompositeData getExecutionTime() {
            //Do nothing. Kept for backward compatibility
            return null;
        }

        @Override
        public CompositeData getIndexedNodesCount() {
            return execStats.getIndexedNodesCount();
        }

        @Override
        public CompositeData getConsolidatedExecutionStats() {
            return execStats.getConsolidatedStats();
        }

        @Override
        public void resetConsolidatedExecutionStats() {
            //Do nothing. Kept for backward compatibility
        }

        @Override
        public String toString() {
            return "AsyncIndexStats [start=" + start + ", done=" + done
                    + ", status=" + status + ", paused=" + isPaused
                    + ", failing=" + failing + ", failingSince=" + failingSince
                    + ", consecutiveFailures=" + consecutiveFailures
                    + ", updates=" + updates + ", referenceCheckpoint="
                    + referenceCp + ", processedCheckpoint=" + processedCp
                    + " ,tempCheckpoints=" + tempCps + ", latestErrorTime="
                    + latestErrorTime + ", latestError=" + latestError + " ]";
        }

        ExecutionStats getExecutionStats() {
            return execStats;
        }

        class ExecutionStats {
            public static final String INDEXER_COUNT = "INDEXER_COUNT";
            public static final String INDEXER_NODE_COUNT = "INDEXER_NODE_COUNT";
            private final MeterStats indexerExecutionCountMeter;
            private final MeterStats indexedNodeCountMeter;
            private final TimerStats indexerTimer;
            private final HistogramStats indexedNodePerCycleHisto;
            private final CounterStats lastIndexedTime;
            private StatisticsProvider statisticsProvider;

            private final String[] names = {"Executions", "Nodes"};
            private final String name;
            private CompositeType consolidatedType;

            public ExecutionStats(String name, StatisticsProvider statsProvider) {
                this.name = name;
                this.statisticsProvider = statsProvider;
                indexerExecutionCountMeter = statsProvider.getMeter(stats(INDEXER_COUNT), StatsOptions.DEFAULT);
                indexedNodeCountMeter = statsProvider.getMeter(stats(INDEXER_NODE_COUNT), StatsOptions.DEFAULT);
                indexerTimer = statsProvider.getTimer(stats("INDEXER_TIME"), StatsOptions.METRICS_ONLY);
                indexedNodePerCycleHisto = statsProvider.getHistogram(stats("INDEXER_NODE_COUNT_HISTO"), StatsOptions
                        .METRICS_ONLY);
                lastIndexedTime = statsProvider.getCounterStats(stats("LAST_INDEXED_TIME"), StatsOptions.DEFAULT);
                try {
                    consolidatedType = new CompositeType("ConsolidatedStats",
                            "Consolidated stats", names,
                            names,
                            new OpenType[] {SimpleType.LONG, SimpleType.LONG});
                } catch (OpenDataException e) {
                    log.warn("[{}] Error in creating CompositeType for consolidated stats", AsyncIndexUpdate.this.name, e);
                }
            }

            public void doneOneCycle(long timeInMillis, long updates){
                indexerExecutionCountMeter.mark();
                indexedNodeCountMeter.mark(updates);
                indexerTimer.update(timeInMillis, TimeUnit.MILLISECONDS);
                indexedNodePerCycleHisto.update(updates);
                long previousLastIndexedTime = lastIndexedTime.getCount();
                lastIndexedTime.inc(System.currentTimeMillis() - previousLastIndexedTime);
            }

            public Counting getExecutionCounter() {
                return indexerExecutionCountMeter;
            }

            public Counting getIndexedNodeCount() {
                return indexedNodeCountMeter;
            }

            private CompositeData getExecutionCount() {
                return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(stats(INDEXER_COUNT)),
                        "Indexer Execution Count");
            }

            private CompositeData getIndexedNodesCount() {
                return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(stats(INDEXER_NODE_COUNT)),
                        "Indexer Node Count");
            }

            private CompositeData getConsolidatedStats() {
                try {
                    Long[] values = new Long[]{indexerExecutionCountMeter.getCount(),
                            indexedNodeCountMeter.getCount()};
                    return new CompositeDataSupport(consolidatedType, names, values);
                } catch (Exception e) {
                    log.error("[{}] Error retrieving consolidated stats", name, e);
                    return null;
                }
            }

            private String stats(String suffix){
                return name + "." + suffix;
            }

            private TimeSeries getTimeSeries(String name) {
                return statisticsProvider.getStats().getTimeSeries(name, true);
            }
        }

        @Override
        public void splitIndexingTask(String paths, String newIndexTaskName) {
            splitIndexingTask(Arrays.stream(paths.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet()), newIndexTaskName);
        }

        private void splitIndexingTask(Set<String> paths,
                                       String newIndexTaskName) {
            taskSplitter.registerSplit(paths, newIndexTaskName);
        }

        @Override
        public void registerAsyncIndexer(String name, long delayInSeconds) {
            taskSplitter.registerAsyncIndexer(name, delayInSeconds);
        }

        @Override
        public String getFailingSince() {
            return failingSince;
        }

        @Override
        public long getConsecutiveFailedExecutions() {
            return consecutiveFailures;
        }

        @Override
        public String getLatestError() {
            return latestError;
        }

        @Override
        public String getLatestErrorTime() {
            return latestErrorTime;
        }

        @Override
        public TabularData getFailingIndexStats() {
            return corruptIndexHandler.getFailingIndexStats(name);
        }
    }

    /**
     * Checks whether there are no visible changes between the given states.
     */
    private static boolean noVisibleChanges(NodeState before, NodeState after) {
        return after.compareAgainstBaseState(before, new NodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                return isHidden(after.getName());
            }
            @Override
            public boolean propertyChanged(
                    PropertyState before, PropertyState after) {
                return isHidden(after.getName());
            }
            @Override
            public boolean propertyDeleted(PropertyState before) {
                return isHidden(before.getName());
            }
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return isHidden(name);
            }
            @Override
            public boolean childNodeChanged(
                    String name, NodeState before, NodeState after) {
                return isHidden(name)
                        || after.compareAgainstBaseState(before, this);
            }
            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return isHidden(name);
            }
        });
    }

    private static boolean isHidden(String name) {
        return name.charAt(0) == ':';
    }

    static class DefaultMissingIndexProviderStrategy extends
            MissingIndexProviderStrategy {

        @Override
        public void onMissingIndex(String type, NodeBuilder definition, String path)
                throws CommitFailedException {
            if (isDisabled(type)) {
                return;
            }
            throw new CommitFailedException("Async", 2,
                    "Missing index provider detected for type [" + type
                            + "] on index [" + path + "]");
        }
    }

    public boolean isFailing() {
        return indexStats.isFailing();
    }


    class IndexTaskSpliter {

        private Set<String> paths = null;
        private String newIndexTaskName = null;
        private String lastReferencedCp;

        private Set<String> registeredTasks = new HashSet<>();

        void registerSplit(Set<String> paths, String newIndexTaskName) {
            log.info(
                    "[{}] Registered split of following index definitions {} to new async task {}.",
                    name, paths, newIndexTaskName);
            this.paths = new HashSet<>(paths);
            this.newIndexTaskName = newIndexTaskName;
        }

        void maybeSplit(@Nullable String refCheckpoint, Long lease)
                throws CommitFailedException {
            if (paths == null) {
                return;
            }
            split(refCheckpoint, lease);
        }

        private void split(@Nullable String refCheckpoint, Long lease) throws CommitFailedException {
            NodeBuilder builder = store.getRoot().builder();
            if (refCheckpoint != null) {
                String tempCpName = getTempCpName(name);
                NodeBuilder async = builder.child(ASYNC);
                // add new reference
                async.setProperty(newIndexTaskName, refCheckpoint);
                // update old 'temp' list: remove refcp so it doesn't get released on next run
                Set<String> temps = new HashSet<>();
                for (String cp : getStrings(async, tempCpName)) {
                    if (cp.equals(refCheckpoint)) {
                        continue;
                    }
                    temps.add(cp);
                }
                async.setProperty(tempCpName, temps, Type.STRINGS);
                indexStats.setTempCheckpoints(temps);
            }

            // update index defs name => newIndexTaskName
            Set<String> updated = new HashSet<>();
            for (String path : paths) {
                NodeBuilder c = builder;
                for (String p : elements(path)) {
                    c = c.getChildNode(p);
                }
                if (c.exists() && name.equals(c.getString("async"))) {
                    //TODO Fix this to account for nrt and sync
                    c.setProperty("async", newIndexTaskName);
                    updated.add(path);
                }
            }

            if (!updated.isEmpty()) {
                mergeWithConcurrencyCheck(store, validatorProviders, builder, refCheckpoint, lease, name);
                log.info(
                        "[{}] Successfully split index definitions {} to async task named {} with referenced checkpoint {}.",
                        name, updated, newIndexTaskName, refCheckpoint);
                lastReferencedCp = refCheckpoint;
            }
            paths = null;
            newIndexTaskName = null;
        }

        public String getLastReferencedCp() {
            return lastReferencedCp;
        }

        void registerAsyncIndexer(String newTask, long delayInSeconds) {
            if (registeredTasks.contains(newTask)) {
                // prevent accidental double call
                log.warn("[{}] Task {} is already registered.", name, newTask);
                return;
            }
            if (mbeanRegistration != null) {
                log.info(
                        "[{}] Registering a new indexing task {} running each {} seconds.",
                        name, newTask, delayInSeconds);
                AsyncIndexUpdate task = new AsyncIndexUpdate(newTask, store,
                        provider);
                mbeanRegistration.registerAsyncIndexer(task, delayInSeconds);
                registeredTasks.add(newTask);
            }
        }
    }

    private static Iterable<String> getStrings(NodeBuilder b, String p) {
        PropertyState ps = b.getProperty(p);
        if (ps != null) {
            return ps.getValue(Type.STRINGS);
        }
        return new HashSet<>();
    }

    IndexTaskSpliter getTaskSplitter() {
        return taskSplitter;
    }

    public void setIndexMBeanRegistration(IndexMBeanRegistration mbeanRegistration) {
        this.mbeanRegistration = mbeanRegistration;
    }

    public String getName() {
        return name;
    }

    private static CommitFailedException newConcurrentUpdateException() {
        return new CommitFailedException("Async", 1, "Concurrent update detected");
    }

}
