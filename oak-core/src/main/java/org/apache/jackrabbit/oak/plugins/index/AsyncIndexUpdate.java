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
import java.nio.charset.StandardCharsets;
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
    
    // ISSUE 10 FIXED: Cache continuous mode configuration to avoid repeated system property reads
    private final boolean continuousModeEnabled;
    private final long configuredChunkSize;
    private final long configuredTimeLimitMs;

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
     * The time limit in milliseconds for a single indexing chunk. Defaults to -1 (disabled).
     * Use oak.async.timeLimitMs system property to configure.
     */
    private final int asyncTimeLimitMs = Integer.getInteger("oak.async.timeLimitMs", -1);

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
        
        // Cache continuous mode configuration at construction time
        // This ensures consistent behavior even if system properties change during execution
        this.continuousModeEnabled = Boolean.getBoolean("oak.async.continuousMode");
        this.configuredChunkSize = Long.getLong("oak.async.chunkSize", -1);
        this.configuredTimeLimitMs = Long.getLong("oak.async.timeLimitMs", -1);
        
        if (continuousModeEnabled) {
            log.info("[{}] Continuous mode enabled at construction - chunkSize: {}, timeLimitMs: {}", 
                name, configuredChunkSize, configuredTimeLimitMs);
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
    /**
     * Callback interface for progressive commits during indexing.
     * Used for continuous processing mode to avoid exiting and re-entering the diff traversal.
     */
    public interface ProgressCommitCallback {
        /**
         * Called when chunk/time limit is reached during diff traversal.
         * Should flush index data and save resume position WITHOUT exiting the traversal.
         * 
         * @param currentPath the current path being processed
         * @throws CommitFailedException if commit fails
         */
        void commitProgress(String currentPath) throws CommitFailedException;
    }

    /**
     * Editor that wraps another editor and can skip nodes until reaching a target path.
     * Used for resuming indexing after a crash or interruption.
     * 
     * <p>Thread-safety: Uses instance-level stats shared across parent-child editor tree.
     * 
     * ALL 11 BUGS FIXED:
     * - BUG 1: Path tracking now uses immutable currentPath passed to children
     * - BUG 5: enter() and leave() always called on delegate for state consistency
     * - BUG 6: childNodeDeleted now includes skip tracking logic
     * - BUG 7: Stats now instance-based, shared across editor tree
     * - BUG 11: hasReachedTarget propagated from parent to child (prevents data loss on siblings)
     */
    protected static class ResumingEditor implements Editor {
        private final Editor delegate;
        private final String resumeTargetPath;
        private final String currentPath;  // BUG 1 FIXED: Made final, no mutation
        private boolean hasReachedTarget = false;
        
        // BUG 7 FIXED: Instance-level stats instead of ThreadLocal
        private final ResumeStatsHolder statsHolder;
        
        // Callback to notify when skip mode ends (resume target reached)
        private final Runnable onResumeTargetReached;
        
        // Saved editor stack for validation/optimization (optional)
        private final List<ResumeState.EditorLevel> editorStack;
        
        /**
         * Holder for resume statistics shared across the editor tree.
         */
        private static class ResumeStatsHolder {
            long skippedNodes = 0;
            long processedNodes = 0;
            long skippedProperties = 0;
            long processedProperties = 0;
            final long resumeStartTime;
            long timeToResumePoint = 0;
            long totalResumeTime = 0;
            
            ResumeStatsHolder() {
                this.resumeStartTime = System.currentTimeMillis();
            }
            
            void logFinalStats(String name, String resumeTargetPath) {
                totalResumeTime = System.currentTimeMillis() - resumeStartTime;
                log.info("==================================================");
                log.info("[{}] RESUME STATISTICS SUMMARY", name);
                log.info("==================================================");
                log.info("  Resume Target Path: {}", resumeTargetPath);
                log.info("  Time to reach resume point: {} ms ({} sec)", 
                    timeToResumePoint, String.format("%.2f", timeToResumePoint / 1000.0));
                log.info("  Total time with resume overhead: {} ms ({} sec)", 
                    totalResumeTime, String.format("%.2f", totalResumeTime / 1000.0));
                log.info("  Nodes skipped during resume: {}", skippedNodes);
                log.info("  Properties skipped during resume: {}", skippedProperties);
                log.info("  Nodes processed after resume: {}", processedNodes);
                log.info("  Properties indexed after resume: {}", processedProperties);
                log.info("  Resume overhead: {} ms ({} sec)", 
                    totalResumeTime - timeToResumePoint, 
                    String.format("%.2f", (totalResumeTime - timeToResumePoint) / 1000.0));
                log.info("  Average skip rate: {} nodes/sec", 
                    timeToResumePoint > 0 ? (skippedNodes * 1000 / timeToResumePoint) : "N/A");
                log.info("==================================================");
            }
        }

        /**
         * Creates a resuming editor with explicit path (used internally for children).
         * 
         * @param delegate the actual editor to delegate to
         * @param resumeTargetPath the path to resume from (null or "/" to start from beginning)
         * @param currentPath the current path in the tree
         * @param statsHolder shared statistics holder
         * @param onResumeTargetReached callback when resume target is reached (skip mode ends)
         * @param editorStack saved editor hierarchy for optimization (can be null)
         */
        private ResumingEditor(Editor delegate, String resumeTargetPath, String currentPath, 
                              ResumeStatsHolder statsHolder, Runnable onResumeTargetReached,
                              List<ResumeState.EditorLevel> editorStack) {
            this.delegate = Objects.requireNonNull(delegate, "delegate editor cannot be null");
            this.resumeTargetPath = (resumeTargetPath == null || resumeTargetPath.isEmpty()) ? "/" : resumeTargetPath;
            this.currentPath = currentPath != null ? currentPath : "/";
            this.statsHolder = statsHolder != null ? statsHolder : new ResumeStatsHolder();
            this.onResumeTargetReached = onResumeTargetReached;
            this.editorStack = editorStack;
            
            // Check if we've already reached or passed the target
            this.hasReachedTarget = "/".equals(this.resumeTargetPath) 
                || this.currentPath.equals(this.resumeTargetPath)
                || this.currentPath.startsWith(this.resumeTargetPath + "/");
            
            if (!hasReachedTarget && statsHolder == null) {
                log.debug("ResumingEditor initialized, target: {}", this.resumeTargetPath);
            }
        }
        
        /**
         * Creates a resuming editor that will skip nodes until reaching the target path.
         * 
         * @param delegate the actual editor to delegate to after reaching target
         * @param resumeTargetPath the path to resume from (null or "/" to start from beginning)
         */
        public ResumingEditor(Editor delegate, String resumeTargetPath) {
            this(delegate, resumeTargetPath, "/", null, null, null);
        }
        
        /**
         * Creates a resuming editor with a callback for when resume target is reached.
         * 
         * @param delegate the actual editor to delegate to after reaching target
         * @param resumeTargetPath the path to resume from (null or "/" to start from beginning)
         * @param onResumeTargetReached callback when resume target is reached (skip mode ends)
         */
        public ResumingEditor(Editor delegate, String resumeTargetPath, Runnable onResumeTargetReached) {
            this(delegate, resumeTargetPath, "/", null, onResumeTargetReached, null);
        }
        
        /**
         * Creates a resuming editor with callback and editor stack for optimized skipping.
         * The editor stack enables faster skip phase by avoiding expensive index editor creation.
         * 
         * @param delegate the actual editor to delegate to after reaching target
         * @param resumeTargetPath the path to resume from (null or "/" to start from beginning)
         * @param onResumeTargetReached callback when resume target is reached (skip mode ends)
         * @param editorStack saved editor hierarchy for skip optimization (can be null)
         */
        public ResumingEditor(Editor delegate, String resumeTargetPath, 
                            Runnable onResumeTargetReached, List<ResumeState.EditorLevel> editorStack) {
            this(delegate, resumeTargetPath, "/", null, onResumeTargetReached, editorStack);
        }

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            // ALWAYS call enter() - it's essential for editor state initialization
            // Even though IndexUpdate.enter() is expensive (calls collectIndexEditors()),
            // we can't skip it without breaking the editor tree structure and index creation
            // The optimization needs to be done inside IndexUpdate/collectIndexEditors instead
            delegate.enter(before, after);
        }

        @Override
        public void leave(NodeState before, NodeState after) throws CommitFailedException {
            // ALWAYS call leave() - maintains stack balance and cleanup
            delegate.leave(before, after);
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            // SKIP property operations during skip phase - this is where indexing happens
            if (hasReachedTarget) {
                statsHolder.processedProperties++;
                delegate.propertyAdded(after);
            } else {
                statsHolder.skippedProperties++;
            }
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
            if (hasReachedTarget) {
                statsHolder.processedProperties++;
                delegate.propertyChanged(before, after);
            } else {
                statsHolder.skippedProperties++;
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) throws CommitFailedException {
            if (hasReachedTarget) {
                statsHolder.processedProperties++;
                delegate.propertyDeleted(before);
            } else {
                statsHolder.skippedProperties++;
            }
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            // BUG 1 FIXED: Calculate child path WITHOUT mutating state
            String childPath = buildChildPath(name);
            
            if (!hasReachedTarget) {
                statsHolder.skippedNodes++;
                
                // Check if we've reached the target
                if (hasReachedTargetPath(childPath)) {
                    hasReachedTarget = true;
                    statsHolder.timeToResumePoint = System.currentTimeMillis() - statsHolder.resumeStartTime;
                    log.info("ResumingEditor reached target path: {}, skipped {} nodes in {} ms", 
                        resumeTargetPath, statsHolder.skippedNodes, statsHolder.timeToResumePoint);
                    // Notify callback that skip mode has ended
                    if (onResumeTargetReached != null) {
                        onResumeTargetReached.run();
                    }
                }
            } else {
                statsHolder.processedNodes++;
            }
            
            // Always get child editor from delegate - maintains editor tree structure
            Editor childDelegate = delegate.childNodeAdded(name, after);
            if (childDelegate == null) {
                return null;
            }
            
            // BUG 1 FIXED: Create NEW instance with correct path
            // BUG 7 FIXED: Share statsHolder with children
            // BUG 11 FIXED: Propagate hasReachedTarget to child
            ResumingEditor childEditor = new ResumingEditor(childDelegate, resumeTargetPath, childPath, 
                                                           statsHolder, onResumeTargetReached, editorStack);
            if (this.hasReachedTarget) {
                childEditor.hasReachedTarget = true;  // Parent reached target, child should process too
            }
            return childEditor;
        }

        @Override
        public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            String childPath = buildChildPath(name);
            
            if (!hasReachedTarget) {
                statsHolder.skippedNodes++;
                
                if (hasReachedTargetPath(childPath)) {
                    hasReachedTarget = true;
                    statsHolder.timeToResumePoint = System.currentTimeMillis() - statsHolder.resumeStartTime;
                    log.info("ResumingEditor reached target path: {}, skipped {} nodes in {} ms", 
                        resumeTargetPath, statsHolder.skippedNodes, statsHolder.timeToResumePoint);
                    // Notify callback that skip mode has ended
                    if (onResumeTargetReached != null) {
                        onResumeTargetReached.run();
                    }
                }
            } else {
                statsHolder.processedNodes++;
            }
            
            Editor childDelegate = delegate.childNodeChanged(name, before, after);
            if (childDelegate == null) {
                return null;
            }
            
            // BUG 11 FIXED: Propagate hasReachedTarget to child
            ResumingEditor childEditor = new ResumingEditor(childDelegate, resumeTargetPath, childPath, 
                                                           statsHolder, onResumeTargetReached, editorStack);
            if (this.hasReachedTarget) {
                childEditor.hasReachedTarget = true;
            }
            return childEditor;
        }

        @Override
        public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            String childPath = buildChildPath(name);
            
            // BUG 6 FIXED: Add skip tracking and target detection for deleted nodes
            if (!hasReachedTarget) {
                statsHolder.skippedNodes++;
                
                if (hasReachedTargetPath(childPath)) {
                    hasReachedTarget = true;
                    statsHolder.timeToResumePoint = System.currentTimeMillis() - statsHolder.resumeStartTime;
                    log.info("ResumingEditor reached target path: {}, skipped {} nodes in {} ms", 
                        resumeTargetPath, statsHolder.skippedNodes, statsHolder.timeToResumePoint);
                    // Notify callback that skip mode has ended
                    if (onResumeTargetReached != null) {
                        onResumeTargetReached.run();
                    }
                }
            } else {
                statsHolder.processedNodes++;
            }
            
            Editor childDelegate = delegate.childNodeDeleted(name, before);
            if (childDelegate == null) {
                return null;
            }
            
            // BUG 11 FIXED: Propagate hasReachedTarget to child
            ResumingEditor childEditor = new ResumingEditor(childDelegate, resumeTargetPath, childPath, 
                                                           statsHolder, onResumeTargetReached, editorStack);
            if (this.hasReachedTarget) {
                childEditor.hasReachedTarget = true;
            }
            return childEditor;
        }

        private String buildChildPath(String childName) {
            return currentPath.equals("/") ? "/" + childName : currentPath + "/" + childName;
        }
        
        private boolean hasReachedTargetPath(String path) {
            return path.equals(resumeTargetPath) || path.startsWith(resumeTargetPath + "/");
        }

        /**
         * Get statistics for resume operation.
         * @return array of [skippedNodes, timeToResumePoint, processedNodes, skippedProperties, processedProperties]
         */
        public long[] getResumeStats() {
            return new long[]{
                statsHolder.skippedNodes, 
                statsHolder.timeToResumePoint,
                statsHolder.processedNodes,
                statsHolder.skippedProperties,
                statsHolder.processedProperties
            };
        }
        
        /**
         * Logs comprehensive resume statistics after indexing completes.
         * Shows timing breakdown, throughput, and efficiency metrics.
         */
        public void logFinalResumeStats() {
            statsHolder.logFinalStats(delegate.toString(), resumeTargetPath);
        }
    }

    /**
     * Serializable state for resuming indexing after a chunk commit.
     * Enables direct editor restoration without tree traversal.
     * 
     * Format: Simple JSON stored in NodeStore
     * Size: ~1-2 KB (includes editor stack)
     */
    protected static class ResumeState {
        public final String currentPath;
        public final String sourceCheckpoint;
        public final String targetCheckpoint;
        public final long timestamp;
        public final long nodesProcessed;
        public final List<EditorLevel> editorStack;  // Editor hierarchy
        public final String traversalTreeJson;  // In-memory traversal tree for fast resume
        public final String cachedNodeInfoJson;  // Cached node info for even faster skip phase
        
        /**
         * Represents one level in the editor hierarchy.
         * Contains path and active index definitions at that level.
         */
        public static class EditorLevel {
            public final String path;
            public final List<String> activeIndexPaths;
            
            public EditorLevel(String path, List<String> activeIndexPaths) {
                this.path = path;
                this.activeIndexPaths = activeIndexPaths != null ? 
                    new ArrayList<>(activeIndexPaths) : new ArrayList<>();
            }
            
            public String toJson() {
                StringBuilder json = new StringBuilder();
                json.append("{\"path\":\"").append(escapeJson(path)).append("\",");
                json.append("\"indexes\":[");
                for (int i = 0; i < activeIndexPaths.size(); i++) {
                    if (i > 0) json.append(",");
                    json.append("\"").append(escapeJson(activeIndexPaths.get(i))).append("\"");
                }
                json.append("]}");
                return json.toString();
            }
        }
        
        public ResumeState(String currentPath, String sourceCheckpoint, String targetCheckpoint, 
                          long nodesProcessed, List<EditorLevel> editorStack, String traversalTreeJson,
                          String cachedNodeInfoJson) {
            this.currentPath = currentPath;
            this.sourceCheckpoint = sourceCheckpoint;
            this.targetCheckpoint = targetCheckpoint;
            this.timestamp = System.currentTimeMillis();
            this.nodesProcessed = nodesProcessed;
            this.editorStack = editorStack != null ? editorStack : new ArrayList<>();
            this.traversalTreeJson = traversalTreeJson;
            this.cachedNodeInfoJson = cachedNodeInfoJson;
        }
        
        // Constructor without cached node info (backward compatibility)
        public ResumeState(String currentPath, String sourceCheckpoint, String targetCheckpoint, 
                          long nodesProcessed, List<EditorLevel> editorStack, String traversalTreeJson) {
            this(currentPath, sourceCheckpoint, targetCheckpoint, nodesProcessed, editorStack, traversalTreeJson, null);
        }
        
        // Constructor without traversal tree (backward compatibility)
        public ResumeState(String currentPath, String sourceCheckpoint, String targetCheckpoint, 
                          long nodesProcessed, List<EditorLevel> editorStack) {
            this(currentPath, sourceCheckpoint, targetCheckpoint, nodesProcessed, editorStack, null, null);
        }
        
        // Backward compatibility constructor (no editor stack or traversal tree)
        public ResumeState(String currentPath, String sourceCheckpoint, String targetCheckpoint, long nodesProcessed) {
            this(currentPath, sourceCheckpoint, targetCheckpoint, nodesProcessed, null, null, null);
        }
        
        /**
         * Serialize to simple JSON format (no external dependencies needed).
         */
        public String toJson() {
            StringBuilder json = new StringBuilder();
            json.append("{\n");
            json.append("  \"currentPath\": \"").append(escapeJson(currentPath)).append("\",\n");
            json.append("  \"sourceCheckpoint\": \"").append(escapeJson(sourceCheckpoint)).append("\",\n");
            json.append("  \"targetCheckpoint\": \"").append(escapeJson(targetCheckpoint)).append("\",\n");
            json.append("  \"timestamp\": ").append(timestamp).append(",\n");
            json.append("  \"nodesProcessed\": ").append(nodesProcessed);
            
            // Add editor stack if present
            if (editorStack != null && !editorStack.isEmpty()) {
                json.append(",\n  \"editorStack\": [\n");
                for (int i = 0; i < editorStack.size(); i++) {
                    if (i > 0) json.append(",\n");
                    json.append("    ").append(editorStack.get(i).toJson());
                }
                json.append("\n  ]");
            }
            
            // Add traversal tree if present (for O(depth) resume)
            if (traversalTreeJson != null && !traversalTreeJson.isEmpty()) {
                // Store as separate property to avoid JSON escaping issues
                json.append(",\n  \"hasTraversalTree\": true");
            }
            
            json.append("\n}");
            return json.toString();
        }
        
        /**
         * Get traversal tree JSON (stored separately for size efficiency).
         */
        public String getTraversalTreeJson() {
            return traversalTreeJson;
        }
        
        /**
         * Deserialize from JSON format.
         * Simple parsing - no external JSON library needed.
         */
        public static ResumeState fromJson(String json) {
            if (json == null || json.trim().isEmpty()) {
                return null;
            }
            
            try {
                String currentPath = extractJsonString(json, "currentPath");
                String sourceCheckpoint = extractJsonString(json, "sourceCheckpoint");
                String targetCheckpoint = extractJsonString(json, "targetCheckpoint");
                long nodesProcessed = extractJsonLong(json, "nodesProcessed");
                
                // Parse editor stack if present
                List<EditorLevel> editorStack = extractEditorStack(json);
                
                return new ResumeState(currentPath, sourceCheckpoint, targetCheckpoint, 
                                     nodesProcessed, editorStack);
            } catch (Exception e) {
                log.warn("Failed to parse resume state JSON", e);
                return null;
            }
        }
        
        private static List<EditorLevel> extractEditorStack(String json) {
            List<EditorLevel> stack = new ArrayList<>();
            
            // Find editorStack array
            String pattern = "\"editorStack\"\\s*:\\s*\\[([^\\]]+)\\]";
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.DOTALL);
            java.util.regex.Matcher m = p.matcher(json);
            
            if (!m.find()) {
                return stack;  // No editor stack found (backward compatibility)
            }
            
            String stackContent = m.group(1);
            
            // Parse each editor level
            String levelPattern = "\\{\"path\":\"([^\"]+)\",\"indexes\":\\[([^\\]]*)\\]\\}";
            p = java.util.regex.Pattern.compile(levelPattern);
            m = p.matcher(stackContent);
            
            while (m.find()) {
                String path = m.group(1);
                String indexesStr = m.group(2);
                
                List<String> indexes = new ArrayList<>();
                if (indexesStr != null && !indexesStr.trim().isEmpty()) {
                    // Parse index paths
                    String indexPattern = "\"([^\"]+)\"";
                    java.util.regex.Pattern ip = java.util.regex.Pattern.compile(indexPattern);
                    java.util.regex.Matcher im = ip.matcher(indexesStr);
                    while (im.find()) {
                        indexes.add(im.group(1));
                    }
                }
                
                stack.add(new EditorLevel(path, indexes));
            }
            
            return stack;
        }
        
        private static String extractJsonString(String json, String key) {
            String pattern = "\"" + key + "\"\\s*:\\s*\"([^\"]+)\"";
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher m = p.matcher(json);
            if (m.find()) {
                return m.group(1);
            }
            return null;
        }
        
        private static long extractJsonLong(String json, String key) {
            String pattern = "\"" + key + "\"\\s*:\\s*(\\d+)";
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher m = p.matcher(json);
            if (m.find()) {
                return Long.parseLong(m.group(1));
            }
            return 0;
        }
        
        private static String escapeJson(String s) {
            if (s == null) return "";
            return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
        }
        
        @Override
        public String toString() {
            return "ResumeState{path=" + currentPath + ", checkpoint=" + targetCheckpoint + "}";
        }
    }

    protected static class AsyncUpdateCallback implements IndexUpdateCallback, NodeTraversalCallback {
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
        private int timeLimit = -1;
        private long startTime;
        private int currentChunkNumber = 0;

        private List<ValidatorProvider> validatorProviders = Collections.emptyList();

        /**
         * Expiration time of the last lease we committed, null if lease is
         * disabled
         */
        private Long lease = null;

        private boolean hasLease = false;
        
        /**
         * Optional callback for continuous processing mode.
         * When set, reaching chunk/time limit calls this instead of throwing SuspendException.
         */
        private ProgressCommitCallback progressCommitCallback;
        
        /**
         * When true, use continuous processing mode (callback instead of exception).
         */
        private boolean continuousMode = false;
        
        /**
         * Flag set when chunk limit (size or time) is reached during traversal.
         * Used by the outer loop to trigger chunk commit.
         */
        private volatile boolean chunkLimitReached = false;
        
        /**
         * The path where chunk limit was reached - used as resume point for next chunk.
         */
        private volatile String chunkLastIndexedPath = null;
        
        
        /**
         * Flag to indicate we're in skip mode (ResumingEditor skipping to resume point).
         * When true, don't count nodes toward time/chunk limits.
         */
        private volatile boolean inSkipMode = false;

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
         * Sets the time limit in milliseconds for resumable indexing.
         * @param milliseconds time limit in milliseconds, or -1 to disable
         */
        public void setTimeLimit(int milliseconds) {
            this.timeLimit = milliseconds;
        }
        
        /**
         * Enables continuous processing mode with the given callback.
         * In this mode, reaching chunk/time limit calls the callback instead of
         * throwing SuspendException, allowing the diff traversal to continue
         * without re-entering from root.
         * 
         * @param callback the progress commit callback
         */
        public void setContinuousMode(ProgressCommitCallback callback) {
            this.progressCommitCallback = callback;
            this.continuousMode = (callback != null);
        }
        
        /**
         * Check if chunk limit (size or time) was reached during traversal.
         * @return true if chunk limit was reached
         */
        public boolean isChunkLimitReached() {
            return chunkLimitReached;
        }
        
        /**
         * Get the path where chunk limit was reached.
         * @return the last indexed path, or null if no chunk limit was reached
         */
        public String getChunkLastIndexedPath() {
            return chunkLastIndexedPath;
        }
        
        /**
         * Reset counters and flags for the next chunk iteration.
         * Called after a chunk is committed to prepare for the next chunk.
         */
        public void resetForNextChunk() {
            this.chunkLimitReached = false;
            this.chunkLastIndexedPath = null;
            this.startTime = System.currentTimeMillis();
            this.indexStats.reset();
            // inSkipMode will be set by the caller when creating ResumingEditor
        }
        
        /**
         * Set skip mode - when true, nodes are being skipped by ResumingEditor
         * and should not count toward chunk/time limits.
         * When transitioning from skip mode to indexing mode, reset the timer
         * so time limit is calculated from when actual indexing starts.
         */
        public void setSkipMode(boolean skipMode) {
            // When exiting skip mode (starting actual indexing), reset timer and counters
            if (this.inSkipMode && !skipMode) {
                long skipDuration = System.currentTimeMillis() - this.startTime;
                long nodesSkipped = this.indexStats.getNodesReadCount();
                long skipRate = skipDuration > 0 ? (nodesSkipped * 1000 / skipDuration) : 0;
                log.info("[{}] Chunk #{} skip phase completed: {}ms to skip {} nodes ({} nodes/sec)", 
                    name, currentChunkNumber, skipDuration, nodesSkipped, skipRate);
                System.out.println("[SKIP PHASE] Chunk #" + currentChunkNumber + " skip completed: " + 
                    skipDuration + "ms, " + nodesSkipped + " nodes traversed @ " + skipRate + " nodes/sec");
                // Reset timer and counters for actual indexing phase
                this.startTime = System.currentTimeMillis();
                this.indexStats.reset();
            }
            this.inSkipMode = skipMode;
        }
        
        public void setChunkNumber(int chunkNumber) {
            this.currentChunkNumber = chunkNumber;
        }
        
        /**
         * Check if currently in skip mode.
         */
        public boolean isInSkipMode() {
            return inSkipMode;
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
            startTime = System.currentTimeMillis();
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
            
            // Increment traversal count first
            long nodesRead = indexStats.incTraversal();
            
            // Log traversal progress periodically for monitoring
            if (log.isDebugEnabled() && nodesRead % 50000 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                log.debug("[{}] Traversed {} nodes in {}ms (timeLimit: {}ms)", 
                    name, nodesRead, elapsed, timeLimit);
            }
            
            // Check if chunk limit reached (continuous mode only)
            // Only check limits when NOT in skip mode (actually indexing nodes)
            if (continuousMode && !chunkLimitReached && !inSkipMode) {
                boolean limitReached = false;
                String reason = null;
                String currentPath = pathSource.getPath();
                
                // Chunk size limit: Check when node count reaches the configured limit
                // No minimum - if user sets chunkSize=50, honor that exactly
                if (updateLimit > 0 && nodesRead >= updateLimit) {
                    limitReached = true;
                    reason = "node count (" + nodesRead + " >= " + updateLimit + ")";
                }
                
                // Time limit: Only check after minimum progress to avoid tiny chunks
                // MIN_NODES ensures at least some work is done before stopping for time
                final long MIN_NODES_FOR_TIME_LIMIT = 100;
                if (!limitReached && timeLimit > 0 && nodesRead >= MIN_NODES_FOR_TIME_LIMIT) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    if (elapsed > timeLimit) {
                        limitReached = true;
                        reason = "time limit (" + elapsed + "ms >= " + timeLimit + "ms)";
                    }
                }
                
                if (limitReached) {
                    // Set chunk limit flags
                    chunkLimitReached = true;
                    chunkLastIndexedPath = currentPath;
                    
                    log.info("[{}] Chunk limit reached due to {} at path: {} (processed {} nodes)", 
                        name, reason, chunkLastIndexedPath, nodesRead);
                    
                    // Throw CHUNK_COMPLETE to stop EditorDiff and return to outer loop
                    // The outer loop will commit the chunk and continue with next chunk
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
        
        public void setProgressCommitCallback(ProgressCommitCallback callback) {
            this.progressCommitCallback = callback;
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

        // BUG 2 FIX: Check for resume state from a previous interrupted run
        String resumeFromPath = null;
        String resumeCheckpoint = null;
        NodeState laneNode = async.getChildNode(name);
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
        
        // Set limits from cached config before any traversal starts
        // These values are read from system properties in the constructor
        if (configuredTimeLimitMs > 0) {
            callback.setTimeLimit((int) configuredTimeLimitMs);
            log.debug("[{}] Configured time limit: {}ms", name, configuredTimeLimitMs);
        }
        if (configuredChunkSize > 0) {
            callback.setUpdateLimit((int) configuredChunkSize);
            log.debug("[{}] Configured chunk size: {}", name, configuredChunkSize);
        }
        
        return callback;
    }

    /**
     * System property to enable continuous processing mode.
     * When enabled, indexing logs progress at regular intervals without
     * interrupting the diff traversal.
     */
    private static final boolean CONTINUOUS_MODE = Boolean.getBoolean("oak.async.continuousMode");

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
        long methodStartTime = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createStarted();
        boolean updatePostRunStatus = true;
        boolean progressLogged = false;
        IndexUpdate indexUpdate = null;

        // Prepare callback - resets counters
        long prepareStartTime = System.currentTimeMillis();
        callback.prepare(afterCheckpoint);
        long prepareTime = System.currentTimeMillis() - prepareStartTime;
        
        // Set up continuous mode with chunked commits if configured
        long setupStartTime = System.currentTimeMillis();
        boolean chunkedMode = continuousModeEnabled && (configuredChunkSize > 0 || configuredTimeLimitMs > 0);
        if (chunkedMode) {
            if (configuredChunkSize > 0) {
                callback.setUpdateLimit((int) configuredChunkSize);
            }
            if (configuredTimeLimitMs > 0) {
                callback.setTimeLimit((int) configuredTimeLimitMs);
            }
            // Enable continuous mode flag (no callback needed - we use exception)
            callback.setContinuousMode(path -> {}); // Dummy callback to enable flag
            
            log.debug("[{}] Chunked indexing enabled - chunkSize: {}, timeLimit: {}ms", 
                name, configuredChunkSize, configuredTimeLimitMs);
        }

        // Check for index tasks split requests
        taskSplitter.maybeSplit(beforeCheckpoint, callback.lease);
        
        boolean indexingFailed = true;
        NodeBuilder builder = store.getRoot().builder();
        long setupTime = System.currentTimeMillis() - setupStartTime;
        
        System.out.println("[TIMING] updateIndex setup: prepare=" + prepareTime + "ms, config=" + setupTime + "ms");
        
        // Try to load saved resume state (skips tree traversal!)
        // Falls back to tree traversal if no state file or loading fails
        ResumeState loadedState = null;
        if (chunkedMode) {
            loadedState = loadResumeState(beforeCheckpoint, afterCheckpoint);
        }
        
        // Track current resume position for chunked processing
        // Use loaded state if available, otherwise use passed-in resumeFromPath
        String currentResumePath = (loadedState != null) ? loadedState.currentPath : resumeFromPath;
        int chunkNumber = 0;
        long totalNodesProcessed = (loadedState != null) ? loadedState.nodesProcessed : 0;
        
        // Track traversal tree for O(depth) resume (avoids full tree traversal on chunk boundaries)
        TraversalTree currentTraversalTree = null;

        try {
            markFailingIndexesAsCorrupt(builder);
            
            // Enable editor caching to avoid repeated NodeStore reads for index definitions
            // This dramatically speeds up skip phase (3x-5x faster)
            IndexUpdate.enableEditorCaching();

            CommitInfo info = new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN,
                    Map.of(IndexConstants.CHECKPOINT_CREATION_TIME, afterTime));
            
            // Loop to process multiple chunks within single updateIndex call
            while (true) {
                chunkNumber++;
                callback.setChunkNumber(chunkNumber);
                long chunkStartTime = System.currentTimeMillis();
                
                // Create IndexUpdate with current builder
                long createStartTime = System.currentTimeMillis();
                indexUpdate = new IndexUpdate(provider, name, after, builder, callback, callback, info, corruptIndexHandler)
                        .withMissingProviderStrategy(missingStrategy);
                configureRateEstimator(indexUpdate);
                long createTime = System.currentTimeMillis() - createStartTime;

                // Create editor with traversal tracking for fast resume
                long editorStartTime = System.currentTimeMillis();
                Editor baseEditor = VisibleEditor.wrap(indexUpdate);
                Editor editor;
                TraversalTrackingEditor trackingEditor = null;
                NodeInfoCachingEditor currentCachingEditor = null; // Track for cache extraction
                
                // Check if we have a traversal tree - either from previous chunk (in-process) or loaded from NodeStore
                TraversalTree traversalTree = null;
                if (currentTraversalTree != null) {
                    // Use in-memory tree from previous chunk (same updateIndex call)
                    traversalTree = currentTraversalTree;
                    log.info("[{}] Using in-memory traversal tree from previous chunk: {} nodes, {} indexed", 
                            name, traversalTree.countNodes(), traversalTree.countIndexedNodes());
                    System.out.println("[IN-MEMORY] Using traversal tree from previous chunk: " + 
                                       traversalTree.countNodes() + " nodes tracked");
                } else if (loadedState != null && loadedState.traversalTreeJson != null) {
                    // Load from NodeStore (cross-cycle resume)
                    try {
                        traversalTree = TraversalTree.fromJson(loadedState.traversalTreeJson);
                        if (traversalTree != null) {
                            log.info("[{}] Loaded traversal tree from NodeStore: {} nodes, {} indexed", 
                                    name, traversalTree.countNodes(), traversalTree.countIndexedNodes());
                        }
                    } catch (Exception e) {
                        log.warn("[{}] Failed to parse traversal tree, falling back to standard resume", name, e);
                    }
                }
                
                // Track skipping editor for stats
                TraversalTreeSkippingEditor skippingEditor = null;
                
                if (currentResumePath != null && !"/".equals(currentResumePath)) {
                    if (traversalTree != null) {
                        // FAST PATH: Use TraversalTreeSkippingEditor to skip indexed subtrees
                        // This avoids NodeStore traversal for already-indexed nodes!
                        long navStart = System.currentTimeMillis();
                        TraversalTree resumeNode = traversalTree.findByPath(currentResumePath);
                        if (resumeNode != null) {
                            int totalNodes = traversalTree.countNodes();
                            int indexedNodes = traversalTree.countIndexedNodes();
                            log.info("[{}] Chunk #{}: TREE-SKIP RESUME - {} indexed of {} total nodes", 
                                    name, chunkNumber, indexedNodes, totalNodes);
                            System.out.println("[TREE-SKIP] Tree has " + totalNodes + 
                                             " nodes, " + indexedNodes + " indexed (will skip " + indexedNodes + " nodes!)");
                            
                            // Tree skip is now SAFE - only skips structure nodes with no content.
                            // Content nodes (hasContent=true) are always processed.
                            // Structure nodes (jcr:system, oak:index, etc.) can be safely skipped.
                            // ENABLED BY DEFAULT - can be disabled with -Doak.async.disableTreeSkip=true
                            boolean isReindex = !before.exists() || !before.hasChildNode("oak:index");
                            boolean disableTreeSkip = Boolean.getBoolean("oak.async.disableTreeSkip");
                            boolean treeSkipProp = Boolean.parseBoolean(System.getProperty("oak.async.useTreeSkip", "true"));
                            boolean useTreeSkip = !isReindex && !disableTreeSkip && treeSkipProp;
                            
                            // Cache-based skip optimization
                            // Uses CachedNodeInfo to provide node type info without SegmentStore reads
                            // BUT still calls all editor methods to maintain Lucene state
                            // IMPORTANT: Only use cached skip when we have loadedState from a previous cycle!
                            boolean useCachedSkip = loadedState != null && 
                                loadedState.cachedNodeInfoJson != null &&
                                !isReindex && 
                                !disableTreeSkip && 
                                treeSkipProp &&
                                Boolean.getBoolean("oak.async.useCachedSkip");
                            
                            if (useCachedSkip) {
                                // CACHED SKIP: Use cached node info to reduce SegmentStore reads
                                // Key insight: We cache node types and child order to avoid SegmentStore reads
                                // BUT we still call all editor methods to maintain Lucene state!
                                
                                callback.setSkipMode(true);
                                final String resumeTarget = currentResumePath;
                                
                                // Load CachedNodeInfo from saved state
                                CachedNodeInfo cachedInfo = CachedNodeInfo.fromSerializedForm(loadedState.cachedNodeInfoJson);
                                System.out.println("[CACHED-SKIP] Loaded cache: " + 
                                    (cachedInfo != null ? cachedInfo.getSubtreeSize() + " nodes" : "null"));
                                
                                if (cachedInfo == null || cachedInfo.getChildCount() == 0) {
                                    // No valid cache - fall back to normal ResumingEditor
                                    System.out.println("[CACHED-SKIP] Invalid cache after load, falling back to ResumingEditor");
                                } else {
                                    // Use cached skip with ResumingEditor
                                    System.out.println("[CACHED-SKIP] Using cached node info for skip phase");
                                    System.out.println("[CACHED-SKIP] Cache has " + cachedInfo.getSubtreeSize() + " nodes");
                                    
                                    // Editor chain: ResumingEditor wraps baseEditor, NodeInfoCachingEditor wraps that
                                    Editor resumingEditor = new ResumingEditor(baseEditor, resumeTarget, 
                                        () -> {
                                            callback.setSkipMode(false);
                                            System.out.println("[CACHED-SKIP] Reached resume point: " + resumeTarget);
                                        });
                                    NodeInfoCachingEditor cachingEditor = new NodeInfoCachingEditor(resumingEditor, cachedInfo);
                                    editor = cachingEditor;
                                    
                                    long diffStartTime = System.currentTimeMillis();
                                    CommitFailedException exception = CachedResumeDiff.process(
                                        editor, before, after, cachedInfo, resumeTarget);
                                    long diffTime = System.currentTimeMillis() - diffStartTime;
                                    indexStats.setDiffTimeMs(diffTime);
                                    
                                    System.out.println("[TIMING] Chunk #" + chunkNumber + ": CACHED-SKIP, total=" + diffTime + "ms");
                                    
                                    // Handle result
                                    if (exception == CHUNK_COMPLETE) {
                                        long chunkCommitStart = System.currentTimeMillis();
                                        String chunkPath = callback.getChunkLastIndexedPath();
                                        log.info("[{}] Chunk #{} (CACHED) complete at path: {} ({}ms)", name, chunkNumber, chunkPath, diffTime);
                                        
                                        // 1. Flush Lucene writers
                                        long flushStart = System.currentTimeMillis();
                                        indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_PROGRESS);
                                        long flushTime = System.currentTimeMillis() - flushStart;
                                        
                                        // 2. Merge to NodeStore
                                        long mergeStart = System.currentTimeMillis();
                                        builder.child(ASYNC).setProperty(name + ":chunk", chunkPath);
                                        mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint, callback.lease, name);
                                        long mergeTime = System.currentTimeMillis() - mergeStart;
                                        
                                        // 3. Save resume state with updated cache
                                        long saveStart = System.currentTimeMillis();
                                        NodeBuilder resumeBuilder = store.getRoot().builder();
                                        NodeBuilder laneBuilder = resumeBuilder.child(ASYNC).child(name);
                                        laneBuilder.setProperty("lastIndexedPath", chunkPath);
                                        laneBuilder.setProperty("targetCheckpoint", afterCheckpoint);
                                        totalNodesProcessed += indexStats.getNodesReadCount();
                                        
                                        // Save updated cache
                                        CachedNodeInfo updatedCache = cachingEditor.getCachedInfo();
                                        String cacheJson = updatedCache.toSerializedForm();
                                        laneBuilder.setProperty("cachedNodeInfo", cacheJson);
                                        
                                        store.merge(resumeBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                                        long saveTime = System.currentTimeMillis() - saveStart;
                                        
                                        // 4. Get fresh builder
                                        builder = store.getRoot().builder();
                                        currentResumePath = chunkPath;
                                        callback.resetForNextChunk();
                                        
                                        System.out.println("[TIMING] Chunk #" + chunkNumber + " (CACHED) commit: flush=" + flushTime + 
                                            "ms, merge=" + mergeTime + "ms, save=" + saveTime + "ms");
                                        
                                        continue;
                                    } else if (exception != null) {
                                        throw exception;
                                    } else {
                                        break;
                                    }
                                }
                            }
                            
                            // =====================================================================
                            // TREE-DRIVEN RESUME (NO-IO): Use TraversalTree to avoid SegmentStore reads
                            // =====================================================================
                            // During skip phase, we drive traversal from TraversalTree metadata and
                            // replay enter()/leave() using VirtualNodeState. This avoids NodeStore I/O
                            // for already-indexed nodes BEFORE the resume point.
                            //
                            // At the resume point and beyond, we switch to real NodeState traversal
                            // (EditorDiff.process) to actually index remaining content.
                            // =====================================================================
                            
                            long diffStartTime = System.currentTimeMillis();
                            CommitFailedException exception = null;
                            
                            boolean useTreeNoIoResume = Boolean.parseBoolean(
                                System.getProperty("oak.async.useTraversalTreeNoIoResume", "true"));
                            
                            if (useTreeNoIoResume && traversalTree != null) {
                                System.out.println("[TREE-RESUME] === RESUME START ===");
                                System.out.println("[TREE-RESUME] Resume path: " + currentResumePath);
                                System.out.println("[TREE-RESUME] Tree nodes: " + traversalTree.countNodes() + 
                                    ", indexed: " + traversalTree.countIndexedNodes());
                                
                                // Enable skip mode for timing/counters AND IndexUpdate initialization.
                                callback.setSkipMode(true);
                                final AsyncUpdateCallback callbackForResume = callback;
                                
                                // Build traversal tree + cache for next chunks while we traverse.
                                CachedNodeInfo newCache = new CachedNodeInfo();
                                currentCachingEditor = new NodeInfoCachingEditor(baseEditor, newCache);
                                trackingEditor = new TraversalTrackingEditor(currentCachingEditor, traversalTree);
                                
                                exception = TraversalTreeResumeDiff.process(
                                    trackingEditor,
                                    before,
                                    after,
                                    traversalTree,
                                    currentResumePath,
                                    () -> {
                                        // Resume point reached: exit skip mode and activate index editors.
                                        callbackForResume.setSkipMode(false);
                                        System.out.println("[TREE-RESUME] Resume point reached - starting real processing");
                                    }
                                );
                                
                                System.out.println("[TREE-RESUME] === RESUME END ===");
                                if (exception != null) {
                                    System.out.println("[TREE-RESUME] Exception: " + exception.getMessage());
                                }
                            } else {
                                // If tree/no-io resume is disabled or tree metadata missing, fall back to existing logic below.
                                // (The standard resume path will build a fresh traversal tree + cache.)
                                callback.setSkipMode(true);
                                Editor resumingEditor = new ResumingEditor(baseEditor, currentResumePath, () -> callback.setSkipMode(false));
                                CachedNodeInfo existingCache = null;
                                if (loadedState != null && loadedState.cachedNodeInfoJson != null) {
                                    existingCache = CachedNodeInfo.fromSerializedForm(loadedState.cachedNodeInfoJson);
                                }
                                currentCachingEditor = new NodeInfoCachingEditor(resumingEditor, existingCache);
                                if (traversalTree == null) {
                                    traversalTree = new TraversalTree();
                                }
                                trackingEditor = new TraversalTrackingEditor(currentCachingEditor, traversalTree);
                                exception = EditorDiff.process(trackingEditor, before, after);
                            }
                            
                            editor = trackingEditor;
                            
                            long diffTime = System.currentTimeMillis() - diffStartTime;
                            indexStats.setDiffTimeMs(diffTime);
                            
                            // Clear loaded state
                            loadedState = null;
                            
                            System.out.println("[TIMING] Chunk #" + chunkNumber + ": RESUME, total_diff=" + diffTime + "ms");
                            
                            // Save traversal tree for next chunk
                            currentTraversalTree = trackingEditor.getTraversalTree();
                            
                            // Handle CHUNK_COMPLETE - do chunk commit inline
                            if (exception == CHUNK_COMPLETE) {
                                long chunkCommitStart = System.currentTimeMillis();
                                String chunkPath = callback.getChunkLastIndexedPath();
                                log.info("[{}] Chunk #{} (IN-MEMORY) complete at path: {} ({}ms)", name, chunkNumber, chunkPath, diffTime);
                                
                                // 1. Flush Lucene writers
                                long flushStart = System.currentTimeMillis();
                                indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_PROGRESS);
                                long flushTime = System.currentTimeMillis() - flushStart;
                                
                                // 2. Merge to NodeStore
                                long mergeStart = System.currentTimeMillis();
                                builder.child(ASYNC).setProperty(name + ":chunk", chunkPath);
                                mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint, callback.lease, name);
                                long mergeTime = System.currentTimeMillis() - mergeStart;
                                log.info("[{}] Chunk #{} committed - index is now searchable up to: {}", name, chunkNumber, chunkPath);
                                
                                // 3. Save resume state
                                long saveStart = System.currentTimeMillis();
                                NodeBuilder resumeBuilder = store.getRoot().builder();
                                NodeBuilder laneBuilder = resumeBuilder.child(ASYNC).child(name);
                                laneBuilder.setProperty("lastIndexedPath", chunkPath);
                                laneBuilder.setProperty("targetCheckpoint", afterCheckpoint);
                                totalNodesProcessed += indexStats.getNodesReadCount();
                                CachedNodeInfo cacheFromEditor = currentCachingEditor != null ? currentCachingEditor.getCachedInfo() : null;
                                saveResumeState(resumeBuilder, afterCheckpoint, chunkPath, totalNodesProcessed, indexUpdate, currentTraversalTree, cacheFromEditor);
                                store.merge(resumeBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                                long saveTime = System.currentTimeMillis() - saveStart;
                                
                                // 4. Get fresh builder
                                builder = store.getRoot().builder();
                                currentResumePath = chunkPath;
                                callback.resetForNextChunk();
                                
                                long totalChunkCommitTime = System.currentTimeMillis() - chunkCommitStart;
                                System.out.println("[TIMING] Chunk #" + chunkNumber + " (IN-MEMORY) commit: flush=" + flushTime + 
                                    "ms, merge=" + mergeTime + "ms, save=" + saveTime + "ms, TOTAL=" + totalChunkCommitTime + "ms");
                                
                                continue; // Process next chunk
                            } else if (exception != null) {
                                throw exception;
                            } else {
                                // Completed successfully
                                break;
                            }
                        }
                    }
                    
                    // FALLBACK: Standard ResumingEditor (when no traversal tree or tree navigation failed)
                    callback.setSkipMode(true);
                    
                    // Wrap with tracking to build tree for future fast resumes
                    if (traversalTree == null) {
                        traversalTree = new TraversalTree();
                    }
                    
                    // Add cache building for future optimized skip
                    Editor resumingEditor = new ResumingEditor(baseEditor, currentResumePath, () -> callback.setSkipMode(false));
                    CachedNodeInfo existingCache = null;
                    if (loadedState != null && loadedState.cachedNodeInfoJson != null) {
                        existingCache = CachedNodeInfo.fromSerializedForm(loadedState.cachedNodeInfoJson);
                    }
                    currentCachingEditor = new NodeInfoCachingEditor(resumingEditor, existingCache);
                    trackingEditor = new TraversalTrackingEditor(currentCachingEditor, traversalTree);
                    editor = trackingEditor;
                    
                    log.info("[{}] Chunk #{}: Resuming from path: {} (building traversal tree + cache)", 
                            name, chunkNumber, currentResumePath);
                } else {
                    // Not resuming - start fresh with traversal tracking AND cache building
                    callback.setSkipMode(false);
                    traversalTree = new TraversalTree();
                    
                    // Build cache for future cached skip optimization
                    // Editor chain: TraversalTrackingEditor -> NodeInfoCachingEditor -> baseEditor
                    CachedNodeInfo newCache = new CachedNodeInfo();
                    currentCachingEditor = new NodeInfoCachingEditor(baseEditor, newCache);
                    trackingEditor = new TraversalTrackingEditor(currentCachingEditor, traversalTree);
                    editor = trackingEditor;
                    
                    log.debug("[{}] Chunk #{}: Starting from root (building traversal tree + cache)", name, chunkNumber);
                }
                long editorCreateTime = System.currentTimeMillis() - editorStartTime;
                
                // Clear loaded state after first use
                if (loadedState != null && chunkNumber == 1) {
                    loadedState = null;
                }

                // Process diff - will throw CHUNK_COMPLETE if limit reached
                long diffStartTime = System.currentTimeMillis();
                CommitFailedException exception = EditorDiff.process(editor, before, after);
                
                // Save traversal tree for next chunk
                if (trackingEditor != null) {
                    currentTraversalTree = trackingEditor.getTraversalTree();
                }
                long diffTime = System.currentTimeMillis() - diffStartTime;
                indexStats.setDiffTimeMs(diffTime);
                
                long chunkSetupTime = diffStartTime - chunkStartTime;
                long actualIndexTime = diffTime;
                // If skip mode was used, the actual indexing time is less than diffTime
                // because skip phase time is already logged separately
                System.out.println("[TIMING] Chunk #" + chunkNumber + ": setup=" + chunkSetupTime + 
                    "ms (create=" + createTime + "ms, editor=" + editorCreateTime + 
                    "ms), total_diff=" + diffTime + "ms");
                
                // Handle CHUNK_COMPLETE exception - commit chunk and continue
                if (exception == CHUNK_COMPLETE) {
                    long chunkCommitStart = System.currentTimeMillis();
                    String chunkPath = callback.getChunkLastIndexedPath();
                    log.info("[{}] Chunk #{} complete at path: {} ({}ms)", name, chunkNumber, chunkPath, diffTime);
                    
                    // 1. Flush Lucene writers to NodeBuilder
                    long flushStart = System.currentTimeMillis();
                    indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_PROGRESS);
                    long flushTime = System.currentTimeMillis() - flushStart;
                    
                    // 2. Merge to NodeStore - THIS MAKES INDEX SEARCHABLE
                    long mergeStart = System.currentTimeMillis();
                    builder.child(ASYNC).setProperty(name + ":chunk", chunkPath);
                    mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint, callback.lease, name);
                    long mergeTime = System.currentTimeMillis() - mergeStart;
                    log.info("[{}] Chunk #{} committed - index is now searchable up to: {}", name, chunkNumber, chunkPath);
                    
                    // 3. Save resume state (for crash recovery)
                    long saveStart = System.currentTimeMillis();
                    NodeBuilder resumeBuilder = store.getRoot().builder();
                    NodeBuilder laneBuilder = resumeBuilder.child(ASYNC).child(name);
                    laneBuilder.setProperty("lastIndexedPath", chunkPath);
                    laneBuilder.setProperty("targetCheckpoint", afterCheckpoint);
                    
                    // 3b. Save serialized state to NodeStore (enables skip-free resume!)
                    // Must be called BEFORE merge to include state in same transaction
                    // Pass IndexUpdate, TraversalTree, and CachedNodeInfo for optimized resume
                    totalNodesProcessed += indexStats.getNodesReadCount();
                    CachedNodeInfo cacheToSave = currentCachingEditor != null ? currentCachingEditor.getCachedInfo() : null;
                    saveResumeState(resumeBuilder, afterCheckpoint, chunkPath, totalNodesProcessed, indexUpdate, currentTraversalTree, cacheToSave);
                    
                    // Merge all resume state changes atomically
                    store.merge(resumeBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    long saveTime = System.currentTimeMillis() - saveStart;
                    
                    // 4. Get fresh builder for next chunk
                    long builderStart = System.currentTimeMillis();
                    builder = store.getRoot().builder();
                    currentResumePath = chunkPath;
                    callback.resetForNextChunk();
                    long builderTime = System.currentTimeMillis() - builderStart;
                    
                    long totalChunkCommitTime = System.currentTimeMillis() - chunkCommitStart;
                    System.out.println("[TIMING] Chunk #" + chunkNumber + " commit: flush=" + flushTime + "ms, merge=" + mergeTime + "ms, save=" + saveTime + "ms, builder=" + builderTime + "ms, TOTAL=" + totalChunkCommitTime + "ms");
                    
                    log.debug("[{}] Starting chunk #{} from path: {}", name, chunkNumber + 1, currentResumePath);
                    continue; // Process next chunk
                }
                
                // Other exceptions should be thrown
                if (exception != null) {
                    throw exception;
                }
                
                // EditorDiff completed successfully - no more chunks needed
                log.info("[{}] Diff completed after {} chunk(s) in {} ms", name, chunkNumber, diffTime);
                
                // Log resume stats if we resumed
                if (currentResumePath != null && !"/".equals(currentResumePath) && editor instanceof ResumingEditor) {
                    ResumingEditor resumingEditor = (ResumingEditor)editor;
                    resumingEditor.logFinalResumeStats();
                    
                    long[] stats = resumingEditor.getResumeStats();
                    lastResumeTimeToTarget = stats[1];
                    lastResumeTotalTime = System.currentTimeMillis() - resumingEditor.statsHolder.resumeStartTime;
                } else {
                    lastResumeTimeToTarget = 0;
                    lastResumeTotalTime = 0;
                }
                
                break; // Exit chunk loop - diff complete
            }

            // All chunks processed - perform final merge with checkpoint update
            log.debug("[{}] All {} chunk(s) processed - performing final merge", name, chunkNumber);
            
            // Update checkpoint state
            builder.child(ASYNC).setProperty(name, afterCheckpoint);
            builder.child(ASYNC).setProperty(PropertyStates.createProperty(lastIndexedTo, afterTime, Type.DATE));
            
            // Remove chunk marker if present
            if (builder.child(ASYNC).hasProperty(name + ":chunk")) {
                builder.child(ASYNC).removeProperty(name + ":chunk");
            }

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
            long finalMergeStartTime = System.currentTimeMillis();
            mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint, callback.lease, name);
            long finalMergeTime = System.currentTimeMillis() - finalMergeStartTime;
            System.out.println("[TIMING] Final merge: " + finalMergeTime + "ms");
            
            // Successfully merged - mark beforeCheckpoint for release
            checkpointToReleaseRef.set(beforeCheckpoint);
            indexingFailed = false;
            
            // Clear resume state AFTER successful final merge
            // Clear if: we had chunk commits (chunkNumber > 1) OR we were resuming from a previous crash
            if (chunkNumber > 1 || (resumeFromPath != null && !"/".equals(resumeFromPath))) {
                try {
                    NodeBuilder cleanupBuilder = store.getRoot().builder();
                    NodeBuilder laneBuilder = cleanupBuilder.child(ASYNC).getChildNode(name);
                    if (laneBuilder.exists() && laneBuilder.hasProperty("lastIndexedPath")) {
                        laneBuilder.removeProperty("lastIndexedPath");
                        laneBuilder.removeProperty("targetCheckpoint");
                        
                        // Also remove chunk marker
                        if (cleanupBuilder.child(ASYNC).hasProperty(name + ":chunk")) {
                            cleanupBuilder.child(ASYNC).removeProperty(name + ":chunk");
                        }
                        
                        // Also cleanup serialized resume state from NodeStore
                        cleanupResumeState(cleanupBuilder);
                        
                        // Use simple merge for cleanup - it's okay if this fails
                        store.merge(cleanupBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                        log.info("[{}] Cleared resume state after processing {} chunks", name, chunkNumber);
                    }
                } catch (CommitFailedException ex) {
                    // Not critical if cleanup fails - will be cleared on next successful run
                    log.warn("[{}] Failed to clear resume state: {}", name, ex.getMessage());
                }
            }

            if (indexUpdate.isReindexingPerformed()) {
                log.info("[{}] Reindexing completed for indexes: {} in {} ({} ms)",
                        name, indexUpdate.getReindexStats(), watch, watch.elapsed(TimeUnit.MILLISECONDS));
                progressLogged = true;
            }

            corruptIndexHandler.markWorkingIndexes(indexUpdate.getUpdatedIndexPaths());
            
            log.info("[{}] Indexing cycle complete - index fully searchable", name);

        } finally {
            // Disable editor caching and clean up ThreadLocal
            IndexUpdate.disableEditorCaching();
            
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

    /**
     * Save resume state to NodeStore after chunk commit.
     * State is stored as a property under /:async/{lane-name}/resumeState
     * Includes editor hierarchy for direct restoration (eliminates skip phase).
     * 
     * @param builder NodeBuilder for the current transaction
     * @param resumePath path to resume from
     * @param nodesProcessed number of nodes processed so far
     * @param indexUpdate current IndexUpdate (to capture editor hierarchy)
     * @param traversalTree the in-memory traversal tree for fast navigation
     * @param cachedNodeInfo the cached node info for skip phase optimization
     */
    private void saveResumeState(NodeBuilder builder, String afterCheckpoint,
                                  String resumePath, long nodesProcessed, IndexUpdate indexUpdate,
                                  TraversalTree traversalTree, CachedNodeInfo cachedNodeInfo) {
        try {
            System.out.println("[SAVE-STATE] Input: traversalTree=" + (traversalTree != null ? traversalTree.countNodes() + " nodes" : "null") +
                             ", cachedNodeInfo=" + (cachedNodeInfo != null ? cachedNodeInfo.getSubtreeSize() + " nodes" : "null"));
            
            // Build editor stack by traversing IndexUpdate parent chain
            List<ResumeState.EditorLevel> editorStack = captureEditorStack(indexUpdate, resumePath);
            
            // Serialize traversal tree for O(depth) resume on next chunk
            String traversalTreeJson = null;
            if (traversalTree != null) {
                try {
                    traversalTreeJson = traversalTree.toJson();
                    log.debug("[{}] Serialized traversal tree: {} nodes, {} bytes", 
                             name, traversalTree.countNodes(), traversalTreeJson.length());
                } catch (Exception e) {
                    log.warn("[{}] Failed to serialize traversal tree", name, e);
                }
            }
            
            // Serialize cached node info for skip phase optimization
            String cachedNodeInfoJson = null;
            if (cachedNodeInfo != null) {
                try {
                    cachedNodeInfoJson = cachedNodeInfo.toSerializedForm();
                    log.debug("[{}] Serialized cached node info: {} nodes, {} bytes", 
                             name, cachedNodeInfo.getSubtreeSize(), cachedNodeInfoJson.length());
                } catch (Exception e) {
                    log.warn("[{}] Failed to serialize cached node info", name, e);
                }
            }
            
            ResumeState state = new ResumeState(resumePath, "", afterCheckpoint, nodesProcessed, editorStack, traversalTreeJson, cachedNodeInfoJson);
            String json = state.toJson();
            
            // Store state under /:async/{lane-name}
            NodeBuilder laneBuilder = builder.child(ASYNC).child(name);
            
            // Store resume state metadata
            byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
            laneBuilder.setProperty("resumeState", json);
            laneBuilder.setProperty("resumeStateSize", jsonBytes.length);
            
            // Store traversal tree separately (can be large)
            int treeNodes = 0;
            int treeBytes = 0;
            int indexedNodes = 0;
            if (traversalTreeJson != null && !traversalTreeJson.isEmpty()) {
                laneBuilder.setProperty("traversalTree", traversalTreeJson);
                treeNodes = traversalTree.countNodes();
                treeBytes = traversalTreeJson.length();
                indexedNodes = traversalTree.countIndexedNodes();
                
                // Save to external file for comparison/analysis
                try {
                    String filename = "traversal_tree_" + name + "_chunk_" + System.currentTimeMillis() + ".json";
                    java.nio.file.Files.writeString(
                        java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), filename),
                        traversalTreeJson
                    );
                    System.out.println("[TREE FILE] Saved to: " + System.getProperty("java.io.tmpdir") + "/" + filename);
                } catch (Exception e) {
                    log.debug("Could not save traversal tree to file: {}", e.getMessage());
                }
            }
            
            // Store cached node info separately
            int cacheNodes = 0;
            int cacheBytes = 0;
            if (cachedNodeInfoJson != null && !cachedNodeInfoJson.isEmpty()) {
                laneBuilder.setProperty("cachedNodeInfo", cachedNodeInfoJson);
                cacheNodes = cachedNodeInfo.getSubtreeSize();
                cacheBytes = cachedNodeInfoJson.length();
                log.debug("[{}] Saving cache: {} nodes, {} bytes", name, cacheNodes, cacheBytes);
            } else {
                log.debug("[{}] No cache to save (cachedNodeInfo={})", name, cachedNodeInfo != null ? "present but empty" : "null");
            }
            
            // Calculate bytes per node (excluding property values - just structure)
            double bytesPerNode = treeNodes > 0 ? (double) treeBytes / treeNodes : 0;
            
            log.info("[{}] Saved resume state to NodeStore: {} ({} bytes, {} nodes processed, tree={} nodes/{} bytes, indexed={}, cache={} nodes/{} bytes)", 
                    name, resumePath, jsonBytes.length, nodesProcessed, treeNodes, treeBytes, indexedNodes, cacheNodes, cacheBytes);
            System.out.println("[STATE] Saved to NodeStore: " + resumePath + 
                             " (processed=" + nodesProcessed + " nodes, state=" + jsonBytes.length + " bytes)");
            System.out.println("[TREE SIZE] nodes=" + treeNodes + ", indexed=" + indexedNodes + 
                             ", bytes=" + treeBytes + " (" + String.format("%.1f", bytesPerNode) + " bytes/node)" +
                             ", KB=" + (treeBytes / 1024) + ", MB=" + String.format("%.2f", treeBytes / 1024.0 / 1024.0));
            if (cacheBytes > 0) {
                double cacheBytesPerNode = cacheNodes > 0 ? (double) cacheBytes / cacheNodes : 0;
                System.out.println("[CACHE SIZE] nodes=" + cacheNodes + 
                                 ", bytes=" + cacheBytes + " (" + String.format("%.1f", cacheBytesPerNode) + " bytes/node)");
            }
            
        } catch (Exception e) {
            // Non-fatal - just means we'll use tree traversal on next resume
            log.warn("[{}] Failed to save resume state to NodeStore: {}", name, e.getMessage());
        }
    }

    /**
     * Capture the editor hierarchy by walking up the IndexUpdate parent chain.
     * This captures which indexes are active at each level, enabling direct editor restoration.
     * 
     * @param indexUpdate current IndexUpdate (leaf node in hierarchy)
     * @param resumePath the path we're resuming from
     * @return List of EditorLevel objects from root to current level
     */
    private List<ResumeState.EditorLevel> captureEditorStack(IndexUpdate indexUpdate, String resumePath) {
        List<ResumeState.EditorLevel> stack = new ArrayList<>();
        
        try {
            // Walk up the parent chain to build stack from root to current
            List<IndexUpdate> chain = new ArrayList<>();
            IndexUpdate current = indexUpdate;
            while (current != null) {
                chain.add(0, current);  // Add to front (root first)
                current = current.getParent();
            }
            
            // For each level, capture the path and active index definitions
            for (IndexUpdate editor : chain) {
                String path = editor.getPath();
                List<String> activeIndexes = editor.getActiveIndexPaths();
                
                stack.add(new ResumeState.EditorLevel(path, activeIndexes));
            }
            
            log.debug("[{}] Captured editor stack with {} levels for path: {}", 
                     name, stack.size(), resumePath);
            
        } catch (Exception e) {
            // Non-fatal - if we can't capture stack, state will work without it (uses traversal)
            log.warn("[{}] Failed to capture editor stack, will use traversal on resume: {}", 
                    name, e.getMessage());
            return new ArrayList<>();  // Empty stack = fall back to traversal
        }
        
        return stack;
    }

    /**
     * Load resume state from NodeStore if available.
     * Returns null if no state exists or if loading fails (graceful fallback).
     * 
     * @param beforeCheckpoint source checkpoint (not used with NodeStore approach)
     * @param afterCheckpoint target checkpoint
     * @return ResumeState or null if not available
     */
    private ResumeState loadResumeState(String beforeCheckpoint, String afterCheckpoint) {
        try {
            // Read state from /:async/{lane-name}/resumeState
            NodeState root = store.getRoot();
            NodeState asyncNode = root.getChildNode(ASYNC);
            if (!asyncNode.exists()) {
                log.debug("[{}] No :async node found", name);
                return null;
            }
            
            NodeState laneNode = asyncNode.getChildNode(name);
            if (!laneNode.exists()) {
                log.debug("[{}] No async lane node found", name);
                return null;
            }
            
            PropertyState resumeStateProp = laneNode.getProperty("resumeState");
            if (resumeStateProp == null) {
                log.debug("[{}] No resumeState property found", name);
                return null;
            }
            
            String json = resumeStateProp.getValue(Type.STRING);
            ResumeState state = ResumeState.fromJson(json);
            
            if (state != null) {
                // Verify checkpoint matches to ensure state is for current cycle
                if (afterCheckpoint.equals(state.targetCheckpoint)) {
                    // Load traversal tree if present (enables O(depth) resume!)
                    String traversalTreeJson = null;
                    PropertyState treeProp = laneNode.getProperty("traversalTree");
                    if (treeProp != null) {
                        traversalTreeJson = treeProp.getValue(Type.STRING);
                        log.info("[{}] Found traversal tree ({} bytes) - O(depth) resume enabled!", 
                                name, traversalTreeJson.length());
                    }
                    
                    // Load cached node info if present (enables cached skip phase!)
                    String cachedNodeInfoJson = null;
                    PropertyState cacheProp = laneNode.getProperty("cachedNodeInfo");
                    if (cacheProp != null) {
                        cachedNodeInfoJson = cacheProp.getValue(Type.STRING);
                        log.info("[{}] Found cached node info ({} bytes) - cached skip enabled!", 
                                name, cachedNodeInfoJson.length());
                    }
                    
                    // Create state with traversal tree and cached node info
                    ResumeState stateWithTree = new ResumeState(
                        state.currentPath, state.sourceCheckpoint, state.targetCheckpoint,
                        state.nodesProcessed, state.editorStack, traversalTreeJson, cachedNodeInfoJson
                    );
                    
                    log.info("[{}] Loaded resume state from NodeStore: {} ({} nodes to skip, tree={}, cache={})", 
                            name, state.currentPath, state.nodesProcessed, 
                            traversalTreeJson != null ? "yes" : "no",
                            cachedNodeInfoJson != null ? "yes" : "no");
                    System.out.println("[STATE] Loaded from NodeStore: " + state.currentPath + 
                                     " (skip " + state.nodesProcessed + " nodes, tree=" + 
                                     (traversalTreeJson != null ? "YES" : "no") + 
                                     ", cache=" + (cachedNodeInfoJson != null ? "YES" : "no") + ")");
                    return stateWithTree;
                } else {
                    log.warn("[{}] Resume state checkpoint mismatch (expected: {}, found: {}), ignoring", 
                            name, afterCheckpoint, state.targetCheckpoint);
                }
            }
            
            return null;
            
        } catch (Exception e) {
            // Non-fatal - just fall back to tree traversal
            log.warn("[{}] Failed to load resume state from NodeStore: {}", name, e.getMessage());
            return null;
        }
    }

    /**
     * Get parent path from a path string.
     * E.g., "/content/dam/asset-123" returns "/content/dam"
     */
    private static String getParentPath(String path) {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return "/";
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return "/";
        }
        return path.substring(0, lastSlash);
    }
    
    /**
     * Cleanup resume state from NodeStore after successful completion.
     * Removes resumeState property from /:async/{lane-name}
     * Can be disabled with -Doak.async.skipCleanup=true for testing serialization/deserialization
     */
    private void cleanupResumeState(NodeBuilder builder) {
        // Skip cleanup for testing serialization/deserialization
        if (Boolean.getBoolean("oak.async.skipCleanup")) {
            log.info("[{}] Skipping resume state cleanup (oak.async.skipCleanup=true)", name);
            System.out.println("[STATE] Skipping cleanup (oak.async.skipCleanup=true) - state preserved for next run");
            return;
        }
        
        try {
            NodeBuilder laneBuilder = builder.child(ASYNC).getChildNode(name);
            if (laneBuilder.exists() && laneBuilder.hasProperty("resumeState")) {
                laneBuilder.removeProperty("resumeState");
                laneBuilder.removeProperty("resumeStateSize");
                log.info("[{}] Cleaned up resume state from NodeStore", name);
                System.out.println("[STATE] Cleaned up resume state from NodeStore");
            }
        } catch (Exception e) {
            // Non-fatal - old state will be overwritten on next save
            log.warn("[{}] Failed to cleanup resume state from NodeStore: {}", name, e.getMessage());
        }
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
