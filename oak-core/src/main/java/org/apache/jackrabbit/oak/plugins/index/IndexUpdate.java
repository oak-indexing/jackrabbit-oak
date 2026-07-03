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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_REINDEX_VALUE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_NRT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_SYNC;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getAsyncLaneName;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexCommitCallback.IndexProgress;
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback.PathSource;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndex;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndexMerger;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.resume.PathTree;
import org.apache.jackrabbit.oak.plugins.index.resume.ResumeContext;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCountEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.TraversalRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.upgrade.IndexDisabler;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.util.ISO8601;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexUpdate implements Editor, PathSource {

    private static final Logger log = LoggerFactory.getLogger(IndexUpdate.class);
    private static final String TYPE_ELASTICSEARCH = "elasticsearch";

    //This is used so that wrong index definitions are sparsely logged. After every 1000 indexing cycles, index definitions
    // with wrong nodetype will be logged.
    public static final long INDEX_JCR_TYPE_INVALID_LOG_LIMITER = Long.parseLong(System.getProperty("oak.indexer.indexJcrTypeInvalidLogLimiter", "1000"));

    // Initial value is set at indexJcrTypeInvalidLogLimiter so that first logging start on first cycle/update itself.
    // This counter is cyclically incremented till indexJcrTypeInvalidLogLimiter and then reset to 0
    private static volatile long cyclicExecutionCount = INDEX_JCR_TYPE_INVALID_LOG_LIMITER;

    // Warnings about missing index providers are rate limited, so the log file is not filled with them.
    // this is the last time such a message was logged (if any).
    private static final AtomicLong lastMissingProviderMessageTime = new AtomicLong();

    // Names of diff index nodes (e.g. "diff.index", "diff.index.optimizer") for which a
    // detection message has already been logged in this JVM. They legitimately use
    // jcr:primaryType=nt:unstructured, so their presence is reported once at INFO.
    private static final Set<String> diffIndexesDetected = ConcurrentHashMap.newKeySet();

    static void resetDiffIndexesDetectedForTest() {
        diffIndexesDetected.clear();
    }

    /**
     * <p>
     * The value of this flag determines the behavior of the IndexUpdate when
     * dealing with {@code reindex} flags.
     * <p>
     * If {@code false} (default value), the indexer will start reindexing
     * immediately in the current thread, blocking a commit until this operation
     * is done.
     * <p>
     * If {@code true}, the indexer will ignore the flag, therefore ignoring any
     * reindex requests.
     * <p>
     * This is only provided as a support tool (see OAK-3505) so it should be
     * used with extreme caution!
     */
    static final boolean IGNORE_REINDEX_FLAGS = Boolean
            .getBoolean("oak.indexUpdate.ignoreReindexFlags");

    static {
        if (IGNORE_REINDEX_FLAGS) {
            log.warn("Reindexing is disabled by configuration. This value is configurable via the 'oak.indexUpdate.ignoreReindexFlags' system property.");
        }
    }

    private final NodeStore store;

    private final IndexUpdateRootState rootState;

    private final NodeBuilder builder;

    /** Parent updater, or {@code null} if this is the root updater. */
    private final IndexUpdate parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /**
     * Editors for indexes that will be normally updated.
     */
    private final List<Editor> editors = new ArrayList<>();

    /**
     * Editors for indexes that need to be re-indexed.
     */
    private final Map<String, Editor> reindex = new HashMap<>();
    
    // ============================================================
    // Skip Mode Support - enables O(depth) resume instead of O(n)
    // ============================================================
    
    /** 
     * Skip mode flag - when true, collectIndexEditors() is deferred.
     * This dramatically speeds up resume by avoiding NodeStore reads.
     */
    private boolean skipMode = false;
    
    /** Deferred before state for lazy initialization */
    private NodeState deferredBefore;
    
    /** Deferred after state for lazy initialization */
    private NodeState deferredAfter;
    
    /** Whether this editor has been fully initialized */
    private boolean fullyInitialized = false;

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback) {
        this(provider, async, root, builder, updateCallback, CommitInfo.EMPTY);
    }

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback, CommitInfo commitInfo) {
        this(provider, async, root, builder, updateCallback, NodeTraversalCallback.NOOP, commitInfo, CorruptIndexHandler.NOOP);
    }

    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback, NodeTraversalCallback traversalCallback,
            CommitInfo commitInfo, CorruptIndexHandler corruptIndexHandler) {
        this(provider, async, root, builder, updateCallback, traversalCallback, commitInfo, corruptIndexHandler, null, null);
    }

    /**
     * Constructor with ResumeContext for resumable indexing.
     *
     * @param provider the index editor provider
     * @param async async lane name
     * @param root the root node state
     * @param builder the node builder
     * @param updateCallback callback for index updates
     * @param traversalCallback callback for node traversal
     * @param commitInfo commit info
     * @param corruptIndexHandler handler for corrupt indexes
     * @param resumeContext context for resumable indexing (can be null)
     * @param store node store used by the diff-index optimization (can be null)
     */
    public IndexUpdate(
            IndexEditorProvider provider, String async,
            NodeState root, NodeBuilder builder,
            IndexUpdateCallback updateCallback, NodeTraversalCallback traversalCallback,
            CommitInfo commitInfo, CorruptIndexHandler corruptIndexHandler,
            @Nullable ResumeContext resumeContext, @Nullable NodeStore store) {
        this.store = store;
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.rootState = new IndexUpdateRootState(provider, async, root, builder, updateCallback, traversalCallback, commitInfo, corruptIndexHandler, resumeContext);
        this.builder = requireNonNull(builder);
        
        // If we have a resume context and it's in skip mode, start in skip mode
        if (resumeContext != null && resumeContext.isInSkipMode()) {
            this.skipMode = true;
        }
    }

    private IndexUpdate(IndexUpdate parent, String name) {
        this.store = parent.store;
        this.parent = requireNonNull(parent);
        this.name = name;
        this.rootState = parent.rootState;
        this.builder = parent.builder.getChildNode(requireNonNull(name));
        // Inherit skip mode from parent
        this.skipMode = parent.skipMode;
    }

    // Static counters for tracking skip statistics
    private static final java.util.concurrent.atomic.AtomicInteger skipFullCount = new java.util.concurrent.atomic.AtomicInteger(0);
    private static final java.util.concurrent.atomic.AtomicInteger skipIndexedCount = new java.util.concurrent.atomic.AtomicInteger(0);
    private static final java.util.concurrent.atomic.AtomicInteger processedCount = new java.util.concurrent.atomic.AtomicInteger(0);
    
    public static void resetSkipCounters() {
        skipFullCount.set(0);
        skipIndexedCount.set(0);
        processedCount.set(0);
    }
    
    public static String getSkipStats() {
        return "skipFull=" + skipFullCount.get() + ", skipIndexed=" + skipIndexedCount.get() + 
               ", processed=" + processedCount.get();
    }
    
    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        // OPTIMIZATION: Check if this node is FULLY PROCESSED (both enter and leave completed)
        // If so, we can skip the expensive nodeRead() call entirely, avoiding NodeStore access
        ResumeContext ctx = rootState.getResumeContext();
        if (ctx != null) {
            String currentPath = getPath();
            
            // IMPORTANT: Only skip nodes that are FULLY PROCESSED (leaveCompleted=true)
            // This ensures:
            // 1. Properties were processed (they happen after enter, before leave)
            // 2. All child nodes were traversed
            // 3. The Lucene document was actually created
            //
            // We do NOT skip on just isIndexed() anymore because:
            // - enterCompleted alone doesn't guarantee properties were processed
            // - Nodes interrupted mid-processing need to be re-processed
            
            if (ctx.getPathTree().isFullyProcessed(currentPath)) {
                // Node fully processed in previous run - skip completely
                skipFullCount.incrementAndGet();
                log.trace("[SKIP-FULL] Path {} fully processed (enter+leave), skipping entirely", currentPath);
                this.skipMode = true;
                return;
            }
            
            // Check if only enterCompleted (enter done but leave never called - interrupted?)
            // These nodes need to be re-processed because their properties may not have been indexed
            if (ctx.getPathTree().isEnterCompleted(currentPath)) {
                // Enter was called but leave wasn't - this node was interrupted mid-processing
                // We still need to process it, but log for debugging
                skipIndexedCount.incrementAndGet();
                log.trace("[RE-PROCESS] Path {} has enterCompleted but not leaveCompleted, re-processing", currentPath);
                // DON'T set skipMode - let this node be fully processed
            }
        }
        
        // Node is NOT processed - call nodeRead() for counting/traversal tracking
        processedCount.incrementAndGet();
        rootState.nodeRead(this);
        
        // Node is NOT indexed - reset skipMode (might have been inherited from parent)
        // This is critical: even if parent was skipped, this node needs to be processed
        this.skipMode = false;
        
        // Full initialization - needed for editors to be set up
        performFullInitialization(before, after);
        
        // Mark enter completed in PathTree
        // NOTE: We do NOT mark as indexed here anymore because:
        // 1. "indexed" should mean properties have been processed
        // 2. Properties are processed AFTER enter() by EditorDiff
        // 3. Marking as indexed here caused nodes to be skipped before their properties were indexed
        // 4. The node will be marked as indexed in leave() via markLeaveCompleted()
        if (ctx != null) {
            PathTree pathTree = ctx.getPathTree();
            pathTree.markEnterCompleted(getPath());
        }
    }
    
    /**
     * Get the ResumeContext from the root state.
     * 
     * @return the resume context, or null if not set
     */
    @Nullable
    public ResumeContext getResumeContext() {
        return rootState.getResumeContext();
    }
    
    /**
     * Check if this editor is in skip mode.
     */
    public boolean isInSkipMode() {
        return skipMode || rootState.isInSkipMode();
    }
    
    /**
     * Perform full initialization including collectIndexEditors.
     * Called either directly from enter() or deferred from activateFromSkipMode().
     */
    private void performFullInitialization(NodeState before, NodeState after) 
            throws CommitFailedException {
        if (fullyInitialized) {
            return; // Already initialized
        }
        
        collectIndexEditors(builder.getChildNode(INDEX_DEFINITIONS_NAME), before);

        if (!reindex.isEmpty()) {
            log.info("Reindexing will be performed for following indexes: {}",
                    reindex.keySet());
            rootState.progressReporter.reindexingTraversalStart(getPath());
        }

        // no-op when reindex is empty
        CommitFailedException exception = EditorDiff.process(
                VisibleEditor.wrap(wrapProgress(CompositeEditor.compose(List.copyOf(reindex.values())))),
                MISSING_NODE,
                after);
        rootState.progressReporter.reindexingTraversalEnd();
        if (exception != null) {
            throw exception;
        }

        for (Editor editor : editors) {
            editor.enter(before, after);
        }
        
        fullyInitialized = true;
    }
    
    // ============================================================
    // Skip Mode Control Methods
    // ============================================================
    
    /**
     * Enable skip mode for this editor and all children.
     * When skip mode is enabled, enter() defers expensive initialization.
     */
    public void setSkipMode(boolean skipMode) {
        this.skipMode = skipMode;
        if (skipMode) {
            log.debug("[SKIP] Skip mode enabled for path: {}", getPath());
        }
    }
    
    /**
     * Check if this editor is in skip mode.
     */
    public boolean isSkipMode() {
        return skipMode;
    }
    
    /**
     * Exit skip mode and perform deferred initialization.
     * Call this when the resume point is reached.
     */
    public void activateFromSkipMode() throws CommitFailedException {
        if (!skipMode) {
            return; // Not in skip mode
        }
        
        skipMode = false;
        
        if (!fullyInitialized && deferredBefore != null && deferredAfter != null) {
            log.info("[SKIP] Activating from skip mode at path: {}", getPath());
            performFullInitialization(deferredBefore, deferredAfter);
            // Clear deferred state
            deferredBefore = null;
            deferredAfter = null;
        }
    }

    public boolean isReindexingPerformed() {
        return !getReindexStats().isEmpty();
    }
    
    /**
     * Check if any reindexing is currently in progress.
     * Used to disable chunk limits during reindex.
     * Must check from root to find reindex state.
     */
    private boolean isReindexing() {
        // Check at root level - that's where reindex map is populated
        if (parent == null) {
            return !reindex.isEmpty();
        }
        // Traverse up to root
        IndexUpdate root = this;
        while (root.parent != null) {
            root = root.parent;
        }
        return !root.reindex.isEmpty();
    }

    public List<String> getReindexStats() {
        return rootState.progressReporter.getReindexStats();
    }

    public Set<String> getUpdatedIndexPaths() {
        return rootState.progressReporter.getUpdatedIndexPaths();
    }

    public void setTraversalRateEstimator(TraversalRateEstimator estimator) {
        rootState.progressReporter.setTraversalRateEstimator(estimator);
    }

    public void setNodeCountEstimator(NodeCountEstimator nodeCountEstimator) {
        rootState.progressReporter.setNodeCountEstimator(nodeCountEstimator);
    }

    public String getIndexingStats() {
        return rootState.getIndexingStats();
    }

    public void setIgnoreReindexFlags(boolean ignoreReindexFlag) {
        rootState.setIgnoreReindexFlags(ignoreReindexFlag);
    }

    private boolean shouldReindex(NodeBuilder definition, NodeState before, String name) {
        PropertyState type = definition.getProperty(TYPE_PROPERTY_NAME);

        // Do not attempt reindex of indexes with no type or disabled
        if (type == null || TYPE_DISABLED.equals(type.getValue(Type.STRING))) {
            return false;
        }

        // Async indexes are not considered for reindexing for sync indexing
        // Skip this check for elastic index
        // TODO : See if the check to skip elastic can be handled in a better way - maybe move isMatchingIndexNode to IndexDefinition ?
        if (!TYPE_ELASTICSEARCH.equals(type.getValue(Type.STRING)) && !isMatchingIndexMode(definition)) {
            return false;
        }

        PropertyState ps = definition.getProperty(REINDEX_PROPERTY_NAME);
        if (ps != null && ps.getValue(BOOLEAN)) {
            return !rootState.ignoreReindexFlags;
        }
        // reindex in the case this is a new node, even though the reindex flag
        // might be set to 'false' (possible via content import).
        // However, if its already indexed i.e. has some hidden nodes (containing hidden data)
        // then no need to reindex

        // WARNING: If there is _any_ hidden node, then it is assumed that
        // no reindex is needed. Even if the hidden node is completely unrelated
        // and doesn't contain index data (for example the node ":status").
        // See also OAK-7991.
        boolean result = !before.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name) && !hasAnyHiddenNodes(definition);
        // See OAK-9449
        // In case of elasticsearch, indexed data is stored remotely and not under hidden nodes, so in case of OutOfBand
        // indexing during content import, there is no hidden node created for elastic (not even :status)
        // So, we log a warning and return false to avoid unnecessary reindexing. The warning is  displayed only if
        // someone added the new index node and forgot to add
        // the reindex flag, in case OutOfBand Indexing has been performed, warning can be ignored.
        // Also, in case the new elastic node has been added with reindex = true , this method would have already returned true
        if (result && TYPE_ELASTICSEARCH.equals((type.getValue(Type.STRING)))) {
            log.warn("Found a new elastic index node [{}]. Please set the reindex flag = true to initiate reindexing." +
                    "Please ignore if OutOfBand Reindexing has already been performed.", name);
            return false;
        } else if (result) {
            log.info("Found a new index node [{}]. Reindexing is requested", name);
        }
        return result;
    }

    private static boolean hasAnyHiddenNodes(NodeBuilder builder) {
        for (String name : builder.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(name)) {
                NodeBuilder childNode = builder.getChildNode(name);
                if (childNode.getBoolean(IndexConstants.REINDEX_RETAIN)) {
                    continue;
                }
                return true;
            }
        }
        return false;
    }

    private void collectIndexEditors(NodeBuilder definitions, NodeState before) throws CommitFailedException {
        // OPTIMIZATION: Check if definitions node exists - skip if not
        // Most nodes (99%+) don't have :oak:index children
        if (!definitions.exists()) {
            return;  // No index definitions - nothing to collect
        }

        if (definitions.hasChildNode(DiffIndexMerger.DIFF_INDEX)
                && "disabled".equals(definitions.child(DiffIndexMerger.DIFF_INDEX).getString("type"))
                && rootState.async == null) {
            DiffIndex.applyDiffIndexChanges(store, definitions);
        }
        for (String name : definitions.getChildNodeNames()) {
            NodeBuilder definition = definitions.getChildNode(name);
            if (isIncluded(rootState.async, definition)) {
                String type = definition.getString(TYPE_PROPERTY_NAME);
                String primaryType = definition.getName(JcrConstants.JCR_PRIMARYTYPE);
                if (type == null) {
                    // probably not an index def
                    continue;
                }
                /*
                 Log a warning after every indexJcrTypeInvalidLogLimiter cycles of indexer where nodeState changed.
                 and skip further execution for invalid nodetype of index definition.
                 */
                if (!IndexConstants.INDEX_DEFINITIONS_NODE_TYPE.equals(primaryType)) {
                    if (DiffIndexMerger.DIFF_INDEX.equals(name)
                            || DiffIndexMerger.DIFF_INDEX_OPTIMIZER.equals(name)) {
                        if (diffIndexesDetected.add(name)) {
                            log.info("index {} is detected", name);
                        }
                        continue;
                    }
                    // It is a cyclic counter which reset back to 0 after INDEX_JCR_TYPE_INVALID_LOG_LIMITER
                    // This is to sparsely log this warning.
                    if ((cyclicExecutionCount >= INDEX_JCR_TYPE_INVALID_LOG_LIMITER)) {
                        log.warn("jcr:primaryType of index {} should be {} instead of {}", name, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, primaryType);
                        cyclicExecutionCount = 0;
                    }
                    cyclicExecutionCount++;
                    continue;
                }

                boolean shouldReindex = shouldReindex(definition, before, name);
                String indexPath = getIndexPath(getPath(), name);
                if (definition.hasProperty(IndexConstants.CORRUPT_PROPERTY_NAME) && !shouldReindex) {
                    String corruptSince = definition.getProperty(IndexConstants.CORRUPT_PROPERTY_NAME).getValue(Type.DATE);
                    rootState.corruptIndexHandler.skippingCorruptIndex(rootState.async, indexPath, ISO8601.parse(corruptSince));
                    continue;
                }

                Editor editor = null;
                try {
                    editor = rootState.provider.getIndexEditor(type, definition, rootState.root,
                            rootState.newCallback(indexPath, shouldReindex, getEstimatedCount(definition)));
                } catch (IllegalStateException e) {
                    // This will be caught here in case there is any config related error in the index definition
                    // where multiple values are assigned to a property that is supposed to be single valued
                    // We log an error message here and continue - this way the bad index defintion is ignored and doesn't block the async index update
                    log.error("Unable to get Index Editor for index at {} . Please correct the index definition " +
                            "and reindex after correction. Additional Info : {}", indexPath, e.getMessage(), e);
                    continue;
                }
                if (editor == null) {
                    // if this isn't an async cycle AND definition has "async" property
                    // (and implicitly isIncluded method allows async def in non-async cycle only for nrt/sync defs)
                    // then we don't need to handle missing handler
                    if (definition.hasProperty(ASYNC_PROPERTY_NAME) && rootState.async == null) {
                        if (!TYPE_DISABLED.equals(type)) {
                            long silenceMessagesSeconds = 60;
                            long silenceMessagesNanos = silenceMessagesSeconds * 1_000_000_000;
                            long now = System.nanoTime();
                            long last = lastMissingProviderMessageTime.get();
                            if (now > last + silenceMessagesNanos
                                    && lastMissingProviderMessageTime.compareAndSet(last, now)) {
                                log.warn("Missing provider for nrt/sync index: {}. " +
                                        "Please note, it means that index data should be trusted only after this index " +
                                        "is processed in an async indexing cycle. " +
                                        "This message is silenced for {} seconds.", indexPath, silenceMessagesSeconds);
                            }
                        }
                    } else {
                        rootState.missingProvider.onMissingIndex(type, definition, indexPath);
                    }
                } else if (shouldReindex) {
                    if (definition.getBoolean(REINDEX_ASYNC_PROPERTY_NAME)
                            && definition.getString(ASYNC_PROPERTY_NAME) == null) {
                        // switch index to an async update mode
                        definition.setProperty(ASYNC_PROPERTY_NAME,
                                ASYNC_REINDEX_VALUE);
                    } else {
                        definition.setProperty(REINDEX_PROPERTY_NAME, false);
                        incrementReIndexCount(definition);
                        removeIndexState(definition);

                        clearCorruptFlag(definition, indexPath);
                        reindex.put(concat(getPath(), INDEX_DEFINITIONS_NAME, name), editor);
                    }

                    rootState.indexDisabler.markDisableFlagIfRequired(indexPath, definition);
                } else {
                    // not async index OR we're indexing in async mode
                    if (getAsyncLaneName(definition.getNodeState(), indexPath) == null || rootState.async != null) {
                        rootState.indexDisabler.disableOldIndexes(indexPath, definition);
                    }
                    editors.add(editor);
                }
            }
        }
    }

    private void removeIndexState(NodeBuilder definition) {
        // as we don't know the index content node name
        // beforehand, we'll remove all child nodes
        for (String rm : definition.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(rm)) {
                NodeBuilder childNode = definition.getChildNode(rm);
                if (!childNode.getBoolean(IndexConstants.REINDEX_RETAIN)) {
                    NodeBuilder child = definition.getChildNode(rm);
                    if (child instanceof ReadOnlyBuilder) {
                        log.debug("Preserve read-only child node on reindex: " + rm);
                    } else {
                        child.remove();
                    }
                }
            }
        }
    }

    private long getEstimatedCount(NodeBuilder indexDefinition) {
        //TODO Implement the estimate
        return -1;
    }

    static boolean isIncluded(String asyncRef, NodeBuilder definition) {
        if (definition.hasProperty(ASYNC_PROPERTY_NAME)) {
            PropertyState p = definition.getProperty(ASYNC_PROPERTY_NAME);
            Iterable<String> opt = p.getValue(Type.STRINGS);
            if (asyncRef == null) {
                // sync index job, accept synonyms
                return IterableUtils.contains(opt, INDEXING_MODE_NRT) || IterableUtils.contains(opt, INDEXING_MODE_SYNC);
            } else {
                return IterableUtils.contains(opt, asyncRef);
            }
        } else {
            return asyncRef == null;
        }
    }

    /**
     * Determines if the current indexing mode matches with the IndexUpdate mode.
     * For this match it only considers indexes either as
     * <ul>
     *     <li>sync - Index definition does not have async property defined</li>
     *     <li>async - Index definition has async property defined. It does not matter what its value is</li>
     * </ul>
     *
     * <p>Same applies for IndexUpdate also.
     *
     * <p>Note that this differs from #isIncluded which also considers the value of <code>async</code>
     * property to determine if the index should be selected for current IndexUpdate run.
     */
    private boolean isMatchingIndexMode(NodeBuilder definition) {
        boolean async = definition.hasProperty(ASYNC_PROPERTY_NAME);
        //Either
        // 1. async index and async index update
        // 2. non async i.e. sync index and sync index update
        return async == rootState.isAsync();
    }

    private void incrementReIndexCount(NodeBuilder definition) {
        long count = 0;
        if (definition.hasProperty(REINDEX_COUNT)) {
            count = definition.getProperty(REINDEX_COUNT).getValue(Type.LONG);
        }
        definition.setProperty(REINDEX_COUNT, count + 1);
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    @Override
    public String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    /**
     * Returns the parent IndexUpdate, or null if this is the root.
     * Used for capturing editor hierarchy for resume state.
     */
    public IndexUpdate getParent() {
        return parent;
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        // CRITICAL: Always mark node as leave-completed in PathTree for proper resume tracking
        // This must happen even in skipMode to ensure fullyProcessed state is accurate
        ResumeContext ctx = rootState.getResumeContext();
        
        try {
            // If this node was already indexed (skipMode), skip actual editor processing
            if (skipMode) {
                log.trace("[SKIP-LEAVE] Skipping leave at {} (already indexed/processed)", getPath());
                return;
            }
            
            for (Editor editor : editors) {
                editor.leave(before, after);
            }
        } finally {
            // Mark this node as FULLY PROCESSED in PathTree ALWAYS
            // This must happen:
            // 1. Even if skipMode=true (to mark already-indexed nodes as leave-completed)
            // 2. Even if CHUNK_COMPLETE exception is thrown (for proper resume)
            if (ctx != null) {
                // Mark leave completed - this also marks as indexed
                ctx.getPathTree().markLeaveCompleted(getPath());
                
                // Debug: log progress periodically
                int fullyProcessed = ctx.getPathTree().getFullyProcessedCount();
                if (fullyProcessed % 1000 == 0 && fullyProcessed > 0) {
                    log.debug("[INDEX] Fully processed {} nodes, current path: {}", fullyProcessed, getPath());
                }
            }
        }
        
        // NOTE: Chunk limits are handled by AsyncUpdateCallback.traversedNode(), not here
        // This keeps the IndexUpdate clean and lets AsyncIndexUpdate control chunking

        if (parent == null) {
            rootState.progressReporter.logReport();
        }
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        rootState.propertyChanged(after.getName());
        
        // OPTIMIZATION: Skip property processing if this node was already indexed
        // This prevents duplicate Lucene document creation during resume
        if (skipMode) {
            log.trace("[SKIP-PROP] Skipping propertyAdded at {} (already indexed)", getPath());
            return;
        }
        
        for (Editor editor : editors) {
            editor.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        rootState.propertyChanged(before.getName());
        
        // OPTIMIZATION: Skip property processing if this node was already indexed
        if (skipMode) {
            log.trace("[SKIP-PROP] Skipping propertyChanged at {} (already indexed)", getPath());
            return;
        }
        
        for (Editor editor : editors) {
            editor.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        rootState.propertyChanged(before.getName());
        
        // OPTIMIZATION: Skip property processing if this node was already indexed
        if (skipMode) {
            log.trace("[SKIP-PROP] Skipping propertyDeleted at {} (already indexed)", getPath());
            return;
        }
        
        for (Editor editor : editors) {
            editor.propertyDeleted(before);
        }
    }

    @Override @NotNull
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        List<Editor> children = new ArrayList<>(1 + editors.size());
        children.add(new IndexUpdate(this, name));
        for (Editor editor : editors) {
            Editor child = editor.childNodeAdded(name, after);
            if (child != null) {
                children.add(child);
            }
        }
        return CompositeEditor.compose(children);
    }

    @Override @NotNull
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        List<Editor> children = new ArrayList<>(1 + editors.size());
        children.add(new IndexUpdate(this, name));
        for (Editor editor : editors) {
            Editor child = editor.childNodeChanged(name, before, after);
            if (child != null) {
                children.add(child);
            }
        }
        return CompositeEditor.compose(children);
    }

    @Override @Nullable
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        List<Editor> children = new ArrayList<>(editors.size());
        for (Editor editor : editors) {
            Editor child = editor.childNodeDeleted(name, before);
            if (child != null) {
                children.add(child);
            }
        }
        return CompositeEditor.compose(children);
    }

    public void commitProgress(IndexProgress indexProgress) {
        rootState.commitProgress(indexProgress);
    }

    protected Set<String> getReindexedDefinitions() {
        return rootState.progressReporter.getReindexedIndexPaths();
    }

    private void clearCorruptFlag(NodeBuilder definition, String indexPath) {
        PropertyState corrupt = definition.getProperty(IndexConstants.CORRUPT_PROPERTY_NAME);
        //Remove any corrupt property
        if (corrupt != null) {
            definition.removeProperty(IndexConstants.CORRUPT_PROPERTY_NAME);
            log.info("Removing corrupt flag from index [{}] which has been marked " +
                    "as corrupt since [{}]", indexPath, corrupt.getValue(Type.DATE));
        }
    }

    private static String getIndexPath(String path, String indexName) {
        if (PathUtils.denotesRoot(path)) {
            return "/" + INDEX_DEFINITIONS_NAME + "/" + indexName;
        }
        return path + "/" + INDEX_DEFINITIONS_NAME + "/" + indexName;
    }

    private Editor wrapProgress(Editor editor) {
        return rootState.progressReporter.wrapProgress(editor);
    }

    public static class MissingIndexProviderStrategy {

        /**
         * The value of this flag determines the behavior of
         * {@link #onMissingIndex(String, NodeBuilder, String)}. If
         * {@code false} (default value), the method will set the
         * {@code reindex} flag to true and log a warning. if {@code true}, the
         * method will throw a {@link CommitFailedException} failing the commit.
         */
        private boolean failOnMissingIndexProvider = Boolean
                .getBoolean("oak.indexUpdate.failOnMissingIndexProvider");

        private final Set<String> ignore = Set.of("disabled", "ordered");

        public void onMissingIndex(String type, NodeBuilder definition, String indexPath)
                throws CommitFailedException {
            if (isDisabled(type)) {
                return;
            }
            // trigger reindexing when an indexer becomes available
            PropertyState ps = definition.getProperty(REINDEX_PROPERTY_NAME);
            if (ps != null && ps.getValue(BOOLEAN)) {
                // already true, skip the update
                return;
            }

            if (failOnMissingIndexProvider) {
                throw new CommitFailedException("IndexUpdate", 1,
                        "Missing index provider detected for type [" + type
                                + "] on index [" + indexPath + "]");
            } else {
                log.warn(
                        "Missing index provider of type [{}], requesting reindex on [{}]",
                        type, indexPath);
                definition.setProperty(REINDEX_PROPERTY_NAME, true);
            }
        }

        boolean isDisabled(String type) {
            return ignore.contains(type);
        }

        void setFailOnMissingIndexProvider(boolean failOnMissingIndexProvider) {
            this.failOnMissingIndexProvider = failOnMissingIndexProvider;
        }
    }

    public IndexUpdate withMissingProviderStrategy(
            MissingIndexProviderStrategy missingProvider) {
        rootState.setMissingProvider(missingProvider);
        return this;
    }

    private static final class IndexUpdateRootState {
        final IndexEditorProvider provider;
        final String async;
        final NodeState root;
        final CommitInfo commitInfo;
        final IndexDisabler indexDisabler;
        private boolean ignoreReindexFlags = IGNORE_REINDEX_FLAGS;
        final Set<IndexCommitCallback> indexCommitCallbacks = SetUtils.newIdentityHashSet();
        final CorruptIndexHandler corruptIndexHandler;
        final IndexingProgressReporter progressReporter;
        private int changedNodeCount;
        private int changedPropertyCount;
        private MissingIndexProviderStrategy missingProvider = new MissingIndexProviderStrategy();
        
        /** ResumeContext for resumable indexing (can be null) */
        @Nullable
        final ResumeContext resumeContext;

        private IndexUpdateRootState(IndexEditorProvider provider, String async, NodeState root,
                                     NodeBuilder builder, IndexUpdateCallback updateCallback,
                                     NodeTraversalCallback traversalCallback,
                                     CommitInfo commitInfo, CorruptIndexHandler corruptIndexHandler,
                                     @Nullable ResumeContext resumeContext) {
            this.provider = requireNonNull(provider);
            this.async = async;
            this.root = requireNonNull(root);
            this.commitInfo = commitInfo;
            this.corruptIndexHandler = corruptIndexHandler;
            this.indexDisabler = new IndexDisabler(builder);
            this.progressReporter = new IndexingProgressReporter(updateCallback, traversalCallback);
            this.resumeContext = resumeContext;
        }
        
        /**
         * Check if we're in skip mode (traversing to resume point).
         */
        public boolean isInSkipMode() {
            return resumeContext != null && resumeContext.isInSkipMode();
        }
        
        /**
         * Get the resume context.
         */
        @Nullable
        public ResumeContext getResumeContext() {
            return resumeContext;
        }

        public IndexUpdateCallback newCallback(String indexPath, boolean reindex, long estimatedCount) {
            progressReporter.registerIndex(indexPath, reindex, estimatedCount);
            return new ReportingCallback(indexPath, reindex);
        }

        public boolean isAsync() {
            return async != null;
        }

        public void nodeRead(PathSource pathSource) throws CommitFailedException {
            changedNodeCount++;
            progressReporter.traversedNode(pathSource);
        }

        public void propertyChanged(String name) {
            changedPropertyCount++;
        }

        public String getIndexingStats() {
            return String.format("changedNodeCount %d, changedPropertyCount %d",
                    changedNodeCount, changedPropertyCount);
        }

        public void setMissingProvider(MissingIndexProviderStrategy missingProvider) {
            this.missingProvider = missingProvider;
        }

        void setIgnoreReindexFlags(boolean ignoreReindexFlags) {
            this.ignoreReindexFlags = ignoreReindexFlags;
        }

        void registerIndexCommitCallbackInternal(IndexCommitCallback callback) {
            indexCommitCallbacks.add(callback);
        }

        public void commitProgress(IndexProgress indexProgress) {
            for (IndexCommitCallback icc : indexCommitCallbacks) {
                try {
                    icc.commitProgress(indexProgress);
                } catch (Exception e) {
                    log.warn("Commit progress callback threw an exception. Saving ourselves.", e);
                }
            }
        }

        private class ReportingCallback implements ContextAwareCallback, IndexingContext {
            final String indexPath;
            final boolean reindex;

            public ReportingCallback(String indexPath, boolean reindex) {
                this.indexPath = indexPath;
                this.reindex = reindex;
            }

            @Override
            public void indexUpdate() throws CommitFailedException {
                progressReporter.indexUpdate(indexPath);
            }

            //~------------------------------< ContextAwareCallback >

            @Override
            public IndexingContext getIndexingContext() {
                return this;
            }

            //~--------------------------------< IndexingContext >

            @Override
            public String getIndexPath() {
                return indexPath;
            }

            @Override
            public CommitInfo getCommitInfo() {
                return commitInfo;
            }

            @Override
            public boolean isReindexing() {
                return reindex;
            }

            @Override
            public boolean isAsync() {
                return IndexUpdateRootState.this.isAsync();
            }

            @Override
            public void indexUpdateFailed(Exception e) {
                corruptIndexHandler.indexUpdateFailed(async, indexPath, e);
            }

            @Override
            public void registerIndexCommitCallback(IndexCommitCallback callback) {
                registerIndexCommitCallbackInternal(callback);
            }
        }
    }

}
