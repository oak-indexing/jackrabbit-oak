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
package org.apache.jackrabbit.oak.plugins.index.lucene.resumeindexing;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.ResumableAsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test proving that resumable/chunked async indexing produces the same
 * aggregated and relative-path index content as the normal (monolithic) lane, on both an
 * in-memory {@link MemoryNodeStore} and a real disk-backed {@link FileStore}-based
 * SegmentNodeStore.
 *
 * <p>It reuses the {@code damAssetLucene} index shape from the resume perf harness: a Lucene
 * index that <em>aggregates</em> {@code jcr:content} and {@code jcr:content/metadata} into the
 * parent {@code dam:Asset} document, and indexes <em>relative-path</em> properties such as
 * {@code jcr:content/metadata/dam:status}. Aggregation and relative-path indexing are exactly
 * the behaviours most at risk from the resume lane's {@code PathTree} subtree-skipping: indexing
 * a {@code dam:Asset} must still descend into its {@code jcr:content} subtree even when chunk
 * boundaries fall inside that subtree.
 *
 * <p>The same content and index definition are indexed twice — once on the normal {@code async}
 * lane (monolithic) and once on the {@code resume_async} lane with a tiny chunk size that forces
 * many chunk boundaries and resume cursors — and the query results are asserted equal. Running
 * against the segment store additionally exercises resume across real segment/TAR persistence
 * boundaries rather than the always-in-memory MemoryNodeStore. All queries use
 * {@code option(traversal fail, index name damAssetLucene)} so a query that the index cannot
 * satisfy fails outright instead of silently falling back to a repository traversal; dropped
 * documents therefore surface as a wrong count, not a false pass.
 */
@RunWith(Parameterized.class)
public class ResumeIndexingAggregationE2ETest {

    public enum StoreType { MEMORY, SEGMENT }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> stores() {
        return Arrays.asList(new Object[]{StoreType.MEMORY}, new Object[]{StoreType.SEGMENT});
    }

    @Parameterized.Parameter
    public StoreType storeType;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private static final String CHUNK_SIZE_PROP = "oak.async.chunkSize";

    /** Total dam:Asset nodes created; well above the chunk size so the resume lane suspends. */
    private static final int ASSET_COUNT = 40;
    /** Every Nth asset gets status=approved; the rest are draft. */
    private static final int APPROVED_INTERVAL = 2;
    private static final int APPROVED_COUNT = ASSET_COUNT / APPROVED_INTERVAL;
    /** Small chunk size for the resume arm's incremental phase — forces many chunk boundaries. */
    private static final int RESUME_CHUNK_SIZE = 5;

    private String previousChunkSize;

    @After
    public void tearDown() {
        if (previousChunkSize == null) {
            System.clearProperty(CHUNK_SIZE_PROP);
        } else {
            System.setProperty(CHUNK_SIZE_PROP, previousChunkSize);
        }
    }

    /** The three queries whose results must be identical across the normal and resume lanes. */
    private static final String Q_RELATIVE_STATUS =
            "SELECT [jcr:path] FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam') "
            + "AND CONTAINS([jcr:content/metadata/dam:status], 'approved') "
            + "option(traversal fail, index name damAssetLucene)";
    private static final String Q_AGGREGATED_NODESCOPE =
            "SELECT [jcr:path] FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam') "
            + "AND CONTAINS(*, 'approved') "
            + "option(traversal fail, index name damAssetLucene)";
    private static final String Q_RELATIVE_TITLE =
            "SELECT [jcr:path] FROM [dam:Asset] WHERE ISDESCENDANTNODE('/content/dam') "
            + "AND CONTAINS([jcr:content/metadata/dc:title], 'Title') "
            + "option(traversal fail, index name damAssetLucene)";

    @Test
    public void aggregationAndRelativePathQueriesSurviveResumeChunking() throws Exception {
        previousChunkSize = System.getProperty(CHUNK_SIZE_PROP);

        // Baseline: monolithic normal lane.
        int[] normal = buildAndQuery(false);

        // Resume lane, chunked incremental indexing.
        int[] resume = buildAndQuery(true);

        // Sanity: the baseline itself must have indexed the aggregated / relative-path content.
        assertEquals("normal: relative-path dam:status query", APPROVED_COUNT, normal[0]);
        assertEquals("normal: aggregated node-scope query", APPROVED_COUNT, normal[1]);
        assertEquals("normal: relative-path dc:title query", ASSET_COUNT, normal[2]);

        // The whole point: resume mode must not break aggregation or relative-path indexing.
        assertEquals("resume must match normal for relative-path dam:status aggregation",
                normal[0], resume[0]);
        assertEquals("resume must match normal for aggregated node-scope fulltext",
                normal[1], resume[1]);
        assertEquals("resume must match normal for relative-path dc:title aggregation",
                normal[2], resume[2]);
    }

    /**
     * Builds the damAssetLucene index and dam:Asset content in a fresh repository, indexes it on
     * either the normal or the resume lane, and returns the counts for the three verification
     * queries.
     *
     * <p>Resume arm staging: the initial index is built monolithically ({@code chunkSize=0}) so
     * this test exercises the resume lane's <em>chunked incremental</em> path (content added after
     * the first checkpoint), which is the production-supported resumable path. The content is then
     * added and indexed with a tiny chunk size, forcing repeated suspend/resume across chunk
     * boundaries that fall inside the aggregated {@code jcr:content} subtrees.
     */
    private int[] buildAndQuery(boolean resume) throws Exception {
        StoreHandle handle = newNodeStore();
        NodeStore nodeStore = handle.nodeStore;
        IndexTracker tracker = new IndexTracker();
        LuceneIndexProvider provider = new LuceneIndexProvider(tracker);
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();

        ContentRepository repository = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();

        ContentSession session = repository.login(null, null);
        AsyncIndexUpdate indexer = null;
        try {
            Root root = session.getLatestRoot();
            registerDamNodeTypes(root);

            String lane = resume ? ResumableAsyncIndexUpdate.resumeLaneName("async") : "async";
            createDamAssetLuceneIndex(root, lane);

            IndexEditorProvider editors = CompositeIndexEditorProvider.compose(Arrays.asList(
                    editorProvider,
                    new PropertyIndexEditorProvider(),
                    new NodeCounterEditorProvider()));

            // Initial build is always monolithic: on the resume arm this establishes the first
            // checkpoint so the subsequent content additions run through the chunked incremental
            // path rather than a chunked reindex-from-scratch.
            System.setProperty(CHUNK_SIZE_PROP, "0");
            indexer = newIndexer(lane, nodeStore, editors, resume);
            runUntilIndexed(indexer, nodeStore);

            // Add the aggregated dam:Asset content.
            createDamAssetContent(session.getLatestRoot());

            if (resume) {
                // Switch to a tiny chunk size and a fresh resume-lane indexer (its checkpoint lives
                // in :async and survives the indexer swap) to force chunked incremental indexing.
                System.setProperty(CHUNK_SIZE_PROP, String.valueOf(RESUME_CHUNK_SIZE));
                indexer.close();
                indexer = newIndexer(lane, nodeStore, editors, true);
                boolean chunked = runResumeUntilComplete(indexer, nodeStore, lane);
                assertTrue("resume lane must actually suspend and then resume to completion across "
                        + "chunks (a livelock that never clears its resume cursor fails here)", chunked);
                assertFalse("resume state must be cleared once chunked indexing completes",
                        resumeStateExists(nodeStore, lane));
            } else {
                runUntilIndexed(indexer, nodeStore);
            }

            return new int[]{
                    query(session, tracker, Q_RELATIVE_STATUS),
                    query(session, tracker, Q_AGGREGATED_NODESCOPE),
                    query(session, tracker, Q_RELATIVE_TITLE)
            };
        } finally {
            if (indexer != null) {
                indexer.close();
            }
            session.close();
            handle.close();
        }
    }

    /** Creates the node store for the current parameter, bundling any resources needing cleanup. */
    private StoreHandle newNodeStore() throws Exception {
        if (storeType == StoreType.MEMORY) {
            return new StoreHandle(new MemoryNodeStore(), null);
        }
        File dir = temporaryFolder.newFolder("segment-" + System.nanoTime());
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(dir)
                .withMaxFileSize(256)
                .withMemoryMapping(false)
                .build();
        NodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        return new StoreHandle(nodeStore, fileStore);
    }

    /** Holds a node store together with the {@link FileStore} that must be closed after the arm. */
    private static final class StoreHandle {
        final NodeStore nodeStore;
        private final FileStore fileStore;

        StoreHandle(NodeStore nodeStore, FileStore fileStore) {
            this.nodeStore = nodeStore;
            this.fileStore = fileStore;
        }

        void close() {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }

    private AsyncIndexUpdate newIndexer(String lane, NodeStore nodeStore,
                                        IndexEditorProvider editors, boolean resume) throws Exception {
        if (!resume) {
            return new AsyncIndexUpdate(lane, nodeStore, editors);
        }
        AsyncIndexUpdate indexer = new ResumableAsyncIndexUpdate(lane, nodeStore, editors);
        enableResumeForTest(indexer);
        return indexer;
    }

    /** Runs the indexer until the definition's reindex flag clears (bounded). */
    private void runUntilIndexed(AsyncIndexUpdate indexer, NodeStore nodeStore) {
        for (int i = 0; i < 60; i++) {
            indexer.run();
            if (!indexDef(nodeStore).getBoolean("reindex")
                    && nodeStore.getRoot().getChildNode(":async").hasProperty(indexer.getName())) {
                // reindex done and lane has a checkpoint -> at least one full pass completed.
                return;
            }
        }
    }

    /**
     * Runs the resume lane until its resume-state node is cleared. Returns {@code true} if a
     * resume cursor was observed at least once (i.e. the run genuinely chunked), so a regression
     * that stopped chunking — or livelocked and never cleared — fails the test.
     */
    private boolean runResumeUntilComplete(AsyncIndexUpdate indexer, NodeStore nodeStore, String lane) {
        boolean sawResumeState = false;
        for (int i = 0; i < 200; i++) {
            indexer.run();
            if (resumeStateExists(nodeStore, lane)) {
                sawResumeState = true;
            } else if (sawResumeState) {
                return true;
            }
        }
        return false;
    }

    private static NodeState indexDef(NodeStore nodeStore) {
        return nodeStore.getRoot().getChildNode("oak:index").getChildNode("damAssetLucene");
    }

    private static boolean resumeStateExists(NodeStore nodeStore, String lane) {
        NodeState resumeNode = nodeStore.getRoot().getChildNode(":async").getChildNode(lane + "-resume");
        return resumeNode.exists() && resumeNode.hasProperty("targetCheckpoint");
    }

    /** Flips AsyncIndexUpdate#setResumableAsyncEnabledForTest(true) (package-private) via reflection. */
    private static void enableResumeForTest(AsyncIndexUpdate indexer) throws Exception {
        java.lang.reflect.Method m = AsyncIndexUpdate.class
                .getDeclaredMethod("setResumableAsyncEnabledForTest", boolean.class);
        m.setAccessible(true);
        m.invoke(indexer, true);
    }

    private static void registerDamNodeTypes(Root root) throws Exception {
        String cnd =
                "<dam = 'http://www.day.com/dam/1.0'>\n"
                + "[dam:Asset] > nt:hierarchyNode\n"
                + "  + jcr:content (nt:unstructured)\n";
        NodeTypeRegistry.register(root, new java.io.ByteArrayInputStream(cnd.getBytes()), "dam-nodetypes.cnd");
        root.commit();
    }

    /** The damAssetLucene index: aggregates jcr:content(/metadata) and indexes relative-path props. */
    private static void createDamAssetLuceneIndex(Root root, String lane) throws Exception {
        Tree index = root.getTree("/oak:index").addChild("damAssetLucene");
        index.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        index.setProperty("type", "lucene");
        index.setProperty("async", lane);
        index.setProperty("compatVersion", 2);
        index.setProperty("reindex", true);
        index.setProperty("evaluatePathRestrictions", true);
        index.setProperty("includedPaths", Collections.singletonList("/content"), Type.STRINGS);

        Tree aggregates = index.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAssetAgg = aggregates.addChild("dam:Asset");
        damAssetAgg.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree include0 = damAssetAgg.addChild("include0");
        include0.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include0.setProperty("path", "jcr:content");
        Tree include1 = damAssetAgg.addChild("include1");
        include1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include1.setProperty("path", "jcr:content/metadata");

        Tree indexRules = index.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree damAsset = indexRules.addChild("dam:Asset");
        damAsset.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree properties = damAsset.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        Tree titleProp = properties.addChild("dcTitle");
        titleProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        titleProp.setProperty("name", "jcr:content/metadata/dc:title");
        titleProp.setProperty("propertyIndex", true);
        titleProp.setProperty("analyzed", true);
        titleProp.setProperty("nodeScopeIndex", true);

        Tree assetIdProp = properties.addChild("damAssetId");
        assetIdProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        assetIdProp.setProperty("name", "jcr:content/metadata/dam:assetId");
        assetIdProp.setProperty("propertyIndex", true);

        Tree statusProp = properties.addChild("damStatus");
        statusProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        statusProp.setProperty("name", "jcr:content/metadata/dam:status");
        statusProp.setProperty("propertyIndex", true);
        statusProp.setProperty("analyzed", true);
        statusProp.setProperty("nodeScopeIndex", true);

        root.commit();
    }

    /**
     * Creates {@link #ASSET_COUNT} dam:Asset nodes under /content/dam. The queried terms
     * ("approved", "Title") live only on the {@code jcr:content/metadata} grandchild, so a matching
     * dam:Asset result proves the child content was aggregated up into the parent document.
     */
    private static void createDamAssetContent(Root root) throws Exception {
        Tree content = root.getTree("/").addChild("content");
        content.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        Tree dam = content.addChild("dam");
        dam.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);

        for (int i = 0; i < ASSET_COUNT; i++) {
            Tree asset = dam.addChild("asset-" + i);
            asset.setProperty("jcr:primaryType", "dam:Asset", Type.NAME);
            Tree jcrContent = asset.addChild("jcr:content");
            jcrContent.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            Tree metadata = jcrContent.addChild("metadata");
            metadata.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            metadata.setProperty("dc:title", "Asset Title " + i);
            metadata.setProperty("dam:assetId", "asset-" + i);
            metadata.setProperty("dam:status", i % APPROVED_INTERVAL == 0 ? "approved" : "draft");
        }
        root.commit();
    }

    private static int query(ContentSession session, IndexTracker tracker,
                             String statement) throws Exception {
        Exception last = null;
        for (int i = 0; i < 30; i++) {
            // Force the tracker to reload the latest index state written by the async indexer.
            tracker.refresh();
            Root root = session.getLatestRoot();
            try {
                Result result = root.getQueryEngine().executeQuery(
                        statement, javax.jcr.query.Query.JCR_SQL2,
                        Collections.emptyMap(), QueryEngine.NO_MAPPINGS);
                int count = 0;
                for (ResultRow row : result.getRows()) {
                    row.getPath();
                    count++;
                }
                return count;
            } catch (Exception e) {
                last = e;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Query never succeeded: " + statement, last);
    }
}
