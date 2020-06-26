package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.RepositoryOptionsUtil;
import org.apache.jackrabbit.oak.plugins.index.TestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.junit.rules.TemporaryFolder;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import static com.google.common.collect.Lists.newArrayList;

public class LuceneTestRepositoryBuilder extends TestRepositoryBuilder {

//    private int defaultAsyncIndexingTimeInSeconds = 5;
//    private long indexCorruptIntervalInMillis = 100;
//    private boolean isAsync;
//    private ElasticConnection esConnection;
//
//    private NodeStore nodeStore;
//    private IndexEditorProvider editorProvider;
//    private QueryIndexProvider indexProvider;
//    private AsyncIndexUpdate asyncIndexUpdate;
//    private TrackingCorruptIndexHandler trackingCorruptIndexHandler;
//    private InitialContent initialContent;
//    private SecurityProvider securityProvider;
//    private IndexEditorProvider indexEditorProvider;
//    private QueryIndexProvider queryIndexProvider;


//    public LuceneTestRepositoryBuilder() {
////        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
////                elasticRule.getElasticConnectionFromString();
//        this.isAsync = isAsync;
//        nodeStore = createNodeStore(RepositoryOptionsUtil.NodeStoreType.MEMORY_NODE_STORE);
//        this.editorProvider = (ElasticIndexEditorProvider) getIndexEditorProvider();
//        this.indexProvider = new ElasticIndexProvider(esConnection);
//        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
//                editorProvider,
//                new NodeCounterEditorProvider()
//        )));
//        this.trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
//        trackingCorruptIndexHandler.setCorruptInterval(indexCorruptIntervalInMillis, TimeUnit.MILLISECONDS);
//        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
//        initialContent = getInitialContent();
//        securityProvider = new OpenSecurityProvider();
//        indexEditorProvider = new PropertyIndexEditorProvider();
//        queryIndexProvider = new NodeTypeIndexProvider();
//
//    }

    ResultCountingIndexProvider resultCountingIndexProvider;
    TestUtil.OptionalEditorProvider optionalEditorProvider;

    public LuceneTestRepositoryBuilder(ExecutorService executorService, TemporaryFolder temporaryFolder) {
        IndexCopier copier = null;
        try {
            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        this.indexProvider = new LuceneIndexProvider(copier);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));

        resultCountingIndexProvider = new ResultCountingIndexProvider(indexProvider);
        queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setStrictPathRestriction(StrictPathRestriction.ENABLE.name());
        optionalEditorProvider = new TestUtil.OptionalEditorProvider();

//        PropertyIndexEditorProvider propertyIndexEditorProvider = new PropertyIndexEditorProvider();
//        NodeTypeIndexProvider nodeTypeIndexProvider = new NodeTypeIndexProvider();

        //this.trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        //trackingCorruptIndexHandler.setCorruptInterval(indexCorruptIntervalInMillis, TimeUnit.MILLISECONDS);
         asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        //initialContent = getInitialContent();
        //securityProvider = new OpenSecurityProvider();
        //indexEditorProvider = new PropertyIndexEditorProvider();
        //queryIndexProvider = new NodeTypeIndexProvider();
        //indexEditorProvider = new PropertyIndexEditorProvider();


//        oak = new Oak(nodeStore)
//                .with(getInitialContent())
//                .with(new OpenSecurityProvider())
//                .with(resultCountingIndexProvider)
//                .with((Observer) indexProvider)
//                .with(editorProvider)
//                .with(optionalEditorProvider)
//                .with(new PropertyIndexEditorProvider())
        //.with(new NodeTypeIndexProvider())
//                .with(queryEngineSettings);
    }

//    private InitialContent getInitialContent() {
//        return new InitialContent() {
//            @Override
//            public void initialize(@NotNull NodeBuilder builder) {
//                super.initialize(builder);
//                // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
//                NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
//                oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());
//            }
//        };
//    }

    public RepositoryOptionsUtil build() {
//        Oak oak = new Oak(nodeStore)
//                .with(initialContent)
//                .with(securityProvider)
//                .with(editorProvider)
//                .with(indexProvider)
//                .with(indexProvider)
//                .with(queryIndexProvider);



                Oak oak = new Oak(nodeStore)
                .with(getInitialContent())
                .with(securityProvider)
                .with(resultCountingIndexProvider)
                .with((Observer) indexProvider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(indexEditorProvider)
                .with(queryIndexProvider)
                .with(queryEngineSettings);
        if (isAsync) {
            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
        }



        return new LuceneRepositoryOptionsUtil(oak).with(isAsync).with(asyncIndexUpdate);
    }

    // Override this to provide a different flavour of node store
    // like segment or mongo mk
    // Tests would need to handle the cleanup accordingly.
    // TODO provide a util here so that test classes simply need to mention the type of store they want to create
    // for now, memory store should suffice.
}
