package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.apache.jackrabbit.oak.plugins.index.RepositoryOptionsUtil;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

public class LuceneRepositoryOptionsUtil extends RepositoryOptionsUtil {

//    private ExecutorService executorService;
//    public TemporaryFolder temporaryFolder;

//    public LuceneRepositoryOptionsUtil(ExecutorService executorService, TemporaryFolder temporaryFolder, boolean isAsync) {
//        this(NodeStoreType.MEMORY_NODE_STORE, executorService, temporaryFolder, isAsync);
//    }
//
//    public LuceneRepositoryOptionsUtil(NodeStoreType nodeStoreType, ExecutorService executorService, TemporaryFolder temporaryFolder, boolean isAsync) {
//        this.executorService = executorService;
//        this.temporaryFolder = temporaryFolder;
//        this.isAsync = isAsync;
//        nodeStore = createNodeStore(nodeStoreType);
//        createDefaultOak(nodeStoreType, nodeStore, isAsync);
//    }

    public LuceneRepositoryOptionsUtil(Oak oak) {
        this.oak = oak;
    }


    /*
Override this to create some other repo initializer if needed
// Make sure to call super.initialize(builder)
 */
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

//    @Override
//    protected void createDefaultOak(RepositoryOptionsUtil.NodeStoreType nodeStoreType, NodeStore nodeStore, boolean isAsync) {
//        IndexCopier copier = null;
//        try {
//            copier = new IndexCopier(executorService, temporaryFolder.getRoot());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
//        LuceneIndexProvider provider = new LuceneIndexProvider(copier);
//        ResultCountingIndexProvider queryIndexProvider = new ResultCountingIndexProvider(provider);
//        QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
//        queryEngineSettings.setStrictPathRestriction(StrictPathRestriction.ENABLE.name());
//        TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();
//        oak = new Oak(nodeStore)
//                .with(getInitialContent())
//                .with(new OpenSecurityProvider())
//                .with(queryIndexProvider)
//                .with((Observer) provider)
//                .with(editorProvider)
//                .with(optionalEditorProvider)
//                .with(new PropertyIndexEditorProvider())
//                .with(new NodeTypeIndexProvider())
//                .with(queryEngineSettings);
//    }

}
