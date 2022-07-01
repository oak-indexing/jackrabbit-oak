package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

public class IncrementalStore implements SortStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final String OAK_INDEXER_DELETE_ORIGINAL = "oak.indexer.deleteOriginal";
    private final NodeState before;
    private final NodeState after;
    private final PathElementComparator comparator;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final Compression algorithm;
    private Predicate<String> pathPredicate;
    private boolean deleteOriginal = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_DELETE_ORIGINAL, "true"));
    private int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private long textSize = 0;
    private long entryCount = 0;

    public IncrementalStore(@NotNull NodeState before, @NotNull NodeState after, File storeDir,
                            PathElementComparator comparator, Compression algorithm, Predicate<String> pathPredicate, NodeStateEntryWriter entryWriter) {
        this.before = before;
        this.after = after;
        this.comparator = comparator;
        this.storeDir = storeDir;
        this.algorithm = algorithm;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        File file = new File(storeDir, getSortedStoreFileName(algorithm));
        try (BufferedWriter w = FlatFileStoreUtils.createWriter(file, algorithm)) {
            EditorDiff.process(VisibleEditor.wrap(new DeltaFFSEditor(w, entryWriter, pathPredicate, this)), before, after);
        }
        String sizeStr = algorithm == null || algorithm.equals(Compression.NONE) ? "" : String.format("compressed/%s actual size", humanReadableByteCount(textSize));
        log.info("Dumped {} nodestates in json format in {} ({} {})", entryCount, sw, humanReadableByteCount(file.length()), sizeStr);
        return sortStoreFile(file);
    }



    @Override
    public long getEntryCount() {
        return 0;
    }

    public void incrementEntryCount() {
        entryCount++;
    }

    public void setTextSize(long textSize) {
        this.textSize = textSize;
    }

    public long getTextSize() {
        return this.textSize;
    }

    private File sortStoreFile(File storeFile) throws IOException {
        File sortWorkDir = new File(storeFile.getParent(), "sort-work-dir");
        FileUtils.forceMkdir(sortWorkDir);
        File sortedFile = new File(storeFile.getParentFile(), getSortedStoreFileName(algorithm));
        NodeStateEntrySorter sorter =
                new NodeStateEntrySorter(comparator, storeFile, sortWorkDir, sortedFile);

        logFlags();

        sorter.setCompressionAlgorithm(algorithm);
        sorter.setMaxMemoryInGB(maxMemory);
        sorter.setDeleteOriginal(deleteOriginal);
        sorter.setActualFileSize(textSize);
        sorter.sort();
        return sorter.getSortedFile();
    }

    private void logFlags() {
        log.info("Delete original dump from traversal : {} ({})", deleteOriginal, OAK_INDEXER_DELETE_ORIGINAL);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }
}
