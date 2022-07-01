package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;


public class MergeIncrementalFFS {

    private final File baseFFS;
    private final File incrementalFFS;
    private final File merged;
    private Comparator<NodeStateHolder> comparator;
    private final Set<String> preferredPathElements;


    public MergeIncrementalFFS(Set<String> preferredPathElements, File baseFFS, File incrementalFFS, File merged) {
        this.preferredPathElements = preferredPathElements;
        this.baseFFS = baseFFS;
        this.incrementalFFS = incrementalFFS;
        this.merged = merged;
    }

    public void doMerge() throws IOException {

        System.out.println("************** Base FFS " + baseFFS.getAbsolutePath());
        System.out.println("************** incrementalFFS FFS " + incrementalFFS.getAbsolutePath());
        System.out.println("**************  merged FFS " + merged.getAbsolutePath());

        try(BufferedWriter writer = FlatFileStoreUtils.createWriter(merged, true);
            BufferedReader br = FlatFileStoreUtils.createReader(baseFFS, true);
            BufferedReader br2 = FlatFileStoreUtils.createReader(incrementalFFS, true)) {
            String baseLine = br.readLine();
            String incLine = br2.readLine();

            System.out.println("Reading first incremental liine - " + incLine);

            comparator = (e1, e2) -> new PathElementComparator(preferredPathElements).compare(e1.getPathElements(), e2.getPathElements());
            int compared;
            while (true) {
                if (incLine == null) {
                    if (baseLine != null) {
                        writer.write(baseLine);
                        writer.newLine();
                        baseLine = br.readLine();
                        continue;
                    } else {
                        break;
                    }
                }
                if (baseLine == null) {
                    if (incLine != null) {
                        writer.write(removeOperand(incLine));
                        writer.newLine();
                        incLine = br2.readLine();
                        continue;
                    } else {
                        break;
                    }
                }
                compared = comparator.compare(new SimpleNodeStateHolder(baseLine), new SimpleNodeStateHolder(incLine));
                if (compared < 0) {
                    writer.write(baseLine);
                    writer.newLine();
                    baseLine = br.readLine();
                } else if (compared > 0) {
                    writer.write(removeOperand(incLine));
                    writer.newLine();
                    incLine = br2.readLine();
                    System.out.println("Reading incremental liine - " + incLine);
                } else {
                    String operand = NodeStateEntryWriter.getParts(incLine)[2];
                    if (!("D".equals(operand))) {
                        writer.write(removeOperand(incLine));
                        writer.newLine();
                    }
                    baseLine = br.readLine();
                    incLine = br2.readLine();
                    System.out.println("Reading incremental liine - " + incLine);
                }
            }
        }
    }


    private String removeOperand(String line) {
        String[] parts = NodeStateEntryWriter.getParts(line);
        return new StringBuilder().append(parts[0]).append("|").append(parts[1]).toString();
    }




}
