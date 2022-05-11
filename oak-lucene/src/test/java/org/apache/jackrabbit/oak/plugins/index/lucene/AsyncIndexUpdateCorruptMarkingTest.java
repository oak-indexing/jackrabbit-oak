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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests marking index as corrupt if blob is missing.
 */
public class AsyncIndexUpdateCorruptMarkingTest {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncIndexUpdateCorruptMarkingTest.class);

    private final long INDEX_CORRUPT_INTERVAL_IN_MILLIS = 100;

    private FileBlobStore blobStore;

    protected Root root;

    private AsyncIndexUpdate asyncIndexUpdate;
    private AsyncIndexUpdate asyncIndexUpdate1;

    @Before
    public void before() throws Exception {
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
    }

    @Rule
    public TemporaryFolder source = new TemporaryFolder(new File("target"));

//    private File getSourceFileStoreFolder() throws IOException {
//         return source.newFolder("segment-tar");
//    }

//    private String getSourceBlobFolder() throws IOException {
//        return source.newFolder("datastore").getAbsolutePath();
//
//    }

    protected SegmentNodeStorePersistence getPersistence(File folder) {
        return new TarPersistence(folder);
    }

    protected ContentRepository createRepository() throws IOException, InvalidFileStoreVersionException, CommitFailedException {

        File segment = source.newFolder("segment-tar");
        String datastore = source.newFolder("datastore").getAbsolutePath();

        FileStore store = FileStoreBuilder.fileStoreBuilder(segment)
                .withStrictVersionCheck(false)
                .withCustomPersistence(getPersistence(segment))
                .build();

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        //store.close();


//        NodeStore nodeStore = new MemoryNodeStore();

        FileBlobStore fileBlobStore = new FileBlobStore(datastore);
        fileBlobStore.setBlockSize(128);
        fileBlobStore.setBlockSizeMin(48);


        blobStore = fileBlobStore;

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                luceneIndexEditorProvider,
                new NodeCounterEditorProvider()
        )));

        asyncIndexUpdate1 = new AsyncIndexUpdate("fulltext-async", nodeStore, compose(newArrayList(
                luceneIndexEditorProvider,
                new NodeCounterEditorProvider()
        )));

        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(INDEX_CORRUPT_INTERVAL_IN_MILLIS, TimeUnit.MILLISECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        asyncIndexUpdate1.setCorruptIndexHandler(trackingCorruptIndexHandler);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(luceneIndexEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }


    @Test
    public void mytest() throws Exception {
        CheckHeapSize checkHeapSize = new CheckHeapSize();
        checkHeapSize.logHeapSize("------------------------------------");
        List<String> list = new LinkedList<>();

        // at 85 MB failure with 10 nodes
        /*int listMB = 81;
        for (int i = 0; i < listMB; i++) { // 250 MB
            for (int j = 0; j < 1024; j++) { // 1 MB
                list.add(getRandomString());
            }
        }*/
        checkHeapSize.logHeapSize("=========================");
        Thread t = new Thread(checkHeapSize);
        t.start();
        int numberOfIndexes = 1;
        for (int i = 0; i < numberOfIndexes; i++){ //
            LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
            idxb.async("async");
            idxb.indexRule("nt:base")
                    .property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
            idxb.build(root.getTree("/oak:index").addChild("lucenePropertyIndex"+i));
        }

//        for (int i = 0; i < numberOfIndexes; i++){ //
//            LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
//            idxb.async("fulltext-async");
//            idxb.indexRule("nt:base")
//                    .property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
//            idxb.build(root.getTree("/oak:index").addChild("lucenePropertyIndex1"+i));
//        }

        root.commit();


        asyncIndexUpdate.run();
   //     asyncIndexUpdate1.run();



        // Add content and index it successfully
//        root.getTree("/").addChild("content").addChild("c1").setProperty("foo", "bar");
        long startTime = System.currentTimeMillis();
        root.getTree("/").addChild("content");
        int maxnodes = 1024* 10;
        for (int i = 0; i < maxnodes; i++){
            root.getTree("/").getChild("content")
                    //.addChild(getRandomString(1024)+i).setProperty("foo", "bar");
                    .addChild("c"+i).setProperty("foo", "bar");
            if (i % 1024 == 0) root.commit();
        }
        root.commit();
        long endTime = System.currentTimeMillis();
        LOG.info("Time (millis) taken to create content: {} ", endTime-startTime);
        asyncIndexUpdate.run();
//        Thread cycle = new Thread(new Cycle());
//        cycle.start();

//        Thread memFiller = new Thread (new MemFiller());
//        memFiller.start();

        root.getTree("/content").remove();
        root.commit();

        asyncIndexUpdate.run();


        LOG.info("==============================indexing complete----------------------------------");
        long indexEndTime = System.currentTimeMillis();
        LOG.info("Time (millis) taken to index: {} ", indexEndTime - endTime);

        poison = true;
        LOG.info("==============================Thread Poisoned----------------------------------");
        LOG.info("Time (millis) taken to create content: {} ", endTime-startTime);
        LOG.info("Time (millis) taken to index: {} ", indexEndTime - endTime);

//        memFiller.join();
//        cycle.join();
        t.join();
    }

    private void deleteBlobs() throws IOException {
        blobStore.clearInUse();
        blobStore.startMark();
        blobStore.sweep();

    }

    public class Cycle implements Runnable{

        @Override
        public void run() {
            asyncIndexUpdate1.run();
        }
    }

    public class MemFiller implements Runnable{
        @Override
        public void run(){
            while (!poison){
                int listMB = 5;
                for (int i = 0; i < listMB; i++) {
                    for (int j = 0; j < 1024; j++) { // 1 MB
                        List<String> list = new LinkedList<>();
                        list.add(getRandomString(1024));
                    }
            }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected String getRandomString(int sizeInBytes) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < sizeInBytes) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }

    private static volatile boolean poison = false;
    public static class CheckHeapSize  implements Runnable {


        public void run() {
            long minFreeSize = Long.MAX_VALUE;
            long heapSize = 0;
            long heapMaxSize = 0;
            while (!poison){
                int count = 0;
                heapSize = Runtime.getRuntime().totalMemory();

                // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
                heapMaxSize = Runtime.getRuntime().maxMemory();

                // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
                long heapFreeSize = Runtime.getRuntime().freeMemory();
                if (minFreeSize > heapFreeSize){
                    minFreeSize = heapFreeSize;
                }

                int threshold = 5000;
                if (count % threshold == 5001) {
                    //count = 1;
                    logHeapSize("*********************");
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                count++;
            }
            LOG.info("Min heap free size: " + formatSize(minFreeSize));
            LOG.info("heap size: " + formatSize(heapSize));
            LOG.info("heap max size: " + formatSize(heapMaxSize));
        }

        private void logHeapSize(String differentiator) {
            LOG.info(differentiator);
            long heapSize = Runtime.getRuntime().totalMemory();

            // Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
            long heapMaxSize = Runtime.getRuntime().maxMemory();

            // Get amount of free memory within the heap in bytes. This size will increase // after garbage collection and decrease as new objects are created.
            long heapFreeSize = Runtime.getRuntime().freeMemory();

            LOG.info("heap size: " + formatSize(heapSize));
            LOG.info("heap max size: " + formatSize(heapMaxSize));
            LOG.info("heap free size: " + formatSize(heapFreeSize));
        }

        public String formatSize(long v) {
            if (v < 1024) return v + " B";
            int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
            return String.format("%.1f %sB", (double)v / (1L << (z*10)), " KMGTPE".charAt(z));
        }
    }
}
