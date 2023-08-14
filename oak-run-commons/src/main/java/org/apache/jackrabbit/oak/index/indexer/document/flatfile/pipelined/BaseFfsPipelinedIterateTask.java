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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoIncompatibleDriverException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.model.Sorts.ascending;

public class BaseFfsPipelinedIterateTask implements Callable<BaseFfsPipelinedIterateTask.Result> {
    public static class Result {
        private final long documentsDownloaded;

        public Result(long documentsDownloaded) {
            this.documentsDownloaded = documentsDownloaded;
        }

        public long getDocumentsDownloaded() {
            return documentsDownloaded;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(BaseFfsPipelinedIterateTask.class);

    /**
     * Whether to retry on connection errors to MongoDB.
     * This property affects the query that is used to download the documents from MongoDB. If set to true, the query
     * will traverse the results by order of the _modified property (does an index scan), which allows it to resume after
     * a failed connection from where it left off. If set to false, it uses a potentially more efficient query that does
     * not impose any order on the results (does a simple column scan).
     */
    public static final String OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS = "oak.indexer.pipelined.retryOnConnectionErrors";
    public static final boolean DEFAULT_OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS = true;
    public static final String OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS = "oak.indexer.pipelined.mongoConnectionRetrySeconds";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS = 300;
    // Use a short initial retry interval. In most cases if the connection to a replica fails, there will be other
    // replicas available so a reconnection attempt will succeed immediately.
    private final static long retryInitialIntervalMillis = 100;
    private final static long retryMaxIntervalMillis = 10_000;

    // TODO: Revise this timeout. It is used to prevent the indexer from blocking forever if the queue is full.
    private static final Duration MONGO_QUEUE_OFFER_TIMEOUT = Duration.ofMinutes(30);
    private static final int MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES = 10;

    private final int batchSize;
    private final BlockingQueue<String[]> mongoDocQueue;
    private final int retryDuringSeconds;
    private final boolean retryOnConnectionErrors;
    private final Logger traversalLog = LoggerFactory.getLogger(BaseFfsPipelinedIterateTask.class.getName() + ".traversal");
    private final ReadPreference readPreference;
    private final Stopwatch downloadStartWatch = Stopwatch.createUnstarted();

    private long totalEnqueueWaitTimeMillis = 0;
    private Instant lastDelayedEnqueueWarningMessageLoggedTimestamp = Instant.now();
    private long documentsRead = 0;
    private long nextLastModified = 0;
    private String lastIdDownloaded = null;
    private String baseFfsPath;

    private Compression algorithm;
    public BaseFfsPipelinedIterateTask(String baseFfsPath,
                                       int batchSize,
                                       BlockingQueue<String[]> queue, Compression algorithm) {
        this.baseFfsPath = baseFfsPath;
        LOG.info("baseFfspath is :{}", baseFfsPath);
        this.batchSize = batchSize;
        this.mongoDocQueue = queue;
        this.algorithm = algorithm;
        // Default retries for 5 minutes.
        this.retryDuringSeconds = ConfigHelper.getSystemPropertyAsInt(
                OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                DEFAULT_OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS);
        Preconditions.checkArgument(retryDuringSeconds > 0,
                "Property " + OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS + " must be > 0. Was: " + retryDuringSeconds);
        this.retryOnConnectionErrors = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS,
                DEFAULT_OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS);

        //TODO This may lead to reads being routed to secondary depending on MongoURI
        //So caller must ensure that its safe to read from secondary
//        this.readPreference = MongoDocumentStoreHelper.getConfiguredReadPreference(mongoStore, collection);
        this.readPreference = ReadPreference.secondaryPreferred();
        LOG.info("Using read preference {}", readPreference.getName());
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("baseffs-dump");
        try {
            LOG.info("Starting to download from MongoDB.");
            //TODO This may lead to reads being routed to secondary depending on MongoURI
            //So caller must ensure that its safe to read from secondary

            this.nextLastModified = 0;
            this.lastIdDownloaded = null;


            downloadStartWatch.start();
            if (!retryOnConnectionErrors) {
                downloadAll();
            }
            LOG.info("Terminating task. Downloaded {} Mongo documents in {}. Total enqueuing delay: {} ms ({}%)",
                    documentsRead, downloadStartWatch,
                    totalEnqueueWaitTimeMillis,
                    String.format("%1.2f", (100.0 * totalEnqueueWaitTimeMillis) / downloadStartWatch.elapsed(TimeUnit.MILLISECONDS)));
            return new Result(documentsRead);
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted", t);
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception.", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void reportProgress(String id) {
        if (this.documentsRead % 10000 == 0) {
            double rate = ((double) this.documentsRead) / downloadStartWatch.elapsed(TimeUnit.SECONDS);
            String formattedRate = String.format("%1.2f nodes/s, %1.2f nodes/hr", rate, rate * 3600);
            LOG.info("Dumping from NSET Traversed #{} {} [{}] (Elapsed {})",
                    this.documentsRead, id, formattedRate, downloadStartWatch);
        }
        traversalLog.trace(id);
    }

    private void downloadAll() throws InterruptedException, TimeoutException {
        LOG.info("Traversing all documents");
        download(this.baseFfsPath);
    }

    private void download(String baseFfsPath) throws InterruptedException, TimeoutException {
        try (BufferedReader br = FlatFileStoreUtils.createReader(new File(baseFfsPath), algorithm)) {
            String[] block = new String[batchSize];
            int nextIndex = 0;
            String line;

            while ((line = br.readLine()) != null) {
                String next = line;
                String id = next.split("\\|")[0];
                // If we are retrying on connection errors, we need to keep track of the last _modified value
                this.lastIdDownloaded = id;
                this.documentsRead++;
                reportProgress(id);
                block[nextIndex] = next;
                nextIndex++;
                if (nextIndex == batchSize) {
                    tryEnqueue(block);
                    block = new String[batchSize];
                    nextIndex = 0;
                }
            }
            if (nextIndex > 0) {
                LOG.info("Enqueueing last block of size: {}", nextIndex);
                enqueuePartialBlock(block, nextIndex);
            }

        } catch (FileNotFoundException e) {
            LOG.error("Baseffs file not present at:{}", baseFfsPath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOG.error("IO exception for Baseffs file not present at:{}", baseFfsPath);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            LOG.error("IO exception for Baseffs file not present at:", e);
            throw  e;
        }
    }

    private void enqueuePartialBlock(String[] block, int nextIndex) throws InterruptedException, TimeoutException {
        if (block.length == nextIndex) {
            tryEnqueue(block);
        } else {
            String[] partialBlock = new String[nextIndex];
            System.arraycopy(block, 0, partialBlock, 0, nextIndex);
            tryEnqueue(partialBlock);
        }
    }

    private void tryEnqueue(String[] block) throws TimeoutException, InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (!mongoDocQueue.offer(block, MONGO_QUEUE_OFFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timeout trying to enqueue batch of MongoDB documents. Waited " + MONGO_QUEUE_OFFER_TIMEOUT);
        }
        long enqueueDelay = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        totalEnqueueWaitTimeMillis += enqueueDelay;
        if (enqueueDelay > 1) {
            logWithRateLimit(() ->
                    LOG.info("Enqueuing of Mongo document batch was delayed, took {} ms. mongoDocQueue size {}. " +
                                    "Consider increasing the number of Transform threads. " +
                                    "(This message is logged at most once every {} seconds)",
                            enqueueDelay, mongoDocQueue.size(), MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES)
            );
        }
    }

    private void logWithRateLimit(Runnable f) {
        Instant now = Instant.now();
        if (Duration.between(lastDelayedEnqueueWarningMessageLoggedTimestamp, now).toSeconds() > MIN_INTERVAL_BETWEEN_DELAYED_ENQUEUING_MESSAGES) {
            f.run();
            lastDelayedEnqueueWarningMessageLoggedTimestamp = now;
        }
    }
}
