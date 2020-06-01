package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;

public class ElasticResultRowIteratorV2 implements Iterator<FulltextResultRow>, ElasticResponseListener.RowListener {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResultRowIteratorV2.class);
    public static final FulltextResultRow POISON_PILL =
            new FulltextResultRow("___OAK_POISON_PILL___", 0d, Collections.emptyMap(), null, null);

    private final BlockingQueue<FulltextResultRow> queue = new LinkedBlockingQueue<>();

    private final ElasticIndexNode indexNode;
    private final IndexPlan indexPlan;
    private final PlanResult planResult;
    private final BiPredicate<String, IndexPlan> rowInclusionPredicate;

    private final ElasticQueryScanner elasticQueryScanner;

    private FulltextResultRow nextRow;

    ElasticResultRowIteratorV2(@NotNull ElasticIndexNode indexNode,
                               @NotNull IndexPlan indexPlan,
                               @NotNull PlanResult planResult,
                               BiPredicate<String, IndexPlan> rowInclusionPredicate) {
        this.indexNode = indexNode;
        this.indexPlan = indexPlan;
        this.planResult = planResult;
        this.rowInclusionPredicate = rowInclusionPredicate != null ? rowInclusionPredicate : (p, ip) -> true;

        this.elasticQueryScanner = initScanner();
    }

    private CyclicBarrier barrier = new CyclicBarrier(2);

    @Override
    public boolean hasNext() {
        try {
            if (queue.isEmpty() && elasticQueryScanner.anyDataLeft) {
                LOG.trace("time to wake up the scanner for more data");
                barrier.await();
            } else {
                LOG.trace("still stuff to process in the queue(size {}) or nothing left in the scanner(barrier size {})",
                        queue.size(), barrier.getNumberWaiting());
            }
            nextRow = queue.take();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new IllegalStateException("Error reading next result from Elastic", e);
        }
        if (POISON_PILL.path.equals(nextRow.path)) {
            nextRow = null;
        }
        return nextRow != null;
    }

    @Override
    public FulltextResultRow next() {
        return nextRow;
    }

    private ElasticQueryScanner initScanner() {
        return new ElasticQueryScanner(indexNode,
                new ElasticRequestHandler(indexPlan, planResult),
                new ElasticResponseHandler(indexPlan, planResult, rowInclusionPredicate),
                Collections.singletonList(this));
    }

    @Override
    public void on(FulltextResultRow row) {
        try {
            queue.put(row);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void endData() {
        try {
            queue.put(POISON_PILL);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class ElasticQueryScanner implements ActionListener<SearchResponse> {

        private static final int SMALL_RESULT_SET_SIZE = 1;
        private static final int LARGE_RESULT_SET_SIZE = 1;

        private final List<ElasticResponseListener.RowListener> rowListeners = new ArrayList<>();

        private final ElasticIndexNode indexNode;
        private final ElasticResponseHandler responseHandler;

        private final SearchRequest searchRequest;

        private int scannedRows = 0;
        public boolean anyDataLeft;

        ElasticQueryScanner(ElasticIndexNode indexNode, ElasticRequestHandler requestHandler,
                            ElasticResponseHandler responseHandler, List<ElasticResponseListener> listeners) {
            this.indexNode = indexNode;
            this.responseHandler = responseHandler;
            for (ElasticResponseListener l: listeners) {
                addResponseListener(l);
            }

            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                    .query(requestHandler.build())
                    .size(SMALL_RESULT_SET_SIZE)
                    .fetchSource(FieldNames.PATH, null)
                    .sort(SortBuilders.fieldSort("_score").order(SortOrder.DESC))
                    .sort(SortBuilders.fieldSort(FieldNames.PATH).order(SortOrder.ASC)); // tie-breaker

            searchRequest = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                    .source(searchSourceBuilder);
            LOG.trace("kick first search");
            indexNode.getConnection().getClient().searchAsync(searchRequest, RequestOptions.DEFAULT, this);
        }

        private void addResponseListener(ElasticResponseListener listener) {
            if (listener instanceof RowListener) {
                rowListeners.add((RowListener) listener);
            }
        }

        public int getScannedRows() {
            return scannedRows;
        }

        @Override
        public void onResponse(SearchResponse searchResponse) {

            SearchHit[] searchHits = searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                scannedRows += searchHits.length;
                anyDataLeft = searchResponse.getHits().getTotalHits().value > scannedRows;
                for (SearchHit hit : searchHits) {
                    FulltextResultRow row = responseHandler.toRow(hit, null);
                    for (RowListener l : rowListeners) {
                        l.on(row);
                    }
                }

                if (!anyDataLeft) {
                    close();
                    return;
                }

                LOG.trace("waiting for next call = " + scannedRows);
                try {
                    ElasticResultRowIteratorV2.this.barrier.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    e.printStackTrace();
                }
                LOG.trace("kicking another search request");

                searchRequest.source()
                        .searchAfter(searchHits[searchHits.length - 1].getSortValues())
                        .size(LARGE_RESULT_SET_SIZE);
                indexNode.getConnection().getClient().searchAsync(searchRequest, RequestOptions.DEFAULT, this);
            } else {
                close();
            }
        }

        @Override
        public void onFailure(Exception e) {
            e.printStackTrace();
        }

        private void close() {
            for (RowListener l : rowListeners) {
                l.endData();
            }
        }
    }
}
