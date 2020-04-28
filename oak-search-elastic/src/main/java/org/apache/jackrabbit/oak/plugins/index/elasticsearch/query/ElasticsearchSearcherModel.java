package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.LinkedList;
import java.util.List;

public class ElasticsearchSearcherModel {

    private QueryBuilder queryBuilder;
    private List<AggregationBuilder> aggregationBuilders = new LinkedList<>();
    private int batchSize;
    private boolean fetchSource = false;
    private String storedField = FieldNames.PATH;

    private ElasticsearchSearcherModel(QueryBuilder queryBuilder, List<AggregationBuilder> aggregationBuilders,
                                       int batchSize, boolean fetchSource, String storedField) {
        this.queryBuilder = queryBuilder;
        this.aggregationBuilders = aggregationBuilders;
        this.batchSize = batchSize;
        this.fetchSource = fetchSource;
        this.storedField = storedField;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public List<AggregationBuilder> getAggregationBuilders() {
        return aggregationBuilders;
    }

    public boolean fetchSource() {
        return fetchSource;
    }

    public String getStoredField() {
        return storedField;
    }

    public static class ElasticsearchSearcherModelBuilder {
        private QueryBuilder queryBuilder;
        private List<AggregationBuilder> aggregationBuilders = new LinkedList<>();
        private int batchSize;
        private boolean fetchSource = false;
        private String storedField = FieldNames.PATH;

        public ElasticsearchSearcherModelBuilder withQuery(QueryBuilder query) {
            this.queryBuilder = query;
            return this;
        }

        public ElasticsearchSearcherModelBuilder withAggregation(List<TermsAggregationBuilder> aggregationBuilders) {
            this.aggregationBuilders.addAll(aggregationBuilders);
            return this;
        }

        public ElasticsearchSearcherModelBuilder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ElasticsearchSearcherModel build() {
            return new ElasticsearchSearcherModel(queryBuilder, aggregationBuilders, batchSize, fetchSource, storedField);
        }
    }

}
