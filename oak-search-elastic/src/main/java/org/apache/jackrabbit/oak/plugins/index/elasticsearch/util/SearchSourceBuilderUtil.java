package org.apache.jackrabbit.oak.plugins.index.elasticsearch.util;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcherModel;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchSourceBuilderUtil {

    public static SearchSourceBuilder createSearchSourceBuilder(ElasticsearchSearcherModel elasticsearchSearcherModel) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(elasticsearchSearcherModel.getQueryBuilder())
                .fetchSource(elasticsearchSearcherModel.fetchSource())
                .storedField(elasticsearchSearcherModel.getStoredField())
                .size(elasticsearchSearcherModel.getBatchSize())
                .from(elasticsearchSearcherModel.getFrom());

        for (AggregationBuilder aggregationBuilder : elasticsearchSearcherModel.getAggregationBuilders()) {
            searchSourceBuilder.aggregation(aggregationBuilder);
        }
        return searchSourceBuilder;
    }
}
