package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcher;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcherModel;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchAggregationBuilderUtil;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsecureElasticSearchFacets implements ElasticsearchFacets {
    private ElasticsearchSearcher searcher;
    private QueryBuilder query;
    private QueryIndex.IndexPlan plan;

    public InsecureElasticSearchFacets(ElasticsearchSearcher searcher, QueryBuilder query,
                                       QueryIndex.IndexPlan plan) {
        this.searcher = searcher;
        this.query = query;
        this.plan = plan;
    }

    @Override
    public Map<String, List<FulltextIndex.Facet>> getElasticSearchFacets(int numberOfFacets) throws IOException {
        List<TermsAggregationBuilder> aggregationBuilders = ElasticsearchAggregationBuilderUtil.getAggregators(plan, numberOfFacets);
        ElasticsearchSearcherModel elasticsearchSearcherModel = new ElasticsearchSearcherModel.ElasticsearchSearcherModelBuilder()
                .withQuery(query)
                .withAggregation(aggregationBuilders)
                .build();
        Map<String, Aggregation> facetResult = searcher.search(elasticsearchSearcherModel).getAggregations().getAsMap();
        return changeToFacetList(facetResult);
    }

    private Map<String, List<FulltextIndex.Facet>> changeToFacetList(Map<String, Aggregation> docs) {
        Map<String, List<FulltextIndex.Facet>> facetMap = new HashMap<>();
        for (String facet : docs.keySet()) {
            Terms terms = (Terms) docs.get(facet);
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            final List<FulltextIndex.Facet> facetList = new ArrayList<>();
            for (Terms.Bucket bucket : buckets) {
                String facetKey = bucket.getKeyAsString();
                long facetCount = bucket.getDocCount();
                facetList.add(new FulltextIndex.Facet(facetKey, (int) facetCount));
            }
            facetMap.put(facet, facetList);
        }
        return facetMap;
    }

    @Override
    public ElasticsearchSearcher getSearcher() {
        return searcher;
    }

    @Override
    public QueryBuilder getQuery() {
        return query;
    }

    @Override
    public QueryIndex.IndexPlan getPlan() {
        return plan;
    }
}
