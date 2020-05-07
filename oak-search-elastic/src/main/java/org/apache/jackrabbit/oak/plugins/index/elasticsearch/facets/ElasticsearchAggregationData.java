package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import org.elasticsearch.search.aggregations.Aggregations;

public class ElasticsearchAggregationData {
    private int numberOfFacets; // topFacet count from indexDefinition
    private long totalDocuments; // total documents in query result.
    private Aggregations aggregations; // Aggregated data for query from ES

    public ElasticsearchAggregationData(int numberOfFacets, long totalDocuments, Aggregations aggregations) {
        this.numberOfFacets = numberOfFacets;
        this.totalDocuments = totalDocuments;
        this.aggregations = aggregations;
    }

    public int getNumberOfFacets() {
        return numberOfFacets;
    }

    public long getTotalDocuments() {
        return totalDocuments;
    }

    public Aggregations getAggregations() {
        return aggregations;
    }
}
