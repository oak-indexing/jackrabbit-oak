package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcher;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ElasticsearchFacets {

    ElasticsearchSearcher getSearcher();

    QueryBuilder getQuery();

    QueryIndex.IndexPlan getPlan();

    Map<String, List<FulltextIndex.Facet>> getElasticSearchFacets(int numberOfFacets) throws IOException;

    ElasticsearchAggregationData getElasticsearchAggregationData();

    class ElasticSearchFacet {

        private final String label;
        private final Long count;

        public ElasticSearchFacet(String label, Long count) {
            this.label = label;
            this.count = count;
        }

        @NotNull
        public String getLabel() {
            return label;
        }

        public Long getCount() {
            return count;
        }

        public FulltextIndex.Facet convertToFacet() {
            return new FulltextIndex.Facet(this.getLabel(), Math.toIntExact(this.getCount()));
        }
    }
}
