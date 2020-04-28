package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ElasticsearchAggregation {

    private ElasticsearchAggregation() {
    }

    public static List<TermsAggregationBuilder> getAggregators(ElasticsearchIndexNode indexNode, QueryIndex.IndexPlan plan, FulltextIndexPlanner.PlanResult pr) {
        List<TermsAggregationBuilder> termsAggregationBuilders = new LinkedList<>();
        int topFacetsSize = getNumberOfTopFacets(indexNode);
        Iterator<Filter.PropertyRestriction> it = plan.getFilter().getPropertyRestrictions().iterator();
        while (it.hasNext()) {
            Filter.PropertyRestriction propertyRestriction = it.next();
            String name = propertyRestriction.propertyName;

            if (QueryConstants.REP_FACET.equals(name)) {
                String value = propertyRestriction.first.getValue(Type.STRING);
                String facetProp = FulltextIndex.parseFacetField(value);
                termsAggregationBuilders.add(AggregationBuilders.terms(facetProp).field(keywordFieldName(facetProp)).size(topFacetsSize));
            }
        }
        return termsAggregationBuilders;
    }

    private static int getNumberOfTopFacets(ElasticsearchIndexNode indexNode) {
        return indexNode.getDefinition().getNumberOfTopFacets();
    }

    private static String keywordFieldName(String propName) {
        return propName + "." + "keyword";
    }
}
