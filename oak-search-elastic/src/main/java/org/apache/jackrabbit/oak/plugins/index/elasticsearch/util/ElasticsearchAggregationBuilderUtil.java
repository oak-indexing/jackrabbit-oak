package org.apache.jackrabbit.oak.plugins.index.elasticsearch.util;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ElasticsearchAggregationBuilderUtil {

    private ElasticsearchAggregationBuilderUtil() {
    }

    public static List<TermsAggregationBuilder> getAggregators(QueryIndex.IndexPlan plan, int numberOfFacets) {
        List<TermsAggregationBuilder> termsAggregationBuilders = new LinkedList<>();
        Iterator<Filter.PropertyRestriction> it = plan.getFilter().getPropertyRestrictions().iterator();
        while (it.hasNext()) {
            Filter.PropertyRestriction propertyRestriction = it.next();
            String name = propertyRestriction.propertyName;
            if (QueryConstants.REP_FACET.equals(name)) {
                String value = propertyRestriction.first.getValue(Type.STRING);
                String facetProp = FulltextIndex.parseFacetField(value);
                termsAggregationBuilders.add(AggregationBuilders.terms(facetProp).field(keywordFieldName(facetProp)).size(numberOfFacets));
            }
        }
        return termsAggregationBuilders;
    }

    private static String keywordFieldName(String propName) {
        return propName + "." + "keyword";
    }
}
