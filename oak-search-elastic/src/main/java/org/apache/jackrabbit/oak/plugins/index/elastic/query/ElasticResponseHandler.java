package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.elasticsearch.search.SearchHit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiPredicate;

public class ElasticResponseHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResponseHandler.class);

    private final IndexPlan indexPlan;
    private final PlanResult planResult;
    private final BiPredicate<String, IndexPlan> rowInclusionPredicate;

    ElasticResponseHandler(@NotNull IndexPlan indexPlan, @NotNull PlanResult planResult,
                           BiPredicate<String, IndexPlan> rowInclusionPredicate) {
        this.indexPlan = indexPlan;
        this.planResult = planResult;
        this.rowInclusionPredicate = rowInclusionPredicate;
    }

    public FulltextResultRow toRow(SearchHit hit, ElasticResultRowIterator.ElasticsearchFacetProvider elasticsearchFacetProvider) {
        final Map<String, Object> sourceMap = hit.getSourceAsMap();
        String path = (String) sourceMap.get(FieldNames.PATH);

        if ("".equals(path)) {
            path = "/";
        }
        if (planResult.isPathTransformed()) {
            String originalPath = path;
            path = planResult.transformPath(path);

            if (path == null) {
                LOG.trace("Ignoring path {} : Transformation returned null", originalPath);
                return null;
            }
        }

        boolean shouldIncludeForHierarchy = rowInclusionPredicate.test(path, indexPlan);
        LOG.trace("Matched path {}; shouldIncludeForHierarchy: {}", path, shouldIncludeForHierarchy);
        return shouldIncludeForHierarchy ? new FulltextResultRow(path, hit.getScore(), null,
                elasticsearchFacetProvider, null)
                : null;
    }
}
