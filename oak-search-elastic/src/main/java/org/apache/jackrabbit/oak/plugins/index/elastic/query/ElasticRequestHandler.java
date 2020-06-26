/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner.PlanResult;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.PropertyType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newAncestorQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newDepthQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newMixinTypeQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newNodeTypeQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newNotNullPropQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newNullPropQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPrefixQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPropertyRestrictionQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardPathQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newWildcardQuery;
import static org.apache.jackrabbit.oak.spi.query.QueryConstants.JCR_PATH;
import static org.apache.jackrabbit.util.ISO8601.parse;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Class to map query plans into Elastic request objects.
 */
public class ElasticRequestHandler {

    private final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    private static final String ES_TRIGRAM_SUFFIX = ".trigram";

    private final IndexPlan indexPlan;
    private final Filter filter;
    private final PlanResult planResult;
    private final ElasticIndexDefinition elasticIndexDefinition;
    private final String propertyRestrictionQuery;

    ElasticRequestHandler(@NotNull IndexPlan indexPlan, @NotNull FulltextIndexPlanner.PlanResult planResult) {
        this.indexPlan = indexPlan;
        this.filter = indexPlan.getFilter();
        this.planResult = planResult;
        this.elasticIndexDefinition = (ElasticIndexDefinition) planResult.indexDefinition;

        //Check if native function is supported
        Filter.PropertyRestriction pr = null;
        if (elasticIndexDefinition.hasFunctionDefined()) {
            pr = filter.getPropertyRestriction(elasticIndexDefinition.getFunctionName());
        }

        this.propertyRestrictionQuery = pr != null ? String.valueOf(pr.first.getValue(pr.first.getType())) : null;
    }

    public BoolQueryBuilder baseQuery() {
        final BoolQueryBuilder boolQuery = boolQuery();

        FullTextExpression ft = filter.getFullTextConstraint();

        if (ft != null) {
            boolQuery.must(fullTextQuery(ft, planResult));
        }

        if (propertyRestrictionQuery != null) {
            boolQuery.must(queryStringQuery(propertyRestrictionQuery));
        } else if (planResult.evaluateNonFullTextConstraints()) {
            for (QueryBuilder constraint: nonFullTextConstraints(indexPlan, planResult)) {
                boolQuery.filter(constraint);
            }
        }

        // TODO: sort with no other restriction

        if (!boolQuery.hasClauses()) {
            // TODO: what happens here in planning mode (specially, apparently for things like rep:similar)
            //For purely nodeType based queries all the documents would have to
            //be returned (if the index definition has a single rule)
            if (planResult.evaluateNodeTypeRestriction()) {
                boolQuery.must(matchAllQuery());
            }
        }

        return boolQuery;
    }

    public String getPropertyRestrictionQuery() {
        return propertyRestrictionQuery;
    }

    public boolean requiresSpellCheck() {
        return propertyRestrictionQuery != null && propertyRestrictionQuery.startsWith(SPELLCHECK_PREFIX);
    }

    public ElasticFacetProvider getAsyncFacetProvider(ElasticResponseHandler responseHandler) {
        return requiresFacets() ?
                ElasticFacetProvider.getProvider(
                        planResult.indexDefinition.getSecureFacetConfiguration(),
                        this, responseHandler,
                        filter::isAccessible
                ) : null;
    }

    private boolean requiresFacets() {
        return filter.getPropertyRestrictions()
                .stream()
                .anyMatch(pr -> QueryConstants.REP_FACET.equals(pr.propertyName));
    }

    public Stream<TermsAggregationBuilder> aggregations() {
        return facetFields()
                .map(facetProp ->
                        AggregationBuilders.terms(facetProp)
                                .field(elasticIndexDefinition.getElasticKeyword(facetProp))
                                .size(elasticIndexDefinition.getNumberOfTopFacets())
                );
    }

    public Stream<String> facetFields() {
        return filter.getPropertyRestrictions()
                .stream()
                .filter(pr -> QueryConstants.REP_FACET.equals(pr.propertyName))
                .map(pr -> FulltextIndex.parseFacetField(pr.first.getValue(Type.STRING)));
    }

    public Stream<String> spellCheckFields() {
        return StreamSupport
                .stream(planResult.indexingRule.getProperties().spliterator(), false)
                .filter(pd -> pd.useInSpellcheck)
                .map(pd -> pd.name);
    }

    public PhraseSuggestionBuilder suggestQuery(String field, String spellCheckQuery) {
        BoolQueryBuilder query = boolQuery()
                .must(new MatchPhraseQueryBuilder(field, "{{suggestion}}"));

        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);

        PhraseSuggestionBuilder.CandidateGenerator candidateGeneratorBuilder =
                new DirectCandidateGeneratorBuilder(getTrigramField(field)).suggestMode("missing");
        return SuggestBuilders
                .phraseSuggestion(getTrigramField(field))
                .size(10)
                .addCandidateGenerator(candidateGeneratorBuilder)
                .text(spellCheckQuery)
                .collateQuery(query.toString());
    }

    public BoolQueryBuilder suggestMatchQuery(String suggestion, String[] fields) {
        BoolQueryBuilder query = boolQuery()
                .must(new MultiMatchQueryBuilder(suggestion, fields)
                        .operator(Operator.AND).fuzzyTranspositions(false)
                        .autoGenerateSynonymsPhraseQuery(false)
                        .type(MatchQuery.Type.PHRASE));

        nonFullTextConstraints(indexPlan, planResult).forEach(query::must);

        return query;
    }

    private String getTrigramField(String field) {
        return field + ES_TRIGRAM_SUFFIX;
    }

    private QueryBuilder fullTextQuery(FullTextExpression ft, final PlanResult pr) {
        // a reference to the query, so it can be set in the visitor
        // (a "non-local return")
        final AtomicReference<QueryBuilder> result = new AtomicReference<>();
        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextContains contains) {
                visitTerm(contains.getPropertyName(), contains.getRawText(), null, contains.isNot());
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                BoolQueryBuilder q = boolQuery();
                for (FullTextExpression e : or.list) {
                    q.should(fullTextQuery(e, pr));
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                BoolQueryBuilder q = boolQuery();
                for (FullTextExpression e : and.list) {
                    QueryBuilder x = fullTextQuery(e, pr);
                    // TODO: see OAK-2434 and see if ES also can't work without unwrapping
                    /* Only unwrap the clause if MUST_NOT(x) */
                    boolean hasMustNot = false;
                    if (x instanceof BoolQueryBuilder) {
                        BoolQueryBuilder bq = (BoolQueryBuilder) x;
                        if (bq.mustNot().size() == 1
                                // no other clauses
                                && bq.should().isEmpty() && bq.must().isEmpty() && bq.filter().isEmpty()) {
                            hasMustNot = true;
                            q.mustNot(bq.mustNot().get(0));
                        }
                    }

                    if (!hasMustNot) {
                        q.must(x);
                    }
                }
                result.set(q);
                return true;
            }

            @Override
            public boolean visit(FullTextTerm term) {
                return visitTerm(term.getPropertyName(), term.getText(), term.getBoost(), term.isNot());
            }

            private boolean visitTerm(String propertyName, String text, String boost, boolean not) {
                String p = getElasticFieldName(propertyName, pr);
                QueryBuilder q = tokenToQuery(text, p, pr);
                if (boost != null) {
                    q.boost(Float.parseFloat(boost));
                }
                if (not) {
                    BoolQueryBuilder bq = boolQuery().mustNot(q);
                    result.set(bq);
                } else {
                    result.set(q);
                }
                return true;
            }
        });
        return result.get();
    }

    private List<QueryBuilder> nonFullTextConstraints(IndexPlan plan, PlanResult planResult) {
        final BiPredicate<Iterable<String>, String> any = (iterable, value) ->
                StreamSupport.stream(iterable.spliterator(), false).anyMatch(value::equals);

        final List<QueryBuilder> queries = new ArrayList<>();

        Filter filter = plan.getFilter();
        if (!filter.matchesAllTypes()) {
            queries.add(nodeTypeConstraints(planResult.indexingRule, filter));
        }

        String path = FulltextIndex.getPathRestriction(plan);
        switch (filter.getPathRestriction()) {
            case ALL_CHILDREN:
                if (!"/".equals(path)) {
                    queries.add(newAncestorQuery(path));
                }
                break;
            case DIRECT_CHILDREN:
                BoolQueryBuilder bq = boolQuery()
                    .must(newAncestorQuery(path))
                    .must(newDepthQuery(path, planResult));
                queries.add(bq);
                break;
            case EXACT:
                // For transformed paths, we can only add path restriction if absolute path to property can be
                // deduced
                if (planResult.isPathTransformed()) {
                    String parentPathSegment = planResult.getParentPathSegment();
                    if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                        queries.add(newPathQuery(path + parentPathSegment));
                    }
                } else {
                    queries.add(newPathQuery(path));
                }
                break;
            case PARENT:
                if (denotesRoot(path)) {
                    // there's no parent of the root node
                    // we add a path that can not possibly occur because there
                    // is no way to say "match no documents" in Lucene
                    queries.add(newPathQuery("///"));
                } else {
                    // For transformed paths, we can only add path restriction if absolute path to property can be
                    // deduced
                    if (planResult.isPathTransformed()) {
                        String parentPathSegment = planResult.getParentPathSegment();
                        if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                            queries.add(newPathQuery(getParentPath(path) + parentPathSegment));
                        }
                    } else {
                        queries.add(newPathQuery(getParentPath(path)));
                    }
                }
                break;
            case NO_RESTRICTION:
                break;
        }

        for (Filter.PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String name = pr.propertyName;

            if (QueryConstants.REP_EXCERPT.equals(name) || QueryConstants.OAK_SCORE_EXPLANATION.equals(name)
                    || QueryConstants.REP_FACET.equals(name)) {
                continue;
            }

            if (QueryConstants.RESTRICTION_LOCAL_NAME.equals(name)) {
                if (planResult.evaluateNodeNameRestriction()) {
                    QueryBuilder q = nodeName(pr);
                    if (q != null) {
                        queries.add(q);
                    }
                }
                continue;
            }

            if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
                String first = pr.first.getValue(Type.STRING);
                first = first.replace("\\", "");
                if (JCR_PATH.equals(name)) {
                    queries.add(newPathQuery(first));
                    continue;
                } else if ("*".equals(name)) {
                    //TODO Revisit reference constraint. For performant impl
                    //references need to be indexed in a different manner
                    queries.add(referenceConstraint(first));
                    continue;
                }
            }

            PropertyDefinition pd = planResult.getPropDefn(pr);
            if (pd == null) {
                continue;
            }

            QueryBuilder q = createQuery(planResult.getPropertyName(pr), pr, pd);
            if (q != null) {
                queries.add(q);
            }
        }
        return queries;
    }

    private static QueryBuilder nodeTypeConstraints(IndexDefinition.IndexingRule defn, Filter filter) {
        final BoolQueryBuilder bq = boolQuery();
        PropertyDefinition primaryType = defn.getConfig(JCR_PRIMARYTYPE);
        //TODO OAK-2198 Add proper nodeType query support

        if (primaryType != null && primaryType.propertyIndex) {
            for (String type : filter.getPrimaryTypes()) {
                bq.should(newNodeTypeQuery(type));
            }
        }

        PropertyDefinition mixinType = defn.getConfig(JCR_MIXINTYPES);
        if (mixinType != null && mixinType.propertyIndex) {
            for (String type : filter.getMixinTypes()) {
                bq.should(newMixinTypeQuery(type));
            }
        }

        return bq;
    }

    private static QueryBuilder nodeName(Filter.PropertyRestriction pr) {
        String first = pr.first != null ? pr.first.getValue(Type.STRING) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                && pr.lastIncluding) {
            // [property]=[value]
            return termQuery(FieldNames.NODE_NAME, first);
        }

        if (pr.isLike) {
            return like(FieldNames.NODE_NAME, first);
        }

        throw new IllegalStateException("For nodeName queries only EQUALS and LIKE are supported " + pr);
    }

    private static QueryBuilder like(String name, String first) {
        first = first.replace('%', WildcardQuery.WILDCARD_STRING);
        first = first.replace('_', WildcardQuery.WILDCARD_CHAR);

        int indexOfWS = first.indexOf(WildcardQuery.WILDCARD_STRING);
        int indexOfWC = first.indexOf(WildcardQuery.WILDCARD_CHAR);
        int len = first.length();

        if (indexOfWS == len || indexOfWC == len) {
            // remove trailing "*" for prefix query
            first = first.substring(0, first.length() - 1);
            if (JCR_PATH.equals(name)) {
                return newPrefixPathQuery(first);
            } else {
                return newPrefixQuery(name, first);
            }
        } else {
            if (JCR_PATH.equals(name)) {
                return newWildcardPathQuery(first);
            } else {
                return newWildcardQuery(name, first);
            }
        }
    }

    private static QueryBuilder referenceConstraint(String uuid) {
        // TODO: this seems very bad as a query - do we really want to support it. In fact, is it even used?
        // reference query
        return QueryBuilders.multiMatchQuery(uuid);
    }

    private static QueryBuilder tokenToQuery(String text, String fieldName, PlanResult pr) {
        // default match query are executed in OR, we need to use AND instead to avoid that
        // every document having at least one term in the `text` will match. If there are multiple
        // contains clause they will go to different match queries and will be executed in OR
        QueryBuilder ret;
        IndexDefinition.IndexingRule indexingRule = pr.indexingRule;
        //Expand the query on fulltext field
        if (FieldNames.FULLTEXT.equals(fieldName) && !indexingRule.getNodeScopeAnalyzedProps().isEmpty()) {
            BoolQueryBuilder in = boolQuery();
            for (PropertyDefinition pd : indexingRule.getNodeScopeAnalyzedProps()) {
                QueryBuilder q = matchQuery(pd.name, text).boost(pd.boost).operator(Operator.AND);
                in.should(q);
            }

            //Add the query for actual fulltext field also. That query would not be boosted
            // TODO: do we need this if all the analyzed fields are queried?
            ret = in.should(matchQuery(fieldName, text).operator(Operator.AND));
        } else {
            ret = matchQuery(fieldName, text).operator(Operator.AND);
        }

        return ret;
    }

    private QueryBuilder createQuery(String propertyName, Filter.PropertyRestriction pr,
                                     PropertyDefinition defn) {
        int propType = FulltextIndex.determinePropertyType(defn, pr);

        if (pr.isNullRestriction()) {
            return newNullPropQuery(defn.name);
        }

        //If notNullCheckEnabled explicitly enabled use the simple TermQuery
        //otherwise later fallback to range query
        if (pr.isNotNullRestriction() && defn.notNullCheckEnabled) {
            return newNotNullPropQuery(defn.name);
        }

        final String field = elasticIndexDefinition.getElasticKeyword(propertyName);

        QueryBuilder in;
        switch (propType) {
            case PropertyType.DATE: {
                in = newPropertyRestrictionQuery(field, pr, value -> parse(value.getValue(Type.DATE)).getTime());
                break;
            }
            case PropertyType.DOUBLE: {
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.DOUBLE));
                break;
            }
            case PropertyType.LONG: {
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.LONG));
                break;
            }
            default: {
                if (pr.isLike) {
                    return like(propertyName, pr.first.getValue(Type.STRING));
                }

                //TODO Confirm that all other types can be treated as string
                in = newPropertyRestrictionQuery(field, pr, value -> value.getValue(Type.STRING));
            }
        }

        if (in != null) {
            return in;
        }

        throw new IllegalStateException("PropertyRestriction not handled " + pr + " for index " + defn);
    }

    private static String getElasticFieldName(@Nullable String p, PlanResult pr) {
        if (p == null) {
            return FieldNames.FULLTEXT;
        }

        if (pr.isPathTransformed()) {
            p = PathUtils.getName(p);
        }

        if ("*".equals(p)) {
            p = FieldNames.FULLTEXT;
        }
        return p;
    }
}
