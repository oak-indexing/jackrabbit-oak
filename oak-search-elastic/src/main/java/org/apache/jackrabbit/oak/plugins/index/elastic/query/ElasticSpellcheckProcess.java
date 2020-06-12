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

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticQueryUtil;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class ElasticSpellcheckProcess implements ElasticProcess {
    final static String SPELLCHECK_PREFIX = "spellcheck?term=";
    private final String query;
    private ElasticResultRowIterator.ElasticRowIteratorState rowIteratorState;

    public ElasticSpellcheckProcess(String query, ElasticResultRowIterator.ElasticRowIteratorState rowIteratorState) {
        this.query = query;
        this.rowIteratorState = rowIteratorState;
    }

    private List<String> getSpellCheckFields(ElasticIndexDefinition indexDefinition) {
        List<String> spellCheckFields = new LinkedList<>();
        IndexDefinition.IndexingRule indexingRule = indexDefinition.getApplicableIndexingRule("nt:base");

        for (PropertyDefinition propertyDefinition : rowIteratorState.planResult.indexingRule.getProperties()) {
            if (propertyDefinition.useInSpellcheck) {
                spellCheckFields.add(propertyDefinition.name);
            }
        }

        return spellCheckFields;
    }

    private MatchPhraseQueryBuilder getCollateQuery(String fieldName, QueryIndex.IndexPlan plan) {
        MatchPhraseQueryBuilder mb = new MatchPhraseQueryBuilder(fieldName, "{{suggestion}}");
        return mb;
    }

    private SuggestBuilder getSuggestBuilder(){
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        ElasticIndexDefinition defn = (ElasticIndexDefinition) rowIteratorState.planResult.indexDefinition;
        String spellcheckQueryString = query.replace(SPELLCHECK_PREFIX, "");
        int i = 0;
        for (String field : getSpellCheckFields(defn)) {
            PhraseSuggestionBuilder.CandidateGenerator candidateGeneratorBuilder = new DirectCandidateGeneratorBuilder(field + ".trigram")
                    .suggestMode("missing");
            SuggestionBuilder phraseSuggestionBuilder = SuggestBuilders.phraseSuggestion(field + ".trigram")
                    .size(10)
                    .addCandidateGenerator(candidateGeneratorBuilder)
                    .text(spellcheckQueryString)
                    .collateQuery(getCollateQuery(field, rowIteratorState.plan).toString()).collatePrune(true);
            suggestBuilder.addSuggestion("cqsuggestion" + i, phraseSuggestionBuilder);
        }
        return suggestBuilder;
    }

    @Override
    public SearchHit process() throws IOException {

        SearchHit lastDocToRecord = null;
        rowIteratorState.noDocs = true;
        ElasticSearcher searcher = new ElasticSearcher(rowIteratorState.indexNode);//getCurrentElasticSearcher(indexNode);
        SuggestBuilder suggestBuilder = getSuggestBuilder();
        ElasticIndexDefinition indexDefinition = (ElasticIndexDefinition) rowIteratorState.planResult.indexDefinition;

        ElasticSearcherModel elasticSearcherModel = new ElasticSearcherModel.ElasticSearcherModelBuilder()
                .withSpellCheck(suggestBuilder).build();
        SearchResponse docs = searcher.search(elasticSearcherModel);
        Suggest suggest = docs.getSuggest();
        // Priority queue to get sorted results with decreasing score
        Queue<Suggest.Suggestion.Entry.Option> pqueue = new PriorityQueue<>((o1, o2) -> Float.compare(o2.getScore(), o1.getScore()));

        Iterator<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestionIterator = suggest.iterator();
        while (suggestionIterator.hasNext()) {
            Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> spellCheckResults = suggestionIterator.next();
            for (Suggest.Suggestion.Entry spell : spellCheckResults) {
                List<Suggest.Suggestion.Entry.Option> options = spell.getOptions();
                for (Suggest.Suggestion.Entry.Option option : options) {
                    if (option.collateMatch()) {
                        pqueue.add(option);
                    }
                }
            }
        }

        List<ElasticSearcherModel> elasticSearcherModels = new LinkedList<>();
        for (Suggest.Suggestion.Entry.Option suggestionoption : pqueue) {
            String suggestion = suggestionoption.getText().string();
            List<QueryBuilder> qbList = new LinkedList<>();
            QueryBuilder queryBuilder = new MultiMatchQueryBuilder(suggestion, getSpellCheckFields(indexDefinition)
                    .toArray(new String[getSpellCheckFields(indexDefinition).size()]))
                    .operator(Operator.AND).fuzzyTranspositions(false)
                    .autoGenerateSynonymsPhraseQuery(false)
                    .type(MatchQuery.Type.PHRASE);
            qbList.add(queryBuilder);
            qbList.addAll(ElasticQueryUtil.getPathRestrictionQuery(rowIteratorState.plan, rowIteratorState.planResult,
                    rowIteratorState.filter));
            QueryBuilder finalqb = ElasticQueryUtil.performAdditionalWraps(qbList);

            elasticSearcherModels.add(new ElasticSearcherModel.ElasticSearcherModelBuilder()
                    .withQuery(finalqb)
                    .withBatchSize(100)
                    .build());
        }
        MultiSearchResponse res = searcher.search(elasticSearcherModels);
        for (MultiSearchResponse.Item response : res.getResponses()) {
            boolean isResult = false;
            for (SearchHit doc : response.getResponse().getHits()) {
                if (rowIteratorState.filter.isAccessible((String) doc.getSourceAsMap().get(":path"))) {
                    isResult = true;
                    break;
                }
            }
            if (isResult) {
                rowIteratorState.queue.add(new FulltextIndex.FulltextResultRow(pqueue.remove().getText().string()));
            } else {
                pqueue.remove();
            }
        }

        return lastDocToRecord;
    }

    @Override
    public String getQuery() {
        return null;
    }
}
