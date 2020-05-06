package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import com.google.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcher;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcherModel;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.util.TapeSampling;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class StatisticalElasticSearchFacets extends InsecureElasticSearchFacets {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticalElasticSearchFacets.class);

    IndexDefinition.SecureFacetConfiguration secureFacetConfiguration;

    public StatisticalElasticSearchFacets(ElasticsearchSearcher searcher, QueryBuilder query,
                                          QueryIndex.IndexPlan plan, IndexDefinition.SecureFacetConfiguration secureFacetConfiguration) {
        super(searcher, query, plan);
        this.secureFacetConfiguration = secureFacetConfiguration;
    }

    public Map<String, List<FulltextIndex.Facet>> getElasticSearchFacets(int numberOfFacets) throws IOException {
        Map<String, List<FulltextIndex.Facet>> result = new HashMap<>();
        Map<String, List<FulltextIndex.Facet>> topChildren = super.getElasticSearchFacets(numberOfFacets);

        Filter filter = getPlan().getFilter();
        int hitCount;
        long totalResults;
        int sampleSize = secureFacetConfiguration.getStatisticalFacetSampleSize();
        ElasticsearchSearcherModel elasticsearchSearcherModel = new ElasticsearchSearcherModel.ElasticsearchSearcherModelBuilder()
                .withQuery(getQuery())
                .withBatchSize(ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE)
                .build();
        SearchResponse docs = getSearcher().search(elasticsearchSearcherModel);
        totalResults = docs.getHits().getTotalHits().value;
        hitCount = Math.toIntExact(totalResults);

        // In case the hit count is less than sample size(A very small reposiotry perhaps)
        // Delegate getting FacetResults to SecureSortedSetDocValuesFacetCounts to get the exact count
        // instead of statistical count. <OAK-8138>
        if (hitCount < sampleSize) {
            LOG.debug("SampleSize: {} is greater than hitcount: {}, Getting secure facet count", sampleSize, hitCount);
            return new SecureElasticSearchFacets(getSearcher(), getQuery(), getPlan()).getElasticSearchFacets(numberOfFacets);
        }
        long randomSeed = secureFacetConfiguration.getRandomSeed();
        Iterator<SearchHit> docIterator = getMatchingDocIterator(getSearcher(), getQuery());
        Iterator<SearchHit> sampleIterator = docIterator;
        if (sampleSize < hitCount) {
            LOG.debug("SampleSize: {} is less than hitcount: {}, sampling data", sampleSize, hitCount);
            sampleIterator = getSampledMatchingDocIterator(docIterator, randomSeed, hitCount, sampleSize);
        }
        int accessibleSampleCount = ElasticFacetHelper.getAccessibleDocCount(sampleIterator, filter);
        for (String facet : topChildren.keySet()) {
            List<FulltextIndex.Facet> labelAndValues = topChildren.get(facet);
            labelAndValues = updateLabelAndValueIfRequired(labelAndValues, sampleSize, accessibleSampleCount);
            result.put(facet, labelAndValues);
        }
        return result;
    }

    private Iterator<SearchHit> getMatchingDocIterator(ElasticsearchSearcher searcher, QueryBuilder query) {
        return new AbstractIterator<SearchHit>() {
            List<SearchHit> matchigDocuments = new LinkedList<>();
            Iterator<SearchHit> matchingDocsListIterator = matchigDocuments.iterator();
            int from;

            @Override
            protected SearchHit computeNext() {
                try {
                    if (matchingDocsListIterator.hasNext()) {
                        return matchingDocsListIterator.next();
                    } else {
                        ElasticsearchSearcherModel elasticsearchSearcherModel = new ElasticsearchSearcherModel.ElasticsearchSearcherModelBuilder()
                                .withQuery(query)
                                .withBatchSize(ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE)
                                .withFrom(from)
                                .build();
                        SearchResponse searchResponse = searcher.search(elasticsearchSearcherModel);
                        SearchHit[] searchHits = searchResponse.getHits().getHits();
                        if (searchHits.length == 0 || searchHits.length < ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE) {
                            return endOfData();
                        } else {
                            matchigDocuments = Arrays.asList(searchHits);
                            matchingDocsListIterator = matchigDocuments.iterator();
                            from += ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE;
                            return matchingDocsListIterator.next();
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Iterator<SearchHit> getSampledMatchingDocIterator(Iterator<SearchHit> matchingDocs,
                                                              long randomdSeed, int hitCount, int sampleSize) {
        TapeSampling<SearchHit> tapeSampling = new TapeSampling<>(new Random(randomdSeed), matchingDocs, hitCount, sampleSize);

        return tapeSampling.getSamples();
    }

    private List<FulltextIndex.Facet> updateLabelAndValueIfRequired(List<FulltextIndex.Facet> labelAndValues,
                                                                    int sampleSize, int accessibleCount) {
        if (accessibleCount < sampleSize) {
            int numZeros = 0;
            List<FulltextIndex.Facet> newValues;
            {
                List<FulltextIndex.Facet> proportionedLVs = new LinkedList<>();
                for (FulltextIndex.Facet labelAndValue : labelAndValues) {
                    FulltextIndex.Facet lv = labelAndValue;
                    long count = lv.getCount() * accessibleCount / sampleSize;
                    if (count == 0) {
                        numZeros++;
                    }
                    proportionedLVs.add(new FulltextIndex.Facet(lv.getLabel(), Math.toIntExact(count)));
                }
                labelAndValues = proportionedLVs;
            }
            if (numZeros > 0) {
                newValues = new LinkedList<>();
                for (FulltextIndex.Facet lv : labelAndValues) {
                    if (lv.getCount() > 0) {
                        newValues.add(lv);
                    }
                }
            } else {
                newValues = labelAndValues;
            }
            return newValues;
        } else {
            return labelAndValues;
        }
    }
}
