package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;

interface ElasticResponseListener {

    void endData();

    interface RowListener extends ElasticResponseListener {
        void on(FulltextIndex.FulltextResultRow row);
    }

    interface AggregationListener extends ElasticResponseListener {

    }
}
