/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.concurrent.TimeUnit;

public class ElasticMetricHandler {

    private static final String QUERY_RATE = "ELASTIC_QUERY_RATE";
    private static final String QUERY_INTERNAL_RATE = "ELASTIC_QUERY_INTERNAL_RATE";

    private static final String QUERY_HITS = "ELASTIC_QUERY_HITS";
    private static final String QUERY_SERVER_TIME = "ELASTIC_QUERY_SERVER_TIME";
    private static final String QUERY_TOTAL_TIME = "ELASTIC_QUERY_TOTAL_TIME";

    private static final String QUERY_TIMED_OUT_RATE = "ELASTIC_QUERY_TIMED_OUT_RATE";
    private static final String QUERY_FAILED_RATE = "ELASTIC_QUERY_FAILED_RATE";

    private final MeterStats queryRate;
    private final MeterStats queryInternalRate;

    private final HistogramStats queryHitsHistogram;
    private final TimerStats queryServerTimer;
    private final TimerStats queryTotalTimer;

    private final MeterStats queryTimedOutRate;
    private final MeterStats queryFailedRate;

    ElasticMetricHandler(StatisticsProvider sp) {
        queryRate = sp.getMeter(QUERY_RATE, StatsOptions.METRICS_ONLY);
        queryInternalRate = sp.getMeter(QUERY_INTERNAL_RATE, StatsOptions.METRICS_ONLY);

        queryHitsHistogram = sp.getHistogram(QUERY_HITS, StatsOptions.METRICS_ONLY);
        queryServerTimer = sp.getTimer(QUERY_SERVER_TIME, StatsOptions.METRICS_ONLY);
        queryTotalTimer = sp.getTimer(QUERY_TOTAL_TIME, StatsOptions.METRICS_ONLY);

        queryTimedOutRate = sp.getMeter(QUERY_TIMED_OUT_RATE, StatsOptions.METRICS_ONLY);
        queryFailedRate = sp.getMeter(QUERY_FAILED_RATE, StatsOptions.METRICS_ONLY);
    }

    public void markQuery(boolean isRootQuery) {
        if (isRootQuery) {
            queryRate.mark();
        }
        queryInternalRate.mark();
    }

    public void measureQuery(int hits, long serverTimeMs, long totalTimeMs, boolean timedOut) {
        queryHitsHistogram.update(hits);
        queryServerTimer.update(serverTimeMs, TimeUnit.MILLISECONDS);
        queryTotalTimer.update(totalTimeMs, TimeUnit.MILLISECONDS);
        if (timedOut) {
            queryTimedOutRate.mark();
        }
    }

    public void measureFailedQuery(long totalTimeMs) {
        queryFailedRate.mark();
        queryTotalTimer.update(totalTimeMs, TimeUnit.MILLISECONDS);
    }
}
