package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.RepositoryOptionsUtil;

public class ElasticRepositoryOptionsUtil extends RepositoryOptionsUtil {

    public ElasticRepositoryOptionsUtil(Oak oak) {
        this.oak = oak;
    }
}
