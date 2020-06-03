package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.LaneSanityChecker;

@Component
@Service(LaneSanityChecker.class)
public class ElasticLaneSanityChecker implements LaneSanityChecker {

    @Reference
    ElasticIndexProviderService elasticIndexProviderService;

    @Override
    public boolean isIndexable() {
        return elasticIndexProviderService.elasticConnection.isConnected();
    }

    @Override
    public String getType() {
        return "elastic-async";
    }
}
