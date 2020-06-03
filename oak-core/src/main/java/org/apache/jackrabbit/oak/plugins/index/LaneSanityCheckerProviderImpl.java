package org.apache.jackrabbit.oak.plugins.index;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;

import static com.google.common.base.Preconditions.checkNotNull;

@Component(service = LaneSanityCheckerProvider.class)
public class LaneSanityCheckerProviderImpl implements LaneSanityCheckerProvider {

    private final Map<String, LaneSanityChecker> sanityCheckers = new ConcurrentHashMap<>();

    @Reference(name = "sanityCheckers",
            policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY,
            service = LaneSanityChecker.class
    )
    public void bindSanityCheckers(LaneSanityChecker sanityChecker){
        sanityCheckers.put(checkNotNull(sanityChecker.getType()), sanityChecker);
    }

    public void unbindSanityCheckers(LaneSanityChecker sanityChecker){
        sanityCheckers.remove(sanityChecker.getType());
    }



    @Override
    public LaneSanityChecker getSanityChecker(String lane) {
        return sanityCheckers.get(lane);
    }
}
