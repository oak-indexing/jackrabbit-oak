package org.apache.jackrabbit.oak.plugins.index;

public interface LaneSanityCheckerProvider {

    LaneSanityChecker getSanityChecker(String lane);

}
