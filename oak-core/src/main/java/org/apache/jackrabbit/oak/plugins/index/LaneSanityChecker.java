package org.apache.jackrabbit.oak.plugins.index;

public interface LaneSanityChecker {

    boolean isIndexable();

    String getType();

}
