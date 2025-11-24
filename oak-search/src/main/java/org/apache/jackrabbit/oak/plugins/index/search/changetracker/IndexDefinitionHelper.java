/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.search.changetracker;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * Helper class for checking if an index definition uses change tracking.
 * 
 * <p>An index opts into change tracking by setting the property:
 * <pre>
 * useChangeTracker: true
 * </pre>
 * 
 * <p>This enables gradual rollout and per-index testing of the change
 * tracking approach.
 */
public class IndexDefinitionHelper {
    
    private static final String PROP_USE_CHANGE_TRACKER = "useChangeTracker";
    
    /**
     * Checks if an index definition has opted into change tracking.
     * 
     * @param indexDefinition the index definition node
     * @return true if the index uses change tracking
     */
    public static boolean usesChangeTracking(@NotNull NodeState indexDefinition) {
        PropertyState prop = indexDefinition.getProperty(PROP_USE_CHANGE_TRACKER);
        if (prop == null) {
            return false;
        }
        return prop.getValue(Type.BOOLEAN);
    }
    
    /**
     * Gets the index name from a path.
     * 
     * @param indexPath the full index path (e.g. "/oak:index/damAssetLucene")
     * @return the index name (e.g. "damAssetLucene")
     */
    @NotNull
    public static String getIndexName(@NotNull String indexPath) {
        if (indexPath.startsWith("/")) {
            int lastSlash = indexPath.lastIndexOf('/');
            if (lastSlash >= 0) {
                return indexPath.substring(lastSlash + 1);
            }
        }
        return indexPath;
    }
}

