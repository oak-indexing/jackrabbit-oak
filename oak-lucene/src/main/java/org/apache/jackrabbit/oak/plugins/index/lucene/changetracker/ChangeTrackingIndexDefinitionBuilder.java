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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Helper class to create the change tracking index definition in the repository.
 * 
 * <p>The change tracking index is a special Lucene index that stores lightweight
 * change entries (path, timestamp, serial number) to track node modifications.
 * 
 * <p><strong>Usage:</strong>
 * <pre>
 * NodeBuilder oakIndex = root.builder().child("oak:index");
 * ChangeTrackingIndexDefinitionBuilder.createChangeTrackingIndex(oakIndex);
 * </pre>
 */
public class ChangeTrackingIndexDefinitionBuilder {
    
    /**
     * Default name for the change tracking index.
     */
    public static final String INDEX_NAME = "changeTrackingIndex";
    
    /**
     * Async lane name for the change tracking index.
     */
    public static final String ASYNC_LANE = "change-tracker-async";
    
    /**
     * Field names in the change tracking index.
     */
    public static final String FIELD_PATH = "ct:path";
    public static final String FIELD_DIFF_TIME = "ct:diffProcessingTime";
    public static final String FIELD_SERIAL = "ct:serialNumber";
    
    /**
     * Creates the change tracking index definition under the given parent node.
     * 
     * @param oakIndexNode the oak:index node builder
     * @return the created index definition node builder
     */
    public static NodeBuilder createChangeTrackingIndex(NodeBuilder oakIndexNode) {
        return createChangeTrackingIndex(oakIndexNode, INDEX_NAME);
    }
    
    /**
     * Creates the change tracking index definition with a custom name.
     * 
     * @param oakIndexNode the oak:index node builder
     * @param indexName the name for the index
     * @return the created index definition node builder
     */
    public static NodeBuilder createChangeTrackingIndex(NodeBuilder oakIndexNode, String indexName) {
        NodeBuilder index = oakIndexNode.child(indexName);
        
        // Basic index properties
        index.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        index.setProperty(LuceneIndexConstants.COMPAT_MODE, 2);
        index.setProperty("type", "lucene", Type.STRING);
        index.setProperty("async", ASYNC_LANE, Type.STRING);
        // index.setProperty(LuceneIndexConstants.CODEC_NAME, "Lucene46");
        index.setProperty("evaluatePathRestrictions", true);
        
        // Include all paths
        index.setProperty("includedPaths", java.util.Collections.singletonList("/"), Type.STRINGS);
        
        // Create index rules
        NodeBuilder indexRules = index.child("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Index all node types (nt:base)
        NodeBuilder ntBase = indexRules.child("nt:base");
        ntBase.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Properties to index
        NodeBuilder properties = ntBase.child("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // ct:path - stored, not analyzed, for exact lookup
        NodeBuilder pathProp = properties.child("ctPath");
        pathProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        pathProp.setProperty("name", FIELD_PATH, Type.STRING);
        pathProp.setProperty("propertyIndex", true);
        pathProp.setProperty("analyzed", false);
        pathProp.setProperty("ordered", false);
        pathProp.setProperty("stored", true);
        
        // ct:diffProcessingTime - ordered, for range queries and sorting
        NodeBuilder timeProp = properties.child("ctDiffProcessingTime");
        timeProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        timeProp.setProperty("name", FIELD_DIFF_TIME, Type.STRING);
        timeProp.setProperty("propertyIndex", true);
        timeProp.setProperty("type", "Long", Type.STRING);
        timeProp.setProperty("analyzed", false);
        timeProp.setProperty("ordered", true);
        timeProp.setProperty("stored", true);
        
        // ct:serialNumber - ordered, for pagination
        NodeBuilder serialProp = properties.child("ctSerialNumber");
        serialProp.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        serialProp.setProperty("name", FIELD_SERIAL, Type.STRING);
        serialProp.setProperty("propertyIndex", true);
        serialProp.setProperty("type", "Long", Type.STRING);
        serialProp.setProperty("analyzed", false);
        serialProp.setProperty("ordered", true);
        serialProp.setProperty("stored", true);
        
        return index;
    }
    
    /**
     * Checks if the change tracking index exists.
     * 
     * @param oakIndexNode the oak:index node builder
     * @return true if the index exists
     */
    public static boolean hasChangeTrackingIndex(NodeBuilder oakIndexNode) {
        return oakIndexNode.hasChildNode(INDEX_NAME);
    }
    
    /**
     * Removes the change tracking index.
     * 
     * @param oakIndexNode the oak:index node builder
     * @return true if the index was removed
     */
    public static boolean removeChangeTrackingIndex(NodeBuilder oakIndexNode) {
        if (hasChangeTrackingIndex(oakIndexNode)) {
            oakIndexNode.getChildNode(INDEX_NAME).remove();
            return true;
        }
        return false;
    }
}

