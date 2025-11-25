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
package org.apache.jackrabbit.oak.plugins.index.lucene.changetracker.perf;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;

import java.util.Arrays;

/**
 * Builder for creating AEM's damAssetLucene-13 index definition.
 * 
 * <p>This index definition mirrors AEM's production damAssetLucene-13 index with:
 * <ul>
 *   <li>Node type: dam:Asset</li>
 *   <li>12 aggregates: jcr:content, metadata, renditions, comments, usages, subassets, etc.</li>
 *   <li>Properties: dc:title, dc:format, dam:status, cq:tags, dam:size, jcr:lastModified, dam:sha1</li>
 *   <li>Facets: Enabled for category queries</li>
 *   <li>Path restriction: /content/dam</li>
 * </ul>
 * 
 * <p><strong>Aggregates (12 total):</strong>
 * <ol>
 *   <li>jcr:content - Main content node</li>
 *   <li>jcr:content/metadata - Metadata node</li>
 *   <li>jcr:content/metadata/&#42; - All metadata children</li>
 *   <li>jcr:content/renditions - Renditions folder</li>
 *   <li>jcr:content/renditions/original - Original rendition</li>
 *   <li>jcr:content/renditions/original/jcr:content - Original rendition content</li>
 *   <li>jcr:content/comments - Comments node</li>
 *   <li>jcr:content/comments/&#42; - All comment children</li>
 *   <li>jcr:content/data/master - Content fragment master</li>
 *   <li>jcr:content/usages - Asset usage tracking</li>
 *   <li>jcr:content/renditions/cqdam.text.txt/jcr:content - Extracted text</li>
 *   <li>subassets/&#42;/jcr:content/renditions/original/jcr:content - Subasset renditions (InDesign, etc.)</li>
 * </ol>
 */
public class DamAssetIndexDefinitionBuilder {
    
    private static final String INDEX_NAME = "damAssetLucene-13";
    private static final String NODE_TYPE = "dam:Asset";
    
    public Tree build(Root root) {
        Tree oakIndex = root.getTree("/oak:index");
        Tree index = oakIndex.addChild(INDEX_NAME);
        
        // Basic index properties
        index.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        index.setProperty("type", "lucene");
        index.setProperty("async", "async");
        index.setProperty("compatVersion", 2);
        index.setProperty("evaluatePathRestrictions", true);
        index.setProperty("includedPaths", Arrays.asList("/content/dam"), Type.STRINGS);
        
        // Aggregation rules (damAssetLucene-13 pattern - 12 includes)
        Tree aggregates = index.addChild("aggregates");
        aggregates.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damAssetAggregate = aggregates.addChild(NODE_TYPE);
        damAssetAggregate.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // include0: jcr:content
        Tree include0 = damAssetAggregate.addChild("include0");
        include0.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include0.setProperty("path", "jcr:content");
        
        // include1: jcr:content/metadata
        Tree include1 = damAssetAggregate.addChild("include1");
        include1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include1.setProperty("path", "jcr:content/metadata");
        
        // include2: jcr:content/metadata/* (all metadata children)
        Tree include2 = damAssetAggregate.addChild("include2");
        include2.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include2.setProperty("path", "jcr:content/metadata/*");
        
        // include3: jcr:content/renditions
        Tree include3 = damAssetAggregate.addChild("include3");
        include3.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include3.setProperty("path", "jcr:content/renditions");
        
        // include4: jcr:content/renditions/original
        Tree include4 = damAssetAggregate.addChild("include4");
        include4.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include4.setProperty("path", "jcr:content/renditions/original");
        
        // include5: jcr:content/renditions/original/jcr:content
        Tree include5 = damAssetAggregate.addChild("include5");
        include5.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include5.setProperty("path", "jcr:content/renditions/original/jcr:content");
        
        // include6: jcr:content/comments
        Tree include6 = damAssetAggregate.addChild("include6");
        include6.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include6.setProperty("path", "jcr:content/comments");
        
        // include7: jcr:content/comments/* (all comment children)
        Tree include7 = damAssetAggregate.addChild("include7");
        include7.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include7.setProperty("path", "jcr:content/comments/*");
        
        // include8: jcr:content/data/master (for content fragments)
        Tree include8 = damAssetAggregate.addChild("include8");
        include8.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include8.setProperty("path", "jcr:content/data/master");
        
        // include9: jcr:content/usages
        Tree include9 = damAssetAggregate.addChild("include9");
        include9.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include9.setProperty("path", "jcr:content/usages");
        
        // include10: jcr:content/renditions/cqdam.text.txt/jcr:content (extracted text)
        Tree include10 = damAssetAggregate.addChild("include10");
        include10.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include10.setProperty("path", "jcr:content/renditions/cqdam.text.txt/jcr:content");
        
        // include11: subassets/*/jcr:content/renditions/original/jcr:content (for InDesign, etc.)
        Tree include11 = damAssetAggregate.addChild("include11");
        include11.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        include11.setProperty("path", "subassets/*/jcr:content/renditions/original/jcr:content");
        
        // Facets configuration
        Tree facets = index.addChild("facets");
        facets.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        facets.setProperty("topChildren", 100);
        facets.setProperty("secure", "statistical");
        
        // Index rules for dam:Asset
        Tree indexRules = index.addChild("indexRules");
        indexRules.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree damAssetRule = indexRules.addChild(NODE_TYPE);
        damAssetRule.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        Tree properties = damAssetRule.addChild("properties");
        properties.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        
        // Property: dc:title (analyzed, fulltext)
        Tree dcTitle = properties.addChild("dcTitle");
        dcTitle.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcTitle.setProperty("name", "jcr:content/metadata/dc:title");
        dcTitle.setProperty("analyzed", true);
        dcTitle.setProperty("nodeScopeIndex", true);
        dcTitle.setProperty("propertyIndex", true);
        dcTitle.setProperty("useInSpellcheck", true);
        
        // Property: dc:format (property index)
        Tree dcFormat = properties.addChild("dcFormat");
        dcFormat.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        dcFormat.setProperty("name", "jcr:content/metadata/dc:format");
        dcFormat.setProperty("propertyIndex", true);
        
        // Property: dam:status (property index)
        Tree damStatus = properties.addChild("damStatus");
        damStatus.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        damStatus.setProperty("name", "jcr:content/metadata/dam:status");
        damStatus.setProperty("propertyIndex", true);
        
        // Property: cq:tags (multi-value, analyzed)
        Tree cqTags = properties.addChild("cqTags");
        cqTags.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        cqTags.setProperty("name", "jcr:content/metadata/cq:tags");
        cqTags.setProperty("nodeScopeIndex", true);
        cqTags.setProperty("propertyIndex", true);
        cqTags.setProperty("analyzed", true);
        cqTags.setProperty("useInSuggest", true);
        
        // Property: dam:size (long, ordered for range queries)
        Tree damSize = properties.addChild("damSize");
        damSize.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        damSize.setProperty("name", "jcr:content/metadata/dam:size");
        damSize.setProperty("propertyIndex", true);
        damSize.setProperty("type", "Long");
        damSize.setProperty("ordered", true);
        
        // Property: jcr:lastModified (date, ordered for sorting)
        Tree jcrLastModified = properties.addChild("jcrLastModified");
        jcrLastModified.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        jcrLastModified.setProperty("name", "jcr:content/metadata/jcr:lastModified");
        jcrLastModified.setProperty("propertyIndex", true);
        jcrLastModified.setProperty("type", "Date");
        jcrLastModified.setProperty("ordered", true);
        
        // Property: dam:sha1 (for deduplication)
        Tree damSha1 = properties.addChild("damSha1");
        damSha1.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        damSha1.setProperty("name", "jcr:content/metadata/dam:sha1");
        damSha1.setProperty("propertyIndex", true);
        
        return index;
    }
}

